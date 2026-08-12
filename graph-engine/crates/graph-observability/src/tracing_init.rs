//! *(tasks OB1, OB2)* Process-wide `tracing` subscriber wiring: JSON
//! structured logs (§9.3) correlated by `trace_id`, plus an OpenTelemetry
//! (OTLP) exporter for distributed traces (§9.2).
//!
//! **Decision (OB2):** OTLP over HTTP (`opentelemetry-otlp`'s
//! `http-proto` transport), not gRPC. Both are valid OTLP transports —
//! HTTP was chosen specifically to avoid pinning a *second*, independent
//! `tonic` version in the workspace's dependency tree purely for the
//! exporter's own internal client, distinct from `graph-proto`'s `tonic`
//! (§8.1's transport for the actual `GraphService`/`PartitionService`
//! RPCs). The two never share types, so a version mismatch between them
//! wouldn't actually break anything — this is a compile-time/dependency-
//! graph hygiene choice, not a correctness one. Any OTLP-compatible
//! collector (the standard target for this stack, e.g. the OpenTelemetry
//! Collector, Jaeger, Tempo) accepts both transports.
//!
//! **Decision (OB1/OB2):** failure to reach the configured OTLP
//! collector never takes a binary down — `init_tracing` falls back to
//! JSON-logging-only and logs one line explaining why. A partition-node
//! or coordinator shouldn't refuse to start (or crash later) because a
//! tracing backend happens to be unreachable; that would make an
//! observability *dependency* into an availability *liability*, which
//! contradicts spec §9's own framing of tracing/metrics as operational
//! aids, not correctness requirements.

use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{trace as sdktrace, Resource};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Env var naming follows the OpenTelemetry SDK's own convention
/// (`OTEL_EXPORTER_OTLP_ENDPOINT`) rather than inventing a
/// `GRAPH_`-prefixed one, so this integrates with standard OTel tooling
/// (collector Helm charts, existing operator dashboards) without a
/// translation layer.
const OTLP_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
const DEFAULT_OTLP_ENDPOINT: &str = "http://localhost:4318/v1/traces";

/// *(tasks OB1, OB2)* Initializes the process-wide `tracing` subscriber:
/// JSON-formatted logs to stdout (§9.3, collected however the Kubernetes
/// deployment target, §10, is configured to ship container logs) plus an
/// OpenTelemetry layer exporting spans via OTLP, when a collector is
/// reachable. `RUST_LOG` (standard `tracing_subscriber::EnvFilter`
/// syntax) controls verbosity; defaults to `info` if unset.
pub fn init_tracing(service_name: &'static str) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let json_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_current_span(true)
        .with_span_list(true);
    let registry = tracing_subscriber::registry()
        .with(env_filter)
        .with(json_layer);

    match build_otlp_tracer(service_name) {
        Ok(tracer) => {
            let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
            registry.with(otel_layer).init();
        }
        Err(err) => {
            // Can't use `tracing::warn!` yet — the subscriber this would
            // go through hasn't been installed. Plain stderr is the only
            // channel guaranteed to exist at this point in startup.
            eprintln!(
                "[{service_name}] OpenTelemetry OTLP exporter unavailable ({err}) — \
                 continuing with JSON logging only, no distributed tracing"
            );
            registry.init();
        }
    }
}

fn build_otlp_tracer(
    service_name: &'static str,
) -> Result<sdktrace::Tracer, opentelemetry::trace::TraceError> {
    let endpoint =
        std::env::var(OTLP_ENDPOINT_ENV).unwrap_or_else(|_| DEFAULT_OTLP_ENDPOINT.to_string());

    // `install_batch` hands back a `TracerProvider`, not a `Tracer`
    // directly (as of opentelemetry_sdk 0.24) — installed globally so any
    // future `opentelemetry::global::tracer(...)` call in this process
    // also picks it up, then a named `Tracer` handle is pulled from it
    // for `tracing-opentelemetry`'s layer to use.
    let provider = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .http()
                .with_endpoint(endpoint),
        )
        .with_trace_config(
            sdktrace::Config::default().with_resource(Resource::new(vec![KeyValue::new(
                "service.name",
                service_name,
            )])),
        )
        .install_batch(opentelemetry_sdk::runtime::Tokio)?;

    let tracer = opentelemetry::trace::TracerProvider::tracer(&provider, service_name);
    opentelemetry::global::set_tracer_provider(provider);
    Ok(tracer)
}
