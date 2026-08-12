//! Shared observability wiring (spec §9): metric names and trace-context
//! propagation helpers used by both binaries, so instrumentation stays
//! consistent instead of drifting between coordinator and partition-node.

mod interceptor;
mod tracing_init;

pub use interceptor::{extract_and_continue, inject_trace_context};
pub use tracing_init::init_tracing;

/// Metric names as specified in §9.1 — kept as constants so dashboards and
/// alerts (built externally, out of scope here) reference stable names.
/// *(task OB4)* Each constant now backs a real, pre-registered
/// Prometheus collector below (`REGISTRY`/the `*_METRIC` statics) instead
/// of being purely documentation — see [`render`] and [`serve`] for the
/// `/metrics` endpoint that exposes them.
pub mod metrics {
    use once_cell::sync::Lazy;
    use prometheus::{
        Encoder, HistogramOpts, HistogramVec, IntGaugeVec, Opts, Registry, TextEncoder,
    };

    // Coordinator
    pub const QUERY_LATENCY_SECONDS: &str = "graph_query_latency_seconds";
    pub const QUERY_HOPS_TOTAL: &str = "graph_query_hops_total";
    pub const QUERY_ERRORS_TOTAL: &str = "graph_query_errors_total";

    // Partition node
    pub const INDEX_SIZE_NODES: &str = "graph_index_size_nodes";
    pub const INDEX_SIZE_EDGES: &str = "graph_index_size_edges";
    pub const INDEX_REBUILD_DURATION_SECONDS: &str = "graph_index_rebuild_duration_seconds";
    pub const INDEX_SNAPSHOT_AGE_SECONDS: &str = "graph_index_snapshot_age_seconds";
    pub const HOP_LATENCY_SECONDS: &str = "graph_hop_latency_seconds";
    /// Fraction of a query's hops that crossed a partition boundary — the
    /// key signal for partitioning quality called out in §9.1/§6.2.
    pub const CROSS_PARTITION_HOP_RATIO: &str = "graph_cross_partition_hop_ratio";

    /// One process-wide registry — both binaries call `render()` from
    /// their own `/metrics` handler (`serve`, below); which constants
    /// actually get non-zero samples depends on which binary is running
    /// (a coordinator never rebuilds an index, a partition-node never
    /// serves `ExecuteQuery` — see each collector's doc comment for which
    /// side is expected to touch it).
    pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

    fn register_histogram(name: &str, help: &str, labels: &[&str]) -> HistogramVec {
        let hist = HistogramVec::new(HistogramOpts::new(name, help), labels)
            .unwrap_or_else(|e| panic!("invalid metric definition for {name}: {e}"));
        REGISTRY
            .register(Box::new(hist.clone()))
            .unwrap_or_else(|e| panic!("failed to register {name}: {e}"));
        hist
    }

    fn register_gauge(name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
        let gauge = IntGaugeVec::new(Opts::new(name, help), labels)
            .unwrap_or_else(|e| panic!("invalid metric definition for {name}: {e}"));
        REGISTRY
            .register(Box::new(gauge.clone()))
            .unwrap_or_else(|e| panic!("failed to register {name}: {e}"));
        gauge
    }

    fn register_counter(name: &str, help: &str, labels: &[&str]) -> prometheus::IntCounterVec {
        let counter = prometheus::IntCounterVec::new(Opts::new(name, help), labels)
            .unwrap_or_else(|e| panic!("invalid metric definition for {name}: {e}"));
        REGISTRY
            .register(Box::new(counter.clone()))
            .unwrap_or_else(|e| panic!("failed to register {name}: {e}"));
        counter
    }

    /// Coordinator: `bin/graph-coordinator`'s `execute_query` observes
    /// this once per request, labeled by outcome (`"ok"`/`"error"`).
    pub static QUERY_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
        register_histogram(
            QUERY_LATENCY_SECONDS,
            "Coordinator end-to-end query latency",
            &["outcome"],
        )
    });

    /// Coordinator: total `ExpandHop` network rounds a query's execution
    /// issued (mono-partition CO2: exactly the plan's `ExpandHop` step
    /// count; distributed CO5: one per `scatter_gather_hop` round, so
    /// this also reflects how many hops a variable-length `*min..max`
    /// pattern actually walked).
    pub static QUERY_HOPS: Lazy<prometheus::IntCounterVec> =
        Lazy::new(|| register_counter(QUERY_HOPS_TOTAL, "Hops executed per query", &["outcome"]));

    /// Coordinator: incremented once per failed `execute_query` call,
    /// labeled by failure stage (`"parse"`, `"validate"`, `"execute"`).
    pub static QUERY_ERRORS: Lazy<prometheus::IntCounterVec> =
        Lazy::new(|| register_counter(QUERY_ERRORS_TOTAL, "Query execution errors", &["stage"]));

    /// Partition-node: set after every successful rebuild
    /// (`rebuild::bootstrap`/`periodic_rebuild_loop`) to the just-built
    /// generation's `node_count`.
    pub static INDEX_NODES: Lazy<IntGaugeVec> = Lazy::new(|| {
        register_gauge(
            INDEX_SIZE_NODES,
            "Nodes in the currently-served index generation",
            &["partition"],
        )
    });

    /// Partition-node: same as `INDEX_NODES`, for `edge_count`.
    pub static INDEX_EDGES: Lazy<IntGaugeVec> = Lazy::new(|| {
        register_gauge(
            INDEX_SIZE_EDGES,
            "Edges in the currently-served index generation",
            &["partition"],
        )
    });

    /// Partition-node: wall-clock time of the `IndexBuilder::build` call
    /// itself (not the full tick interval), observed on every rebuild
    /// attempt regardless of success — a slow-but-eventually-successful
    /// rebuild is exactly the signal an operator watching staleness (see
    /// `INDEX_SNAPSHOT_AGE`) needs to diagnose.
    pub static INDEX_REBUILD_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
        register_histogram(
            INDEX_REBUILD_DURATION_SECONDS,
            "Duration of one IndexBuilder::build call",
            &["partition", "outcome"],
        )
    });

    /// Partition-node: age (seconds) of the currently-*served* generation's
    /// pinned snapshot, sampled at the start of every rebuild tick — i.e.
    /// "how stale is what queries are seeing right now", not "how long
    /// did the last rebuild take" (that's `INDEX_REBUILD_DURATION`).
    pub static INDEX_SNAPSHOT_AGE: Lazy<IntGaugeVec> = Lazy::new(|| {
        register_gauge(
            INDEX_SNAPSHOT_AGE_SECONDS,
            "Age of the currently-served index generation",
            &["partition"],
        )
    });

    /// Partition-node: observed once per `PartitionService::expand_hop`
    /// call — the local BFS/property-index work for one hop, excluding
    /// gRPC transport time.
    pub static HOP_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
        register_histogram(
            HOP_LATENCY_SECONDS,
            "Local latency of one expand_hop call",
            &["partition"],
        )
    });

    /// Coordinator: observed once per query, in `graph-query`'s
    /// `ScatterGatherExecutor` — fraction of scatter rounds (task Q6's
    /// per-hop rounds) that touched more than one partition. Declared
    /// alongside the other collectors so the `/metrics` output always
    /// carries every name from §9.1, even before the first distributed
    /// query has run (an absent time series is harder for a dashboard to
    /// distinguish from "this deployment doesn't emit this metric" than
    /// a present-but-zero one).
    pub static CROSS_PARTITION_RATIO: Lazy<HistogramVec> = Lazy::new(|| {
        register_histogram(
            CROSS_PARTITION_HOP_RATIO,
            "Fraction of a query's scatter rounds touching more than one partition",
            &[],
        )
    });

    /// Renders every registered collector in Prometheus's text exposition
    /// format — what a `/metrics` GET handler returns verbatim.
    pub fn render() -> String {
        let families = REGISTRY.gather();
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&families, &mut buf)
            .expect("Prometheus text encoding never fails for well-formed collectors");
        String::from_utf8(buf).expect("Prometheus text encoder always emits valid UTF-8")
    }
}

/// *(task OB4)* A minimal `axum` app exposing `GET /metrics` (Prometheus
/// text format, §9.1) — shared by both binaries so the route/format
/// never drifts between them. Bound to its own listener, separate from
/// the gRPC service port, matching the usual Prometheus convention of a
/// dedicated metrics port distinct from the application's own traffic.
pub fn metrics_router() -> axum::Router {
    axum::Router::new().route(
        "/metrics",
        axum::routing::get(|| async { metrics::render() }),
    )
}

/// Binds `addr` and serves [`metrics_router`] until the process exits or
/// the bind itself fails. Intended to be `tokio::spawn`ed alongside the
/// binary's main gRPC server — a metrics-endpoint outage (e.g. the port
/// is already taken) is logged, not propagated as a fatal error, for the
/// same reason `init_tracing`'s OTLP fallback isn't fatal (see
/// `tracing_init`'s doc comment): observability infrastructure shouldn't
/// gate whether the graph engine itself serves traffic.
pub async fn serve_metrics(addr: std::net::SocketAddr) {
    match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => {
            tracing::info!(%addr, "serving /metrics");
            if let Err(err) = axum::serve(listener, metrics_router()).await {
                tracing::error!(%err, "metrics server exited");
            }
        }
        Err(err) => {
            tracing::error!(%addr, %err, "failed to bind metrics listener");
        }
    }
}
