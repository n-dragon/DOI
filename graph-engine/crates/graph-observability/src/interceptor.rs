//! *(task OB3)* Propagates `tracing`/OpenTelemetry trace context across
//! the coordinator -> partition gRPC boundary (spec §9.2): "chaque saut
//! inter-partition doit propager le contexte de trace pour permettre de
//! visualiser le fan-out réel d'une requête à travers le cluster." Without
//! this, each RPC would start a brand-new, disconnected trace instead of
//! being a child span of the query that triggered it — defeating the
//! whole point of distributed tracing for a scatter-gather query
//! (§7.4's per-hop fan-out is exactly what an operator needs to see as
//! *one* trace, not N unrelated ones).
//!
//! Both directions reuse the same `tonic::Request<()> -> Result<...>`
//! shape tonic's `Interceptor` trait wants (any `FnMut` with that
//! signature qualifies, no separate trait impl needed):
//! - [`inject_trace_context`] — client side (`bin/graph-coordinator`):
//!   attaches the *current* span's context to outgoing metadata.
//! - [`extract_and_continue`] — server side (`bin/graph-partition-node`):
//!   reads incoming metadata and sets it as the current span's parent,
//!   before the request ever reaches a handler.

// tonic::Status (176 bytes) is the framework's standard RPC error type -
// tonic's Interceptor signature requires returning it by value, so this
// lint would need boxing Status everywhere rather than flagging a real
// problem here (same rationale as the service impls' own allow).
#![allow(clippy::result_large_err)]

use opentelemetry::propagation::{Extractor, Injector};
use tonic::metadata::{KeyRef, MetadataMap};
use tracing_opentelemetry::OpenTelemetrySpanExt;

struct MetadataInjector<'a>(&'a mut MetadataMap);

impl Injector for MetadataInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let Ok(key) = tonic::metadata::MetadataKey::from_bytes(key.as_bytes()) {
            if let Ok(value) = value.parse() {
                self.0.insert(key, value);
            }
        }
        // A key/value the gRPC metadata format rejects (rare — trace
        // headers are always valid header bytes in practice) is dropped
        // rather than panicking; the worst case is a missing span link,
        // not a broken request.
    }
}

struct MetadataExtractor<'a>(&'a MetadataMap);

impl Extractor for MetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .filter_map(|k| match k {
                KeyRef::Ascii(k) => Some(k.as_str()),
                KeyRef::Binary(_) => None,
            })
            .collect()
    }
}

/// *(task OB3)* Client-side interceptor — attach with
/// `PartitionServiceClient::with_interceptor(channel, inject_trace_context)`
/// wherever `bin/graph-coordinator` constructs one (both the mono-partition
/// CO2 path and CO5's per-replica clients).
pub fn inject_trace_context(
    mut request: tonic::Request<()>,
) -> Result<tonic::Request<()>, tonic::Status> {
    let context = tracing::Span::current().context();
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut MetadataInjector(request.metadata_mut()));
    });
    Ok(request)
}

/// *(task OB3)* Server-side interceptor — attach with
/// `PartitionServiceServer::with_interceptor(service, extract_and_continue)`
/// in `bin/graph-partition-node`. Every RPC handler's span (already
/// instrumented via `#[tracing::instrument]` where present, or the
/// implicit request span tonic's own tracing integration creates) then
/// links back to whichever coordinator span issued the call.
pub fn extract_and_continue(
    request: tonic::Request<()>,
) -> Result<tonic::Request<()>, tonic::Status> {
    let parent_context = opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&MetadataExtractor(request.metadata()))
    });
    tracing::Span::current().set_parent(parent_context);
    Ok(request)
}
