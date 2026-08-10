//! Shared observability wiring (spec §9): metric names and trace-context
//! propagation helpers used by both binaries, so instrumentation stays
//! consistent instead of drifting between coordinator and partition-node.

/// Metric names as specified in §9.1 — kept as constants so dashboards and
/// alerts (built externally, out of scope here) reference stable names.
pub mod metrics {
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
}

/// Initializes the process-wide `tracing` subscriber wired to an
/// OpenTelemetry exporter (§9.2) plus structured JSON logging correlated
/// by `trace_id` (§9.3). Concrete exporter configuration (collector
/// endpoint, sampling) is deployment-time config, not fixed here.
pub fn init_tracing(_service_name: &str) {
    // Phase 0/1 implementation work: wire `tracing-opentelemetry` +
    // `tracing-subscriber` with a JSON formatter. Left as a stub so both
    // binaries can call this from day one without re-deciding the setup.
}

/// Trace context carried on every coordinator -> partition RPC (§9.2), so
/// a single query's fan-out across hops/partitions stays one connected
/// trace rather than N disconnected spans.
pub struct TraceContext {
    pub trace_id: String,
    pub span_id: String,
}
