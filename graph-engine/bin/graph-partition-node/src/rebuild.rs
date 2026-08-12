//! Index (re)build lifecycle (spec §5.3).

use graph_index::{GenerationHandle, IndexBuilder, PartitionId, RebuildError};
use graph_observability::metrics;
use graph_schema::Label;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// *(task PN1)* The first build, run synchronously before the process
/// starts serving — there's no "serve with an empty index" state.
pub async fn bootstrap(
    builder: &impl IndexBuilder,
    partition: PartitionId,
    labels: &[Label],
) -> Result<graph_index::IndexGeneration, RebuildError> {
    let start = Instant::now();
    let result = builder.build(partition, labels).await;
    record_rebuild_metrics(partition, start, result.as_ref());
    result
}

/// *(task PN2)* Rebuilds on a fixed interval and swaps the result in
/// atomically on success. A failed rebuild is logged and the previous
/// (still-valid) generation keeps being served — a transient Iceberg/
/// catalog hiccup should never take a healthy partition-node offline.
pub async fn periodic_rebuild_loop(
    builder: impl IndexBuilder,
    handle: Arc<GenerationHandle>,
    partition: PartitionId,
    labels: Vec<Label>,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.tick().await; // fires immediately; bootstrap already did that build
    loop {
        ticker.tick().await;

        // *(task OB4)* Age of the generation queries are *currently*
        // seeing, sampled right before attempting to replace it — a
        // successful rebuild will reset this to ~0 on the next tick's
        // `record_rebuild_metrics` call.
        let served = handle.acquire();
        let age_secs = (now_unix_ms() - served.meta.built_at_unix_ms).max(0) as f64 / 1000.0;
        metrics::INDEX_SNAPSHOT_AGE
            .with_label_values(&[&partition.0.to_string()])
            .set(age_secs as i64);
        drop(served);

        let start = Instant::now();
        let result = builder.build(partition, &labels).await;
        record_rebuild_metrics(partition, start, result.as_ref());
        match result {
            Ok(generation) => {
                let (nodes, edges) = (generation.meta.node_count, generation.meta.edge_count);
                handle.swap(generation);
                tracing::info!(partition = partition.0, nodes, edges, "rebuild succeeded");
            }
            Err(err) => {
                tracing::error!(
                    partition = partition.0,
                    %err,
                    "rebuild failed, keeping previous generation"
                );
            }
        }
    }
}

fn record_rebuild_metrics(
    partition: PartitionId,
    start: Instant,
    result: Result<&graph_index::IndexGeneration, &RebuildError>,
) {
    let partition_label = partition.0.to_string();
    let outcome = if result.is_ok() { "ok" } else { "error" };
    metrics::INDEX_REBUILD_DURATION
        .with_label_values(&[&partition_label, outcome])
        .observe(start.elapsed().as_secs_f64());
    if let Ok(generation) = result {
        metrics::INDEX_NODES
            .with_label_values(&[&partition_label])
            .set(generation.meta.node_count as i64);
        metrics::INDEX_EDGES
            .with_label_values(&[&partition_label])
            .set(generation.meta.edge_count as i64);
        metrics::INDEX_SNAPSHOT_AGE
            .with_label_values(&[&partition_label])
            .set(0);
    }
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
