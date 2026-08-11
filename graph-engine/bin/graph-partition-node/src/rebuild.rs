//! Index (re)build lifecycle (spec §5.3).

use graph_index::{GenerationHandle, IndexBuilder, PartitionId, RebuildError};
use graph_schema::Label;
use std::sync::Arc;
use std::time::Duration;

/// *(task PN1)* The first build, run synchronously before the process
/// starts serving — there's no "serve with an empty index" state.
pub async fn bootstrap(
    builder: &impl IndexBuilder,
    partition: PartitionId,
    labels: &[Label],
) -> Result<graph_index::IndexGeneration, RebuildError> {
    builder.build(partition, labels).await
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
        match builder.build(partition, &labels).await {
            Ok(generation) => {
                let (nodes, edges) = (generation.meta.node_count, generation.meta.edge_count);
                handle.swap(generation);
                println!("[graph-partition-node] rebuild succeeded: {nodes} nodes, {edges} edges");
            }
            Err(err) => {
                eprintln!(
                    "[graph-partition-node] rebuild failed, keeping previous generation: {err}"
                );
            }
        }
    }
}
