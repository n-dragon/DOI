//! The canonical `hash(node_id) % n_partitions` formula (spec §6.2) —
//! lives here, in the workspace's dependency-free foundation crate,
//! rather than in `graph-cluster` (where task CL1 originally introduced
//! it) so that **both** call sites that need it can depend on it without
//! a cycle:
//! - `graph-cluster::PartitionHasher` (the coordinator/client-facing
//!   entry point — "which partition owns this `node_id` I'm about to
//!   query").
//! - `graph-index`'s builder (task IX3, revisited for Phase 2): "is this
//!   edge's destination node owned by the partition I'm building an
//!   index for, or does it belong elsewhere and need a [`RemoteRef`]".
//!   `graph-index` already depends on `graph-schema`; it cannot depend
//!   on `graph-cluster`, which itself depends on `graph-index`
//!   (`ReplicaEndpoint`/`PartitionMap` key on `graph_index::PartitionId`)
//!   — that would be a cycle.
//!
//! Moving the formula here keeps the spec's "the only place in the
//! workspace this formula is computed, so it can never drift between
//! coordinator and partition-node binaries" invariant true even with two
//! independent call sites, instead of letting it silently become two
//! *copies* of the same formula that could drift apart.
//!
//! [`RemoteRef`]: ../../graph_index/struct.RemoteRef.html

use crate::NodeId;

/// Stable hash of a `node_id` — xxh3 over its little-endian bytes.
/// *(task CL1's decision — see `graph-cluster`'s original `hash.rs` doc
/// comment, preserved there as a re-export, for the full "why xxh3"
/// rationale.)*
pub fn stable_hash(node_id: u64) -> u64 {
    xxhash_rust::xxh3::xxh3_64(&node_id.to_le_bytes())
}

/// `hash(node_id) % n_partitions` (spec §6.2) as a bare `u32` — callers
/// wrap it in their own partition-id newtype (`graph_index::PartitionId`
/// downstream) rather than this crate defining one, since a partition id
/// is a `graph-index`/`graph-cluster` concept, not a schema one.
pub fn partition_of(node_id: NodeId, n_partitions: u32) -> u32 {
    (stable_hash(node_id.0) % n_partitions as u64) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_of_is_deterministic_and_in_range() {
        for id in 0..1000u64 {
            let p = partition_of(NodeId(id), 16);
            assert!(p < 16);
            assert_eq!(p, partition_of(NodeId(id), 16));
        }
    }
}
