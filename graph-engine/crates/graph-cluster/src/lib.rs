//! Cluster membership, partition placement and replication (spec §6).
//!
//! `n_partitions` is fixed forever at graph creation, deliberately
//! over-provisioned relative to the initial machine count (§6.2 decision).
//! Everything in this crate operates on *placement* — which physical
//! replica hosts which logical partition — never on `n_partitions` itself,
//! which this crate treats as immutable input.

mod discovery;
mod hash;
mod rebalance;

pub use discovery::{KubernetesDiscovery, StaticDiscovery, PARTITIONS_ANNOTATION};
pub use rebalance::RendezvousRebalancePlanner;

use async_trait::async_trait;
use graph_index::PartitionId;
use std::collections::HashMap;
use std::net::SocketAddr;

/// One physical replica hosting a given logical partition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReplicaId(pub String); // e.g. Kubernetes pod name

#[derive(Debug, Clone)]
pub struct ReplicaEndpoint {
    pub replica: ReplicaId,
    pub address: SocketAddr,
    pub healthy: bool,
}

/// `partition_id = hash(node_id) % n_partitions` (§6.2) — the formula
/// itself is canonically defined in `graph_schema::partitioning` (see
/// that module's doc comment for why it moved there); this type is the
/// coordinator/client-facing wrapper around it, keyed by
/// `graph-index::PartitionId` rather than a bare `u32`.
pub struct PartitionHasher {
    pub n_partitions: u32,
}

impl PartitionHasher {
    /// *(task CL1)* See `graph_schema::partitioning` for why this
    /// specific hash, and why it, together with `n_partitions`, must
    /// never change for a graph's lifetime once nodes exist (§6.2).
    pub fn partition_of(&self, node_id: graph_schema::NodeId) -> PartitionId {
        PartitionId(graph_schema::partitioning::partition_of(
            node_id,
            self.n_partitions,
        ))
    }
}

/// Current assignment of logical partitions to physical replicas —
/// exactly the piece of state that changes on rebalance (§6.2) or
/// failover (§6.4); `n_partitions` and the hash function never do.
#[derive(Debug, Clone, Default)]
pub struct PartitionMap {
    pub replicas_by_partition: HashMap<PartitionId, Vec<ReplicaEndpoint>>,
}

impl PartitionMap {
    /// Any healthy replica for a partition — all replicas are equivalent
    /// for reads (no leader/follower, §6.4), so the coordinator just
    /// load-balances (round-robin / least-loaded) rather than routing to
    /// a specific one.
    pub fn healthy_replicas(&self, partition: PartitionId) -> Vec<&ReplicaEndpoint> {
        self.replicas_by_partition
            .get(&partition)
            .map(|replicas| replicas.iter().filter(|r| r.healthy).collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod partition_map_tests {
    use super::*;

    fn replica(name: &str, healthy: bool) -> ReplicaEndpoint {
        ReplicaEndpoint {
            replica: ReplicaId(name.to_string()),
            address: "127.0.0.1:7001".parse().unwrap(),
            healthy,
        }
    }

    /// *(task CL4)* Unhealthy replicas are filtered out, healthy ones
    /// kept.
    #[test]
    fn healthy_replicas_filters_out_unhealthy_ones() {
        let mut map = PartitionMap::default();
        map.replicas_by_partition.insert(
            PartitionId(0),
            vec![
                replica("r0", true),
                replica("r1", false),
                replica("r2", true),
            ],
        );

        let healthy = map.healthy_replicas(PartitionId(0));
        let names: Vec<&str> = healthy.iter().map(|r| r.replica.0.as_str()).collect();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"r0"));
        assert!(names.contains(&"r2"));
        assert!(!names.contains(&"r1"));
    }

    /// *(task CL4)* A partition with no replicas at all, or where every
    /// replica is unhealthy, returns an empty list rather than panicking
    /// — the coordinator (CO5) treats this as "no route available" for
    /// that partition, not a crash.
    #[test]
    fn healthy_replicas_is_empty_when_none_are_healthy_or_partition_is_unknown() {
        let mut map = PartitionMap::default();
        map.replicas_by_partition
            .insert(PartitionId(0), vec![replica("r0", false)]);

        assert!(map.healthy_replicas(PartitionId(0)).is_empty());
        assert!(map.healthy_replicas(PartitionId(1)).is_empty());
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error("Kubernetes API error: {0}")]
    KubernetesApi(String),
}

/// Discovers partition-node replicas. v1 implementation is Kubernetes-
/// native (Pods/Endpoints API or headless Service, §6.3/§10) — no external
/// registry (etcd/Consul) needed given the Kubernetes deployment target.
///
/// v1 polls `current_map` on an interval; a push-based `watch` (streaming
/// updates as replicas join/leave/change health) is a natural extension
/// once the polling version is validated, not a v1 requirement.
#[async_trait]
pub trait Discovery {
    async fn current_map(&self) -> Result<PartitionMap, DiscoveryError>;
}

/// Computes a rebalance plan when the set of available machines changes:
/// reassign a subset of existing logical partitions to different
/// replicas — never touches `n_partitions` or the hash function (§6.2).
/// See [`RendezvousRebalancePlanner`] (task CL3) for the concrete
/// placement strategy.
pub trait RebalancePlanner {
    fn plan(&self, current: &PartitionMap, available_machines: &[String]) -> RebalancePlan;
}

#[derive(Debug, Clone, Default)]
pub struct RebalancePlan {
    pub moves: Vec<PartitionMove>,
}

#[derive(Debug, Clone)]
pub struct PartitionMove {
    pub partition: PartitionId,
    pub from_machine: Option<String>,
    pub to_machine: String,
}
