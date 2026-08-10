//! Cluster membership, partition placement and replication (spec §6).
//!
//! `n_partitions` is fixed forever at graph creation, deliberately
//! over-provisioned relative to the initial machine count (§6.2 decision).
//! Everything in this crate operates on *placement* — which physical
//! replica hosts which logical partition — never on `n_partitions` itself,
//! which this crate treats as immutable input.

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

/// `partition_id = hash(node_id) % n_partitions` (§6.2) — the only place
/// in the workspace this formula is computed, so it can never drift
/// between coordinator and partition-node binaries.
pub struct PartitionHasher {
    pub n_partitions: u32,
}

impl PartitionHasher {
    pub fn partition_of(&self, node_id: graph_schema::NodeId) -> PartitionId {
        // Placeholder hash — Phase 0 work is picking a stable, well-
        // distributed hash function (e.g. xxhash) and fixing it for the
        // lifetime of a graph, since `n_partitions` never changes (§6.2).
        PartitionId((node_id.0 % self.n_partitions as u64) as u32)
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
/// Concrete placement strategy (e.g. minimize data movement, balance load)
/// is Phase 2 implementation work.
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
