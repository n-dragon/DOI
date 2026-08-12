//! *(task CL2)* [`Discovery`] implementations: how the coordinator learns
//! which partition-node replicas currently exist and which logical
//! partitions each one serves (spec §6.3).
//!
//! **Decision (CL2):** read Kubernetes `Pod` objects directly (via the
//! `kube` crate's typed `Api<Pod>`), not the `Endpoints`/`EndpointSlice`
//! API of a headless `Service`. Both were on the table per spec §6.3
//! ("API des Pods/Endpoints, ou headless Service"); Pods win here
//! because partition ownership (*which* logical partitions a replica
//! serves) has to be read from Pod-level metadata (an annotation, see
//! below) that `Endpoints`/`EndpointSlice` objects don't carry — they
//! only give back IP:port, not arbitrary key/value metadata about the
//! backing Pod. Reading Pods directly avoids a second API call per
//! replica to cross-reference an Endpoint back to its Pod.
//!
//! **Decision (CL2):** partition ownership is read from a Pod annotation
//! (`graph.io/partitions`, comma-separated logical partition ids) rather
//! than computed here from the Pod's `StatefulSet` ordinal. `Discovery`'s
//! job is narrowly "what does the cluster currently look like" (read);
//! deciding what the assignment *should* be is `RebalancePlanner`'s job
//! (CL3, write) — conflating the two would mean the coordinator computes
//! placement on every poll instead of reading a value some other
//! component (a small controller applying `RebalancePlanner`'s output,
//! not built as part of this v1 scope) already committed. This keeps
//! `Discovery` a pure read path with no coordination logic of its own,
//! consistent with the "no external registry, no consensus" posture the
//! rest of §6 takes (§6.3, §6.4).

use crate::{Discovery, DiscoveryError, PartitionMap, ReplicaEndpoint, ReplicaId};
use async_trait::async_trait;
use graph_index::PartitionId;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, ListParams};
use std::collections::HashMap;
use std::net::SocketAddr;

/// Annotation a partition-node Pod carries, set by whatever deployment
/// tooling applies a `RebalancePlan` (CL3) — comma-separated logical
/// partition ids, e.g. `"3,17,42"`.
pub const PARTITIONS_ANNOTATION: &str = "graph.io/partitions";

/// Discovers partition-node replicas via the Kubernetes Pods API
/// (§6.3/§10) — no external registry (etcd/Consul), consistent with the
/// Kubernetes-native deployment target (§10).
pub struct KubernetesDiscovery {
    client: kube::Client,
    namespace: String,
    /// Label selector identifying partition-node Pods, e.g.
    /// `"app=graph-partition-node"` — scoped so this never picks up the
    /// coordinator's own Pods or unrelated workloads in the namespace.
    label_selector: String,
    /// Port every partition-node Pod listens on for `PartitionService`
    /// (same for all replicas — set via the shared Pod spec/container
    /// image config, not per-Pod).
    port: u16,
}

impl KubernetesDiscovery {
    /// Builds a client from the in-cluster service account (or local
    /// kubeconfig when running outside a cluster, e.g. `cargo test`
    /// against a real cluster) via `kube::Client::try_default`.
    pub async fn try_new(
        namespace: impl Into<String>,
        label_selector: impl Into<String>,
        port: u16,
    ) -> Result<Self, DiscoveryError> {
        let client = kube::Client::try_default()
            .await
            .map_err(|e| DiscoveryError::KubernetesApi(e.to_string()))?;
        Ok(Self {
            client,
            namespace: namespace.into(),
            label_selector: label_selector.into(),
            port,
        })
    }
}

#[async_trait]
impl Discovery for KubernetesDiscovery {
    async fn current_map(&self) -> Result<PartitionMap, DiscoveryError> {
        let api: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        let params = ListParams::default().labels(&self.label_selector);
        let pods = api
            .list(&params)
            .await
            .map_err(|e| DiscoveryError::KubernetesApi(e.to_string()))?;

        let mut map = PartitionMap::default();
        for pod in pods.items {
            let Some(endpoint) = pod_to_replica_endpoint(&pod, self.port) else {
                continue;
            };
            for partition in pod_partitions(&pod) {
                map.replicas_by_partition
                    .entry(partition)
                    .or_default()
                    .push(endpoint.clone());
            }
        }
        Ok(map)
    }
}

fn pod_to_replica_endpoint(pod: &Pod, port: u16) -> Option<ReplicaEndpoint> {
    let name = pod.metadata.name.clone()?;
    let ip = pod.status.as_ref()?.pod_ip.clone()?;
    let address: SocketAddr = format!("{ip}:{port}").parse().ok()?;
    Some(ReplicaEndpoint {
        replica: ReplicaId(name),
        address,
        healthy: pod_is_ready(pod),
    })
}

/// A Pod is a healthy replica once Kubernetes itself reports its `Ready`
/// condition true — reusing kubelet's own liveness/readiness probing
/// (whatever the partition-node's probe is configured to check, e.g. the
/// gRPC `HealthCheck` RPC, §8.2) rather than this crate re-implementing
/// health checking on top of `current_map`'s periodic poll.
fn pod_is_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .into_iter()
        .flatten()
        .any(|c| c.type_ == "Ready" && c.status == "True")
}

fn pod_partitions(pod: &Pod) -> Vec<PartitionId> {
    pod.metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(PARTITIONS_ANNOTATION))
        .map(|raw| {
            raw.split(',')
                .filter_map(|s| s.trim().parse::<u32>().ok())
                .map(PartitionId)
                .collect()
        })
        .unwrap_or_default()
}

/// A fixed, in-memory [`Discovery`] — used by tests, by the single-
/// process demo (`examples/demo`), and by any deployment target that
/// isn't Kubernetes (e.g. local dev via `bin/graph-partition-node`
/// processes on fixed ports, as the existing multi-process deployment
/// already does — see `ARCHITECTURE.md` §5). `KubernetesDiscovery` is
/// the only implementation that actually varies over time; this one
/// hands back whatever `PartitionMap` it was constructed with.
pub struct StaticDiscovery {
    map: PartitionMap,
}

impl StaticDiscovery {
    pub fn new(map: PartitionMap) -> Self {
        Self { map }
    }

    /// Convenience constructor: one replica per partition, all healthy,
    /// from a flat `partition_id -> address` map — the common case for
    /// local dev/tests where there's no replication to model.
    pub fn single_replica_per_partition(addrs: HashMap<u32, SocketAddr>) -> Self {
        let mut map = PartitionMap::default();
        for (partition, address) in addrs {
            map.replicas_by_partition.insert(
                PartitionId(partition),
                vec![ReplicaEndpoint {
                    replica: ReplicaId(format!("partition-{partition}")),
                    address,
                    healthy: true,
                }],
            );
        }
        Self::new(map)
    }
}

#[async_trait]
impl Discovery for StaticDiscovery {
    async fn current_map(&self) -> Result<PartitionMap, DiscoveryError> {
        Ok(self.map.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn static_discovery_returns_its_fixed_map() {
        let discovery = StaticDiscovery::single_replica_per_partition(HashMap::from([
            (0u32, "127.0.0.1:7001".parse().unwrap()),
            (1u32, "127.0.0.1:7002".parse().unwrap()),
        ]));

        let map = discovery.current_map().await.expect("static never fails");
        assert_eq!(map.healthy_replicas(PartitionId(0)).len(), 1);
        assert_eq!(map.healthy_replicas(PartitionId(1)).len(), 1);
        assert!(map.healthy_replicas(PartitionId(2)).is_empty());
    }
}
