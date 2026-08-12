//! *(task CL3)* Computes where logical partitions should live when the
//! set of available machines changes — never touches `n_partitions` or
//! the hash function (spec §6.2), only the partition -> machine
//! assignment.
//!
//! **Decision (CL3):** rendezvous hashing (a.k.a. highest-random-weight,
//! HRW), not plain `partition_id % available_machines.len()`.
//!
//! Why: the spec explicitly frames rebalancing as "inspired by Nebula
//! Graph / consistent hashing (Cassandra)" (§6.2) — the property that
//! matters is that adding or removing one machine only moves the
//! partitions assigned to/from *that* machine, not a large fraction of
//! all partitions. A naive `% available_machines.len()` reshuffles most
//! partitions on every membership change (classic modulo-hashing
//! problem), which would mean rebuilding most of the cluster's index on
//! every scale event — defeating the point of a rebalance-only-what-
//! moved design (§6.2's "Seules les partitions déplacées ont leur index
//! reconstruit"). Rendezvous hashing gives that minimal-disruption
//! property without needing a full consistent-hash ring data structure:
//! for each partition, score every candidate machine with a stable hash
//! of `(partition_id, machine_name)` and take the top-N by score — only
//! partitions whose top-N changes (because a scored machine left, or a
//! new one now outscores an existing member) ever move.

use graph_index::PartitionId;
use std::collections::HashSet;

use crate::{PartitionMap, PartitionMove, RebalancePlan, RebalancePlanner};

/// Rendezvous-hashing-based [`RebalancePlanner`] (task CL3).
pub struct RendezvousRebalancePlanner {
    pub n_partitions: u32,
    /// How many distinct machines should host each logical partition
    /// (spec §6.4's replication factor `N`, e.g. 3) — kept here rather
    /// than inferred from `current`, since the very first plan (`current`
    /// empty) needs a target replication factor with nothing to infer it
    /// from.
    pub replication_factor: usize,
}

impl RebalancePlanner for RendezvousRebalancePlanner {
    fn plan(&self, current: &PartitionMap, available_machines: &[String]) -> RebalancePlan {
        if available_machines.is_empty() {
            return RebalancePlan::default();
        }
        let replication_factor = self.replication_factor.min(available_machines.len());

        let mut moves = Vec::new();
        for i in 0..self.n_partitions {
            let partition = PartitionId(i);
            let target = target_machines(partition, available_machines, replication_factor);
            let target_set: HashSet<&String> = target.iter().collect();

            let current_machines: HashSet<String> = current
                .replicas_by_partition
                .get(&partition)
                .map(|replicas| replicas.iter().map(|r| r.replica.0.clone()).collect())
                .unwrap_or_default();

            let mut to_add: Vec<String> = target
                .iter()
                .filter(|m| !current_machines.contains(*m))
                .cloned()
                .collect();
            let mut to_remove: Vec<String> = current_machines
                .iter()
                .filter(|m| !target_set.contains(m))
                .cloned()
                .collect();
            // Deterministic pairing order — the exact from/to pairing
            // within a partition is arbitrary (any old replica can be
            // torn down while any new one bootstraps), but tests need it
            // stable.
            to_add.sort();
            to_remove.sort();

            let mut removes = to_remove.into_iter();
            for add in to_add {
                moves.push(PartitionMove {
                    partition,
                    from_machine: removes.next(),
                    to_machine: add,
                });
            }
            // Leftover `removes` (replication factor shrank for this
            // partition) aren't represented as a "move" — there's no
            // `to_machine` to move them to; the caller is expected to
            // just stop routing to/decommission that replica.
        }
        RebalancePlan { moves }
    }
}

/// Top `replication_factor` machines for `partition`, ranked by a stable
/// hash of `(partition, machine)` — the rendezvous-hashing score.
/// Deterministic across calls and across processes (coordinator and any
/// tooling computing the same plan agree without coordination).
fn target_machines(
    partition: PartitionId,
    available_machines: &[String],
    replication_factor: usize,
) -> Vec<String> {
    let mut scored: Vec<(u64, &String)> = available_machines
        .iter()
        .map(|m| (rendezvous_score(partition, m), m))
        .collect();
    // Descending score; tie-break on machine name for determinism (a
    // genuine hash collision between two machines for the same partition
    // is astronomically unlikely, but sort() must still be total).
    scored.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1)));
    scored
        .into_iter()
        .take(replication_factor)
        .map(|(_, m)| m.clone())
        .collect()
}

/// Same underlying primitive as CL1's `stable_hash` (xxh3 — see
/// `hash.rs` for why), just applied to an arbitrary `(partition,
/// machine-name)` byte string instead of `stable_hash`'s `u64`-only
/// node-id signature.
fn rendezvous_score(partition: PartitionId, machine: &str) -> u64 {
    let mut bytes = Vec::with_capacity(4 + machine.len());
    bytes.extend_from_slice(&partition.0.to_le_bytes());
    bytes.extend_from_slice(machine.as_bytes());
    xxhash_rust::xxh3::xxh3_64(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ReplicaEndpoint, ReplicaId};
    use std::net::SocketAddr;

    fn endpoint(name: &str) -> ReplicaEndpoint {
        ReplicaEndpoint {
            replica: ReplicaId(name.to_string()),
            address: "127.0.0.1:7001".parse::<SocketAddr>().unwrap(),
            healthy: true,
        }
    }

    /// *(task CL3)* First-ever placement (empty `current`): every
    /// partition gets `replication_factor` machines assigned, all moves
    /// have `from_machine: None`.
    #[test]
    fn initial_placement_assigns_every_partition() {
        let planner = RendezvousRebalancePlanner {
            n_partitions: 12,
            replication_factor: 2,
        };
        let plan = planner.plan(
            &PartitionMap::default(),
            &["m0".into(), "m1".into(), "m2".into()],
        );

        assert_eq!(plan.moves.len(), 12 * 2);
        assert!(plan.moves.iter().all(|m| m.from_machine.is_none()));

        // Every partition got exactly 2 *distinct* target machines.
        for i in 0..12u32 {
            let targets: HashSet<&str> = plan
                .moves
                .iter()
                .filter(|m| m.partition == PartitionId(i))
                .map(|m| m.to_machine.as_str())
                .collect();
            assert_eq!(
                targets.len(),
                2,
                "partition {i} should get 2 distinct replicas"
            );
        }
    }

    /// *(task CL3)* Adding one machine to an already-placed cluster moves
    /// only a minority of partitions — the whole point of rendezvous
    /// hashing over `% machines.len()`.
    #[test]
    fn adding_a_machine_moves_only_a_minority_of_partitions() {
        let n_partitions = 100;
        let planner = RendezvousRebalancePlanner {
            n_partitions,
            replication_factor: 1,
        };
        let before = planner.plan(&PartitionMap::default(), &["m0".into(), "m1".into()]);

        let mut current = PartitionMap::default();
        for mv in &before.moves {
            current
                .replicas_by_partition
                .entry(mv.partition)
                .or_default()
                .push(endpoint(&mv.to_machine));
        }

        let after = planner.plan(&current, &["m0".into(), "m1".into(), "m2".into()]);

        // Expected fraction that should move onto the new machine is
        // roughly 1/3 (each partition independently has a 1-in-3 chance
        // the new machine now scores highest) — assert a loose bound
        // rather than the exact expectation to avoid a flaky test.
        assert!(
            after.moves.len() < (n_partitions as usize * 2) / 3,
            "too many partitions moved: {} of {n_partitions}",
            after.moves.len()
        );
        assert!(
            !after.moves.is_empty(),
            "adding a machine should move at least some partitions"
        );
    }

    /// *(task CL3)* A partition that's already correctly placed produces
    /// no move when `plan` is called again with the same inputs
    /// (idempotent).
    #[test]
    fn replanning_with_unchanged_inputs_produces_no_moves() {
        let planner = RendezvousRebalancePlanner {
            n_partitions: 8,
            replication_factor: 1,
        };
        let machines = vec!["m0".into(), "m1".into()];
        let first = planner.plan(&PartitionMap::default(), &machines);

        let mut current = PartitionMap::default();
        for mv in &first.moves {
            current
                .replicas_by_partition
                .entry(mv.partition)
                .or_default()
                .push(endpoint(&mv.to_machine));
        }

        let second = planner.plan(&current, &machines);
        assert!(second.moves.is_empty());
    }
}
