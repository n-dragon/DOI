//! *(tasks Q5-Q7)* The coordinator's scatter-gather loop (spec §7.4):
//! [`ScatterGatherExecutor::execute`] drives a [`QueryPlan`] across
//! however many partitions it touches, one network hop at a time,
//! exactly as §7.4 describes ("à chaque hop, le coordinateur envoie les
//! frontières courantes aux partitions concernées, attend les réponses,
//! ré-agrège... et ré-envoie pour le hop suivant").
//!
//! This module depends on `graph-cluster` (for [`PartitionHasher`],
//! [`PartitionMap`], [`ReplicaEndpoint`]) but deliberately **not** on
//! `graph-proto`/`tonic` — the actual RPC transport is abstracted behind
//! [`PartitionRpc`], a trait this module depends on rather than a
//! concrete gRPC client. `bin/graph-coordinator` implements it for real
//! against `graph-proto`'s generated `PartitionServiceClient` (task
//! CO5); Q8's test implements it with an in-process fake. This keeps the
//! crate dependency graph exactly as documented in `ARCHITECTURE.md` §2
//! ("graph-query orchestrate graph-index mais ignore gRPC") and keeps
//! the scatter-gather logic itself testable without spinning up real
//! servers.
//!
//! **Decision (Q5, ResolveStart):** broadcast to every partition rather
//! than routing to one via `PartitionHasher`. The property index (§5.2)
//! is built per-partition, over that partition's own local nodes only
//! (see `graph-index`'s builder) — there is no global secondary index in
//! v1. `PartitionHasher` answers "which partition owns this *known*
//! `node_id`", which is exactly what every hop *after* the first uses it
//! for (`scatter_gather_hop` below) — but `ResolveStart`'s property
//! equality filter (`{name: "Alice"}`) doesn't have a `node_id` to hash
//! yet, so there is no single partition to route it to. Every partition
//! is asked, and whatever each one finds (ownership is disjoint by
//! construction, so results never overlap) is unioned.
//!
//! **Decision (Q6, WHERE filters):** pushed down per-hop rather than
//! deferred to one final pass the way the mono-partition executor
//! (`local_executor.rs`'s `apply_filters`) still does. This is safe
//! specifically because this DSL's `WHERE` clauses only ever constrain a
//! single alias's own properties (never compare across bindings, spec
//! §7.1) — filtering commutes with union
//! (`filter(A ∪ B) == filter(A) ∪ filter(B)`), so evaluating it once per
//! round or once at the end over the merged set are equivalent. Per-hop
//! (here: per-`ExpandHop`-*step*, evaluated once its own multi-round walk
//! completes — see `filter_frontier`) is what §7.3 already names as the
//! optimization the mono-partition baseline should evolve toward, so
//! this isn't a new tradeoff, just the distributed path adopting it
//! directly instead of inheriting the single-partition "final pass"
//! shape it wouldn't even benefit from.

use crate::{Binding, ExecutionError, Frontier, PlanStep, QueryPlan};
use async_trait::async_trait;
use graph_cluster::{PartitionHasher, PartitionMap, ReplicaEndpoint};
use graph_dsl::{ComparisonOp, HopRange, Literal, PropertyFilter};
use graph_index::{NodeRecord, PartitionId};
use graph_schema::NodeId;
use graph_storage::PropertyValue;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Coordinator -> partition-replica RPC boundary (spec §7.4). See this
/// module's doc comment for why `graph-query` depends on this trait
/// instead of `graph-proto`/`tonic` directly.
#[async_trait]
pub trait PartitionRpc: Send + Sync {
    async fn resolve_start(
        &self,
        replica: &ReplicaEndpoint,
        step: &PlanStep,
    ) -> Result<Vec<Binding>, ExecutionError>;

    async fn expand_hop(
        &self,
        replica: &ReplicaEndpoint,
        step: &PlanStep,
        frontier: &[Binding],
    ) -> Result<Frontier, ExecutionError>;

    /// Used only by `filter_frontier` (task Q6) to evaluate `WHERE`
    /// against a small, already-resolved candidate set — the same RPC
    /// `bin/graph-coordinator`'s `GraphServiceImpl::execute_query`
    /// already calls to hydrate `RETURN` projections (spec §8.2's
    /// `GetNodeProperties` extension), reused here rather than adding a
    /// second, filter-specific partition-side RPC.
    async fn get_node_properties(
        &self,
        replica: &ReplicaEndpoint,
        ids: &[NodeId],
    ) -> Result<HashMap<NodeId, NodeRecord>, ExecutionError>;
}

/// [`DistributedExecutor`]-equivalent (spec §7.4) — named for what it
/// does rather than reusing the trait name from `lib.rs`, since a trait
/// object wasn't needed here: one concrete implementation, generic over
/// [`PartitionRpc`] so tests can substitute a fake.
pub struct ScatterGatherExecutor<R> {
    pub rpc: R,
    pub hasher: PartitionHasher,
}

impl<R: PartitionRpc> ScatterGatherExecutor<R> {
    pub fn new(rpc: R, hasher: PartitionHasher) -> Self {
        Self { rpc, hasher }
    }

    /// Runs `plan` to completion against the cluster membership snapshot
    /// `partitions` (the caller — `bin/graph-coordinator`, task CO5 —
    /// owns polling `graph_cluster::Discovery` on whatever cadence it
    /// chooses; this method just consumes one snapshot per call, same as
    /// `graph-index`'s `GenerationHandle::acquire` pattern of "one
    /// consistent view per operation").
    pub async fn execute(
        &self,
        plan: &QueryPlan,
        partitions: &PartitionMap,
    ) -> Result<Vec<Binding>, ExecutionError> {
        let mut steps = plan.steps.iter();
        let first = steps.next().ok_or_else(|| {
            ExecutionError::UnknownAlias(
                "a plan always has at least a ResolveStart step".to_string(),
            )
        })?;

        let mut frontier = self.broadcast_resolve_start(first, partitions).await?;

        for step in steps {
            if !matches!(step, PlanStep::ExpandHop { .. }) {
                return Err(ExecutionError::UnknownAlias(
                    "every plan step after the first must be ExpandHop".to_string(),
                ));
            }
            frontier = self.scatter_gather_hop(step, frontier, partitions).await?;
            frontier = self.filter_frontier(step, frontier, partitions).await?;
        }

        Ok(dedup(frontier))
    }

    /// *(task Q5)* See this module's doc comment for why this broadcasts
    /// rather than routing to a single partition.
    async fn broadcast_resolve_start(
        &self,
        step: &PlanStep,
        partitions: &PartitionMap,
    ) -> Result<Vec<Binding>, ExecutionError> {
        let mut merged = Vec::new();
        let mut seen = HashSet::new();
        for partition in partitions.replicas_by_partition.keys().copied() {
            let Some(replica) = pick_replica(partitions, partition) else {
                continue;
            };
            let bindings = self.rpc.resolve_start(replica, step).await?;
            for binding in bindings {
                if seen.insert(binding_key(&binding)) {
                    merged.push(binding);
                }
            }
        }
        Ok(merged)
    }

    /// *(task Q6)* Decomposes one `PlanStep::ExpandHop { hops: min..max,
    /// .. }` into up to `max` sequential single-hop scatter rounds
    /// (§7.4). Every round groups the current frontier by the partition
    /// owning each binding's current node (via `PartitionHasher` — this
    /// is the "route by known node_id" case the hasher is for) and asks
    /// only that partition to advance it one hop; a binding that steps
    /// onto a `RemoteRef` (surfaced by `local_executor.rs`'s revised
    /// `expand_hop`, task IX3) re-seeds the *next* round on the owning
    /// partition instead of being lost. Returns the unfiltered union of
    /// every depth in `[hops.min, hops.max]`, deduplicated — `WHERE`
    /// filtering happens afterward, once, in `filter_frontier`.
    async fn scatter_gather_hop(
        &self,
        step: &PlanStep,
        frontier: Vec<Binding>,
        partitions: &PartitionMap,
    ) -> Result<Vec<Binding>, ExecutionError> {
        let PlanStep::ExpandHop {
            from_alias,
            to_alias,
            edge_type,
            direction,
            hops,
            ..
        } = step
        else {
            return Err(ExecutionError::UnknownAlias(
                "scatter_gather_hop called with a non-ExpandHop step".to_string(),
            ));
        };

        let mut current = frontier;
        let mut accepted = Vec::new();
        let mut seen = HashSet::new();
        let mut cursor_alias = from_alias.clone();

        for depth in 1..=hops.max {
            if current.is_empty() {
                break;
            }

            // A single physical hop, unfiltered (task Q6's decision:
            // filters apply once, after the whole multi-round walk,
            // in `filter_frontier`) — `to_label: None` is safe precisely
            // because `filters` is empty here (see
            // `local_executor::apply_filters`, which no-ops before ever
            // looking at `to_label` when there's nothing to filter).
            let round_step = PlanStep::ExpandHop {
                from_alias: cursor_alias.clone(),
                to_alias: to_alias.clone(),
                to_label: None,
                edge_type: edge_type.clone(),
                direction: *direction,
                hops: HopRange { min: 1, max: 1 },
                filters: Vec::new(),
            };

            current = self.one_round(&round_step, &current, partitions).await?;

            if depth >= hops.min {
                for binding in &current {
                    if seen.insert(binding_key(binding)) {
                        accepted.push(binding.clone());
                    }
                }
            }
            // From the second round on, the walk continues from
            // `to_alias`'s own binding — the same key `expand_hop`
            // just wrote a fresher `NodeId` into, one hop deeper.
            cursor_alias = to_alias.clone();
        }
        Ok(accepted)
    }

    /// One physical network round: groups `frontier` by the partition
    /// owning each binding's `round_step.from_alias` node, fans
    /// [`PartitionRpc::expand_hop`] out to each group's owning
    /// partition, and folds every response's `Frontier.local` *and*
    /// `Frontier.remote` back into one flat `Vec<Binding>` — remote
    /// entries are not resolved yet, just handed back so the *next*
    /// round routes them correctly; nothing here distinguishes them from
    /// `local` ones, since both are already `to_alias`-bound and ready
    /// to be re-grouped by `PartitionHasher` on the next call.
    async fn one_round(
        &self,
        round_step: &PlanStep,
        frontier: &[Binding],
        partitions: &PartitionMap,
    ) -> Result<Vec<Binding>, ExecutionError> {
        let PlanStep::ExpandHop { from_alias, .. } = round_step else {
            return Err(ExecutionError::UnknownAlias(
                "one_round called with a non-ExpandHop step".to_string(),
            ));
        };
        let by_partition = self.group_by_owning_partition(frontier, from_alias)?;

        let mut next = Vec::new();
        for (partition, group) in by_partition {
            let Some(replica) = pick_replica(partitions, partition) else {
                // No healthy replica for a partition this frontier needs
                // right now — those bindings simply stop advancing this
                // round rather than failing the whole query. An operator
                // would notice via the cross-partition-hop / error-rate
                // metrics (spec §9.1), not via this call returning an
                // error for what may be a transient gap.
                continue;
            };
            let result = self.rpc.expand_hop(replica, round_step, &group).await?;
            next.extend(result.local);
            next.extend(result.remote.into_iter().map(|(_, binding)| binding));
        }
        Ok(next)
    }

    fn group_by_owning_partition(
        &self,
        frontier: &[Binding],
        alias: &str,
    ) -> Result<HashMap<PartitionId, Vec<Binding>>, ExecutionError> {
        let mut groups: HashMap<PartitionId, Vec<Binding>> = HashMap::new();
        for binding in frontier {
            let &node = binding
                .get(alias)
                .ok_or_else(|| ExecutionError::UnknownAlias(alias.to_string()))?;
            groups
                .entry(self.hasher.partition_of(node))
                .or_default()
                .push(binding.clone());
        }
        Ok(groups)
    }

    /// *(task Q6, WHERE handling)* See this module's doc comment for why
    /// this evaluates filters client-side against hydrated properties
    /// (`GetNodeProperties`) instead of routing through each partition's
    /// property index the way the mono-partition `apply_filters` does.
    async fn filter_frontier(
        &self,
        step: &PlanStep,
        frontier: Vec<Binding>,
        partitions: &PartitionMap,
    ) -> Result<Vec<Binding>, ExecutionError> {
        let PlanStep::ExpandHop {
            to_alias, filters, ..
        } = step
        else {
            return Err(ExecutionError::UnknownAlias(
                "filter_frontier called with a non-ExpandHop step".to_string(),
            ));
        };
        if filters.is_empty() || frontier.is_empty() {
            return Ok(frontier);
        }

        let ids: Vec<NodeId> = frontier
            .iter()
            .filter_map(|b| b.get(to_alias).copied())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let properties = self.get_node_properties(&ids, partitions).await?;

        Ok(frontier
            .into_iter()
            .filter(|binding| {
                let Some(&id) = binding.get(to_alias) else {
                    return false;
                };
                let Some(record) = properties.get(&id) else {
                    // Couldn't fetch this candidate's properties (its
                    // partition was unreachable, see `one_round`'s same
                    // reasoning) — excluded rather than assumed to pass,
                    // since `WHERE` is a positive constraint.
                    return false;
                };
                filters
                    .iter()
                    .all(|f| evaluate_filter(&record.properties, f))
            })
            .collect())
    }

    /// Fetches property records for `ids`, grouped and routed by owning
    /// partition. `filter_frontier` (above) uses this internally for
    /// `WHERE` evaluation; it's also `pub` so `bin/graph-coordinator` can
    /// call it directly for `RETURN` projection hydration (spec §8.2's
    /// `GetNodeProperties` extension) — the exact same "which partition
    /// actually has this id" routing problem, just triggered from
    /// outside this module instead of from a `WHERE` clause.
    pub async fn get_node_properties(
        &self,
        ids: &[NodeId],
        partitions: &PartitionMap,
    ) -> Result<HashMap<NodeId, NodeRecord>, ExecutionError> {
        let mut properties: HashMap<NodeId, NodeRecord> = HashMap::new();
        for (partition, ids) in group_ids_by_partition(&self.hasher, ids) {
            let Some(replica) = pick_replica(partitions, partition) else {
                continue;
            };
            properties.extend(self.rpc.get_node_properties(replica, &ids).await?);
        }
        Ok(properties)
    }
}

/// v1 load balancing: first healthy replica. Round-robin/least-loaded
/// (spec §6.4 — "toutes les répliques étant équivalentes... round-robin
/// ou least-loaded") is a future refinement over this, not a correctness
/// requirement: every replica of a partition serves an identical index
/// generation (§6.4), so any healthy one is a valid answer.
fn pick_replica(partitions: &PartitionMap, partition: PartitionId) -> Option<&ReplicaEndpoint> {
    partitions.healthy_replicas(partition).into_iter().next()
}

fn group_ids_by_partition(
    hasher: &PartitionHasher,
    ids: &[NodeId],
) -> HashMap<PartitionId, Vec<NodeId>> {
    let mut groups: HashMap<PartitionId, Vec<NodeId>> = HashMap::new();
    for &id in ids {
        groups.entry(hasher.partition_of(id)).or_default().push(id);
    }
    groups
}

fn evaluate_filter(properties: &BTreeMap<String, PropertyValue>, filter: &PropertyFilter) -> bool {
    match properties.get(&filter.property) {
        Some(value) => literal_matches(value, filter.op, &filter.value),
        None => false,
    }
}

fn literal_matches(value: &PropertyValue, op: ComparisonOp, literal: &Literal) -> bool {
    use std::cmp::Ordering;
    let ordering = match (value, literal) {
        (PropertyValue::Int64(v), Literal::Int64(l)) => v.partial_cmp(l),
        (PropertyValue::Float64(v), Literal::Float64(l)) => v.partial_cmp(l),
        (PropertyValue::Bool(v), Literal::Bool(l)) => Some(v.cmp(l)),
        (PropertyValue::String(v), Literal::String(l)) => Some(v.as_str().cmp(l.as_str())),
        // Type mismatch between the stored property and the filter
        // literal — the DSL validator (D7-D9) already rejects this
        // combination before a plan is ever built, so this is
        // unreachable in practice; treated as "doesn't match" rather
        // than panicking, since a partition-side data/schema drift
        // shouldn't take a query down.
        _ => None,
    };
    let Some(ordering) = ordering else {
        return false;
    };
    match op {
        ComparisonOp::Eq => ordering == Ordering::Equal,
        ComparisonOp::Ne => ordering != Ordering::Equal,
        ComparisonOp::Lt => ordering == Ordering::Less,
        ComparisonOp::Lte => ordering != Ordering::Greater,
        ComparisonOp::Gt => ordering == Ordering::Greater,
        ComparisonOp::Gte => ordering != Ordering::Less,
    }
}

/// *(task Q7)* Deterministic dedup key for a [`Binding`] — sorted
/// `(alias, node_id)` pairs, since `HashMap` (what `Binding` is a type
/// alias for) implements `PartialEq` but not `Hash`. Used at every merge
/// point (`ResolveStart` broadcast, each scatter round's implicit
/// continuation, and the final result set) — this only ever eliminates
/// exact duplicate bindings, never reduces or groups them (no
/// aggregation, spec §7.1/§1.4).
fn binding_key(binding: &Binding) -> Vec<(String, u64)> {
    let mut key: Vec<(String, u64)> = binding
        .iter()
        .map(|(alias, id)| (alias.clone(), id.0))
        .collect();
    key.sort();
    key
}

fn dedup(bindings: Vec<Binding>) -> Vec<Binding> {
    let mut seen = HashSet::new();
    bindings
        .into_iter()
        .filter(|b| seen.insert(binding_key(b)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LocalExecutor, NaivePlanner, Planner, SimpleLocalExecutor};
    use async_trait::async_trait as async_trait_attr;
    use graph_cluster::ReplicaId;
    use graph_dsl::{Parser as DslParser, PestParser};
    use graph_index::{GenerationHandle, IcebergIndexBuilder, IndexBuilder};
    use graph_schema::{partitioning::partition_of, EdgeId, Label, PestSchemaParser, SchemaParser};
    use graph_storage::{EdgeRow, IcebergReader, NodeRow, SnapshotId, StorageError};
    use std::collections::BTreeMap;

    struct FakeReader {
        nodes: Vec<NodeRow>,
        edges: Vec<EdgeRow>,
    }

    #[async_trait_attr]
    impl IcebergReader for FakeReader {
        async fn latest_snapshot(&self, _table: &str) -> Result<SnapshotId, StorageError> {
            Ok(SnapshotId(1))
        }

        async fn scan_nodes(
            &self,
            _schema: &graph_schema::Schema,
            _label: &Label,
            _snapshot: SnapshotId,
        ) -> Result<graph_storage::NodeRowStream, StorageError> {
            Ok(Box::pin(futures::stream::iter(
                self.nodes.clone().into_iter().map(Ok),
            )))
        }

        async fn scan_edges(
            &self,
            _schema: &graph_schema::Schema,
            _edge_type: &graph_schema::EdgeType,
            _snapshot: SnapshotId,
        ) -> Result<graph_storage::EdgeRowStream, StorageError> {
            Ok(Box::pin(futures::stream::iter(
                self.edges.clone().into_iter().map(Ok),
            )))
        }
    }

    fn schema() -> graph_schema::Schema {
        PestSchemaParser
            .parse(
                r#"
                schema graph_v1 {
                  node Person {
                    id: NodeId
                    @indexed name: String
                    @indexed birth_year: Int64
                  }
                  edge KNOWS {
                    from: Person
                    to: Person
                  }
                }
                "#,
            )
            .expect("valid IDL")
    }

    fn node(id: u64, name: &str, birth_year: i64) -> NodeRow {
        let mut properties = BTreeMap::new();
        properties.insert("name".to_string(), PropertyValue::String(name.to_string()));
        properties.insert("birth_year".to_string(), PropertyValue::Int64(birth_year));
        NodeRow {
            id: NodeId(id),
            properties,
        }
    }

    fn edge(id: u64, src: u64, dst: u64) -> EdgeRow {
        EdgeRow {
            id: EdgeId(id),
            src: NodeId(src),
            dst: NodeId(dst),
            properties: BTreeMap::new(),
        }
    }

    /// Brute-force an id landing on `target_partition` under
    /// `n_partitions`, starting the search at `*cursor` and advancing it
    /// past whatever id is returned — same technique `graph-index`'s
    /// own IX3 test uses, so no two calls in one test ever collide.
    fn find_id(n_partitions: u32, target_partition: u32, cursor: &mut u64) -> u64 {
        loop {
            *cursor += 1;
            if partition_of(NodeId(*cursor), n_partitions) == target_partition {
                return *cursor;
            }
        }
    }

    struct FakeRpc {
        generations: HashMap<u32, GenerationHandle>,
    }

    impl FakeRpc {
        fn handle(&self, replica: &ReplicaEndpoint) -> &GenerationHandle {
            let partition: u32 = replica
                .replica
                .0
                .strip_prefix("partition-")
                .expect("test replica ids are always \"partition-<n>\"")
                .parse()
                .expect("test replica ids always end in a valid u32");
            &self.generations[&partition]
        }
    }

    #[async_trait_attr]
    impl PartitionRpc for FakeRpc {
        async fn resolve_start(
            &self,
            replica: &ReplicaEndpoint,
            step: &PlanStep,
        ) -> Result<Vec<Binding>, ExecutionError> {
            SimpleLocalExecutor
                .resolve_start(self.handle(replica), step)
                .await
        }

        async fn expand_hop(
            &self,
            replica: &ReplicaEndpoint,
            step: &PlanStep,
            frontier: &[Binding],
        ) -> Result<Frontier, ExecutionError> {
            SimpleLocalExecutor
                .expand_hop(self.handle(replica), step, frontier)
                .await
        }

        async fn get_node_properties(
            &self,
            replica: &ReplicaEndpoint,
            ids: &[NodeId],
        ) -> Result<HashMap<NodeId, NodeRecord>, ExecutionError> {
            let generation = self.handle(replica).acquire();
            Ok(ids
                .iter()
                .filter_map(|id| generation.node_records.get(id).map(|r| (*id, r.clone())))
                .collect())
        }
    }

    fn replica_endpoint(partition: u32) -> ReplicaEndpoint {
        ReplicaEndpoint {
            replica: ReplicaId(format!("partition-{partition}")),
            address: format!("127.0.0.1:{}", 7001 + partition).parse().unwrap(),
            healthy: true,
        }
    }

    /// *(task Q8)* End-to-end distributed test: two partitions, Alice and
    /// Bob local to partition 0, Carol and Dave local to partition 1 —
    /// so the `KNOWS` chain Alice->Bob->Carol->Dave crosses the
    /// partition boundary exactly at hop 2 (Bob->Carol). Runs the same
    /// query and dataset shape as `local_executor`'s mono-partition Q4
    /// test (`*1..3`, `WHERE birth_year > 1990`), and expects the exact
    /// same final result — proving the distributed path is observably
    /// equivalent to the mono-partition one for a query that happens to
    /// cross a partition boundary mid-traversal.
    #[tokio::test]
    async fn distributed_k_hop_query_crosses_a_partition_boundary() {
        let n_partitions = 2u32;
        let mut cursor = 0u64;
        let alice = find_id(n_partitions, 0, &mut cursor);
        let bob = find_id(n_partitions, 0, &mut cursor);
        let carol = find_id(n_partitions, 1, &mut cursor);
        let dave = find_id(n_partitions, 1, &mut cursor);

        // Every replica scans the *same* full dataset (spec §4.1 — no
        // partition pushdown at the storage layer yet, see
        // `graph-index`'s builder doc comment) and filters to its own
        // partition in memory.
        let full_nodes = vec![
            node(alice, "Alice", 1970),
            node(bob, "Bob", 1990),
            node(carol, "Carol", 1995),
            node(dave, "Dave", 2000),
        ];
        let full_edges = vec![
            edge(100, alice, bob),
            edge(101, bob, carol),
            edge(102, carol, dave),
        ];

        let schema = schema();
        let labels = [Label("Person".to_string())];

        let mut generations = HashMap::new();
        for partition in [0u32, 1u32] {
            let reader = FakeReader {
                nodes: full_nodes.clone(),
                edges: full_edges.clone(),
            };
            let builder = IcebergIndexBuilder::new(reader, schema.clone(), n_partitions);
            let generation = builder
                .build(graph_index::PartitionId(partition), &labels)
                .await
                .expect("build should succeed");
            generations.insert(partition, GenerationHandle::new(generation));
        }

        let mut partitions = PartitionMap::default();
        partitions
            .replicas_by_partition
            .insert(PartitionId(0), vec![replica_endpoint(0)]);
        partitions
            .replicas_by_partition
            .insert(PartitionId(1), vec![replica_endpoint(1)]);

        let executor =
            ScatterGatherExecutor::new(FakeRpc { generations }, PartitionHasher { n_partitions });

        let query = PestParser
            .parse(
                r#"
                MATCH (p:Person {name: "Alice"})-[:KNOWS*1..3]->(friend:Person)
                WHERE friend.birth_year > 1990
                RETURN friend
                "#,
            )
            .expect("valid DSL");
        let plan = NaivePlanner.plan(&query);

        let bindings = executor
            .execute(&plan, &partitions)
            .await
            .expect("distributed execution should succeed");

        let friend_alias = plan.project.first().expect("RETURN friend");
        let friends: HashSet<NodeId> = bindings.iter().map(|b| b[friend_alias]).collect();

        // Bob (1990) excluded by the WHERE filter; Carol and Dave remain
        // — same expectation as the mono-partition Q4 test, despite Bob
        // and Carol living on different partitions in this one.
        assert_eq!(friends, HashSet::from([NodeId(carol), NodeId(dave)]));
    }
}
