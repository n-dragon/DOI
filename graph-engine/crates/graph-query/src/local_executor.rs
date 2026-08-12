//! [`LocalExecutor`] (tasks Q2, Q3): drives one [`QueryPlan`] against a
//! single partition's currently-served [`IndexGeneration`] — the whole
//! execution engine for the mono-partition MVP (spec §12 Phase 1).
//!
//! Extended for the least-privilege-via-telemetry use case with
//! `resolve_all` (label-wide scan, no property lookup) and
//! `check_anti_join` (the correlated `NOT EXISTS` check) — see each
//! method's doc comment.

use crate::filter_eval::evaluate_filter;
use crate::planner::literal_to_property_key;
use crate::{AntiJoinStep, Binding, ExecutionError, Frontier, LocalExecutor, PlanStep};
use async_trait::async_trait;
use graph_dsl::{ComparisonOp, Direction, PropertyFilter};
use graph_index::{GenerationHandle, PropertyIndex};
use graph_schema::NodeId;
use std::collections::HashSet;
use std::ops::Bound;

pub struct SimpleLocalExecutor;

#[async_trait]
impl LocalExecutor for SimpleLocalExecutor {
    /// *(task Q2)* Delegates to `PropertyIndex::lookup_eq`, one binding
    /// per matching `NodeId`.
    async fn resolve_start(
        &self,
        index: &GenerationHandle,
        step: &PlanStep,
    ) -> Result<Vec<Binding>, ExecutionError> {
        let PlanStep::ResolveStart {
            alias,
            label_or_type,
            property,
            key,
        } = step
        else {
            return Err(ExecutionError::UnknownAlias(
                "resolve_start called with a non-ResolveStart step".to_string(),
            ));
        };

        let generation = index.acquire();
        Ok(generation
            .properties
            .lookup_eq(label_or_type, property, key)
            .iter()
            .map(|&node| Binding::from_node(alias.clone(), node))
            .collect())
    }

    /// *(least-privilege-via-telemetry use case)* Every local node of
    /// `label`, read straight off `node_records` rather than through
    /// `PropertyIndex` — there's no value to look up by (`MATCH
    /// (w:Workload)` with no `{...}`), so this is a scan, not an index
    /// lookup. `node_records` already holds every locally-owned node's
    /// label alongside its properties (populated by the same builder
    /// pass that constructs `PropertyIndex`, task IX4), so no separate
    /// storage is needed just for this.
    async fn resolve_all(
        &self,
        index: &GenerationHandle,
        step: &PlanStep,
    ) -> Result<Vec<Binding>, ExecutionError> {
        let PlanStep::ResolveAll { alias, label } = step else {
            return Err(ExecutionError::UnknownAlias(
                "resolve_all called with a non-ResolveAll step".to_string(),
            ));
        };

        let generation = index.acquire();
        Ok(generation
            .node_records
            .iter()
            .filter(|(_, record)| &record.label.0 == label)
            .map(|(&id, _)| Binding::from_node(alias.clone(), id))
            .collect())
    }

    /// *(task Q3)* For each binding in `frontier`, walks the topological
    /// index `hops.min..=hops.max` steps from `from_alias` (breadth-first,
    /// stopping early once the frontier is empty) and binds every node
    /// reached at `hops.min` or deeper to `to_alias`. `WHERE` filters are
    /// applied as a final pass over the collected bindings via the
    /// property index (§7.3 notes early pushdown as a Phase 2+
    /// optimization — this is the correct-but-naive v1 baseline it's
    /// meant to improve on).
    async fn expand_hop(
        &self,
        index: &GenerationHandle,
        step: &PlanStep,
        frontier: &[Binding],
    ) -> Result<Frontier, ExecutionError> {
        let PlanStep::ExpandHop {
            from_alias,
            to_alias,
            to_label,
            edge_type,
            direction,
            hops,
            filters,
            edge_alias,
        } = step
        else {
            return Err(ExecutionError::UnknownAlias(
                "expand_hop called with a non-ExpandHop step".to_string(),
            ));
        };

        let generation = index.acquire();
        let mut local = Vec::new();
        let mut remote = Vec::new();
        let mut seen_per_binding: HashSet<(usize, NodeId)> = HashSet::new();

        for (binding_idx, binding) in frontier.iter().enumerate() {
            let &start = binding
                .nodes
                .get(from_alias)
                .ok_or_else(|| ExecutionError::UnknownAlias(from_alias.clone()))?;

            let mut current: HashSet<NodeId> = HashSet::from([start]);
            for depth in 1..=hops.max {
                let mut next = HashSet::new();
                for &node in &current {
                    let neighbors = match direction {
                        Direction::Outgoing => generation.topology.out_neighbors(node),
                        Direction::Incoming => generation.topology.in_neighbors(node),
                    };
                    for entry in neighbors {
                        if let Some(t) = edge_type {
                            if &entry.edge_type.0 != t {
                                continue;
                            }
                        }
                        if let Some(dst) = entry.dst_local {
                            next.insert(dst);
                            // *(edge alias)* Only ever `Some` when
                            // `hops == {1,1}` (D7-D9 rejects an alias on
                            // a variable-length hop), so there's exactly
                            // one depth iteration to attach it at — no
                            // ambiguity about *which* traversed edge the
                            // alias would mean. Pushed here, per
                            // `entry`, rather than via the `next`-based
                            // pass below: two *distinct* parallel edges
                            // (multigraph, spec §3.3) to the same `dst`
                            // must produce two distinct bindings (each
                            // with its own bound `EdgeId`), which
                            // deduplicating on `(binding_idx, dst)` alone
                            // — as the unaliased path below correctly
                            // does, since it doesn't care *which* edge
                            // was taken — would collapse into one.
                            if let Some(edge_alias) = edge_alias {
                                let mut extended = binding.clone();
                                extended.nodes.insert(to_alias.clone(), dst);
                                extended.edges.insert(edge_alias.clone(), entry.edge_id);
                                local.push(extended);
                            }
                        } else if let Some(remote_ref) = entry.dst_remote {
                            // *(task Q6)* This partition can't continue the
                            // BFS past a cross-partition edge — it hands
                            // the binding-so-far back to the coordinator's
                            // `DistributedExecutor`, which re-seeds its own
                            // next scatter round from `remote_ref.node` on
                            // `remote_ref.partition` (see
                            // `graph_query::distributed_executor`'s doc
                            // comment for why this only needs to handle a
                            // single graph-hop per call in the distributed
                            // case — `hops` is always `{1,1}` there, so
                            // there's no "how many hops remain" to track:
                            // every remote hand-off starts a fresh round).
                            // Same per-`entry` reasoning as the local
                            // branch above applies whenever `edge_alias`
                            // is set; otherwise this keeps deduplicating
                            // on `(binding_idx, node)` exactly as before.
                            let should_push = match edge_alias {
                                Some(_) => true,
                                None => seen_per_binding.insert((binding_idx, remote_ref.node)),
                            };
                            if should_push {
                                let mut extended = binding.clone();
                                extended.nodes.insert(to_alias.clone(), remote_ref.node);
                                if let Some(edge_alias) = edge_alias {
                                    extended.edges.insert(edge_alias.clone(), entry.edge_id);
                                }
                                remote.push((remote_ref, extended));
                            }
                        }
                    }
                }
                // The `edge_alias` branch above already pushed its own
                // `local` entries (it needs to attach the edge id at the
                // exact moment the matching neighbor is found, before
                // this depth/hops.min bookkeeping); the unaliased path
                // below handles every other case exactly as before.
                if edge_alias.is_none() && depth >= hops.min {
                    for &node in &next {
                        if seen_per_binding.insert((binding_idx, node)) {
                            let mut extended = binding.clone();
                            extended.nodes.insert(to_alias.clone(), node);
                            local.push(extended);
                        }
                    }
                }
                if next.is_empty() {
                    break;
                }
                current = next;
            }
        }

        let local = apply_filters(
            &generation.properties,
            to_label.as_deref(),
            to_alias,
            filters,
            local,
        )?;
        Ok(Frontier { local, remote })
    }

    /// *(least-privilege-via-telemetry use case)* Evaluates
    /// `PlanStep::AntiJoin` locally: for each binding, walks the (always
    /// outgoing — D7-D9 enforces this) adjacency from `from_alias`'s
    /// bound node looking for **any** edge of `edge_type` landing on
    /// `to_alias`'s bound node whose properties satisfy every one of
    /// `conditions`. If one is found, `NOT EXISTS` is false for this
    /// binding (dropped); otherwise it survives. `from_alias`'s node is
    /// guaranteed local (the caller groups by its owning partition, same
    /// as `expand_hop`'s rounds) — and since an edge's record lives with
    /// its *source* partition (`graph-index`'s builder) and this edge is
    /// always outgoing from `from_alias`, this partition has every
    /// candidate edge's full record, never just its topology.
    async fn check_anti_join(
        &self,
        index: &GenerationHandle,
        step: &AntiJoinStep,
        conditions: &[PropertyFilter],
        frontier: &[Binding],
    ) -> Result<Vec<Binding>, ExecutionError> {
        let generation = index.acquire();
        let mut survivors = Vec::new();

        for binding in frontier {
            let &from_node = binding
                .nodes
                .get(&step.from_alias)
                .ok_or_else(|| ExecutionError::UnknownAlias(step.from_alias.clone()))?;
            let &to_node = binding
                .nodes
                .get(&step.to_alias)
                .ok_or_else(|| ExecutionError::UnknownAlias(step.to_alias.clone()))?;

            let neighbors = match step.direction {
                Direction::Outgoing => generation.topology.out_neighbors(from_node),
                Direction::Incoming => generation.topology.in_neighbors(from_node),
            };

            let candidate_exists = neighbors.iter().any(|entry| {
                if let Some(t) = &step.edge_type {
                    if &entry.edge_type.0 != t {
                        return false;
                    }
                }
                let lands_on_to_node = entry.dst_local == Some(to_node)
                    || entry.dst_remote.map(|r| r.node) == Some(to_node);
                if !lands_on_to_node {
                    return false;
                }
                match generation.edge_records.get(&entry.edge_id) {
                    Some(record) => conditions
                        .iter()
                        .all(|f| evaluate_filter(&record.properties, f)),
                    // Edge exists topologically but this partition
                    // doesn't have its record — shouldn't happen for an
                    // outgoing edge from a locally-owned source (see
                    // this method's doc comment), but treated the same
                    // conservative way `filter_frontier`/`resolve_and_
                    // group_by_condition` (distributed_executor.rs)
                    // treat any other unresolvable lookup: can't verify
                    // it satisfies the conditions, so it doesn't count
                    // as a match.
                    None => false,
                }
            });

            if !candidate_exists {
                survivors.push(binding.clone());
            }
        }
        Ok(survivors)
    }
}

/// Intersects `bindings` down to those whose `to_alias` node satisfies
/// every filter, using the property index rather than re-reading each
/// candidate's raw property values (`IndexGeneration` doesn't store those
/// at all, only the CSR topology and the property->NodeId index - see
/// `graph-index`'s doc comment on `PropertyIndex`).
fn apply_filters(
    properties: &PropertyIndex,
    to_label: Option<&str>,
    to_alias: &str,
    filters: &[PropertyFilter],
    bindings: Vec<Binding>,
) -> Result<Vec<Binding>, ExecutionError> {
    if filters.is_empty() {
        return Ok(bindings);
    }
    let Some(label) = to_label else {
        // The planner (Q1) never emits filters without a label to route
        // them through - reaching this would be a planner bug.
        return Ok(bindings);
    };

    let mut allowed: Option<HashSet<NodeId>> = None;
    for filter in filters {
        let matches = property_filter_matches(properties, label, filter)?;
        allowed = Some(match allowed {
            None => matches,
            Some(prev) => prev.intersection(&matches).copied().collect(),
        });
    }
    let allowed = allowed.unwrap_or_default();

    Ok(bindings
        .into_iter()
        .filter(|b| allowed.contains(&b.nodes[to_alias]))
        .collect())
}

fn property_filter_matches(
    properties: &PropertyIndex,
    label: &str,
    filter: &PropertyFilter,
) -> Result<HashSet<NodeId>, ExecutionError> {
    let key = literal_to_property_key(&filter.value);
    let ids: Vec<NodeId> = match filter.op {
        ComparisonOp::Eq => properties.lookup_eq(label, &filter.property, &key).to_vec(),
        ComparisonOp::Gt => properties.lookup_range(
            label,
            &filter.property,
            (Bound::Excluded(key), Bound::Unbounded),
        ),
        ComparisonOp::Gte => properties.lookup_range(
            label,
            &filter.property,
            (Bound::Included(key), Bound::Unbounded),
        ),
        ComparisonOp::Lt => properties.lookup_range(
            label,
            &filter.property,
            (Bound::Unbounded, Bound::Excluded(key)),
        ),
        ComparisonOp::Lte => properties.lookup_range(
            label,
            &filter.property,
            (Bound::Unbounded, Bound::Included(key)),
        ),
        ComparisonOp::Ne => {
            // Not supported by v1's index-backed pushdown: the property
            // index maps value -> NodeIds, so "not equal to X" would need
            // "every indexed NodeId minus X's bucket", which isn't
            // something the index tracks. Not exercised by the spec's two
            // priority query shapes (§7.1) - both only use Eq/Gt.
            return Err(ExecutionError::UnknownAlias(format!(
                "WHERE ... <> on property {} is not supported in v1",
                filter.property
            )));
        }
    };
    Ok(ids.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::NaivePlanner;
    use crate::Planner;
    use graph_dsl::{Parser as DslParser, PestParser};
    use graph_index::{IcebergIndexBuilder, IndexBuilder, PartitionId};
    use graph_schema::{EdgeId, Label, PestSchemaParser, SchemaParser};
    use graph_storage::{EdgeRow, IcebergReader, NodeRow, PropertyValue, SnapshotId, StorageError};
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Mutex;

    struct FakeReader {
        nodes: Mutex<HashMap<String, Vec<NodeRow>>>,
        edges: Mutex<HashMap<String, Vec<EdgeRow>>>,
    }

    #[async_trait]
    impl IcebergReader for FakeReader {
        async fn latest_snapshot(&self, _table: &str) -> Result<SnapshotId, StorageError> {
            Ok(SnapshotId(1))
        }

        async fn scan_nodes(
            &self,
            _schema: &graph_schema::Schema,
            label: &Label,
            _snapshot: SnapshotId,
        ) -> Result<graph_storage::NodeRowStream, StorageError> {
            let rows = self
                .nodes
                .lock()
                .unwrap()
                .get(&label.0)
                .cloned()
                .unwrap_or_default();
            Ok(Box::pin(futures::stream::iter(rows.into_iter().map(Ok))))
        }

        async fn scan_edges(
            &self,
            _schema: &graph_schema::Schema,
            edge_type: &graph_schema::EdgeType,
            _snapshot: SnapshotId,
        ) -> Result<graph_storage::EdgeRowStream, StorageError> {
            let rows = self
                .edges
                .lock()
                .unwrap()
                .get(&edge_type.0)
                .cloned()
                .unwrap_or_default();
            Ok(Box::pin(futures::stream::iter(rows.into_iter().map(Ok))))
        }
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

    /// *(task Q4)* End-to-end: plan + execute the spec §7.1 k-hop example
    /// against an in-memory `IndexGeneration` - Alice knows Bob (1 hop)
    /// who knows Carol (2 hops) who knows Dave (3 hops); Dave is also
    /// directly reachable in a way that's *not* within 3 hops (Erin, hop
    /// 4) to prove the range is respected. `WHERE birth_year > 1990`
    /// should exclude Bob (born 1990, not `> 1990`).
    #[tokio::test]
    async fn end_to_end_k_hop_query() {
        let schema = PestSchemaParser
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
            .expect("valid IDL");

        let reader = FakeReader {
            nodes: Mutex::new(HashMap::from([(
                "Person".to_string(),
                vec![
                    node(1, "Alice", 1970),
                    node(2, "Bob", 1990),
                    node(3, "Carol", 1995),
                    node(4, "Dave", 2000),
                    node(5, "Erin", 2005),
                ],
            )])),
            edges: Mutex::new(HashMap::from([(
                "KNOWS".to_string(),
                vec![
                    edge(100, 1, 2), // Alice -> Bob (hop 1)
                    edge(101, 2, 3), // Bob -> Carol (hop 2)
                    edge(102, 3, 4), // Carol -> Dave (hop 3)
                    edge(103, 4, 5), // Dave -> Erin (hop 4, out of *1..3 range)
                ],
            )])),
        };

        let builder = IcebergIndexBuilder::new(reader, schema, 1);
        let generation = builder
            .build(PartitionId(0), &[Label("Person".to_string())])
            .await
            .expect("index should build");
        let handle = GenerationHandle::new(generation);

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

        let executor = SimpleLocalExecutor;
        let mut bindings = executor
            .resolve_start(&handle, &plan.steps[0])
            .await
            .expect("resolve_start should succeed");
        assert_eq!(bindings.len(), 1, "exactly Alice should resolve the start");

        for step in &plan.steps[1..] {
            let frontier = executor
                .expand_hop(&handle, step, &bindings)
                .await
                .expect("expand_hop should succeed");
            bindings = frontier.local;
        }

        let friend_alias = &plan.project.first().expect("RETURN friend").alias;
        let friends: HashSet<NodeId> = bindings.iter().map(|b| b.nodes[friend_alias]).collect();

        // Bob (1990) is excluded by birth_year > 1990; Erin (hop 4) is
        // excluded by the *1..3 range; Carol and Dave remain.
        assert_eq!(friends, HashSet::from([NodeId(3), NodeId(4)]));
    }

    /// *(least-privilege-via-telemetry use case)* `resolve_all` returns
    /// every local node of the label, unfiltered.
    #[tokio::test]
    async fn resolve_all_returns_every_node_of_the_label() {
        let schema = PestSchemaParser
            .parse(
                r#"
                schema graph_v1 {
                  node Person {
                    id: NodeId
                    @indexed name: String
                    @indexed birth_year: Int64
                  }
                }
                "#,
            )
            .expect("valid IDL");

        let reader = FakeReader {
            nodes: Mutex::new(HashMap::from([(
                "Person".to_string(),
                vec![node(1, "Alice", 1970), node(2, "Bob", 1990)],
            )])),
            edges: Mutex::new(HashMap::new()),
        };
        let builder = IcebergIndexBuilder::new(reader, schema, 1);
        let generation = builder
            .build(PartitionId(0), &[Label("Person".to_string())])
            .await
            .expect("index should build");
        let handle = GenerationHandle::new(generation);

        let step = PlanStep::ResolveAll {
            alias: "p".to_string(),
            label: "Person".to_string(),
        };
        let bindings = SimpleLocalExecutor
            .resolve_all(&handle, &step)
            .await
            .expect("resolve_all should succeed");

        let ids: HashSet<NodeId> = bindings.iter().map(|b| b.nodes["p"]).collect();
        assert_eq!(ids, HashSet::from([NodeId(1), NodeId(2)]));
    }

    /// *(least-privilege-via-telemetry use case)* `check_anti_join`
    /// drops a binding when a matching, condition-satisfying edge
    /// exists, and keeps it otherwise — the core "declared minus
    /// observed" mechanism.
    #[tokio::test]
    async fn check_anti_join_filters_bindings_with_a_matching_edge() {
        let schema = PestSchemaParser
            .parse(
                r#"
                schema graph_v1 {
                  node Person {
                    id: NodeId
                    @indexed name: String
                  }
                  edge KNOWS {
                    from: Person
                    to: Person
                    weight: Int64
                  }
                }
                "#,
            )
            .expect("valid IDL");

        let mut heavy = edge(100, 1, 2); // Alice -[weight:5]-> Bob
        heavy
            .properties
            .insert("weight".to_string(), PropertyValue::Int64(5));
        let mut light = edge(101, 3, 4); // Carol -[weight:1]-> Dave
        light
            .properties
            .insert("weight".to_string(), PropertyValue::Int64(1));

        let reader = FakeReader {
            nodes: Mutex::new(HashMap::from([(
                "Person".to_string(),
                vec![
                    node(1, "Alice", 1970),
                    node(2, "Bob", 1990),
                    node(3, "Carol", 1995),
                    node(4, "Dave", 2000),
                ],
            )])),
            edges: Mutex::new(HashMap::from([("KNOWS".to_string(), vec![heavy, light])])),
        };
        let builder = IcebergIndexBuilder::new(reader, schema, 1);
        let generation = builder
            .build(PartitionId(0), &[Label("Person".to_string())])
            .await
            .expect("index should build");
        let handle = GenerationHandle::new(generation);

        let step = AntiJoinStep {
            from_alias: "x".to_string(),
            to_alias: "y".to_string(),
            direction: Direction::Outgoing,
            edge_alias: None,
            edge_type: Some("KNOWS".to_string()),
            literal_conditions: vec![],
            outer_conditions: vec![],
        };
        let conditions = vec![PropertyFilter {
            property: "weight".to_string(),
            op: ComparisonOp::Gte,
            value: graph_dsl::Literal::Int64(2),
        }];

        let mut alice_to_bob = Binding::default();
        alice_to_bob.nodes.insert("x".to_string(), NodeId(1));
        alice_to_bob.nodes.insert("y".to_string(), NodeId(2));
        let mut carol_to_dave = Binding::default();
        carol_to_dave.nodes.insert("x".to_string(), NodeId(3));
        carol_to_dave.nodes.insert("y".to_string(), NodeId(4));

        let survivors = SimpleLocalExecutor
            .check_anti_join(
                &handle,
                &step,
                &conditions,
                &[alice_to_bob.clone(), carol_to_dave.clone()],
            )
            .await
            .expect("check_anti_join should succeed");

        // Alice->Bob (weight 5 >= 2) satisfies the condition, so NOT
        // EXISTS is false for it — dropped. Carol->Dave (weight 1) does
        // not, so NOT EXISTS holds — survives.
        assert_eq!(survivors, vec![carol_to_dave]);
    }
}
