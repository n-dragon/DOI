//! [`LocalExecutor`] (tasks Q2, Q3): drives one [`QueryPlan`] against a
//! single partition's currently-served [`IndexGeneration`] — the whole
//! execution engine for the mono-partition MVP (spec §12 Phase 1).

use crate::planner::literal_to_property_key;
use crate::{Binding, ExecutionError, Frontier, LocalExecutor, PlanStep};
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
            .map(|&node| Binding::from([(alias.clone(), node)]))
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
                            if seen_per_binding.insert((binding_idx, remote_ref.node)) {
                                let mut extended = binding.clone();
                                extended.insert(to_alias.clone(), remote_ref.node);
                                remote.push((remote_ref, extended));
                            }
                        }
                    }
                }
                if depth >= hops.min {
                    for &node in &next {
                        if seen_per_binding.insert((binding_idx, node)) {
                            let mut extended = binding.clone();
                            extended.insert(to_alias.clone(), node);
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
        .filter(|b| allowed.contains(&b[to_alias]))
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

        let friends: HashSet<NodeId> = bindings
            .iter()
            .map(|b| b[plan.project.first().expect("RETURN friend")])
            .collect();

        // Bob (1990) is excluded by birth_year > 1990; Erin (hop 4) is
        // excluded by the *1..3 range; Carol and Dave remain.
        assert_eq!(friends, HashSet::from([NodeId(3), NodeId(4)]));
    }
}
