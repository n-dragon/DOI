//! Builds one [`IndexGeneration`] from Iceberg via [`IcebergReader`]
//! (tasks IX1, IX2, IX4). §4.1 leaves Iceberg's own partition spec `TBD`
//! ("candidat : bucket par node_id... pour aligner les partitions de
//! lecture avec le partitionnement du cluster"), unresolved as of this
//! revision — so every replica still scans *every* row of every
//! node/edge table (no partition pushdown at the storage layer yet) and
//! filters down to the nodes it owns in memory, via
//! `graph_schema::partitioning::partition_of` (task IX3, revised for
//! Phase 2 — see below). Once that Iceberg partition-spec TBD is
//! resolved, a physical filter could be pushed into `scan_nodes`/
//! `scan_edges` instead of scanning-then-discarding; not done here so as
//! not to couple this task to that separate, still-open TBD (spec §13
//! only lists schema-migration as open, but §4.1's partition spec is a
//! second one, both out of scope for this revision).
//!
//! *(task IX3, revised)* An edge's destination is [`RemoteRef`] whenever
//! it hashes to a different partition than the one being built —
//! Phase 1's "always `None`" placeholder is gone now that `n_partitions`
//! can be greater than 1. An edge whose endpoint is missing from Iceberg
//! entirely (a genuine schema/data mismatch, not just "owned by another
//! partition") is still silently dropped from that side's adjacency,
//! same as before.

use crate::{
    AdjacencyEntry, EdgeRecord, GenerationMeta, IndexBuilder, IndexGeneration, LocalIdx,
    NodeRecord, PartitionId, PropertyIndex, PropertyKey, RebuildError, RemoteRef, TopologicalIndex,
};
use futures::StreamExt;
use graph_schema::{partitioning::partition_of, EdgeId, EdgeType, Label, NodeId, Schema};
use graph_storage::{
    edge_table_name, node_table_name, EdgeRow, IcebergReader, NodeRow, PropertyValue,
};
use std::collections::{BTreeMap, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};

/// [`IndexBuilder`] backed by an [`IcebergReader`] and the schema that
/// names which tables to scan.
pub struct IcebergIndexBuilder<R> {
    reader: R,
    schema: Schema,
    /// Fixed for the graph's lifetime (spec §6.2) — passed once at
    /// construction rather than per `build()` call, since it can never
    /// legitimately differ between two rebuild cycles of the same
    /// process. `1` (the Phase 1 default at every existing call site)
    /// makes every node hash to partition 0, i.e. "everything is local"
    /// — identical behavior to Phase 1's mono-partition builder.
    n_partitions: u32,
}

impl<R: IcebergReader> IcebergIndexBuilder<R> {
    pub fn new(reader: R, schema: Schema, n_partitions: u32) -> Self {
        Self {
            reader,
            schema,
            n_partitions,
        }
    }
}

#[async_trait::async_trait]
impl<R: IcebergReader> IndexBuilder for IcebergIndexBuilder<R> {
    async fn build(
        &self,
        partition: PartitionId,
        labels: &[Label],
    ) -> Result<IndexGeneration, RebuildError> {
        let mut snapshot_by_table = HashMap::new();
        let mut nodes: Vec<(Label, NodeRow)> = Vec::new();

        for label in labels {
            let table = node_table_name(label);
            let snapshot = self.reader.latest_snapshot(&table).await?;
            let mut stream = self
                .reader
                .scan_nodes(&self.schema, label, snapshot)
                .await?;
            while let Some(row) = stream.next().await {
                nodes.push((label.clone(), row?));
            }
            snapshot_by_table.insert(table, snapshot);
        }

        let mut edges: Vec<(EdgeType, EdgeRow)> = Vec::new();
        for edge_def in self.schema.edges.values() {
            let edge_type = edge_def.edge_type.clone();
            let table = edge_table_name(&edge_type);
            let snapshot = self.reader.latest_snapshot(&table).await?;
            let mut stream = self
                .reader
                .scan_edges(&self.schema, &edge_type, snapshot)
                .await?;
            while let Some(row) = stream.next().await {
                edges.push((edge_type.clone(), row?));
            }
            snapshot_by_table.insert(table, snapshot);
        }

        // *(task IX3, revised)* Keep only the nodes this partition owns —
        // everything downstream (topology, property index, node_records)
        // operates on `nodes` after this filter, so nothing beyond this
        // point needs to know about `n_partitions`/`partition` again.
        // With `n_partitions == 1` (every Phase 1 call site) this keeps
        // every node, matching the old mono-partition behavior exactly.
        nodes.retain(|(_, row)| partition_of(row.id, self.n_partitions) == partition.0);

        let node_count = nodes.len() as u64;
        let edge_count = edges.len() as u64;
        let topology = build_topology(partition, &nodes, &edges, self.n_partitions);
        let properties = build_property_index(&self.schema, &nodes);
        // Consumes `nodes` — nothing needs the scanned rows after this,
        // so this moves the already-read properties instead of cloning
        // them a second time.
        let node_records: HashMap<NodeId, NodeRecord> = nodes
            .into_iter()
            .map(|(label, row)| {
                (
                    row.id,
                    NodeRecord {
                        label,
                        properties: row.properties,
                    },
                )
            })
            .collect();

        // An edge's record is owned by whichever partition owns its
        // *source* — the same criterion `build_csr` already uses to
        // decide whether an edge belongs in this partition's outgoing
        // adjacency at all (`id_to_local.get(&owner)`, `owner == src`
        // for the outgoing direction). Consistent with that: a
        // `GetEdgeProperties`-style lookup for a given `EdgeId` only
        // ever needs to reach the partition that would have handed that
        // id out via `ExpandHop`/`ResolveStart` in the first place.
        let edge_records: HashMap<EdgeId, EdgeRecord> = edges
            .into_iter()
            .filter(|(_, row)| partition_of(row.src, self.n_partitions) == partition.0)
            .map(|(edge_type, row)| {
                (
                    row.id,
                    EdgeRecord {
                        edge_type,
                        properties: row.properties,
                    },
                )
            })
            .collect();

        Ok(IndexGeneration {
            meta: GenerationMeta {
                partition,
                snapshot_by_table,
                built_at_unix_ms: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0),
                node_count,
                edge_count,
            },
            topology,
            properties,
            node_records,
            edge_records,
        })
    }
}

/// *(tasks IX1, IX2)* Builds outgoing and incoming CSR arrays from the
/// scanned rows. `node_ids`/`id_to_local` are shared by both directions
/// since they only depend on which nodes exist, not on the edges. `nodes`
/// is already filtered down to this partition's owned set (see `build`
/// above) — `id_to_local` therefore only ever contains local nodes,
/// which is exactly what `build_csr` needs to tell local neighbors from
/// [`RemoteRef`]s (task IX3).
fn build_topology(
    partition: PartitionId,
    nodes: &[(Label, NodeRow)],
    edges: &[(EdgeType, EdgeRow)],
    n_partitions: u32,
) -> TopologicalIndex {
    let node_ids: Vec<NodeId> = nodes.iter().map(|(_, row)| row.id).collect();
    let id_to_local: HashMap<NodeId, LocalIdx> = node_ids
        .iter()
        .enumerate()
        .map(|(i, &id)| (id, LocalIdx(i as u32)))
        .collect();

    let (out_offsets, out_entries) = build_csr(
        partition,
        &node_ids,
        &id_to_local,
        edges,
        true,
        n_partitions,
    );
    let (in_offsets, in_entries) = build_csr(
        partition,
        &node_ids,
        &id_to_local,
        edges,
        false,
        n_partitions,
    );

    TopologicalIndex {
        node_ids,
        id_to_local,
        out_offsets,
        out_entries,
        in_offsets,
        in_entries,
    }
}

fn build_csr(
    partition: PartitionId,
    node_ids: &[NodeId],
    id_to_local: &HashMap<NodeId, LocalIdx>,
    edges: &[(EdgeType, EdgeRow)],
    outgoing: bool,
    n_partitions: u32,
) -> (Vec<u32>, Vec<AdjacencyEntry>) {
    let mut buckets: Vec<Vec<AdjacencyEntry>> = vec![Vec::new(); node_ids.len()];

    for (edge_type, row) in edges {
        let (owner, dst) = if outgoing {
            (row.src, row.dst)
        } else {
            (row.dst, row.src)
        };

        let Some(&LocalIdx(i)) = id_to_local.get(&owner) else {
            continue;
        };
        // *(task IX3, revised)* `dst` is local if this partition owns
        // it. If not, and `dst` genuinely hashes to a *different*
        // partition, it's a `RemoteRef` scatter-gather (§7.4) hops to.
        // If `dst` hashes to this same partition but still isn't in
        // `id_to_local`, that's a genuine data mismatch (the node is
        // simply missing from Iceberg) rather than a remote reference —
        // fabricating a `RemoteRef` back to the partition that just
        // failed to find it would only move the same "missing data"
        // problem one network hop later, so this still silently drops
        // the neighbor, same as Phase 1.
        let dst_local = id_to_local.contains_key(&dst).then_some(dst);
        let dst_home = PartitionId(partition_of(dst, n_partitions));
        let dst_remote = (dst_local.is_none() && dst_home != partition).then_some(RemoteRef {
            partition: dst_home,
            node: dst,
        });

        buckets[i as usize].push(AdjacencyEntry {
            edge_id: row.id,
            edge_type: edge_type.clone(),
            dst_local,
            dst_remote,
        });
    }

    let mut offsets = Vec::with_capacity(node_ids.len() + 1);
    let mut entries = Vec::new();
    offsets.push(0u32);
    for bucket in buckets {
        entries.extend(bucket);
        offsets.push(entries.len() as u32);
    }

    (offsets, entries)
}

/// *(task IX4)* Indexes only node properties explicitly marked `indexed`
/// in the schema (§5.2) — matches [`PropertyIndex`]'s own contract.
/// Edge properties are never indexed: the DSL has no syntax to filter on
/// one (§7.1's grammar only supports `alias.property` where `alias`
/// binds to a node), so building an index nothing can query would be
/// dead weight. Properties without a well-ordered [`PropertyKey`]
/// representation (`Float64`, `Bytes`, `List`, `Vector`, `Null`) are
/// silently skipped for the same reason `PropertyKey` itself has no
/// variant for them — see its doc comment.
fn build_property_index(schema: &Schema, nodes: &[(Label, NodeRow)]) -> PropertyIndex {
    let mut by_key: HashMap<(String, String), BTreeMap<PropertyKey, Vec<NodeId>>> = HashMap::new();

    for (label, row) in nodes {
        let Some(node_def) = schema.node_def(label) else {
            continue;
        };

        for prop in node_def.properties.iter().filter(|p| p.indexed) {
            let Some(value) = row.properties.get(&prop.name) else {
                continue;
            };
            let Some(key) = property_key(value) else {
                continue;
            };

            by_key
                .entry((label.0.clone(), prop.name.clone()))
                .or_default()
                .entry(key)
                .or_default()
                .push(row.id);
        }
    }

    PropertyIndex { by_key }
}

fn property_key(value: &PropertyValue) -> Option<PropertyKey> {
    match value {
        PropertyValue::Int64(v) => Some(PropertyKey::Int64(*v)),
        PropertyValue::Bool(v) => Some(PropertyKey::Bool(*v)),
        PropertyValue::String(v) => Some(PropertyKey::String(v.clone())),
        PropertyValue::Timestamp(v) => Some(PropertyKey::Timestamp(*v)),
        PropertyValue::Float64(_)
        | PropertyValue::Bytes(_)
        | PropertyValue::List(_)
        | PropertyValue::Null => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GenerationHandle;
    use async_trait::async_trait;
    use graph_schema::{EdgeId, PestSchemaParser, SchemaParser};
    use graph_storage::{EdgeRowStream, NodeRowStream, SnapshotId, StorageError};
    use std::collections::BTreeMap as StdBTreeMap;
    use std::sync::Mutex;

    /// An in-memory [`IcebergReader`] fake — IX1/IX2/IX4/IX6/IX7 only
    /// need to exercise the CSR/property-index construction logic, not
    /// real Iceberg I/O (that's ST6's job).
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
            _schema: &Schema,
            label: &Label,
            _snapshot: SnapshotId,
        ) -> Result<NodeRowStream, StorageError> {
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
            _schema: &Schema,
            edge_type: &EdgeType,
            _snapshot: SnapshotId,
        ) -> Result<EdgeRowStream, StorageError> {
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

    fn small_social_schema() -> Schema {
        PestSchemaParser
            .parse(
                r#"
                schema graph_v1 {
                  node Person {
                    id: NodeId
                    @indexed name: String
                    @indexed birth_year: Int64?
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
        let mut properties = StdBTreeMap::new();
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
            properties: StdBTreeMap::new(),
        }
    }

    /// *(tasks IX6, IX7)* A small synthetic graph — Alice -> Bob -> Carol,
    /// Alice -> Carol directly — checked from every angle: outgoing/
    /// incoming neighbors and both property-index lookup modes.
    async fn build_test_generation() -> IndexGeneration {
        let reader = FakeReader {
            nodes: Mutex::new(HashMap::from([(
                "Person".to_string(),
                vec![
                    node(1, "Alice", 1985),
                    node(2, "Bob", 1990),
                    node(3, "Carol", 1995),
                ],
            )])),
            edges: Mutex::new(HashMap::from([(
                "KNOWS".to_string(),
                vec![edge(100, 1, 2), edge(101, 2, 3), edge(102, 1, 3)],
            )])),
        };

        // n_partitions: 1 - mono-partition, matches every existing Phase
        // 1 test's expectation that nothing is ever remote.
        let builder = IcebergIndexBuilder::new(reader, small_social_schema(), 1);
        builder
            .build(PartitionId(0), &[Label("Person".to_string())])
            .await
            .expect("build should succeed against the fake reader")
    }

    /// *(task IX6)* Outgoing neighbors are correct.
    #[tokio::test]
    async fn topological_index_out_neighbors_are_correct() {
        let gen = build_test_generation().await;

        let alice_out: Vec<NodeId> = gen
            .topology
            .out_neighbors(NodeId(1))
            .iter()
            .map(|e| e.dst_local.unwrap())
            .collect();
        assert_eq!(alice_out.len(), 2);
        assert!(alice_out.contains(&NodeId(2)));
        assert!(alice_out.contains(&NodeId(3)));

        let carol_out = gen.topology.out_neighbors(NodeId(3));
        assert!(carol_out.is_empty());
    }

    /// *(task IX6)* Incoming neighbors are correct.
    #[tokio::test]
    async fn topological_index_in_neighbors_are_correct() {
        let gen = build_test_generation().await;

        let carol_in: Vec<NodeId> = gen
            .topology
            .in_neighbors(NodeId(3))
            .iter()
            .map(|e| e.dst_local.unwrap())
            .collect();
        assert_eq!(carol_in.len(), 2);
        assert!(carol_in.contains(&NodeId(2)));
        assert!(carol_in.contains(&NodeId(1)));

        let alice_in = gen.topology.in_neighbors(NodeId(1));
        assert!(alice_in.is_empty());
    }

    /// *(task IX3, folded into the same fixture)* Every adjacency entry
    /// is local when the builder is mono-partition (`n_partitions: 1`) —
    /// `dst_remote` is always `None`.
    #[tokio::test]
    async fn no_adjacency_entry_is_ever_remote_when_mono_partition() {
        let gen = build_test_generation().await;
        for node in gen.topology.node_ids() {
            for entry in gen.topology.out_neighbors(*node) {
                assert!(entry.dst_remote.is_none());
            }
        }
    }

    /// *(task IX3, revised)* With `n_partitions > 1`, an edge whose
    /// destination hashes to a *different* partition than the one being
    /// built shows up as a [`RemoteRef`] instead of being dropped, and a
    /// destination hashing to the *same* partition stays local.
    #[tokio::test]
    async fn remote_edges_are_flagged_as_remote_ref_across_partitions() {
        let n_partitions = 4u32;
        // Brute-force two node ids known to land on partition 0 and on
        // some other partition respectively — the exact hash output for
        // a given id isn't something a test should hardcode, only that
        // the builder respects whatever it is.
        let mut local_id = None;
        let mut remote_id = None;
        for candidate in 1..10_000u64 {
            let p = partition_of(NodeId(candidate), n_partitions);
            if p == 0 && local_id.is_none() {
                local_id = Some(candidate);
            } else if p != 0 && remote_id.is_none() {
                remote_id = Some(candidate);
            }
            if local_id.is_some() && remote_id.is_some() {
                break;
            }
        }
        let local_id = local_id.expect("found an id hashing to partition 0");
        let remote_id = remote_id.expect("found an id hashing to a non-zero partition");

        let reader = FakeReader {
            nodes: Mutex::new(HashMap::from([(
                "Person".to_string(),
                // Both nodes present in Iceberg (as every replica scans
                // every row, §4.1) — `remote_id` still won't end up
                // local to this build, since it's filtered by ownership.
                vec![node(local_id, "Alice", 1985), node(remote_id, "Bob", 1990)],
            )])),
            edges: Mutex::new(HashMap::from([(
                "KNOWS".to_string(),
                vec![edge(100, local_id, remote_id)],
            )])),
        };

        let builder = IcebergIndexBuilder::new(reader, small_social_schema(), n_partitions);
        let gen = builder
            .build(PartitionId(0), &[Label("Person".to_string())])
            .await
            .expect("build should succeed");

        assert!(gen.topology.contains(NodeId(local_id)));
        assert!(!gen.topology.contains(NodeId(remote_id)));

        let out = gen.topology.out_neighbors(NodeId(local_id));
        assert_eq!(out.len(), 1);
        assert!(out[0].dst_local.is_none());
        let remote = out[0].dst_remote.expect("Bob should be a RemoteRef");
        assert_eq!(remote.node, NodeId(remote_id));
        assert_eq!(
            remote.partition,
            PartitionId(partition_of(NodeId(remote_id), n_partitions))
        );
        assert_ne!(remote.partition, PartitionId(0));
    }

    /// *(task IX7)* Equality lookup returns the right `NodeId`.
    #[tokio::test]
    async fn property_index_lookup_eq_is_correct() {
        let gen = build_test_generation().await;
        let ids =
            gen.properties
                .lookup_eq("Person", "name", &PropertyKey::String("Bob".to_string()));
        assert_eq!(ids, &[NodeId(2)]);

        let none =
            gen.properties
                .lookup_eq("Person", "name", &PropertyKey::String("Zed".to_string()));
        assert!(none.is_empty());
    }

    /// *(task IX7)* Range lookup returns the right `NodeId`s, in either
    /// direction.
    #[tokio::test]
    async fn property_index_lookup_range_is_correct() {
        use std::ops::Bound;

        let gen = build_test_generation().await;

        let born_after_1985: Vec<NodeId> = gen.properties.lookup_range(
            "Person",
            "birth_year",
            (Bound::Excluded(PropertyKey::Int64(1985)), Bound::Unbounded),
        );
        let mut born_after_1985 = born_after_1985;
        born_after_1985.sort();
        assert_eq!(born_after_1985, vec![NodeId(2), NodeId(3)]);

        let born_1990_or_before: Vec<NodeId> = gen.properties.lookup_range(
            "Person",
            "birth_year",
            (Bound::Unbounded, Bound::Included(PropertyKey::Int64(1990))),
        );
        let mut born_1990_or_before = born_1990_or_before;
        born_1990_or_before.sort();
        assert_eq!(born_1990_or_before, vec![NodeId(1), NodeId(2)]);
    }

    /// `node_records` retains each node's full property set, forward-
    /// keyed by id — what `GetNodeProperties` reads from.
    #[tokio::test]
    async fn node_records_hold_the_full_property_set_per_id() {
        let gen = build_test_generation().await;

        let bob = gen.node_records.get(&NodeId(2)).expect("Bob was scanned");
        assert_eq!(bob.label.0, "Person");
        assert_eq!(
            bob.properties.get("name"),
            Some(&PropertyValue::String("Bob".to_string()))
        );
        assert_eq!(
            bob.properties.get("birth_year"),
            Some(&PropertyValue::Int64(1990))
        );

        assert!(!gen.node_records.contains_key(&NodeId(999)));
    }

    /// `edge_records` retains each *locally-owned* edge's full property
    /// set, forward-keyed by id — what a `GetEdgeProperties`-style
    /// lookup reads from. "Locally-owned" here (mono-partition,
    /// `n_partitions: 1`) is every edge, so this also exercises the
    /// simple case before the cross-partition one below.
    #[tokio::test]
    async fn edge_records_hold_the_full_property_set_per_locally_owned_edge() {
        let mut alice_knows_bob = edge(100, 1, 2);
        alice_knows_bob
            .properties
            .insert("since".to_string(), PropertyValue::Int64(2020));

        let reader = FakeReader {
            nodes: Mutex::new(HashMap::from([(
                "Person".to_string(),
                vec![node(1, "Alice", 1985), node(2, "Bob", 1990)],
            )])),
            edges: Mutex::new(HashMap::from([(
                "KNOWS".to_string(),
                vec![alice_knows_bob],
            )])),
        };
        let builder = IcebergIndexBuilder::new(reader, small_social_schema(), 1);
        let gen = builder
            .build(PartitionId(0), &[Label("Person".to_string())])
            .await
            .expect("build should succeed");

        let record = gen
            .edge_records
            .get(&EdgeId(100))
            .expect("edge 100 should have a record");
        assert_eq!(record.edge_type.0, "KNOWS");
        assert_eq!(
            record.properties.get("since"),
            Some(&PropertyValue::Int64(2020))
        );

        assert!(!gen.edge_records.contains_key(&EdgeId(999)));
    }

    /// *(edge ownership)* An edge's record follows its *source* node's
    /// partition — the same criterion `build_csr` already uses for
    /// outgoing adjacency. A partition that doesn't own the edge's
    /// source never sees that edge's record at all, even though the
    /// edge itself is still correctly represented as a `RemoteRef` from
    /// the owning partition's perspective (see the `remote_edges_are_
    /// flagged_as_remote_ref_across_partitions` test above).
    #[tokio::test]
    async fn edge_records_follow_the_source_nodes_partition() {
        let n_partitions = 4u32;
        let mut local_id = None;
        let mut remote_id = None;
        for candidate in 1..10_000u64 {
            let p = partition_of(NodeId(candidate), n_partitions);
            if p == 0 && local_id.is_none() {
                local_id = Some(candidate);
            } else if p != 0 && remote_id.is_none() {
                remote_id = Some(candidate);
            }
            if local_id.is_some() && remote_id.is_some() {
                break;
            }
        }
        let local_id = local_id.expect("found an id hashing to partition 0");
        let remote_id = remote_id.expect("found an id hashing to a non-zero partition");

        // The edge's source is the *remote* node — this partition (0)
        // doesn't own it, so it must not retain the edge's record even
        // though it does scan the edge row (every replica scans every
        // row, §4.1).
        let reader = FakeReader {
            nodes: Mutex::new(HashMap::from([(
                "Person".to_string(),
                vec![node(local_id, "Alice", 1985), node(remote_id, "Bob", 1990)],
            )])),
            edges: Mutex::new(HashMap::from([(
                "KNOWS".to_string(),
                vec![edge(100, remote_id, local_id)],
            )])),
        };
        let builder = IcebergIndexBuilder::new(reader, small_social_schema(), n_partitions);
        let gen = builder
            .build(PartitionId(0), &[Label("Person".to_string())])
            .await
            .expect("build should succeed");

        assert!(!gen.edge_records.contains_key(&EdgeId(100)));
    }

    /// *(task IX8)* A query that has already `acquire()`d a generation
    /// keeps seeing it consistently after a concurrent `swap()` — no
    /// torn reads, no panic. The old generation isn't dropped until this
    /// held `Arc` goes out of scope.
    #[tokio::test]
    async fn generation_handle_swap_does_not_disturb_an_in_flight_acquire() {
        let old_generation = build_test_generation().await;
        let handle = GenerationHandle::new(old_generation);

        let held = handle.acquire();
        assert_eq!(held.meta.node_count, 3);

        let new_generation = IndexGeneration {
            meta: GenerationMeta {
                partition: PartitionId(0),
                snapshot_by_table: HashMap::new(),
                built_at_unix_ms: 0,
                node_count: 999,
                edge_count: 999,
            },
            topology: build_topology(PartitionId(0), &[], &[], 1),
            properties: build_property_index(&small_social_schema(), &[]),
            node_records: HashMap::new(),
            edge_records: HashMap::new(),
        };
        handle.swap(new_generation);

        // The handle to a query already in flight still sees the old,
        // consistent generation...
        assert_eq!(held.meta.node_count, 3);
        // ...while a fresh acquire sees the swapped-in one.
        assert_eq!(handle.acquire().meta.node_count, 999);
    }
}
