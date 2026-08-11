//! In-memory indexing subsystem (spec §5): a CSR-style topological
//! (adjacency) index plus a B-Tree property index, rebuilt in full on a
//! periodic cycle and swapped in atomically (§5.3).
//!
//! One [`IndexGeneration`] is a complete, immutable, point-in-time view of
//! a partition's slice of the graph — built once from a pinned Iceberg
//! snapshot (§4.2), served to queries, then dropped once no in-flight
//! query references it anymore. There is no in-place mutation: a rebuild
//! always produces a brand new generation.

mod builder;

pub use builder::IcebergIndexBuilder;

use arc_swap::ArcSwap;
use graph_schema::{EdgeId, EdgeType, Label, NodeId};
use graph_storage::{PropertyValue, SnapshotId};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// Which physical partition this process is hosting a replica of. Assigned
/// by graph-cluster (§6.2/§6.4); the index layer only needs to know its
/// own boundary to decide what stays local vs. becomes a remote reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PartitionId(pub u32);

/// A node that exists outside this partition. Traversal that steps onto a
/// remote reference is where scatter-gather hands off to another partition
/// (spec §7.4) — the index itself never resolves it, it just flags it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteRef {
    pub partition: PartitionId,
    pub node: NodeId,
}

/// Compact local index into this generation's node arrays — never exposed
/// outside this crate; everything downstream speaks in [`NodeId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LocalIdx(u32);

/// One adjacency entry: which edge, of which type, to which endpoint
/// (local if `dst_local` is `Some`, otherwise `dst_remote`).
#[derive(Debug, Clone)]
pub struct AdjacencyEntry {
    pub edge_id: EdgeId,
    pub edge_type: EdgeType,
    pub dst_local: Option<NodeId>,
    pub dst_remote: Option<RemoteRef>,
}

/// CSR-like (Compressed Sparse Row) adjacency structure (§5.1): an offsets
/// array indexed by local node position, and a contiguous array of
/// adjacency entries. Held separately for outgoing and incoming edges to
/// support traversal in both directions.
pub struct TopologicalIndex {
    node_ids: Vec<NodeId>,
    id_to_local: HashMap<NodeId, LocalIdx>,
    out_offsets: Vec<u32>,
    out_entries: Vec<AdjacencyEntry>,
    in_offsets: Vec<u32>,
    in_entries: Vec<AdjacencyEntry>,
}

impl TopologicalIndex {
    pub fn out_neighbors(&self, node: NodeId) -> &[AdjacencyEntry] {
        self.neighbors(node, &self.out_offsets, &self.out_entries)
    }

    pub fn in_neighbors(&self, node: NodeId) -> &[AdjacencyEntry] {
        self.neighbors(node, &self.in_offsets, &self.in_entries)
    }

    fn neighbors<'a>(
        &self,
        node: NodeId,
        offsets: &'a [u32],
        entries: &'a [AdjacencyEntry],
    ) -> &'a [AdjacencyEntry] {
        match self.id_to_local.get(&node) {
            Some(&LocalIdx(i)) => {
                let start = offsets[i as usize] as usize;
                let end = offsets[i as usize + 1] as usize;
                &entries[start..end]
            }
            None => &[],
        }
    }

    /// Every node this partition owns, in local storage order — used for
    /// full-partition scans (e.g. rebuild verification, metrics).
    pub fn node_ids(&self) -> &[NodeId] {
        &self.node_ids
    }

    pub fn contains(&self, node: NodeId) -> bool {
        self.id_to_local.contains_key(&node)
    }
}

/// A single property index: `(label_or_type, property) -> sorted values`
/// (§5.2), built only for properties explicitly marked `indexed` in the
/// schema. Backed by a `BTreeMap` to serve both equality and range
/// predicates from the DSL's `WHERE` clause.
pub struct PropertyIndex {
    by_key: HashMap<(String, String), std::collections::BTreeMap<PropertyKey, Vec<NodeId>>>,
}

/// Orderable wrapper so numeric/string/timestamp property values all sort
/// consistently in the `BTreeMap`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum PropertyKey {
    Int64(i64),
    Bool(bool),
    String(String),
    Timestamp(i64),
}

impl PropertyIndex {
    pub fn lookup_eq(&self, label_or_type: &str, property: &str, key: &PropertyKey) -> &[NodeId] {
        self.by_key
            .get(&(label_or_type.to_string(), property.to_string()))
            .and_then(|tree| tree.get(key))
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Range lookup (`WHERE friend.birth_year > 1990`, spec §7.1 example),
    /// via a plain [`std::ops::Bound`] pair rather than `graph_dsl`'s
    /// `ComparisonOp` directly — this crate doesn't depend on `graph-dsl`
    /// (see the workspace dependency graph in `TASKS.md`), so translating
    /// an operator into bounds is `graph-query`'s job, which depends on
    /// both.
    pub fn lookup_range(
        &self,
        label_or_type: &str,
        property: &str,
        bounds: (std::ops::Bound<PropertyKey>, std::ops::Bound<PropertyKey>),
    ) -> Vec<NodeId> {
        self.by_key
            .get(&(label_or_type.to_string(), property.to_string()))
            .map(|tree| {
                tree.range(bounds)
                    .flat_map(|(_, ids)| ids.iter().copied())
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// A node's full property record, retained forward (`NodeId -> record`)
/// alongside the topology/property-index structures that are built from
/// the same scan — the only reason this exists is `GetNodeProperties`
/// (spec §8.2 extension): a client that gets a `NodeId` back from a
/// traversal needs a way to ask what it actually is.
#[derive(Debug, Clone)]
pub struct NodeRecord {
    pub label: Label,
    pub properties: BTreeMap<String, PropertyValue>,
}

/// Metadata surfaced verbatim by the `GetIndexStatus` RPC (§8.2).
#[derive(Debug, Clone)]
pub struct GenerationMeta {
    pub partition: PartitionId,
    pub snapshot_by_table: HashMap<String, SnapshotId>,
    pub built_at_unix_ms: i64,
    pub node_count: u64,
    pub edge_count: u64,
}

/// One immutable, fully-built index generation for a partition (§5.3).
pub struct IndexGeneration {
    pub meta: GenerationMeta,
    pub topology: TopologicalIndex,
    pub properties: PropertyIndex,
    pub node_records: HashMap<NodeId, NodeRecord>,
}

/// Holds the currently-served generation and swaps it atomically once a
/// rebuild completes. Readers hold an `Arc` for the duration of a single
/// query; a generation is freed once its last query-held `Arc` drops —
/// in-flight queries are never interrupted by a rebuild (§5.3).
pub struct GenerationHandle {
    current: ArcSwap<IndexGeneration>,
}

impl GenerationHandle {
    pub fn new(initial: IndexGeneration) -> Self {
        Self {
            current: ArcSwap::from_pointee(initial),
        }
    }

    /// Snapshot the currently-served generation for the lifetime of one
    /// query. Cheap: bumps a reference count, no locking on the hot path.
    pub fn acquire(&self) -> Arc<IndexGeneration> {
        self.current.load_full()
    }

    /// Installs a freshly built generation, published atomically (§5.3).
    pub fn swap(&self, next: IndexGeneration) {
        self.current.store(Arc::new(next));
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RebuildError {
    #[error("storage error while rebuilding index: {0}")]
    Storage(#[from] graph_storage::StorageError),
    #[error("schema error while rebuilding index: {0}")]
    Schema(#[from] graph_schema::SchemaError),
}

/// Builds one full [`IndexGeneration`] for a partition from Iceberg
/// (§5.3): pin a snapshot per table, scan, filter to this partition's
/// owned node/edge set, construct CSR + property structures. Triggered on
/// a periodic timer by graph-partition-node; the concrete scan/filter
/// pipeline is Phase 1 implementation work.
#[async_trait::async_trait]
pub trait IndexBuilder {
    async fn build(
        &self,
        partition: PartitionId,
        labels: &[Label],
    ) -> Result<IndexGeneration, RebuildError>;
}
