//! Read-only access to the durable Apache Iceberg storage layer (spec §4).
//!
//! The graph engine never writes here (§4.3) — ingestion is an external
//! batch pipeline. This crate's only job is: given a schema and a table
//! name, resolve a consistent snapshot and stream rows from it. Everything
//! above this layer (graph-index) treats storage as an immutable, pinned
//! view (§4.2).

mod catalog;
mod iceberg_reader;
mod property_value;

pub use catalog::open_sql_catalog;
pub use iceberg_reader::{edge_table_name, node_table_name, IcebergCatalogReader};

use async_trait::async_trait;
use futures::stream::BoxStream;
use graph_schema::{EdgeId, EdgeType, Label, NodeId, Schema};
use std::collections::BTreeMap;

pub type NodeRowStream = BoxStream<'static, Result<NodeRow, StorageError>>;
pub type EdgeRowStream = BoxStream<'static, Result<EdgeRow, StorageError>>;

/// Opaque handle to a single Iceberg snapshot, pinned once per index rebuild
/// cycle (§4.2) so every table read during that cycle sees the same
/// logical point in time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SnapshotId(pub i64);

#[derive(Debug, Clone)]
pub struct NodeRow {
    pub id: NodeId,
    pub properties: BTreeMap<String, PropertyValue>,
}

#[derive(Debug, Clone)]
pub struct EdgeRow {
    pub id: EdgeId,
    pub src: NodeId,
    pub dst: NodeId,
    pub properties: BTreeMap<String, PropertyValue>,
}

/// Runtime property value — the storage-layer counterpart of
/// `graph_schema::ScalarType`.
#[derive(Debug, Clone, PartialEq)]
pub enum PropertyValue {
    Int64(i64),
    Float64(f64),
    Bool(bool),
    String(String),
    Timestamp(i64),
    Bytes(Vec<u8>),
    List(Vec<PropertyValue>),
    Null,
}

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("table not found for {0}")]
    TableNotFound(String),
    #[error("snapshot resolution failed: {0}")]
    SnapshotResolution(String),
    #[error("row does not conform to schema: {0}")]
    NonConformingRow(String),
    #[error("underlying Iceberg/object-store error: {0}")]
    Backend(String),
}

/// Read-only Iceberg access, one implementation per catalog (see
/// [`IcebergCatalogReader`]). Table layout: one table per node label /
/// edge type (§4.1) — table names are derived from the schema via
/// [`node_table_name`]/[`edge_table_name`], not passed by callers.
///
/// `scan_nodes`/`scan_edges` are `async fn`s that *return* a stream,
/// rather than plain sync iterators: resolving the table (a catalog call)
/// is itself I/O, and the object-store reads driving the returned stream
/// are inherently async — there's no synchronous way to drive either
/// without blocking a runtime thread.
#[async_trait]
pub trait IcebergReader: Send + Sync {
    /// Resolves the latest committed snapshot for a table at call time.
    /// Called once per rebuild cycle per table, then the *same*
    /// `SnapshotId` is reused for every subsequent read in that cycle
    /// (§4.2 consistency guarantee).
    async fn latest_snapshot(&self, table: &str) -> Result<SnapshotId, StorageError>;

    /// Streams every row of a node table as of `snapshot`, filtered to the
    /// given label. Callers (graph-index) further filter by partition
    /// ownership (§6.2) after scanning — physical Iceberg partition
    /// alignment with cluster partitioning is still `TBD` (spec §4.1).
    async fn scan_nodes(
        &self,
        schema: &Schema,
        label: &Label,
        snapshot: SnapshotId,
    ) -> Result<NodeRowStream, StorageError>;

    /// Streams every row of an edge table as of `snapshot`.
    async fn scan_edges(
        &self,
        schema: &Schema,
        edge_type: &EdgeType,
        snapshot: SnapshotId,
    ) -> Result<EdgeRowStream, StorageError>;
}
