//! [`IcebergReader`] (tasks ST1-ST5): the graph engine's actual Apache
//! Iceberg integration, via the official `iceberg` crate
//! (apache/iceberg-rust).
//!
//! ## ST1 — decisions recorded here
//!
//! - **Crate**: `iceberg` (v0.10, apache/iceberg-rust) — the project's own
//!   Rust implementation; its `to_arrow()` scan API returns Arrow
//!   `RecordBatch`es, integrating directly with this crate's row
//!   deserialization (task ST3, `property_value.rs`).
//! - **Dev catalog**: `iceberg::memory::MemoryCatalog` paired with a
//!   local-filesystem `FileIO`. Catalog *metadata* (which tables exist,
//!   where each one's current `metadata.json` lives) is in-process and
//!   ephemeral, but table *data* still lands on local disk since that's
//!   owned by `FileIO`, not the catalog. Zero extra infrastructure (no
//!   embedded database, no external service) for local dev/demo use — the
//!   tradeoff (catalog state lost on process restart) is acceptable
//!   there. [`IcebergCatalogReader`] is generic over any `iceberg::Catalog`
//!   implementation, so this is a construction-site choice, not a
//!   hardcoded one.
//! - **Prod catalog**: deliberately left `TBD`, same as spec §4.1 leaves
//!   physical Iceberg partitioning `TBD` — candidates are a REST catalog
//!   (`iceberg-catalog-rest`) in front of a managed service (AWS Glue,
//!   Polaris, Unity Catalog, Nessie), decided per deployment target.
//! - **Table naming** (§4.1): `nodes_<label>` / `edges_<edge_type>`,
//!   lowercased — see [`node_table_name`]/[`edge_table_name`].

use crate::property_value::read_property_value;
use crate::{
    EdgeRow, EdgeRowStream, IcebergReader, NodeRow, NodeRowStream, PropertyValue, SnapshotId,
    StorageError,
};
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use graph_schema::{EdgeDef, EdgeId, EdgeType, Label, NodeDef, NodeId, PropertyDef, Schema};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use std::collections::BTreeMap;

/// Table name for a node label (spec §4.1), e.g. `Person` -> `nodes_person`.
pub fn node_table_name(label: &Label) -> String {
    format!("nodes_{}", label.0.to_lowercase())
}

/// Table name for an edge type (spec §4.1), e.g. `WORKS_AT` -> `edges_works_at`.
pub fn edge_table_name(edge_type: &EdgeType) -> String {
    format!("edges_{}", edge_type.0.to_lowercase())
}

/// [`IcebergReader`] backed by any `iceberg::Catalog` implementation — see
/// the dev/prod catalog decision above for which one to construct it with.
pub struct IcebergCatalogReader<C> {
    catalog: C,
    namespace: NamespaceIdent,
}

impl<C: Catalog> IcebergCatalogReader<C> {
    pub fn new(catalog: C, namespace: NamespaceIdent) -> Self {
        Self { catalog, namespace }
    }

    fn table_ident(&self, table_name: &str) -> TableIdent {
        TableIdent::new(self.namespace.clone(), table_name.to_string())
    }

    async fn load_table(&self, table_name: &str) -> Result<iceberg::table::Table, StorageError> {
        self.catalog
            .load_table(&self.table_ident(table_name))
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))
    }

    async fn arrow_stream(
        &self,
        table_name: &str,
        snapshot: SnapshotId,
    ) -> Result<iceberg::scan::ArrowRecordBatchStream, StorageError> {
        let table = self.load_table(table_name).await?;
        table
            .scan()
            .snapshot_id(snapshot.0)
            .build()
            .map_err(|e| StorageError::Backend(e.to_string()))?
            .to_arrow()
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))
    }
}

#[async_trait]
impl<C: Catalog> IcebergReader for IcebergCatalogReader<C> {
    async fn latest_snapshot(&self, table: &str) -> Result<SnapshotId, StorageError> {
        let table = self.load_table(table).await?;
        table
            .metadata()
            .current_snapshot_id()
            .map(SnapshotId)
            .ok_or_else(|| {
                StorageError::SnapshotResolution(format!(
                    "table {} has no committed snapshot",
                    table.identifier()
                ))
            })
    }

    async fn scan_nodes(
        &self,
        schema: &Schema,
        label: &Label,
        snapshot: SnapshotId,
    ) -> Result<NodeRowStream, StorageError> {
        let table_name = node_table_name(label);
        let node_def = schema
            .node_def(label)
            .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?
            .clone();

        let batches = self.arrow_stream(&table_name, snapshot).await?;

        Ok(batches
            .flat_map(move |batch| stream::iter(batch_to_node_rows(batch, &node_def)))
            .boxed())
    }

    async fn scan_edges(
        &self,
        schema: &Schema,
        edge_type: &EdgeType,
        snapshot: SnapshotId,
    ) -> Result<EdgeRowStream, StorageError> {
        let table_name = edge_table_name(edge_type);
        let edge_def = schema
            .edge_def(edge_type)
            .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?
            .clone();

        let batches = self.arrow_stream(&table_name, snapshot).await?;

        Ok(batches
            .flat_map(move |batch| stream::iter(batch_to_edge_rows(batch, &edge_def)))
            .boxed())
    }
}

fn batch_to_node_rows(
    batch: iceberg::Result<RecordBatch>,
    node_def: &NodeDef,
) -> Vec<Result<NodeRow, StorageError>> {
    let batch = match batch {
        Ok(b) => b,
        Err(e) => return vec![Err(StorageError::Backend(e.to_string()))],
    };

    let Some(id_column) = batch.column_by_name("node_id") else {
        return vec![Err(StorageError::NonConformingRow(
            "missing node_id column".to_string(),
        ))];
    };

    (0..batch.num_rows())
        .map(|row| {
            let id = read_id(id_column, row)?;
            let properties = read_properties(&batch, row, &node_def.properties)?;
            Ok(NodeRow {
                id: NodeId(id),
                properties,
            })
        })
        .collect()
}

fn batch_to_edge_rows(
    batch: iceberg::Result<RecordBatch>,
    edge_def: &EdgeDef,
) -> Vec<Result<EdgeRow, StorageError>> {
    let batch = match batch {
        Ok(b) => b,
        Err(e) => return vec![Err(StorageError::Backend(e.to_string()))],
    };

    let columns = ["edge_id", "src_node_id", "dst_node_id"].map(|name| batch.column_by_name(name));
    let [Some(edge_id_col), Some(src_col), Some(dst_col)] = columns else {
        return vec![Err(StorageError::NonConformingRow(
            "missing edge_id/src_node_id/dst_node_id column".to_string(),
        ))];
    };

    (0..batch.num_rows())
        .map(|row| {
            let id = read_id(edge_id_col, row)?;
            let src = read_id(src_col, row)?;
            let dst = read_id(dst_col, row)?;
            let properties = read_properties(&batch, row, &edge_def.properties)?;
            Ok(EdgeRow {
                id: EdgeId(id),
                src: NodeId(src),
                dst: NodeId(dst),
                properties,
            })
        })
        .collect()
}

/// `node_id`/`edge_id`/`src_node_id`/`dst_node_id` are stored as signed
/// `Int64` columns (Parquet/Iceberg have no unsigned integer type) and
/// bit-cast back to the engine's `u64` ids — the same technique used in
/// reverse when these tables are written by the external ingestion
/// pipeline (spec §4.3).
fn read_id(column: &ArrayRef, row: usize) -> Result<u64, StorageError> {
    column
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| StorageError::NonConformingRow("id column is not Int64".to_string()))
        .map(|a| a.value(row) as u64)
}

fn read_properties(
    batch: &RecordBatch,
    row: usize,
    properties: &[PropertyDef],
) -> Result<BTreeMap<String, PropertyValue>, StorageError> {
    properties
        .iter()
        .map(|prop| {
            let column = batch.column_by_name(&prop.name).ok_or_else(|| {
                StorageError::NonConformingRow(format!("missing column {}", prop.name))
            })?;
            read_property_value(column, row, &prop.ty).map(|v| (prop.name.clone(), v))
        })
        .collect()
}
