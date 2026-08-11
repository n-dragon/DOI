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

/// *(task ST6)* Writes small `nodes_person`/`edges_knows` Iceberg tables
/// for real (`MemoryCatalog` + local-filesystem `FileIO`, task ST1's dev
/// setup) and checks `scan_nodes`/`scan_edges` read back exactly the rows
/// written — including a `NULL` property. This is the only place in the
/// crate that exercises actual Iceberg I/O; everything else (ST3's
/// `property_value` tests) is pure in-memory Arrow.
#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use futures::StreamExt;
    use graph_schema::SchemaParser;
    use iceberg::io::LocalFsStorageFactory;
    use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
    use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type as IcebergType};
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use iceberg::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use iceberg::writer::file_writer::ParquetWriterBuilder;
    use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
    use iceberg::{CatalogBuilder, TableCreation};
    use parquet::file::properties::WriterProperties;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    /// A fresh `MemoryCatalog` + local-filesystem warehouse. Returns the
    /// `TempDir` alongside it — dropping it deletes the warehouse, so it
    /// must outlive every table operation in the test.
    async fn test_catalog() -> (impl Catalog, NamespaceIdent, TempDir) {
        let warehouse = tempfile::tempdir().expect("tempdir");
        let namespace = NamespaceIdent::new("test_ns".to_string());

        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    format!("file://{}", warehouse.path().display()),
                )]),
            )
            .await
            .expect("catalog should load");

        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("namespace should be created");

        (catalog, namespace, warehouse)
    }

    /// Creates `table_name` with `iceberg_schema`, writes `batch` into it
    /// as a single Parquet data file, and commits via a fast-append
    /// transaction. Returns the post-commit `Table`.
    async fn create_and_write_table(
        catalog: &impl Catalog,
        namespace: &NamespaceIdent,
        table_name: &str,
        iceberg_schema: IcebergSchema,
        batch: RecordBatch,
    ) -> iceberg::table::Table {
        let table = catalog
            .create_table(
                namespace,
                TableCreation::builder()
                    .name(table_name.to_string())
                    .schema(iceberg_schema)
                    .build(),
            )
            .await
            .expect("table should be created");

        let location_generator = DefaultLocationGenerator::new(table.metadata())
            .expect("location generator should build");
        let file_name_generator = DefaultFileNameGenerator::new(
            "test".to_string(),
            None,
            iceberg::spec::DataFileFormat::Parquet,
        );
        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::default(),
            table.metadata().current_schema().clone(),
        );
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            table.file_io().clone(),
            location_generator,
            file_name_generator,
        );
        let mut writer = DataFileWriterBuilder::new(rolling_writer_builder)
            .build(None)
            .await
            .expect("data file writer should build");
        writer.write(batch).await.expect("write should succeed");
        let data_files = writer.close().await.expect("close should succeed");

        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .add_data_files(data_files)
            .apply(tx)
            .expect("fast-append action should apply");
        let table = tx.commit(catalog).await.expect("commit should succeed");
        assert!(
            table.metadata().current_snapshot_id().is_some(),
            "commit should have produced a snapshot"
        );
        table
    }

    fn person_iceberg_schema() -> IcebergSchema {
        IcebergSchema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "node_id", IcebergType::Primitive(PrimitiveType::Long))
                    .into(),
                NestedField::required(2, "name", IcebergType::Primitive(PrimitiveType::String))
                    .into(),
                NestedField::optional(3, "age", IcebergType::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("a schema with three well-formed fields should build")
    }

    fn person_graph_schema() -> Schema {
        graph_schema::PestSchemaParser
            .parse(
                r#"
                schema graph_v1 {
                  node Person {
                    id: NodeId
                    name: String
                    age: Int64?
                  }
                  edge KNOWS {
                    from: Person
                    to: Person
                    since: Int64?
                  }
                }
                "#,
            )
            .expect("valid IDL")
    }

    #[tokio::test]
    async fn scan_nodes_reads_back_a_written_table() {
        let (catalog, namespace, _warehouse) = test_catalog().await;

        let arrow_schema = iceberg::arrow::schema_to_arrow_schema(&person_iceberg_schema())
            .expect("iceberg schema should convert to an arrow schema");
        // Alice has an age; Bob's is NULL, to exercise the Null branch of
        // ST3's read_property_value too.
        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
                Arc::new(Int64Array::from(vec![Some(30), None])),
            ],
        )
        .expect("batch should match the table's arrow schema");

        let table_name = node_table_name(&Label("Person".to_string()));
        create_and_write_table(
            &catalog,
            &namespace,
            &table_name,
            person_iceberg_schema(),
            batch,
        )
        .await;

        let reader = IcebergCatalogReader::new(catalog, namespace);
        let snapshot = reader
            .latest_snapshot(&table_name)
            .await
            .expect("latest_snapshot should resolve the committed snapshot");

        let graph_schema = person_graph_schema();
        let rows: Vec<NodeRow> = reader
            .scan_nodes(&graph_schema, &Label("Person".to_string()), snapshot)
            .await
            .expect("scan_nodes should succeed")
            .map(|r| r.expect("every row should deserialize"))
            .collect()
            .await;

        assert_eq!(rows.len(), 2);

        let alice = rows
            .iter()
            .find(|r| r.id == NodeId(1))
            .expect("Alice's row");
        assert!(matches!(
            alice.properties.get("name"),
            Some(PropertyValue::String(s)) if s == "Alice"
        ));
        assert!(matches!(
            alice.properties.get("age"),
            Some(PropertyValue::Int64(30))
        ));

        let bob = rows.iter().find(|r| r.id == NodeId(2)).expect("Bob's row");
        assert!(matches!(
            bob.properties.get("age"),
            Some(PropertyValue::Null)
        ));
    }

    #[tokio::test]
    async fn scan_edges_reads_back_a_written_table() {
        let (catalog, namespace, _warehouse) = test_catalog().await;

        let knows_iceberg_schema = IcebergSchema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "edge_id", IcebergType::Primitive(PrimitiveType::Long))
                    .into(),
                NestedField::required(
                    2,
                    "src_node_id",
                    IcebergType::Primitive(PrimitiveType::Long),
                )
                .into(),
                NestedField::required(
                    3,
                    "dst_node_id",
                    IcebergType::Primitive(PrimitiveType::Long),
                )
                .into(),
                NestedField::optional(4, "since", IcebergType::Primitive(PrimitiveType::Long))
                    .into(),
            ])
            .build()
            .expect("a schema with four well-formed fields should build");

        let arrow_schema = iceberg::arrow::schema_to_arrow_schema(&knows_iceberg_schema)
            .expect("iceberg schema should convert to an arrow schema");
        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema),
            vec![
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(Int64Array::from(vec![Some(2020)])),
            ],
        )
        .expect("batch should match the table's arrow schema");

        let table_name = edge_table_name(&EdgeType("KNOWS".to_string()));
        create_and_write_table(
            &catalog,
            &namespace,
            &table_name,
            knows_iceberg_schema,
            batch,
        )
        .await;

        let reader = IcebergCatalogReader::new(catalog, namespace);
        let snapshot = reader
            .latest_snapshot(&table_name)
            .await
            .expect("latest_snapshot should resolve the committed snapshot");

        let graph_schema = person_graph_schema();
        let rows: Vec<EdgeRow> = reader
            .scan_edges(&graph_schema, &EdgeType("KNOWS".to_string()), snapshot)
            .await
            .expect("scan_edges should succeed")
            .map(|r| r.expect("every row should deserialize"))
            .collect()
            .await;

        assert_eq!(rows.len(), 1);
        let edge = &rows[0];
        assert_eq!(edge.id, EdgeId(100));
        assert_eq!(edge.src, NodeId(1));
        assert_eq!(edge.dst, NodeId(2));
        assert!(matches!(
            edge.properties.get("since"),
            Some(PropertyValue::Int64(2020))
        ));
    }
}
