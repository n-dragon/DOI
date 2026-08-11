//! Persistent Iceberg catalog construction (spec §4, ST1's dev-catalog
//! decision made concrete). `MemoryCatalog` (still used by
//! `examples/demo`, `graph-storage`'s own ST6 tests, and CO4's
//! integration test) only works within a single process: its table
//! registry lives in memory, so a `graph-partition-node` started as a
//! separate process from whatever ingested the data can never see it -
//! confirmed empirically, `NamespaceNotFound` despite the Parquet files
//! existing on disk. `iceberg-catalog-sql` backed by a SQLite file fixes
//! that: the registry is a small file on disk both processes point at,
//! while table *data* keeps living wherever `warehouse_path` says
//! (local disk here too, but any `FileIO`-supported backend works) -
//! still "zero infra" (no server to run), just persistent instead of
//! ephemeral.

use iceberg::io::LocalFsStorageFactory;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent};
use iceberg_catalog_sql::{SqlBindStyle, SqlCatalog, SqlCatalogBuilder};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::StorageError;

/// Opens (creating on first use) a SQLite-backed Iceberg catalog rooted
/// at `warehouse_path` on the local filesystem, and ensures `namespace`
/// exists in it. Two processes pointed at the same `sqlite_path` +
/// `warehouse_path` share the same tables - that's the whole point.
pub async fn open_sql_catalog(
    sqlite_path: &Path,
    warehouse_path: &Path,
    namespace: &str,
) -> Result<SqlCatalog, StorageError> {
    let abs_warehouse = std::fs::canonicalize(warehouse_path)
        .map_err(|e| StorageError::Backend(format!("Failed to canonicalize warehouse path: {e}")))?;
    let catalog = SqlCatalogBuilder::default()
        .uri(format!("sqlite://{}?mode=rwc", sqlite_path.display()))
        .warehouse_location(format!("file://{}", abs_warehouse.display()))
        .sql_bind_style(SqlBindStyle::QMark) // SQLite dialect, not Postgres
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("graph-engine", HashMap::new())
        .await
        .map_err(|e| StorageError::Backend(e.to_string()))?;

    let namespace = NamespaceIdent::new(namespace.to_string());
    if !catalog
        .namespace_exists(&namespace)
        .await
        .map_err(|e| StorageError::Backend(e.to_string()))?
    {
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
    }

    Ok(catalog)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type as IcebergType};
    use iceberg::TableCreation;

    /// The regression this crate exists to prevent: two catalog handles
    /// opened separately (standing in for two OS processes) against the
    /// same SQLite file must see the same tables. `MemoryCatalog` fails
    /// this by construction - its registry never leaves the process that
    /// created it.
    #[tokio::test]
    async fn a_table_created_through_one_handle_is_visible_through_another() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sqlite_path = dir.path().join("catalog.sqlite");
        let warehouse_path = dir.path().join("warehouse");

        let writer = open_sql_catalog(&sqlite_path, &warehouse_path, "ns")
            .await
            .expect("first handle should open");

        let schema = IcebergSchema::builder()
            .with_schema_id(0)
            .with_fields(vec![NestedField::required(
                1,
                "node_id",
                IcebergType::Primitive(PrimitiveType::Long),
            )
            .into()])
            .build()
            .expect("schema should build");
        writer
            .create_table(
                &iceberg::NamespaceIdent::new("ns".to_string()),
                TableCreation::builder()
                    .name("nodes_person".to_string())
                    .schema(schema)
                    .build(),
            )
            .await
            .expect("table should be created");

        // A brand-new handle, as a second process would open - not the
        // same `writer` instance.
        let reader = open_sql_catalog(&sqlite_path, &warehouse_path, "ns")
            .await
            .expect("second handle should open");

        let exists = reader
            .table_exists(&iceberg::TableIdent::new(
                iceberg::NamespaceIdent::new("ns".to_string()),
                "nodes_person".to_string(),
            ))
            .await
            .expect("table_exists should succeed");
        assert!(
            exists,
            "a second catalog handle over the same SQLite file must see \
             the first handle's tables"
        );
    }
}
