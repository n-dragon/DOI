//! Standalone ingestion tool — the "external batch pipeline" role spec
//! §4.3 assigns to something other than the graph engine itself, now
//! genuinely runnable as its own process rather than folded into
//! `examples/demo`'s single process. Ingests the same Cloud Cost
//! Management dataset `examples/demo` uses, but into the *persistent*
//! SQLite-backed catalog (`graph_storage::open_sql_catalog`) so a
//! separately-started `graph-partition-node` can actually see it -
//! `examples/demo`'s in-memory catalog can't cross that process
//! boundary (see `graph-storage`'s `catalog` module doc comment).
//!
//! Run with (from `graph-engine/`):
//!
//!   cargo run -p ingest-cloud-cost
//!   cargo run -p graph-partition-node   # separate process, same warehouse
//!
//! Both read `GRAPH_CATALOG_DB_PATH`/`GRAPH_WAREHOUSE_PATH`/
//! `GRAPH_NAMESPACE` env vars with matching defaults, so pointing them at
//! the same catalog needs no extra configuration in the common case.

use arrow_array::{BooleanArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use graph_schema::{EdgeType, Label};
use graph_storage::{edge_table_name, node_table_name, open_sql_catalog};
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type as IcebergType};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, NamespaceIdent, TableCreation};
use parquet::file::properties::WriterProperties;
use std::path::PathBuf;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let catalog_db_path: PathBuf = std::env::var("GRAPH_CATALOG_DB_PATH")
        .unwrap_or_else(|_| "./catalog.sqlite".to_string())
        .into();
    let warehouse_path: PathBuf = std::env::var("GRAPH_WAREHOUSE_PATH")
        .unwrap_or_else(|_| "./warehouse".to_string())
        .into();
    let namespace = std::env::var("GRAPH_NAMESPACE").unwrap_or_else(|_| "graph".to_string());

    // Idempotent re-runs: start from a clean catalog/warehouse each time
    // rather than fail on "table already exists".
    let _ = std::fs::remove_file(&catalog_db_path);
    let _ = std::fs::remove_dir_all(&warehouse_path);
    std::fs::create_dir_all(&warehouse_path)?;

    println!(
        "Ingesting into catalog={} warehouse={} namespace={namespace}",
        catalog_db_path.display(),
        warehouse_path.display()
    );
    let catalog = open_sql_catalog(&catalog_db_path, &warehouse_path, &namespace).await?;
    let namespace = NamespaceIdent::new(namespace);

    let account_schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "node_id", IcebergType::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(
                2,
                "account_id",
                IcebergType::Primitive(PrimitiveType::String),
            )
            .into(),
            NestedField::required(3, "provider", IcebergType::Primitive(PrimitiveType::String))
                .into(),
        ])
        .build()?;
    let account_batch = RecordBatch::try_new(
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&account_schema)?),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["123456789012"])),
            Arc::new(StringArray::from(vec!["aws"])),
        ],
    )?;
    write_table(
        &catalog,
        &namespace,
        &node_table_name(&Label("CloudAccount".to_string())),
        account_schema,
        account_batch,
    )
    .await?;

    // `internet_facing` / `max_severity` are the security-posture columns
    // (CVSS ×10, Int64 — the property index has no Float64 key). They
    // model a separate scanner feed landing on the *same* Resource rows
    // the billing feed describes, which is the whole point of the shared
    // node label: the attack-path tab and the cost tab traverse one
    // inventory, not two copies of it.
    //
    // The three ec2/rds rows below are deliberately near-identical on
    // paper so the attack-path query has real decoys to reject:
    //   - i-checkout-api-1: exposed + critical CVE + reaches PII  -> the finding
    //   - i-checkout-api-2: same CVE, same role, NOT exposed      -> rejected by WHERE
    //   - rds-orders-db:    exposed, but its role reaches nothing sensitive
    //                                                             -> never reached by the pattern
    let mut resources: Vec<(i64, String, &str, &str, bool, i64)> = vec![
        (2, "vpc-prod".to_string(), "vpc", "us-east-1", false, 0),
        (
            3,
            "subnet-prod-a".to_string(),
            "subnet",
            "us-east-1",
            false,
            0,
        ),
        (
            4,
            "i-checkout-api-1".to_string(),
            "ec2_instance",
            "us-east-1",
            true,
            94,
        ),
        (
            5,
            "i-checkout-api-2".to_string(),
            "ec2_instance",
            "us-east-1",
            false,
            94,
        ),
        (
            6,
            "vol-checkout-api-1".to_string(),
            "ebs_volume",
            "us-east-1",
            false,
            0,
        ),
        (
            7,
            "snap-checkout-api-1".to_string(),
            "ebs_snapshot",
            "us-east-1",
            false,
            0,
        ),
        (
            8,
            "rds-orders-db".to_string(),
            "rds_instance",
            "us-east-1",
            true,
            21,
        ),
    ];

    // Scale the fleet to ~1000 nodes total so the "unused permissions" tab
    // has enough workloads to be worth rendering as a graph rather than a
    // hand-drawn 4-box diagram (viewer/force-graph-widget-src/entry.js).
    // `ROLE_GROUPS` is also where `can_read`/`accessed` below source their
    // per-role workload counts and IDs from — one source of truth, so the
    // three tables can't drift out of sync with each other.
    //
    // Four of the ten roles can read a PII store; the other six can't
    // reach anything sensitive at all, same shape as the original
    // fixture's role-deploy/role-rds-monitor decoys, just replicated
    // across many more workloads instead of two.
    let role_groups: [(i64, &str, usize, bool); 10] = [
        (201, "i-checkout-api", 393, true), // continues from the 2 hand-crafted -1/-2
        (203, "i-data-admin", 200, true),
        (205, "i-analytics", 150, true),
        (206, "i-batch-etl", 150, true),
        (202, "i-deploy", 12, false),
        (204, "i-rds-monitor", 12, false),
        (207, "i-web-frontend", 12, false),
        (208, "i-support-tools", 13, false),
        (209, "i-ci-runner", 12, false),
        (210, "i-sandbox", 12, false),
    ];

    // The one PII store each PII-capable role's own `CAN_READ` grant
    // targets (matches the `can_read` table below) — kept next to
    // `role_groups` so a role can't end up generating an `ACCESSED` edge
    // toward a store it was never granted `CAN_READ` on in the first
    // place.
    let pii_store_for_role = |role_id: i64| -> Option<i64> {
        match role_id {
            201 => Some(301),
            203 => Some(301),
            205 => Some(303),
            206 => Some(304),
            _ => None,
        }
    };

    let mut next_resource_id: i64 = 10_000;
    // (resource_id, role_id) for every generated workload — consumed
    // below by RUNS_AS.
    let mut generated_workloads: Vec<(i64, i64)> = Vec::new();
    // (resource_id, store_id) for the generated workloads that DID use
    // their declared access — consumed below by ACCESSED. Roughly 4 in 5
    // of each PII-capable group (`i % 5 != 0`); the rest are exactly the
    // "declared but never used" gap the anti-join surfaces, just at fleet
    // scale instead of the original fixture's single example.
    let mut generated_accessed: Vec<(i64, i64)> = Vec::new();
    for &(role_id, prefix, count, pii_capable) in &role_groups {
        // i-checkout-api-3.. picks up right after the 2 hand-crafted rows;
        // every other prefix is entirely new, so it starts at -1.
        let start = if prefix == "i-checkout-api" { 3 } else { 1 };
        for i in 0..count {
            let id = next_resource_id;
            next_resource_id += 1;
            resources.push((
                id,
                format!("{prefix}-{}", start + i),
                "ec2_instance",
                "us-east-1",
                false,
                0,
            ));
            generated_workloads.push((id, role_id));
            if pii_capable && i % 5 != 0 {
                if let Some(store_id) = pii_store_for_role(role_id) {
                    generated_accessed.push((id, store_id));
                }
            }
        }
    }

    let resource_schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "node_id", IcebergType::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "name", IcebergType::Primitive(PrimitiveType::String)).into(),
            NestedField::required(
                3,
                "resource_type",
                IcebergType::Primitive(PrimitiveType::String),
            )
            .into(),
            NestedField::required(4, "region", IcebergType::Primitive(PrimitiveType::String))
                .into(),
            NestedField::required(
                5,
                "internet_facing",
                IcebergType::Primitive(PrimitiveType::Boolean),
            )
            .into(),
            NestedField::required(
                6,
                "max_severity",
                IcebergType::Primitive(PrimitiveType::Long),
            )
            .into(),
        ])
        .build()?;
    let resource_batch = RecordBatch::try_new(
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&resource_schema)?),
        vec![
            Arc::new(Int64Array::from(
                resources.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                resources.iter().map(|r| r.1.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                resources.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                resources.iter().map(|r| r.3).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                resources.iter().map(|r| r.4).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                resources.iter().map(|r| r.5).collect::<Vec<_>>(),
            )),
        ],
    )?;
    write_table(
        &catalog,
        &namespace,
        &node_table_name(&Label("Resource".to_string())),
        resource_schema,
        resource_batch,
    )
    .await?;

    let costs: [(i64, i64, &str); 6] = [
        (101, 5, "2024-06"),
        (102, 245, "2024-06"),
        (103, 40, "2024-06"),
        (104, 12, "2024-06"),
        (105, 500, "2024-06"),
        (106, 340, "2024-06"),
    ];
    let cost_schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "node_id", IcebergType::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "amount_usd", IcebergType::Primitive(PrimitiveType::Long))
                .into(),
            NestedField::required(3, "period", IcebergType::Primitive(PrimitiveType::String))
                .into(),
        ])
        .build()?;
    let cost_batch = RecordBatch::try_new(
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&cost_schema)?),
        vec![
            Arc::new(Int64Array::from(
                costs.iter().map(|c| c.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                costs.iter().map(|c| c.1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                costs.iter().map(|c| c.2).collect::<Vec<_>>(),
            )),
        ],
    )?;
    write_table(
        &catalog,
        &namespace,
        &node_table_name(&Label("CostRecord".to_string())),
        cost_schema,
        cost_batch,
    )
    .await?;

    // --- Security posture: the IAM side of the same inventory ---------
    //
    // Node ids are namespaced by decade (2xx roles, 3xx stores) purely
    // so the demo dataset stays readable; nothing in the engine cares.
    // 4 of these (matched against `role_groups` above by id) can read a
    // PII store; the other 6 can't reach anything sensitive at all.
    let roles: [(i64, &str, bool); 10] = [
        (201, "role-checkout-api", false),
        (202, "role-deploy", false),
        (203, "role-data-admin", true),
        (204, "role-rds-monitor", false),
        (205, "role-analytics", false),
        (206, "role-batch-etl", false),
        (207, "role-web-frontend", false),
        (208, "role-support-tools", false),
        (209, "role-ci-runner", false),
        (210, "role-sandbox", false),
    ];
    let role_schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "node_id", IcebergType::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(
                2,
                "role_name",
                IcebergType::Primitive(PrimitiveType::String),
            )
            .into(),
            NestedField::required(
                3,
                "is_admin",
                IcebergType::Primitive(PrimitiveType::Boolean),
            )
            .into(),
        ])
        .build()?;
    let role_batch = RecordBatch::try_new(
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&role_schema)?),
        vec![
            Arc::new(Int64Array::from(
                roles.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                roles.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                roles.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )?;
    write_table(
        &catalog,
        &namespace,
        &node_table_name(&Label("IAMRole".to_string())),
        role_schema,
        role_batch,
    )
    .await?;

    // 3 pii, 7 internal — a handful of crown jewels among many mundane
    // buckets, same imbalance a real inventory has.
    let stores: [(i64, &str, &str); 10] = [
        (301, "s3-customer-exports", "pii"),
        (302, "s3-ops-logs", "internal"),
        (303, "s3-billing-records", "pii"),
        (304, "s3-support-tickets", "pii"),
        (305, "s3-build-artifacts", "internal"),
        (306, "s3-terraform-state", "internal"),
        (307, "s3-ci-cache", "internal"),
        (308, "s3-app-logs", "internal"),
        (309, "s3-metrics-export", "internal"),
        (310, "s3-backup-staging", "internal"),
    ];
    let store_schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "node_id", IcebergType::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "name", IcebergType::Primitive(PrimitiveType::String)).into(),
            NestedField::required(
                3,
                "classification",
                IcebergType::Primitive(PrimitiveType::String),
            )
            .into(),
        ])
        .build()?;
    let store_batch = RecordBatch::try_new(
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&store_schema)?),
        vec![
            Arc::new(Int64Array::from(
                stores.iter().map(|s| s.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                stores.iter().map(|s| s.1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                stores.iter().map(|s| s.2).collect::<Vec<_>>(),
            )),
        ],
    )?;
    write_table(
        &catalog,
        &namespace,
        &node_table_name(&Label("DataStore".to_string())),
        store_schema,
        store_batch,
    )
    .await?;

    write_table(
        &catalog,
        &namespace,
        &edge_table_name(&EdgeType("OWNS".to_string())),
        edge_schema()?,
        edge_batch(&[(1000, 1, 2)])?,
    )
    .await?;

    write_table(
        &catalog,
        &namespace,
        &edge_table_name(&EdgeType("CONTAINS".to_string())),
        edge_schema()?,
        edge_batch(&[
            (1001, 2, 3),
            (1002, 2, 8),
            (1003, 3, 4),
            (1004, 3, 5),
            (1005, 4, 6),
            (1006, 6, 7),
        ])?,
    )
    .await?;

    write_table(
        &catalog,
        &namespace,
        &edge_table_name(&EdgeType("HAS_COST".to_string())),
        edge_schema()?,
        edge_batch(&[
            (1007, 3, 101),
            (1008, 4, 102),
            (1009, 5, 103),
            (1010, 6, 104),
            (1011, 7, 105),
            (1012, 8, 106),
        ])?,
    )
    .await?;

    // Both checkout instances run as the *same* role — so exposure, not
    // identity, is what separates them in the attack-path query. Every
    // generated workload (`generated_workloads`) gets one more, at a
    // disjoint edge-id range so it can never collide with the hand-crafted
    // ids above.
    let mut runs_as: Vec<(i64, i64, i64)> = vec![(2000, 4, 201), (2001, 5, 201), (2002, 8, 204)];
    let mut next_edge_id: i64 = 100_000;
    for &(resource_id, role_id) in &generated_workloads {
        runs_as.push((next_edge_id, resource_id, role_id));
        next_edge_id += 1;
    }
    write_table(
        &catalog,
        &namespace,
        &edge_table_name(&EdgeType("RUNS_AS".to_string())),
        edge_schema()?,
        edge_batch(&runs_as)?,
    )
    .await?;

    // The pivot: checkout-api -> deploy -> data-admin. Two hops, neither
    // of which is suspicious on its own.
    write_table(
        &catalog,
        &namespace,
        &edge_table_name(&EdgeType("CAN_ASSUME".to_string())),
        edge_schema()?,
        edge_batch(&[(2003, 201, 202), (2004, 202, 203)])?,
    )
    .await?;

    // role-checkout-api reads ops-logs directly — a CAN_READ edge that
    // leads nowhere sensitive, so "has a CAN_READ edge" is not by itself
    // the finding. #2008 (role-checkout-api -> the PII store) is the
    // grant the "unused permissions" tab's query cross-references
    // against `ACCESSED` below — `action` carries the concrete IAM
    // action so the anti-join has something to match on.
    let can_read_schema = edge_schema_with(vec![NestedField::required(
        4,
        "action",
        IcebergType::Primitive(PrimitiveType::String),
    )
    .into()])?;
    // Every PII-capable role (see `role_groups` above) gets exactly one
    // grant onto a PII store — that store is also where its workloads'
    // `ACCESSED` edges land below, so the anti-join has a single
    // `g.action`/`a.action` pair to match per role rather than an
    // ambiguous choice of stores.
    let can_read: [(i64, i64, i64, &str); 11] = [
        (2005, 203, 301, "s3:GetObject"),
        (2006, 204, 302, "s3:GetObject"),
        (2007, 201, 302, "s3:GetObject"),
        (2008, 201, 301, "s3:GetObject"),
        (2010, 205, 303, "s3:GetObject"),
        (2011, 206, 304, "s3:GetObject"),
        (2012, 202, 305, "s3:GetObject"),
        (2013, 207, 306, "s3:GetObject"),
        (2014, 208, 307, "s3:GetObject"),
        (2015, 209, 308, "s3:GetObject"),
        (2016, 210, 309, "s3:GetObject"),
    ];
    let can_read_batch = RecordBatch::try_new(
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&can_read_schema)?),
        vec![
            Arc::new(Int64Array::from(
                can_read.iter().map(|e| e.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                can_read.iter().map(|e| e.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                can_read.iter().map(|e| e.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                can_read.iter().map(|e| e.3).collect::<Vec<_>>(),
            )),
        ],
    )?;
    write_table(
        &catalog,
        &namespace,
        &edge_table_name(&EdgeType("CAN_READ".to_string())),
        can_read_schema,
        can_read_batch,
    )
    .await?;

    // Observed access: i-checkout-api-1 (4) actually reads the PII store
    // #2008 declares it can; i-checkout-api-2 (5) runs as the same role,
    // holds the same declared grant transitively, and never does — that
    // gap is exactly what the "unused permissions" tab's `NOT EXISTS`
    // query surfaces. `generated_accessed` extends the same pattern
    // across the generated fleet. Timestamp: 2024-07-01 throughout,
    // inside the query's default `>= 2024-05-01` trust window.
    const ACCESSED_LAST_SEEN: i64 = 1_719_792_000_000_000;
    let mut accessed_edge_ids: Vec<i64> = vec![2009];
    let mut accessed_src: Vec<i64> = vec![4];
    let mut accessed_dst: Vec<i64> = vec![301];
    for &(resource_id, store_id) in &generated_accessed {
        accessed_edge_ids.push(next_edge_id);
        next_edge_id += 1;
        accessed_src.push(resource_id);
        accessed_dst.push(store_id);
    }
    let accessed_count = accessed_edge_ids.len();

    let accessed_schema = edge_schema_with(vec![
        NestedField::required(4, "action", IcebergType::Primitive(PrimitiveType::String)).into(),
        NestedField::required(
            5,
            "last_seen",
            IcebergType::Primitive(PrimitiveType::Timestamp),
        )
        .into(),
    ])?;
    let accessed_batch = RecordBatch::try_new(
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&accessed_schema)?),
        vec![
            Arc::new(Int64Array::from(accessed_edge_ids)),
            Arc::new(Int64Array::from(accessed_src)),
            Arc::new(Int64Array::from(accessed_dst)),
            Arc::new(StringArray::from(vec!["s3:GetObject"; accessed_count])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                ACCESSED_LAST_SEEN;
                accessed_count
            ])),
        ],
    )?;
    write_table(
        &catalog,
        &namespace,
        &edge_table_name(&EdgeType("ACCESSED".to_string())),
        accessed_schema,
        accessed_batch,
    )
    .await?;

    println!("Done: 5 node tables, 8 edge tables committed.");
    Ok(())
}

fn edge_schema() -> iceberg::Result<IcebergSchema> {
    IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "edge_id", IcebergType::Primitive(PrimitiveType::Long)).into(),
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
        ])
        .build()
}

/// `edge_schema()` plus extra property columns, for edge tables that
/// carry more than bare `(edge_id, src, dst)` — `CAN_READ.action` and
/// `ACCESSED.{action,last_seen}` below.
fn edge_schema_with(extra_fields: Vec<Arc<NestedField>>) -> Result<IcebergSchema, iceberg::Error> {
    let mut fields = vec![
        NestedField::required(1, "edge_id", IcebergType::Primitive(PrimitiveType::Long)).into(),
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
    ];
    fields.extend(extra_fields);
    IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(fields)
        .build()
}

fn edge_batch(edges: &[(i64, i64, i64)]) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    Ok(RecordBatch::try_new(
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&edge_schema()?)?),
        vec![
            Arc::new(Int64Array::from(
                edges.iter().map(|e| e.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                edges.iter().map(|e| e.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                edges.iter().map(|e| e.2).collect::<Vec<_>>(),
            )),
        ],
    )?)
}

async fn write_table(
    catalog: &impl Catalog,
    namespace: &NamespaceIdent,
    table_name: &str,
    schema: IcebergSchema,
    batch: RecordBatch,
) -> Result<(), Box<dyn std::error::Error>> {
    let table = catalog
        .create_table(
            namespace,
            TableCreation::builder()
                .name(table_name.to_string())
                .schema(schema)
                .build(),
        )
        .await?;

    let location_generator = DefaultLocationGenerator::new(table.metadata())?;
    let file_name_generator = DefaultFileNameGenerator::new(
        "ingest".to_string(),
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
        .await?;
    writer.write(batch).await?;
    let data_files = writer.close().await?;

    let tx = Transaction::new(&table);
    let tx = tx.fast_append().add_data_files(data_files).apply(tx)?;
    tx.commit(catalog).await?;

    println!("  wrote table {table_name}");
    Ok(())
}
