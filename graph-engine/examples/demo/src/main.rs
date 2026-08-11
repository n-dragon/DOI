//! End-to-end smoke test for the graph engine, modeled on Datadog's own
//! Cloud Cost Management product: a cloud resource inventory graph
//! (account -> VPC -> subnet -> instances -> volumes) joined to billing
//! data. Cost data and cloud inventory data are two genuinely separate
//! sources in real CCM systems (a billing export vs. a resource
//! discovery scan) — modeling the join as a `HAS_COST` edge rather than
//! flattening cost onto the resource itself is what a metadata graph
//! buys you here: the join is materialized and traversable, not
//! recomputed per query the way a SQL join over two tables would be.
//!
//! Run with:
//!
//!   cd graph-engine && cargo run -p graph-engine-demo
//!
//! ## Why this binary writes Iceberg tables directly
//!
//! The engine itself never writes to Iceberg (spec §4.3) — ingestion is
//! an external batch pipeline, and `graph-storage::IcebergReader` is
//! read-only by design. This binary plays that external pipeline's role
//! for demo purposes, using the `iceberg` crate directly to create
//! tables and commit data files — exactly what a real ingestion job
//! would do (one job scanning the cloud provider's API, another parsing
//! the billing export), just inline here instead of as separate
//! processes.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use graph_dsl::{Parser as DslParser, PestParser, SchemaValidator, Validator};
use graph_index::{GenerationHandle, IcebergIndexBuilder, IndexBuilder, PartitionId};
use graph_query::{LocalExecutor, NaivePlanner, Planner, SimpleLocalExecutor};
use graph_schema::{EdgeType, Label, PestSchemaParser, SchemaParser};
use graph_storage::{edge_table_name, node_table_name, IcebergCatalogReader};
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
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::sync::Arc;

// Kept identical to `schema/cloud_cost.graphidl` — that file is what
// `graph-partition-node`/`graph-coordinator` load via GRAPH_SCHEMA_PATH
// for the multi-process deployment; this constant is this binary's own
// in-process copy so `graph-engine-demo` has no file-path dependency.
const SCHEMA_IDL: &str = r#"
schema graph_v1 {
  node CloudAccount {
    id: NodeId
    @indexed account_id: String
    provider: String
  }

  node Resource {
    id: NodeId
    @indexed name: String
    resource_type: String
    region: String
  }

  node CostRecord {
    id: NodeId
    @indexed amount_usd: Int64
    period: String
  }

  edge OWNS {
    from: CloudAccount
    to: Resource
  }

  edge CONTAINS {
    from: Resource
    to: Resource
  }

  edge HAS_COST {
    from: Resource
    to: CostRecord
  }
}
"#;

/// Joins the resource hierarchy to billing data in one traversal: walk
/// 1..3 hops down from the account's top-level resource, then follow
/// each one's cost record and keep the expensive ones. Both constraints
/// do real work against the dataset below — a resource can be deep
/// enough to be excluded by the hop range even if it's expensive
/// (`snap-checkout-api-1`), or cheap enough to be excluded by the cost
/// filter even if it's close by (`i-checkout-api-2`).
const DEMO_QUERY: &str = r#"
MATCH (a:CloudAccount {account_id: "123456789012"})-[:OWNS]->(vpc:Resource)-[:CONTAINS*1..3]->(r:Resource)-[:HAS_COST]->(c:CostRecord)
WHERE c.amount_usd > 100
RETURN r
"#;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = PestSchemaParser.parse(SCHEMA_IDL)?;
    println!(
        "1. Parsed schema '{}' ({} node label(s), {} edge type(s))",
        schema.version,
        schema.nodes.len(),
        schema.edges.len()
    );

    let warehouse_dir = std::env::current_dir()?.join("demo-warehouse");
    std::fs::create_dir_all(&warehouse_dir)?;
    println!(
        "2. Ingesting a small cloud cost dataset into a local Iceberg warehouse at {}",
        warehouse_dir.display()
    );
    let (catalog, namespace, resource_names) = ingest(&warehouse_dir).await?;

    println!("3. Building the in-memory index from Iceberg (IX1/IX2/IX4)...");
    let reader = IcebergCatalogReader::new(catalog, namespace);
    let builder = IcebergIndexBuilder::new(reader, schema.clone());
    let generation = builder
        .build(
            PartitionId(0),
            &[
                Label("CloudAccount".to_string()),
                Label("Resource".to_string()),
                Label("CostRecord".to_string()),
            ],
        )
        .await?;
    println!(
        "   -> {} nodes, {} edges indexed",
        generation.meta.node_count, generation.meta.edge_count
    );
    let handle = GenerationHandle::new(generation);

    println!("4. Query:\n{}", indent(DEMO_QUERY));
    let query = PestParser.parse(DEMO_QUERY)?;
    SchemaValidator
        .validate(&query, &schema)
        .map_err(|errors| format!("query failed validation: {errors:?}"))?;
    println!("   -> validated against the schema");

    let plan = NaivePlanner.plan(&query);
    println!("   -> planned into {} step(s)", plan.steps.len());

    let executor = SimpleLocalExecutor;
    let mut bindings = executor.resolve_start(&handle, &plan.steps[0]).await?;
    for step in &plan.steps[1..] {
        let frontier = executor.expand_hop(&handle, step, &bindings).await?;
        bindings = frontier.local;
    }

    let return_alias = plan
        .project
        .first()
        .ok_or("query has no RETURN projection")?;
    println!(
        "5. Resources over $100/mo within 3 hops of the account's VPC ({} row(s)):",
        bindings.len()
    );
    for binding in &bindings {
        let node_id = binding[return_alias];
        match resource_names.get(&(node_id.0 as i64)) {
            Some(name) => println!("   - {name} (NodeId({}))", node_id.0),
            None => println!("   - NodeId({})", node_id.0),
        }
    }

    Ok(())
}

fn indent(s: &str) -> String {
    s.trim()
        .lines()
        .map(|l| format!("     {l}"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Writes the three node tables and three edge tables as real Iceberg
/// tables (local FileIO, in-process `MemoryCatalog` — task ST1's dev
/// setup). The resource hierarchy under the account's VPC:
///
/// ```text
/// prod-aws (account)
///  └─OWNS─> vpc-prod                                          (hop 0 from vpc itself)
///            ├─CONTAINS─> subnet-prod-a                       (hop 1) - cost   $5
///            │              ├─CONTAINS─> i-checkout-api-1     (hop 2) - cost $245  <- kept
///            │              │              └─CONTAINS─> vol-checkout-api-1 (hop 3) - cost  $12
///            │              │                             └─CONTAINS─> snap-checkout-api-1 (hop 4, OUT OF RANGE) - cost $500
///            │              └─CONTAINS─> i-checkout-api-2     (hop 2) - cost  $40
///            └─CONTAINS─> rds-orders-db                       (hop 1) - cost $340  <- kept
/// ```
///
/// Returns the id -> name lookup for pretty-printing results, alongside
/// the catalog/namespace to read back from.
async fn ingest(
    warehouse_dir: &std::path::Path,
) -> Result<(impl Catalog, NamespaceIdent, HashMap<i64, &'static str>), Box<dyn std::error::Error>>
{
    let namespace = NamespaceIdent::new("demo".to_string());
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "memory",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                format!("file://{}", warehouse_dir.display()),
            )]),
        )
        .await?;
    catalog.create_namespace(&namespace, HashMap::new()).await?;

    // -- CloudAccount --------------------------------------------------
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

    // -- Resource (the hierarchy diagrammed above) ----------------------
    // (node_id, name, resource_type, region)
    let resources: [(i64, &str, &str, &str); 7] = [
        (2, "vpc-prod", "vpc", "us-east-1"),
        (3, "subnet-prod-a", "subnet", "us-east-1"),
        (4, "i-checkout-api-1", "ec2_instance", "us-east-1"),
        (5, "i-checkout-api-2", "ec2_instance", "us-east-1"),
        (6, "vol-checkout-api-1", "ebs_volume", "us-east-1"),
        (7, "snap-checkout-api-1", "ebs_snapshot", "us-east-1"),
        (8, "rds-orders-db", "rds_instance", "us-east-1"),
    ];
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
        ])
        .build()?;
    let resource_batch = RecordBatch::try_new(
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&resource_schema)?),
        vec![
            Arc::new(Int64Array::from(
                resources.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                resources.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                resources.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                resources.iter().map(|r| r.3).collect::<Vec<_>>(),
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

    // -- CostRecord (a separate "billing export" style dataset) --------
    // (node_id, amount_usd, period)
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

    // -- OWNS: account -> top-level resource -----------------------------
    write_table(
        &catalog,
        &namespace,
        &edge_table_name(&EdgeType("OWNS".to_string())),
        edge_schema()?,
        edge_batch(&[(1000, 1, 2)])?, // prod-aws -> vpc-prod
    )
    .await?;

    // -- CONTAINS: the resource hierarchy --------------------------------
    write_table(
        &catalog,
        &namespace,
        &edge_table_name(&EdgeType("CONTAINS".to_string())),
        edge_schema()?,
        edge_batch(&[
            (1001, 2, 3), // vpc-prod -> subnet-prod-a         (hop 1)
            (1002, 2, 8), // vpc-prod -> rds-orders-db          (hop 1)
            (1003, 3, 4), // subnet-prod-a -> i-checkout-api-1  (hop 2)
            (1004, 3, 5), // subnet-prod-a -> i-checkout-api-2  (hop 2)
            (1005, 4, 6), // i-checkout-api-1 -> vol-checkout-api-1 (hop 3)
            (1006, 6, 7), // vol-checkout-api-1 -> snap-checkout-api-1 (hop 4, out of *1..3 range)
        ])?,
    )
    .await?;

    // -- HAS_COST: resource -> cost record, the actual cloud/cost join --
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

    let names: HashMap<i64, &'static str> = resources.iter().map(|r| (r.0, r.1)).collect();
    Ok((catalog, namespace, names))
}

/// The Iceberg schema shared by all three edge tables (§4.1: `edge_id`,
/// `src_node_id`, `dst_node_id` + declared properties — none declared
/// for `OWNS`/`CONTAINS`/`HAS_COST` here).
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

fn edge_batch(edges: &[(i64, i64, i64)]) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let batch = RecordBatch::try_new(
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
    )?;
    Ok(batch)
}

/// Creates `table_name`, writes `batch` as a single Parquet data file,
/// and commits it via a fast-append transaction — the same sequence
/// `graph-storage`'s ST6 integration tests exercise, factored out here
/// since the demo needs it for six tables instead of one.
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
        "demo".to_string(),
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

    Ok(())
}
