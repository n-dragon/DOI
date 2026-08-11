//! End-to-end smoke test for the graph engine: ingest a small dataset
//! into a real (local) Apache Iceberg warehouse, build an in-memory
//! index generation from it, then parse/validate/plan/execute the
//! spec's own flagship DSL query (§7.1) against it.
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
//! would do, just inline here instead of running as a separate process.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use graph_dsl::{Parser as DslParser, PestParser, SchemaValidator, Validator};
use graph_index::{GenerationHandle, IcebergIndexBuilder, IndexBuilder, PartitionId};
use graph_query::{LocalExecutor, NaivePlanner, Planner, SimpleLocalExecutor};
use graph_schema::{Label, PestSchemaParser, SchemaParser};
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

const SCHEMA_IDL: &str = r#"
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
"#;

const DEMO_QUERY: &str = r#"
MATCH (p:Person {name: "Alice"})-[:KNOWS*1..3]->(friend:Person)
WHERE friend.birth_year > 1990
RETURN friend
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
        "2. Ingesting a small dataset into a local Iceberg warehouse at {}",
        warehouse_dir.display()
    );
    let (catalog, namespace, people) = ingest(&warehouse_dir).await?;
    let names: HashMap<i64, (&str, i64)> = people
        .iter()
        .map(|&(id, name, birth_year)| (id, (name, birth_year)))
        .collect();

    println!("3. Building the in-memory index from Iceberg (IX1/IX2/IX4)...");
    let reader = IcebergCatalogReader::new(catalog, namespace);
    let builder = IcebergIndexBuilder::new(reader, schema.clone());
    let generation = builder
        .build(PartitionId(0), &[Label("Person".to_string())])
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
    println!("5. Results ({} row(s)):", bindings.len());
    for binding in &bindings {
        let node_id = binding[return_alias];
        match names.get(&(node_id.0 as i64)) {
            Some((name, birth_year)) => {
                println!(
                    "   - {return_alias} = {name} (born {birth_year}, NodeId({}))",
                    node_id.0
                )
            }
            None => println!("   - {return_alias} = NodeId({})", node_id.0),
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

/// Writes `nodes_person` and `edges_knows` as real Iceberg tables (local
/// FileIO, in-process `MemoryCatalog` — task ST1's dev setup) with a
/// small hand-picked social graph:
///
///   Alice(1970) -> Bob(1990) -> Carol(1995) -> Dave(2000) -> Erin(2005)
///
/// chosen so the demo query's two constraints both do real work: Bob is
/// excluded by `birth_year > 1990`, Erin is excluded by `*1..3` (she's a
/// 4th hop out).
async fn ingest(
    warehouse_dir: &std::path::Path,
) -> Result<(impl Catalog, NamespaceIdent, [(i64, &'static str, i64); 5]), Box<dyn std::error::Error>>
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

    let person_schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "node_id", IcebergType::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "name", IcebergType::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "birth_year", IcebergType::Primitive(PrimitiveType::Long))
                .into(),
        ])
        .build()?;
    let people: [(i64, &str, i64); 5] = [
        (1, "Alice", 1970),
        (2, "Bob", 1990),
        (3, "Carol", 1995),
        (4, "Dave", 2000),
        (5, "Erin", 2005),
    ];
    let people_batch = RecordBatch::try_new(
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&person_schema)?),
        vec![
            Arc::new(Int64Array::from(
                people.iter().map(|p| p.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                people.iter().map(|p| p.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                people.iter().map(|p| p.2).collect::<Vec<_>>(),
            )),
        ],
    )?;
    write_table(
        &catalog,
        &namespace,
        &node_table_name(&Label("Person".to_string())),
        person_schema,
        people_batch,
    )
    .await?;

    let knows_schema = IcebergSchema::builder()
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
        .build()?;
    let edges: [(i64, i64, i64); 4] = [(100, 1, 2), (101, 2, 3), (102, 3, 4), (103, 4, 5)];
    let knows_batch = RecordBatch::try_new(
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&knows_schema)?),
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
    write_table(
        &catalog,
        &namespace,
        &edge_table_name(&graph_schema::EdgeType("KNOWS".to_string())),
        knows_schema,
        knows_batch,
    )
    .await?;

    Ok((catalog, namespace, people))
}

/// Creates `table_name`, writes `batch` as a single Parquet data file,
/// and commits it via a fast-append transaction — the same sequence
/// `graph-storage`'s ST6 integration tests exercise, factored out here
/// since the demo needs it for two tables instead of one.
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
