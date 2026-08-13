//! *(tasks D14/D15)* End-to-end proof that `ORDER BY`/`LIMIT` work over
//! real gRPC, not just against an in-memory `Vec<Binding>` — same shape
//! as `co4_end_to_end.rs` (real `PartitionServiceImpl` + `GraphServiceImpl`
//! on real TCP loopback sockets), same dataset, so this test's only job is
//! proving `GraphServiceImpl::execute_query`'s post-gather sort/truncate
//! step (`service.rs`'s `sort_bindings_by_property`) actually reaches the
//! client in the right order and count.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use graph_index::{GenerationHandle, IcebergIndexBuilder, PartitionId};
use graph_partition_node::{rebuild, service::PartitionServiceImpl};
use graph_proto::v1::graph_service_client::GraphServiceClient;
use graph_proto::v1::graph_service_server::GraphServiceServer;
use graph_proto::v1::partition_service_client::PartitionServiceClient;
use graph_proto::v1::partition_service_server::PartitionServiceServer;
use graph_proto::v1::value::Kind;
use graph_proto::v1::ExecuteQueryRequest;
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
use tonic::transport::Server;

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

const PARTITION_ADDR: &str = "127.0.0.1:18911";
const COORDINATOR_ADDR: &str = "127.0.0.1:18910";

#[tokio::test]
async fn order_by_desc_and_limit_are_applied_over_real_grpc() {
    let schema = PestSchemaParser.parse(SCHEMA_IDL).expect("valid IDL");

    let warehouse = tempfile::tempdir().expect("tempdir");
    let namespace = NamespaceIdent::new("order_by_limit".to_string());
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

    ingest(&catalog, &namespace).await;

    let reader = IcebergCatalogReader::new(catalog, namespace);
    let builder = IcebergIndexBuilder::new(reader, schema.clone(), 1);
    let generation = rebuild::bootstrap(&builder, PartitionId(0), &[Label("Person".to_string())])
        .await
        .expect("index should build");
    let handle = Arc::new(GenerationHandle::new(generation));

    tokio::spawn(
        Server::builder()
            .add_service(PartitionServiceServer::new(PartitionServiceImpl::new(
                handle,
                PartitionId(0),
            )))
            .serve(PARTITION_ADDR.parse().unwrap()),
    );

    let _partition_client = connect_with_retry(|| async {
        PartitionServiceClient::connect(format!("http://{PARTITION_ADDR}")).await
    })
    .await;

    let discovery: std::sync::Arc<dyn graph_cluster::Discovery + Send + Sync> = std::sync::Arc::new(
        graph_cluster::StaticDiscovery::single_replica_per_partition(HashMap::from([(
            0u32,
            PARTITION_ADDR.parse().unwrap(),
        )])),
    );
    let graph_service = graph_coordinator::service::GraphServiceImpl::new(
        schema.clone(),
        SCHEMA_IDL.to_string(),
        discovery,
        1,
    );
    tokio::spawn(
        Server::builder()
            .add_service(GraphServiceServer::new(graph_service))
            .serve(COORDINATOR_ADDR.parse().unwrap()),
    );

    let mut client = connect_with_retry(|| async {
        GraphServiceClient::connect(format!("http://{COORDINATOR_ADDR}")).await
    })
    .await;

    // Same chain as CO4 (Alice->Bob->Carol->Dave->Erin, birth years
    // ascending along the chain), no WHERE filter this time — *1..3 from
    // Alice reaches Bob(1990)/Carol(1995)/Dave(2000), excludes Erin (hop
    // 4). Sorting those three by birth_year DESC and taking the top 2
    // should yield Dave then Carol, in that order.
    let response = client
        .execute_query(ExecuteQueryRequest {
            dsl: r#"
                MATCH (p:Person {name: "Alice"})-[:KNOWS*1..3]->(friend:Person)
                RETURN friend.name
                ORDER BY friend.birth_year DESC
                LIMIT 2
            "#
            .to_string(),
        })
        .await
        .expect("execute_query should succeed")
        .into_inner();

    let results: Vec<_> = {
        use futures::StreamExt;
        response
            .map(|r| r.expect("every result should decode"))
            .collect()
            .await
    };

    assert_eq!(
        results.len(),
        2,
        "LIMIT 2 should cap the result set at 2 rows"
    );

    let names: Vec<String> = results
        .iter()
        .map(
            |r| match r.projection.get("friend.name").and_then(|v| v.kind.clone()) {
                Some(Kind::StringValue(name)) => name,
                other => panic!("expected a StringValue, got {other:?}"),
            },
        )
        .collect();

    assert_eq!(
        names,
        vec!["Dave".to_string(), "Carol".to_string()],
        "ORDER BY birth_year DESC should put the youngest-in-id (latest birth_year) friends first"
    );
}

/// Real servers need a moment to start listening - retry the connect a
/// few times instead of racing it.
async fn connect_with_retry<F, Fut, T, E>(mut connect: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Debug,
{
    for attempt in 0..20 {
        match connect().await {
            Ok(client) => return client,
            Err(_) if attempt < 19 => {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await
            }
            Err(e) => panic!("failed to connect after retries: {e:?}"),
        }
    }
    unreachable!()
}

async fn ingest(catalog: &impl Catalog, namespace: &NamespaceIdent) {
    let person_schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "node_id", IcebergType::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "name", IcebergType::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "birth_year", IcebergType::Primitive(PrimitiveType::Long))
                .into(),
        ])
        .build()
        .unwrap();
    let people: [(i64, &str, i64); 5] = [
        (1, "Alice", 1970),
        (2, "Bob", 1990),
        (3, "Carol", 1995),
        (4, "Dave", 2000),
        (5, "Erin", 2005),
    ];
    let people_batch = RecordBatch::try_new(
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&person_schema).unwrap()),
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
    )
    .unwrap();
    write_table(
        catalog,
        namespace,
        &node_table_name(&Label("Person".to_string())),
        person_schema,
        people_batch,
    )
    .await;

    let edge_schema = IcebergSchema::builder()
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
        .unwrap();
    let edges: [(i64, i64, i64); 4] = [(100, 1, 2), (101, 2, 3), (102, 3, 4), (103, 4, 5)];
    let edges_batch = RecordBatch::try_new(
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&edge_schema).unwrap()),
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
    )
    .unwrap();
    write_table(
        catalog,
        namespace,
        &edge_table_name(&graph_schema::EdgeType("KNOWS".to_string())),
        edge_schema,
        edges_batch,
    )
    .await;
}

async fn write_table(
    catalog: &impl Catalog,
    namespace: &NamespaceIdent,
    table_name: &str,
    schema: IcebergSchema,
    batch: RecordBatch,
) {
    let table = catalog
        .create_table(
            namespace,
            TableCreation::builder()
                .name(table_name.to_string())
                .schema(schema)
                .build(),
        )
        .await
        .unwrap();

    let location_generator = DefaultLocationGenerator::new(table.metadata()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "order_by_limit".to_string(),
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
        .unwrap();
    writer.write(batch).await.unwrap();
    let data_files = writer.close().await.unwrap();

    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(data_files)
        .apply(tx)
        .unwrap();
    tx.commit(catalog).await.unwrap();
}
