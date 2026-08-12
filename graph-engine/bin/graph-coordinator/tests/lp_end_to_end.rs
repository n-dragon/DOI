//! *(task LP1)* End-to-end proof of the least-privilege-via-telemetry use
//! case (`schema/least_privilege.graphidl`) over real gRPC — the same
//! shape as `co4_end_to_end.rs`, but exercising the DSL extension this
//! session added (edge aliases, `RETURN alias.property`,
//! `alias.prop = alias.prop`, correlated `NOT EXISTS`) against the
//! actual central query from the design note, run across **two**
//! partitions so the anti-join's `CheckAntiJoin` RPC genuinely crosses a
//! partition boundary rather than only ever hitting local memory.
//!
//! Dataset: one `IAMRole` (`prod-role`) granted one action
//! (`s3:GetObject`) on one sensitive `DataStore`, assumed by four
//! `Workload`s that land on both partitions (`partition_of` below is
//! deterministic — see the `assert!` right after computing the ids).
//! Exactly one workload used the permission inside the trust window;
//! the other three didn't (never accessed, accessed a different action,
//! accessed but before the window) — all three are "unused permission"
//! findings the central query's `NOT EXISTS` must surface, and the used
//! one must not.

use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use graph_cluster::{Discovery, StaticDiscovery};
use graph_index::{GenerationHandle, IcebergIndexBuilder, PartitionId};
use graph_partition_node::{rebuild, service::PartitionServiceImpl};
use graph_proto::v1::graph_service_client::GraphServiceClient;
use graph_proto::v1::graph_service_server::GraphServiceServer;
use graph_proto::v1::partition_service_server::PartitionServiceServer;
use graph_proto::v1::value::Kind;
use graph_proto::v1::ExecuteQueryRequest;
use graph_schema::partitioning::partition_of;
use graph_schema::{EdgeType, Label, NodeId, PestSchemaParser, SchemaParser};
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

const SCHEMA_IDL: &str = include_str!("../../../schema/least_privilege.graphidl");
const N_PARTITIONS: u32 = 2;
const PARTITION_ADDR: [&str; 2] = ["127.0.0.1:18911", "127.0.0.1:18912"];
const COORDINATOR_ADDR: &str = "127.0.0.1:18910";

// Fixed node ids chosen so each workload's `partition_of` lands where the
// doc comment above claims — pinned by the `assert!`s in `main()` rather
// than trusted blindly, since a hash-partitioning scheme silently
// "working" on the wrong partition would defeat the point of this test.
const ROLE_ID: i64 = 1;
const STORE_ID: i64 = 2;
const W_USED: i64 = 10; // accessed the granted action, in window -> excluded
const W_NEVER: i64 = 11; // never accessed the store -> unused finding
const W_WRONG_ACTION: i64 = 12; // accessed, but a different action -> unused finding
const W_STALE: i64 = 13; // accessed the right action, but before the window -> unused finding

#[tokio::test]
async fn least_privilege_query_surfaces_exactly_the_unused_permissions() {
    // Every workload used in this test must land on both partitions, or
    // the anti-join's correlated hop never actually crosses one.
    let by_partition: Vec<u32> = [W_USED, W_NEVER, W_WRONG_ACTION, W_STALE]
        .iter()
        .map(|&id| partition_of(NodeId(id as u64), N_PARTITIONS))
        .collect();
    assert!(
        by_partition.iter().any(|&p| p == 0) && by_partition.iter().any(|&p| p == 1),
        "fixture ids must spread across both partitions, got {by_partition:?}"
    );

    let schema = PestSchemaParser.parse(SCHEMA_IDL).expect("valid IDL");

    let warehouse = tempfile::tempdir().expect("tempdir");
    let namespace = NamespaceIdent::new("lp".to_string());
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

    let labels = [
        Label("Workload".to_string()),
        Label("IAMRole".to_string()),
        Label("DataStore".to_string()),
    ];

    // One reader/builder shared across both partitions — `IndexBuilder::
    // build` takes `&self`, so this doesn't need a second catalog handle
    // (`MemoryCatalog`'s namespace/table state is in-process and not
    // `Clone`, unlike a real REST/SQL catalog backed by external state).
    let reader = IcebergCatalogReader::new(catalog, namespace);
    let builder = IcebergIndexBuilder::new(reader, schema.clone(), N_PARTITIONS);

    let mut discovery_map = HashMap::new();
    for (i, addr) in PARTITION_ADDR.iter().enumerate() {
        let generation = rebuild::bootstrap(&builder, PartitionId(i as u32), &labels)
            .await
            .expect("index should build");
        let handle = Arc::new(GenerationHandle::new(generation));

        tokio::spawn(
            Server::builder()
                .add_service(PartitionServiceServer::new(PartitionServiceImpl::new(
                    handle,
                    PartitionId(i as u32),
                )))
                .serve(addr.parse().unwrap()),
        );
        discovery_map.insert(i as u32, addr.parse().unwrap());
    }

    for addr in &PARTITION_ADDR {
        connect_with_retry(|| async {
            graph_proto::v1::partition_service_client::PartitionServiceClient::connect(format!(
                "http://{addr}"
            ))
            .await
        })
        .await;
    }

    let discovery: Arc<dyn Discovery + Send + Sync> =
        Arc::new(StaticDiscovery::single_replica_per_partition(discovery_map));
    let graph_service = graph_coordinator::service::GraphServiceImpl::new(
        schema.clone(),
        SCHEMA_IDL.to_string(),
        discovery,
        N_PARTITIONS,
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

    // The design note's central query, verbatim (same DSL text pinned by
    // graph-dsl's `parses_the_least_privilege_example_into_the_expected_ast`
    // / `the_least_privilege_example_validates` tests).
    let response = client
        .execute_query(ExecuteQueryRequest {
            dsl: r#"
                MATCH (w:Workload)-[:ASSUMES]->(r:IAMRole)-[g:GRANTS]->(d:DataStore)
                WHERE d.data_class = "sensitive"
                  AND NOT EXISTS {
                    MATCH (w)-[a:ACCESSED]->(d)
                    WHERE a.action = g.action AND a.last_seen >= "2024-05-01T00:00:00Z"
                  }
                RETURN w.workload_id, w.team, r.arn, g.action, d.arn
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

    let mut unused: Vec<String> = results
        .iter()
        .map(|r| {
            match r
                .projection
                .get("w.workload_id")
                .and_then(|v| v.kind.clone())
            {
                Some(Kind::StringValue(s)) => s,
                other => panic!("expected a StringValue projection, got {other:?}"),
            }
        })
        .collect();
    unused.sort();

    assert_eq!(
        unused,
        vec![
            "never".to_string(),
            "stale".to_string(),
            "wrong-action".to_string()
        ],
        "the used workload must be excluded by NOT EXISTS, the other three must be findings"
    );

    // Every surviving row must carry the sensitive datastore's arn and
    // the declared action, proving `RETURN alias.property` projection
    // (not just the bare-alias whole-record path) round-trips correctly.
    for row in &results {
        assert_eq!(
            row.projection.get("d.arn").and_then(|v| v.kind.clone()),
            Some(Kind::StringValue("arn:aws:s3:::prod-bucket".to_string()))
        );
        assert_eq!(
            row.projection.get("g.action").and_then(|v| v.kind.clone()),
            Some(Kind::StringValue("s3:GetObject".to_string()))
        );
    }
}

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

/// RFC3339 -> epoch microseconds, matching `filter_eval`'s own
/// `timestamp_micros()` coercion so this fixture's literals mean what
/// the assertions above assume.
fn ts(rfc3339: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(rfc3339)
        .expect("valid RFC3339 literal")
        .timestamp_micros()
}

async fn ingest(catalog: &impl Catalog, namespace: &NamespaceIdent) {
    // -- Workload --
    let workload_schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "node_id", IcebergType::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(
                2,
                "workload_id",
                IcebergType::Primitive(PrimitiveType::String),
            )
            .into(),
            NestedField::required(3, "kind", IcebergType::Primitive(PrimitiveType::String)).into(),
            NestedField::required(
                4,
                "cloud_account",
                IcebergType::Primitive(PrimitiveType::String),
            )
            .into(),
            NestedField::required(5, "env", IcebergType::Primitive(PrimitiveType::String)).into(),
            NestedField::required(6, "service", IcebergType::Primitive(PrimitiveType::String))
                .into(),
            NestedField::required(7, "team", IcebergType::Primitive(PrimitiveType::String)).into(),
            NestedField::required(
                8,
                "observed_since",
                IcebergType::Primitive(PrimitiveType::Timestamp),
            )
            .into(),
        ])
        .build()
        .unwrap();
    let workloads: [(i64, &str, &str); 4] = [
        (W_USED, "used", "team-checkout"),
        (W_NEVER, "never", "team-checkout"),
        (W_WRONG_ACTION, "wrong-action", "team-checkout"),
        (W_STALE, "stale", "team-checkout"),
    ];
    let observed_since = ts("2024-01-01T00:00:00Z");
    let workloads_batch = RecordBatch::try_new(
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&workload_schema).unwrap()),
        vec![
            Arc::new(Int64Array::from(
                workloads.iter().map(|w| w.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                workloads.iter().map(|w| w.1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec!["pod"; workloads.len()])),
            Arc::new(StringArray::from(vec!["111111111111"; workloads.len()])),
            Arc::new(StringArray::from(vec!["prod"; workloads.len()])),
            Arc::new(StringArray::from(vec!["checkout"; workloads.len()])),
            Arc::new(StringArray::from(
                workloads.iter().map(|w| w.2).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampMicrosecondArray::from(vec![
                observed_since;
                workloads.len()
            ])),
        ],
    )
    .unwrap();
    write_table(
        catalog,
        namespace,
        &node_table_name(&Label("Workload".to_string())),
        workload_schema,
        workloads_batch,
    )
    .await;

    // -- IAMRole --
    let role_schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "node_id", IcebergType::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "arn", IcebergType::Primitive(PrimitiveType::String)).into(),
            NestedField::required(
                3,
                "cloud_account",
                IcebergType::Primitive(PrimitiveType::String),
            )
            .into(),
            NestedField::required(
                4,
                "trust_principals",
                IcebergType::List(iceberg::spec::ListType::new(
                    NestedField::list_element(
                        5,
                        IcebergType::Primitive(PrimitiveType::String),
                        false,
                    )
                    .into(),
                )),
            )
            .into(),
        ])
        .build()
        .unwrap();
    let role_arrow_schema = iceberg::arrow::schema_to_arrow_schema(&role_schema).unwrap();
    // Iceberg's writer checks the batch's arrow `DataType` against the
    // table's own strictly (inner field name + `PARQUET:field_id`
    // metadata included, unlike `RecordBatch::try_new`'s looser
    // `equals_datatype`) — so the list's element field must be the exact
    // one `schema_to_arrow_schema` derived, not a `ListBuilder` default.
    let trust_principals = {
        use arrow_array::builder::{ListBuilder, StringBuilder};
        use arrow_schema::DataType;
        let element_field = match role_arrow_schema
            .field_with_name("trust_principals")
            .unwrap()
            .data_type()
        {
            DataType::List(field) => field.clone(),
            other => panic!("expected a List field, got {other:?}"),
        };
        let mut builder = ListBuilder::new(StringBuilder::new()).with_field(element_field);
        builder.values().append_value("prod-role");
        builder.append(true);
        builder.finish()
    };
    let role_batch = RecordBatch::try_new(
        Arc::new(role_arrow_schema),
        vec![
            Arc::new(Int64Array::from(vec![ROLE_ID])),
            Arc::new(StringArray::from(vec![
                "arn:aws:iam::111111111111:role/prod-role",
            ])),
            Arc::new(StringArray::from(vec!["111111111111"])),
            Arc::new(trust_principals),
        ],
    )
    .unwrap();
    write_table(
        catalog,
        namespace,
        &node_table_name(&Label("IAMRole".to_string())),
        role_schema,
        role_batch,
    )
    .await;

    // -- DataStore --
    let store_schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "node_id", IcebergType::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "arn", IcebergType::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "kind", IcebergType::Primitive(PrimitiveType::String)).into(),
            NestedField::required(
                4,
                "data_class",
                IcebergType::Primitive(PrimitiveType::String),
            )
            .into(),
        ])
        .build()
        .unwrap();
    let store_batch = RecordBatch::try_new(
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&store_schema).unwrap()),
        vec![
            Arc::new(Int64Array::from(vec![STORE_ID])),
            Arc::new(StringArray::from(vec!["arn:aws:s3:::prod-bucket"])),
            Arc::new(StringArray::from(vec!["s3"])),
            Arc::new(StringArray::from(vec!["sensitive"])),
        ],
    )
    .unwrap();
    write_table(
        catalog,
        namespace,
        &node_table_name(&Label("DataStore".to_string())),
        store_schema,
        store_batch,
    )
    .await;

    // -- ASSUMES: every workload assumes prod-role --
    let assumes_schema = edge_schema_with(vec![
        NestedField::required(4, "via", IcebergType::Primitive(PrimitiveType::String)).into(),
        NestedField::required(
            5,
            "evaluated_at",
            IcebergType::Primitive(PrimitiveType::Timestamp),
        )
        .into(),
    ]);
    let evaluated_at = ts("2024-06-01T00:00:00Z");
    let assumes_ids: [i64; 4] = [W_USED, W_NEVER, W_WRONG_ACTION, W_STALE];
    let assumes_batch = RecordBatch::try_new(
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&assumes_schema).unwrap()),
        vec![
            Arc::new(Int64Array::from(vec![200, 201, 202, 203])),
            Arc::new(Int64Array::from(assumes_ids.to_vec())),
            Arc::new(Int64Array::from(vec![ROLE_ID; 4])),
            Arc::new(StringArray::from(vec!["irsa"; 4])),
            Arc::new(TimestampMicrosecondArray::from(vec![evaluated_at; 4])),
        ],
    )
    .unwrap();
    write_table(
        catalog,
        namespace,
        &edge_table_name(&EdgeType("ASSUMES".to_string())),
        assumes_schema,
        assumes_batch,
    )
    .await;

    // -- RAN_AS: every workload's observed role usage. Not touched by the
    // central query below, but `IcebergIndexBuilder::build` scans every
    // edge type the schema declares (no per-query table pruning yet), so
    // the table still has to exist.
    let ran_as_schema = edge_schema_with(vec![
        NestedField::required(
            4,
            "first_seen",
            IcebergType::Primitive(PrimitiveType::Timestamp),
        )
        .into(),
        NestedField::required(
            5,
            "last_seen",
            IcebergType::Primitive(PrimitiveType::Timestamp),
        )
        .into(),
        NestedField::required(6, "count", IcebergType::Primitive(PrimitiveType::Long)).into(),
        NestedField::required(7, "window", IcebergType::Primitive(PrimitiveType::String)).into(),
    ]);
    let ran_as_batch = RecordBatch::try_new(
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&ran_as_schema).unwrap()),
        vec![
            Arc::new(Int64Array::from(vec![250, 251, 252, 253])),
            Arc::new(Int64Array::from(assumes_ids.to_vec())),
            Arc::new(Int64Array::from(vec![ROLE_ID; 4])),
            Arc::new(TimestampMicrosecondArray::from(vec![evaluated_at; 4])),
            Arc::new(TimestampMicrosecondArray::from(vec![evaluated_at; 4])),
            Arc::new(Int64Array::from(vec![10; 4])),
            Arc::new(StringArray::from(vec!["90d"; 4])),
        ],
    )
    .unwrap();
    write_table(
        catalog,
        namespace,
        &edge_table_name(&EdgeType("RAN_AS".to_string())),
        ran_as_schema,
        ran_as_batch,
    )
    .await;

    // -- GRANTS: prod-role -> prod-bucket, s3:GetObject --
    let grants_schema = edge_schema_with(vec![
        NestedField::required(4, "action", IcebergType::Primitive(PrimitiveType::String)).into(),
        NestedField::required(5, "effect", IcebergType::Primitive(PrimitiveType::String)).into(),
        NestedField::required(
            6,
            "resource_pattern",
            IcebergType::Primitive(PrimitiveType::String),
        )
        .into(),
        NestedField::required(
            7,
            "policy_arn",
            IcebergType::Primitive(PrimitiveType::String),
        )
        .into(),
        NestedField::required(
            8,
            "evaluated_at",
            IcebergType::Primitive(PrimitiveType::Timestamp),
        )
        .into(),
    ]);
    let grants_batch = RecordBatch::try_new(
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&grants_schema).unwrap()),
        vec![
            Arc::new(Int64Array::from(vec![300])),
            Arc::new(Int64Array::from(vec![ROLE_ID])),
            Arc::new(Int64Array::from(vec![STORE_ID])),
            Arc::new(StringArray::from(vec!["s3:GetObject"])),
            Arc::new(StringArray::from(vec!["Allow"])),
            Arc::new(StringArray::from(vec!["arn:aws:s3:::prod-bucket/*"])),
            Arc::new(StringArray::from(vec![
                "arn:aws:iam::111111111111:policy/prod-read",
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![evaluated_at])),
        ],
    )
    .unwrap();
    write_table(
        catalog,
        namespace,
        &edge_table_name(&EdgeType("GRANTS".to_string())),
        grants_schema,
        grants_batch,
    )
    .await;

    // -- ACCESSED: only `used`, `wrong-action`, `stale` have telemetry --
    let accessed_schema = edge_schema_with(vec![
        NestedField::required(4, "action", IcebergType::Primitive(PrimitiveType::String)).into(),
        NestedField::required(
            5,
            "first_seen",
            IcebergType::Primitive(PrimitiveType::Timestamp),
        )
        .into(),
        NestedField::required(
            6,
            "last_seen",
            IcebergType::Primitive(PrimitiveType::Timestamp),
        )
        .into(),
        NestedField::required(7, "count", IcebergType::Primitive(PrimitiveType::Long)).into(),
        NestedField::required(8, "window", IcebergType::Primitive(PrimitiveType::String)).into(),
        NestedField::required(
            9,
            "evidence_ref",
            IcebergType::Primitive(PrimitiveType::String),
        )
        .into(),
        NestedField::required(10, "source", IcebergType::Primitive(PrimitiveType::String)).into(),
    ]);
    // used: matching action, inside the window (>= 2024-05-01).
    // wrong-action: right workload/store, action doesn't match GRANTS.action.
    // stale: matching action, but last_seen before the window cutoff.
    let accessed: [(i64, i64, &str, i64); 3] = [
        (400, W_USED, "s3:GetObject", ts("2024-07-01T00:00:00Z")),
        (
            401,
            W_WRONG_ACTION,
            "s3:PutObject",
            ts("2024-07-01T00:00:00Z"),
        ),
        (402, W_STALE, "s3:GetObject", ts("2024-02-01T00:00:00Z")),
    ];
    let first_seen = ts("2024-01-15T00:00:00Z");
    let accessed_batch = RecordBatch::try_new(
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&accessed_schema).unwrap()),
        vec![
            Arc::new(Int64Array::from(
                accessed.iter().map(|a| a.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                accessed.iter().map(|a| a.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(vec![STORE_ID; accessed.len()])),
            Arc::new(StringArray::from(
                accessed.iter().map(|a| a.2).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampMicrosecondArray::from(vec![
                first_seen;
                accessed.len()
            ])),
            Arc::new(TimestampMicrosecondArray::from(
                accessed.iter().map(|a| a.3).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(vec![42; accessed.len()])),
            Arc::new(StringArray::from(vec!["90d"; accessed.len()])),
            Arc::new(StringArray::from(vec!["evidence-1"; accessed.len()])),
            Arc::new(StringArray::from(vec!["cloudtrail"; accessed.len()])),
        ],
    )
    .unwrap();
    write_table(
        catalog,
        namespace,
        &edge_table_name(&EdgeType("ACCESSED".to_string())),
        accessed_schema,
        accessed_batch,
    )
    .await;
}

fn edge_schema_with(extra_fields: Vec<Arc<NestedField>>) -> IcebergSchema {
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
        .unwrap()
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
        "lp".to_string(),
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
