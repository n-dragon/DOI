//! Partition-node process (spec §6.1): hosts one replica of a partition,
//! serving traversal RPCs against a periodically-rebuilt in-memory index.
//! Deployed as a Kubernetes `StatefulSet` (§10); config PN5's `Config`
//! documents.

use graph_index::{GenerationHandle, IcebergIndexBuilder, PartitionId};
use graph_partition_node::{config::Config, rebuild, service};
use graph_proto::v1::partition_service_server::PartitionServiceServer;
use graph_schema::{PestSchemaParser, SchemaParser};
use graph_storage::{open_sql_catalog, IcebergCatalogReader};
use std::sync::Arc;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    graph_observability::init_tracing("graph-partition-node");
    let config = Config::from_env()?;
    tracing::info!(
        partition = config.partition_id,
        n_partitions = config.n_partitions,
        listen_addr = %config.listen_addr,
        "starting graph-partition-node"
    );

    let idl = std::fs::read_to_string(&config.schema_path)?;
    let schema = PestSchemaParser.parse(&idl)?;

    // ST1's dev catalog, revised to a persistent SQLite-backed one -
    // MemoryCatalog's table registry doesn't survive being a separate
    // process from whatever ingested the data (see graph-storage's
    // catalog module doc comment for why, and its own regression test).
    // The prod candidate (a REST catalog) is still a construction-site
    // swap here, not a code change: IcebergIndexBuilder is generic over
    // any iceberg::Catalog via IcebergCatalogReader<C>.
    tracing::info!(path = %config.catalog_db_path.display(), "opening catalog");
    let catalog = open_sql_catalog(
        &config.catalog_db_path,
        &config.warehouse_path,
        &config.namespace,
    )
    .await?;

    let reader = IcebergCatalogReader::new(
        catalog,
        iceberg::NamespaceIdent::new(config.namespace.clone()),
    );
    let builder = IcebergIndexBuilder::new(reader, schema.clone(), config.n_partitions);
    let labels: Vec<_> = schema.nodes.keys().cloned().collect();
    let partition = PartitionId(config.partition_id);

    tracing::info!("bootstrapping index from Iceberg...");
    let generation = rebuild::bootstrap(&builder, partition, &labels).await?;
    tracing::info!(
        nodes = generation.meta.node_count,
        edges = generation.meta.edge_count,
        "bootstrap complete"
    );
    let handle = Arc::new(GenerationHandle::new(generation));

    tokio::spawn(rebuild::periodic_rebuild_loop(
        builder,
        handle.clone(),
        partition,
        labels,
        config.rebuild_interval,
    ));

    tokio::spawn(graph_observability::serve_metrics(
        config.metrics_listen_addr,
    ));

    // *(task OB3)* Every RPC's trace context (injected client-side by
    // `bin/graph-coordinator`'s `GrpcPartitionRpc`) is extracted here and
    // set as this handler's span parent, before `PartitionServiceImpl`
    // ever sees the request — keeps a query's fan-out across
    // hops/partitions one connected trace (spec §9.2).
    let service = PartitionServiceServer::with_interceptor(
        service::PartitionServiceImpl::new(handle, partition),
        graph_observability::extract_and_continue,
    );
    tracing::info!(listen_addr = %config.listen_addr, "serving PartitionService");
    Server::builder()
        .add_service(service)
        .serve(config.listen_addr)
        .await?;

    Ok(())
}
