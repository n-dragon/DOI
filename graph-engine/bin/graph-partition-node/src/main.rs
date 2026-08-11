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
    println!(
        "[graph-partition-node] starting: partition={}/{}, listen={}",
        config.partition_id, config.n_partitions, config.listen_addr
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
    println!(
        "[graph-partition-node] opening catalog at {}",
        config.catalog_db_path.display()
    );
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
    let builder = IcebergIndexBuilder::new(reader, schema.clone());
    let labels: Vec<_> = schema.nodes.keys().cloned().collect();
    let partition = PartitionId(config.partition_id);

    println!("[graph-partition-node] bootstrapping index from Iceberg...");
    let generation = rebuild::bootstrap(&builder, partition, &labels).await?;
    println!(
        "[graph-partition-node] bootstrap complete: {} nodes, {} edges",
        generation.meta.node_count, generation.meta.edge_count
    );
    let handle = Arc::new(GenerationHandle::new(generation));

    tokio::spawn(rebuild::periodic_rebuild_loop(
        builder,
        handle.clone(),
        partition,
        labels,
        config.rebuild_interval,
    ));

    let service = service::PartitionServiceImpl::new(handle);
    println!(
        "[graph-partition-node] serving PartitionService on {}",
        config.listen_addr
    );
    Server::builder()
        .add_service(PartitionServiceServer::new(service))
        .serve(config.listen_addr)
        .await?;

    Ok(())
}
