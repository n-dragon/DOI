//! Partition-node process (spec §6.1): hosts one replica of a partition,
//! serving traversal RPCs against a periodically-rebuilt in-memory index.
//! Deployed as a Kubernetes `StatefulSet` (§10); config PN5's `Config`
//! documents.

use graph_index::{GenerationHandle, IcebergIndexBuilder, PartitionId};
use graph_partition_node::{config::Config, rebuild, service};
use graph_proto::v1::partition_service_server::PartitionServiceServer;
use graph_schema::{PestSchemaParser, SchemaParser};
use graph_storage::IcebergCatalogReader;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::{CatalogBuilder, NamespaceIdent};
use std::collections::HashMap;
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

    // ST1's documented dev catalog (MemoryCatalog + local FileIO); the
    // prod candidate (a REST catalog) is a construction-site swap here,
    // not a code change, since IcebergIndexBuilder is generic over any
    // iceberg::Catalog via IcebergCatalogReader<C>.
    let namespace = NamespaceIdent::new(config.namespace.clone());
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "memory",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                format!("file://{}", config.warehouse_path.display()),
            )]),
        )
        .await?;

    let reader = IcebergCatalogReader::new(catalog, namespace);
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
