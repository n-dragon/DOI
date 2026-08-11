//! Coordinator process (spec §6.1): the client-facing `GraphService`
//! (§8.2), routing each query to the partition replicas that own it.
//! Deployed as a Kubernetes `Deployment` (§10). Mono-partition v1 (Q5-Q8/
//! CO5, Phase 2, are the distributed scatter-gather this will grow into).

use graph_coordinator::{config::Config, service};
use graph_proto::v1::graph_service_server::GraphServiceServer;
use graph_proto::v1::partition_service_client::PartitionServiceClient;
use graph_schema::{PestSchemaParser, SchemaParser};
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    graph_observability::init_tracing("graph-coordinator");
    let config = Config::from_env()?;
    println!(
        "[graph-coordinator] starting: listen={}, partition_node={}",
        config.listen_addr, config.partition_node_addr
    );

    let schema_idl = std::fs::read_to_string(&config.schema_path)?;
    let schema = PestSchemaParser.parse(&schema_idl)?;

    let partition_client =
        PartitionServiceClient::connect(config.partition_node_addr.clone()).await?;

    let service = service::GraphServiceImpl::new(schema, schema_idl, partition_client);
    println!(
        "[graph-coordinator] serving GraphService on {}",
        config.listen_addr
    );
    Server::builder()
        .add_service(GraphServiceServer::new(service))
        .serve(config.listen_addr)
        .await?;

    Ok(())
}
