//! Coordinator process (spec §6.1): the client-facing `GraphService`
//! (§8.2), routing each query to the partition replicas that own it via
//! `graph_query::ScatterGatherExecutor` (tasks Q5-Q7) and
//! `graph_cluster::Discovery` (task CL2). Deployed as a Kubernetes
//! `Deployment` (§10).

use graph_cluster::{Discovery, KubernetesDiscovery, StaticDiscovery};
use graph_coordinator::{config::Config, config::DiscoveryMode, service};
use graph_proto::v1::graph_service_server::GraphServiceServer;
use graph_schema::{PestSchemaParser, SchemaParser};
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    graph_observability::init_tracing("graph-coordinator");
    let config = Config::from_env()?;

    let schema_idl = std::fs::read_to_string(&config.schema_path)?;
    let schema = PestSchemaParser.parse(&schema_idl)?;

    let discovery: Arc<dyn Discovery + Send + Sync> = match &config.discovery {
        DiscoveryMode::Static { partition_addrs } => {
            tracing::info!(
                partitions = partition_addrs.len(),
                "starting with static discovery"
            );
            Arc::new(StaticDiscovery::single_replica_per_partition(
                partition_addrs.iter().copied().collect::<HashMap<_, _>>(),
            ))
        }
        DiscoveryMode::Kubernetes {
            namespace,
            label_selector,
            partition_port,
        } => {
            tracing::info!(%namespace, %label_selector, "starting with Kubernetes discovery");
            Arc::new(
                KubernetesDiscovery::try_new(
                    namespace.clone(),
                    label_selector.clone(),
                    *partition_port,
                )
                .await?,
            )
        }
    };

    let service =
        service::GraphServiceImpl::new(schema, schema_idl, discovery, config.n_partitions);

    tokio::spawn(graph_observability::serve_metrics(
        config.metrics_listen_addr,
    ));

    tracing::info!(listen_addr = %config.listen_addr, "serving GraphService");
    Server::builder()
        .add_service(GraphServiceServer::new(service))
        .serve(config.listen_addr)
        .await?;

    Ok(())
}
