//! Runtime config, read once from the environment at startup — a
//! Kubernetes `Deployment` (§10) sets these via its pod spec.

use std::net::SocketAddr;
use std::path::PathBuf;

pub struct Config {
    /// Same `.graphschema` IDL file every partition replica agrees on
    /// (§3.4).
    pub schema_path: PathBuf,
    /// The single partition node to call for every step, mono-partition
    /// v1 (CO2) — e.g. `http://127.0.0.1:7001`. Phase 2 (CO5) replaces
    /// this with `graph-cluster::Discovery` routing across replicas.
    pub partition_node_addr: String,
    pub listen_addr: SocketAddr,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("missing required environment variable {0}")]
    MissingEnv(&'static str),
    #[error("invalid value for {0}: {1}")]
    InvalidEnv(&'static str, String),
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let schema_path = std::env::var("GRAPH_SCHEMA_PATH")
            .map_err(|_| ConfigError::MissingEnv("GRAPH_SCHEMA_PATH"))?
            .into();
        let partition_node_addr = std::env::var("GRAPH_PARTITION_NODE_ADDR")
            .unwrap_or_else(|_| "http://127.0.0.1:7001".to_string());
        let listen_addr = std::env::var("GRAPH_COORDINATOR_LISTEN_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:7000".to_string())
            .parse()
            .map_err(|_| {
                ConfigError::InvalidEnv(
                    "GRAPH_COORDINATOR_LISTEN_ADDR",
                    "not a valid socket address".to_string(),
                )
            })?;

        Ok(Self {
            schema_path,
            partition_node_addr,
            listen_addr,
        })
    }
}
