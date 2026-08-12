//! Runtime config, read once from the environment at startup — a
//! Kubernetes `Deployment` (§10) sets these via its pod spec.

use std::net::SocketAddr;
use std::path::PathBuf;

/// *(task CO5)* Which `graph_cluster::Discovery` implementation to wire
/// up. Kubernetes-native discovery (§6.3) is the v1 decision for a real
/// cluster deployment; `Static` exists for local dev, tests, and any
/// non-Kubernetes deployment (`bin/graph-partition-node` processes on
/// fixed, known addresses) — the same "generalize the mono-partition
/// wiring, don't force everyone through the same discovery path" spirit
/// as `graph_cluster::StaticDiscovery`'s own doc comment.
pub enum DiscoveryMode {
    Static {
        partition_addrs: Vec<(u32, SocketAddr)>,
    },
    Kubernetes {
        namespace: String,
        label_selector: String,
        partition_port: u16,
    },
}

pub struct Config {
    /// Same `.graphschema` IDL file every partition replica agrees on
    /// (§3.4).
    pub schema_path: PathBuf,
    pub discovery: DiscoveryMode,
    /// Fixed for the graph's lifetime (§6.2) — must match every
    /// partition-node's own `GRAPH_N_PARTITIONS`.
    pub n_partitions: u32,
    pub listen_addr: SocketAddr,
    /// *(task OB4)* Separate from `listen_addr` — the usual Prometheus
    /// convention of a dedicated metrics port distinct from application
    /// traffic (spec §9.1).
    pub metrics_listen_addr: SocketAddr,
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
        let n_partitions = parse_env("GRAPH_N_PARTITIONS", 1u32)?;
        let listen_addr = parse_env("GRAPH_COORDINATOR_LISTEN_ADDR", "0.0.0.0:7000".to_string())?
            .parse()
            .map_err(|_| {
                ConfigError::InvalidEnv(
                    "GRAPH_COORDINATOR_LISTEN_ADDR",
                    "not a valid socket address".to_string(),
                )
            })?;
        let metrics_listen_addr = parse_env(
            "GRAPH_COORDINATOR_METRICS_LISTEN_ADDR",
            "0.0.0.0:9100".to_string(),
        )?
        .parse()
        .map_err(|_| {
            ConfigError::InvalidEnv(
                "GRAPH_COORDINATOR_METRICS_LISTEN_ADDR",
                "not a valid socket address".to_string(),
            )
        })?;

        let mode = std::env::var("GRAPH_DISCOVERY_MODE").unwrap_or_else(|_| "static".to_string());
        let discovery = match mode.as_str() {
            "kubernetes" => DiscoveryMode::Kubernetes {
                namespace: std::env::var("GRAPH_K8S_NAMESPACE")
                    .unwrap_or_else(|_| "default".to_string()),
                label_selector: std::env::var("GRAPH_K8S_LABEL_SELECTOR")
                    .unwrap_or_else(|_| "app=graph-partition-node".to_string()),
                partition_port: parse_env("GRAPH_K8S_PARTITION_PORT", 7001u16)?,
            },
            "static" => DiscoveryMode::Static {
                partition_addrs: parse_static_partition_addrs(
                    &std::env::var("GRAPH_PARTITION_NODE_ADDRS")
                        .unwrap_or_else(|_| "0=127.0.0.1:7001".to_string()),
                )?,
            },
            other => {
                return Err(ConfigError::InvalidEnv(
                    "GRAPH_DISCOVERY_MODE",
                    format!("{other:?} (expected \"static\" or \"kubernetes\")"),
                ))
            }
        };

        Ok(Self {
            schema_path,
            discovery,
            n_partitions,
            listen_addr,
            metrics_listen_addr,
        })
    }
}

/// Parses `"0=127.0.0.1:7001,1=127.0.0.1:7002"` — one healthy replica
/// per partition, the common case for local dev/tests where there's no
/// replication to model (mirrors
/// `graph_cluster::StaticDiscovery::single_replica_per_partition`).
fn parse_static_partition_addrs(raw: &str) -> Result<Vec<(u32, SocketAddr)>, ConfigError> {
    raw.split(',')
        .map(|entry| {
            let (partition, addr) = entry.split_once('=').ok_or_else(|| {
                ConfigError::InvalidEnv(
                    "GRAPH_PARTITION_NODE_ADDRS",
                    format!("expected \"<partition_id>=<host:port>\", got {entry:?}"),
                )
            })?;
            let partition: u32 = partition.parse().map_err(|_| {
                ConfigError::InvalidEnv(
                    "GRAPH_PARTITION_NODE_ADDRS",
                    format!("{partition:?} is not a valid partition id"),
                )
            })?;
            let addr: SocketAddr = addr.parse().map_err(|_| {
                ConfigError::InvalidEnv(
                    "GRAPH_PARTITION_NODE_ADDRS",
                    format!("{addr:?} is not a valid socket address"),
                )
            })?;
            Ok((partition, addr))
        })
        .collect()
}

fn parse_env<T: std::str::FromStr>(key: &'static str, default: T) -> Result<T, ConfigError> {
    match std::env::var(key) {
        Ok(v) => v
            .parse()
            .map_err(|_| ConfigError::InvalidEnv(key, v.clone())),
        Err(_) => Ok(default),
    }
}
