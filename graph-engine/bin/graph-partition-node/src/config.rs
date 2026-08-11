//! *(task PN5)* Runtime config, read once from the environment at
//! startup. No config file/flags — a `StatefulSet` (§10) sets these via
//! its pod spec, which is simplest as plain env vars.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

pub struct Config {
    /// Path to a `.graphschema` IDL file (§3.4) — same file every
    /// partition replica and the coordinator must agree on.
    pub schema_path: PathBuf,
    /// Local-filesystem `FileIO` root where Iceberg table data lives
    /// (ST1's dev setup).
    pub warehouse_path: PathBuf,
    /// SQLite file backing the Iceberg catalog's table registry (ST1's
    /// dev setup, revised: a persistent catalog, not `MemoryCatalog` -
    /// the registry has to survive being a different process than
    /// whatever ingested the data, which `MemoryCatalog`'s in-process
    /// registry cannot).
    pub catalog_db_path: PathBuf,
    pub namespace: String,
    pub partition_id: u32,
    /// Informational only in Phase 1 (mono-partition, IX3 is a no-op) -
    /// not yet used to filter which nodes/edges this replica owns. Read
    /// now so it's already part of the config surface once Phase 2's
    /// partition-aware filtering needs it.
    pub n_partitions: u32,
    pub rebuild_interval: Duration,
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
        let warehouse_path = std::env::var("GRAPH_WAREHOUSE_PATH")
            .unwrap_or_else(|_| "./warehouse".to_string())
            .into();
        let catalog_db_path = std::env::var("GRAPH_CATALOG_DB_PATH")
            .unwrap_or_else(|_| "./catalog.sqlite".to_string())
            .into();
        let namespace = std::env::var("GRAPH_NAMESPACE").unwrap_or_else(|_| "graph".to_string());
        let partition_id = parse_env("GRAPH_PARTITION_ID", 0)?;
        let n_partitions = parse_env("GRAPH_N_PARTITIONS", 1)?;
        let rebuild_interval_secs: u64 = parse_env("GRAPH_REBUILD_INTERVAL_SECS", 30)?;
        let listen_addr = parse_env("GRAPH_PARTITION_LISTEN_ADDR", "0.0.0.0:7001".to_string())?
            .parse()
            .map_err(|_| {
                ConfigError::InvalidEnv(
                    "GRAPH_PARTITION_LISTEN_ADDR",
                    "not a valid socket address".to_string(),
                )
            })?;

        Ok(Self {
            schema_path,
            warehouse_path,
            catalog_db_path,
            namespace,
            partition_id,
            n_partitions,
            rebuild_interval: Duration::from_secs(rebuild_interval_secs),
            listen_addr,
        })
    }
}

fn parse_env<T: FromStr>(key: &'static str, default: T) -> Result<T, ConfigError> {
    match std::env::var(key) {
        Ok(v) => v
            .parse()
            .map_err(|_| ConfigError::InvalidEnv(key, v.clone())),
        Err(_) => Ok(default),
    }
}
