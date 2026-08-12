//! `graph-coordinator`'s internals, exposed as a library for the same
//! reason `graph-partition-node` is: CO4's integration test needs a real
//! `GraphServiceImpl` reachable from outside the binary crate.

pub mod config;
pub mod grpc_partition_rpc;
pub mod service;
