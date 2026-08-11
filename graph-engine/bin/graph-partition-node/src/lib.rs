//! `graph-partition-node`'s internals, exposed as a library so
//! `graph-coordinator`'s CO4 integration test can spin up a real
//! `PartitionServiceImpl` in-process rather than shelling out to a
//! separately-built binary — the point of that test is exercising the
//! real gRPC wire path end to end, not process orchestration.

pub mod config;
pub mod rebuild;
pub mod service;
