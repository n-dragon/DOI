//! `GraphServiceImpl` (tasks CO1-CO3, CO5): the client-facing surface
//! (spec §8.2) — parses/validates/plans a DSL query, drives it against
//! the cluster via `graph_query::ScatterGatherExecutor` (tasks Q5-Q7),
//! and streams projected results.
//!
//! *(task CO5)* Replaces the mono-partition-only direct single-client
//! call CO2 originally used (`remote_executor.rs`, now removed) with the
//! real scatter-gather path: every query polls `graph_cluster::Discovery`
//! for the current cluster membership, then routes through however many
//! partitions the query actually touches — one partition and one replica
//! for the still-fully-supported mono-partition case, `n_partitions`-many
//! otherwise. There's no separate mono-partition code path to keep in
//! sync with the distributed one; `ScatterGatherExecutor` handles
//! `n_partitions: 1` correctly already (every hop routes to the same
//! single partition, `ResolveStart`'s broadcast has exactly one partition
//! to broadcast to) — see `graph-query`'s Q8 test for this working with
//! more than one partition.

// tonic::Status (176 bytes) is the framework's standard RPC error type -
// every handler in a tonic service returns Result<_, Status> by
// convention, so this lint would need boxing Status everywhere rather
// than flagging a real problem here.
#![allow(clippy::result_large_err)]

use crate::grpc_partition_rpc::GrpcPartitionRpc;
use graph_cluster::{Discovery, PartitionHasher};
use graph_dsl::{Parser as DslParser, PestParser, SchemaValidator, Validator};
use graph_proto::v1::graph_service_server::GraphService;
use graph_proto::v1::value::Kind as ValueKind;
use graph_proto::v1::{
    health_check_response, ExecuteQueryRequest, GetIndexStatusRequest, GetSchemaRequest,
    HealthCheckRequest, HealthCheckResponse, IndexStatusResponse, QueryResult, SchemaResponse,
    Value as ProtoValue,
};
use graph_query::{NaivePlanner, Planner, ScatterGatherExecutor};
use graph_schema::Schema;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tonic::{Request, Response, Status};

type QueryResultStream = Pin<Box<dyn futures::Stream<Item = Result<QueryResult, Status>> + Send>>;

pub struct GraphServiceImpl {
    schema: Schema,
    schema_idl: String,
    discovery: Arc<dyn Discovery + Send + Sync>,
    executor: ScatterGatherExecutor<GrpcPartitionRpc>,
}

impl GraphServiceImpl {
    pub fn new(
        schema: Schema,
        schema_idl: String,
        discovery: Arc<dyn Discovery + Send + Sync>,
        n_partitions: u32,
    ) -> Self {
        Self {
            schema,
            schema_idl,
            discovery,
            executor: ScatterGatherExecutor::new(
                GrpcPartitionRpc::new(),
                PartitionHasher { n_partitions },
            ),
        }
    }
}

#[tonic::async_trait]
impl GraphService for GraphServiceImpl {
    type ExecuteQueryStream = QueryResultStream;

    /// *(tasks CO2, CO5)* parse -> validate (D7-D9) -> plan (Q1) ->
    /// execute (scatter-gather, Q5-Q7) -> project `RETURN` -> stream.
    /// Fail-fast: a syntax or validation error is returned before any RPC
    /// to a partition node is made (spec §7.2).
    async fn execute_query(
        &self,
        request: Request<ExecuteQueryRequest>,
    ) -> Result<Response<Self::ExecuteQueryStream>, Status> {
        let start = Instant::now();
        let dsl = request.into_inner().dsl;

        let query = PestParser.parse(&dsl).map_err(|e| {
            graph_observability::metrics::QUERY_ERRORS
                .with_label_values(&["parse"])
                .inc();
            Status::invalid_argument(format!("syntax error: {e}"))
        })?;
        SchemaValidator
            .validate(&query, &self.schema)
            .map_err(|errors| {
                graph_observability::metrics::QUERY_ERRORS
                    .with_label_values(&["validate"])
                    .inc();
                Status::invalid_argument(format!("query failed validation: {errors:?}"))
            })?;

        let plan = NaivePlanner.plan(&query);
        let hop_count = plan
            .steps
            .iter()
            .filter(|s| matches!(s, graph_query::PlanStep::ExpandHop { .. }))
            .count() as u64;

        let partitions = self.discovery.current_map().await.map_err(|e| {
            graph_observability::metrics::QUERY_ERRORS
                .with_label_values(&["discovery"])
                .inc();
            Status::unavailable(format!("cluster discovery failed: {e}"))
        })?;

        let bindings = self
            .executor
            .execute(&plan, &partitions)
            .await
            .map_err(|e| {
                graph_observability::metrics::QUERY_ERRORS
                    .with_label_values(&["execute"])
                    .inc();
                Status::internal(e.to_string())
            })?;

        let project = plan.project;

        // Every distinct NodeId that will end up in some row's
        // projection, deduped once up front so a result set with
        // repeated nodes across rows still costs one GetNodeProperties
        // round-trip per owning partition rather than one per row.
        let needed_ids: Vec<graph_schema::NodeId> = bindings
            .iter()
            .flat_map(|binding| {
                project
                    .iter()
                    .filter_map(|alias| binding.get(alias).copied())
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let properties_by_id = if needed_ids.is_empty() {
            HashMap::new()
        } else {
            self.executor
                .get_node_properties(&needed_ids, &partitions)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        };

        let results: Vec<Result<QueryResult, Status>> = bindings
            .into_iter()
            .map(|binding| {
                // NodeId is bit-cast into int64_value (Value has no
                // unsigned-integer case) — the same technique
                // graph-storage uses at the Iceberg boundary.
                let projection: std::collections::HashMap<String, ProtoValue> = project
                    .iter()
                    .filter_map(|alias| {
                        binding.get(alias).map(|&id| {
                            (
                                alias.clone(),
                                ProtoValue {
                                    kind: Some(ValueKind::Int64Value(id.0 as i64)),
                                },
                            )
                        })
                    })
                    .collect();
                let properties = project
                    .iter()
                    .filter_map(|alias| {
                        let id = *binding.get(alias)?;
                        let record = properties_by_id.get(&id)?;
                        Some((alias.clone(), node_record_to_proto(record)))
                    })
                    .collect();
                Ok(QueryResult {
                    projection,
                    properties,
                })
            })
            .collect();

        graph_observability::metrics::QUERY_LATENCY
            .with_label_values(&["ok"])
            .observe(start.elapsed().as_secs_f64());
        graph_observability::metrics::QUERY_HOPS
            .with_label_values(&["ok"])
            .inc_by(hop_count);

        Ok(Response::new(Box::pin(futures::stream::iter(results))))
    }

    /// *(task CO1)*
    async fn get_schema(
        &self,
        _request: Request<GetSchemaRequest>,
    ) -> Result<Response<SchemaResponse>, Status> {
        Ok(Response::new(SchemaResponse {
            schema_idl: self.schema_idl.clone(),
            version: self.schema.version.clone(),
        }))
    }

    /// *(task CO3)* Relays partition 0's `GetIndexStatus` as a
    /// representative sample — a single `IndexStatusResponse` (one
    /// snapshot-per-table map, one rebuild timestamp, §8.2) can't
    /// represent N partitions' worth of independently-rebuilt
    /// generations at once, and the spec doesn't define a cluster-wide
    /// aggregate shape for this RPC. Mono-partition v1 (§12 Phase 1) had
    /// exactly one partition to relay, so this was never ambiguous
    /// before; a proper multi-partition status view (e.g. min/max/
    /// distribution of staleness across partitions) is future work, not
    /// part of the CO5 task this method is named for — documented here
    /// as a known gap rather than silently faked.
    async fn get_index_status(
        &self,
        _request: Request<GetIndexStatusRequest>,
    ) -> Result<Response<IndexStatusResponse>, Status> {
        let partitions = self
            .discovery
            .current_map()
            .await
            .map_err(|e| Status::unavailable(format!("cluster discovery failed: {e}")))?;

        let partition = *partitions
            .replicas_by_partition
            .keys()
            .min_by_key(|p| p.0)
            .ok_or_else(|| Status::unavailable("no partitions currently discovered"))?;
        let replica = partitions
            .healthy_replicas(partition)
            .into_iter()
            .next()
            .ok_or_else(|| {
                Status::unavailable(format!("no healthy replica for partition {}", partition.0))
            })?;

        let channel = tonic::transport::Channel::from_shared(format!("http://{}", replica.address))
            .map_err(|e| Status::internal(e.to_string()))?
            .connect()
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;
        let mut client =
            graph_proto::v1::partition_service_client::PartitionServiceClient::new(channel);
        let response = client.get_index_status(GetIndexStatusRequest {}).await?;
        Ok(Response::new(response.into_inner()))
    }

    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        Ok(Response::new(HealthCheckResponse {
            status: health_check_response::Status::Serving as i32,
        }))
    }
}

fn node_record_to_proto(record: &graph_index::NodeRecord) -> graph_proto::v1::NodeProperties {
    graph_proto::v1::NodeProperties {
        label: record.label.0.clone(),
        fields: record
            .properties
            .iter()
            .filter_map(|(name, value)| property_value_to_proto(value).map(|v| (name.clone(), v)))
            .collect(),
    }
}

/// `List`/`Null` have no `Value` oneof variant (§8.2's wire schema, same
/// gap `bin/graph-partition-node`'s own conversion has) — dropped from
/// the projected field set rather than erroring the whole record over
/// one unrepresentable property.
fn property_value_to_proto(value: &graph_storage::PropertyValue) -> Option<ProtoValue> {
    use graph_storage::PropertyValue;
    let kind = match value {
        PropertyValue::Int64(v) => ValueKind::Int64Value(*v),
        PropertyValue::Float64(v) => ValueKind::Float64Value(*v),
        PropertyValue::Bool(v) => ValueKind::BoolValue(*v),
        PropertyValue::String(v) => ValueKind::StringValue(v.clone()),
        PropertyValue::Timestamp(v) => ValueKind::TimestampValue(*v),
        PropertyValue::Bytes(v) => ValueKind::BytesValue(v.clone()),
        PropertyValue::List(_) | PropertyValue::Null => return None,
    };
    Some(ProtoValue { kind: Some(kind) })
}
