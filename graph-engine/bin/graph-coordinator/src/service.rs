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
//!
//! *(least-privilege-via-telemetry use case)* `RETURN` can now project a
//! bare alias (whole node/edge, existing behavior) or `alias.property`
//! (one scalar) — see `execute_query`'s projection-building code for how
//! it tells a node alias from an edge alias (checked against the actual
//! bindings, since the wire/plan layers don't carry that distinction
//! explicitly) and routes each to `GetNodeProperties`/`GetEdgeProperties`
//! accordingly.

// tonic::Status (176 bytes) is the framework's standard RPC error type -
// every handler in a tonic service returns Result<_, Status> by
// convention, so this lint would need boxing Status everywhere rather
// than flagging a real problem here.
#![allow(clippy::result_large_err)]

use crate::grpc_partition_rpc::GrpcPartitionRpc;
use graph_cluster::{Discovery, PartitionHasher};
use graph_dsl::{Parser as DslParser, PestParser, ReturnItem, SchemaValidator, Validator};
use graph_proto::v1::graph_service_server::GraphService;
use graph_proto::v1::value::Kind as ValueKind;
use graph_proto::v1::{
    health_check_response, EdgeProperties, ExecuteQueryRequest, GetIndexStatusRequest,
    GetSchemaRequest, HealthCheckRequest, HealthCheckResponse, IndexStatusResponse, NodeProperties,
    QueryResult, SchemaResponse, Value as ProtoValue,
};
use graph_query::{Binding, NaivePlanner, Planner, ScatterGatherExecutor};
use graph_schema::{EdgeId, NodeId, Schema};
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

        // *(least-privilege-via-telemetry use case)* An alias is
        // exclusively a node alias or an edge alias, fixed by the
        // pattern — but nothing at this layer records which, so it's
        // read off the first binding that actually has it bound. A
        // `RETURN` alias that never appears in any binding at all (a
        // valid but empty result set) is simply never looked up below.
        let mut alias_is_edge: HashMap<&str, bool> = HashMap::new();
        for item in &project {
            if alias_is_edge.contains_key(item.alias.as_str()) {
                continue;
            }
            let kind = bindings.iter().find_map(|b| {
                if b.edges.contains_key(&item.alias) {
                    Some(true)
                } else if b.nodes.contains_key(&item.alias) {
                    Some(false)
                } else {
                    None
                }
            });
            if let Some(is_edge) = kind {
                alias_is_edge.insert(item.alias.as_str(), is_edge);
            }
        }

        // Every distinct node/edge id that will end up in some row's
        // projection, deduped once up front so a result set with
        // repeated nodes/edges across rows still costs one
        // GetNodeProperties/GetEdgeProperties round-trip per owning
        // partition rather than one per row.
        let needed_node_ids: Vec<NodeId> = bindings
            .iter()
            .flat_map(|binding| {
                project
                    .iter()
                    .filter_map(|item| resolve_node_id(item, &alias_is_edge, binding))
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let needed_edge_ids: Vec<EdgeId> = bindings
            .iter()
            .flat_map(|binding| {
                project
                    .iter()
                    .filter_map(|item| resolve_edge_id(item, &alias_is_edge, binding))
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let node_properties = if needed_node_ids.is_empty() {
            HashMap::new()
        } else {
            self.executor
                .get_node_properties(&needed_node_ids, &partitions)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        };
        let edge_properties = if needed_edge_ids.is_empty() {
            HashMap::new()
        } else {
            self.executor
                .get_edge_properties(&needed_edge_ids, &partitions)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        };

        let results: Vec<Result<QueryResult, Status>> = bindings
            .into_iter()
            .map(|binding| {
                let mut projection: HashMap<String, ProtoValue> = HashMap::new();
                let mut properties: HashMap<String, NodeProperties> = HashMap::new();
                let mut edge_props_out: HashMap<String, EdgeProperties> = HashMap::new();

                for item in &project {
                    let is_edge = alias_is_edge
                        .get(item.alias.as_str())
                        .copied()
                        .unwrap_or(false);
                    match &item.property {
                        None => {
                            // Bare alias: project the whole node/edge —
                            // its id (bit-cast into int64_value, same
                            // technique graph-storage uses at the
                            // Iceberg boundary — `Value` has no
                            // unsigned-integer case) plus its full
                            // hydrated record.
                            if is_edge {
                                if let Some(&id) = binding.edges.get(&item.alias) {
                                    projection.insert(item.alias.clone(), int64_value(id.0));
                                    if let Some(record) = edge_properties.get(&id) {
                                        edge_props_out.insert(
                                            item.alias.clone(),
                                            edge_record_to_proto(record),
                                        );
                                    }
                                }
                            } else if let Some(&id) = binding.nodes.get(&item.alias) {
                                projection.insert(item.alias.clone(), int64_value(id.0));
                                if let Some(record) = node_properties.get(&id) {
                                    properties
                                        .insert(item.alias.clone(), node_record_to_proto(record));
                                }
                            }
                        }
                        Some(property) => {
                            // `alias.property`: project just the one
                            // resolved scalar, keyed by `"alias.property"`
                            // — the flat `projection` map has no room for
                            // two different properties of the same alias
                            // under one key, so the property name has to
                            // be folded into the key itself.
                            let value = if is_edge {
                                binding
                                    .edges
                                    .get(&item.alias)
                                    .and_then(|id| edge_properties.get(id))
                                    .and_then(|record| record.properties.get(property))
                            } else {
                                binding
                                    .nodes
                                    .get(&item.alias)
                                    .and_then(|id| node_properties.get(id))
                                    .and_then(|record| record.properties.get(property))
                            };
                            if let Some(value) = value.and_then(property_value_to_proto) {
                                projection.insert(format!("{}.{property}", item.alias), value);
                            }
                        }
                    }
                }

                Ok(QueryResult {
                    projection,
                    properties,
                    edge_properties: edge_props_out,
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

fn resolve_node_id(
    item: &ReturnItem,
    alias_is_edge: &HashMap<&str, bool>,
    binding: &Binding,
) -> Option<NodeId> {
    if alias_is_edge
        .get(item.alias.as_str())
        .copied()
        .unwrap_or(false)
    {
        return None;
    }
    binding.nodes.get(&item.alias).copied()
}

fn resolve_edge_id(
    item: &ReturnItem,
    alias_is_edge: &HashMap<&str, bool>,
    binding: &Binding,
) -> Option<EdgeId> {
    if !alias_is_edge
        .get(item.alias.as_str())
        .copied()
        .unwrap_or(false)
    {
        return None;
    }
    binding.edges.get(&item.alias).copied()
}

fn int64_value(id: u64) -> ProtoValue {
    ProtoValue {
        kind: Some(ValueKind::Int64Value(id as i64)),
    }
}

fn node_record_to_proto(record: &graph_index::NodeRecord) -> NodeProperties {
    NodeProperties {
        label: record.label.0.clone(),
        fields: record
            .properties
            .iter()
            .filter_map(|(name, value)| property_value_to_proto(value).map(|v| (name.clone(), v)))
            .collect(),
    }
}

/// Edge-alias counterpart of `node_record_to_proto` (least-privilege
/// use case's `RETURN g` / `g.action`).
fn edge_record_to_proto(record: &graph_index::EdgeRecord) -> EdgeProperties {
    EdgeProperties {
        edge_type: record.edge_type.0.clone(),
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
