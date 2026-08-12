//! `PartitionServiceImpl` (tasks PN3, PN4): the gRPC surface a
//! `graph-coordinator` drives one hop at a time (spec §7.4). Pure
//! translation layer — the actual traversal logic lives in
//! `graph_query::SimpleLocalExecutor`, this just marshals wire types to
//! and from it.
//!
//! Extended for the least-privilege-via-telemetry use case with
//! `resolve_all`, `check_anti_join`, and `get_edge_properties` handlers
//! — the partition-side counterparts of `graph_query::LocalExecutor`'s
//! own extensions.

// tonic::Status (176 bytes) is the framework's standard RPC error type -
// every handler in a tonic service returns Result<_, Status> by
// convention, so this lint would need boxing Status everywhere rather
// than flagging a real problem here.
#![allow(clippy::result_large_err)]

use graph_dsl::{ComparisonOp, Direction, HopRange, Literal, PropertyFilter};
use graph_index::{EdgeRecord, GenerationHandle, NodeRecord, PartitionId, PropertyKey};
use graph_observability::metrics;
use graph_proto::v1::partition_service_server::PartitionService;
use graph_proto::v1::value::Kind;
use graph_proto::v1::{
    health_check_response, Binding as ProtoBinding, CheckAntiJoinRequest, CheckAntiJoinResponse,
    ComparisonOp as ProtoComparisonOp, EdgeProperties, ExpandHopRequest, GetEdgePropertiesRequest,
    GetEdgePropertiesResponse, GetIndexStatusRequest, GetNodePropertiesRequest,
    GetNodePropertiesResponse, HealthCheckRequest, HealthCheckResponse, IndexStatusResponse,
    NodeProperties, PropertyFilter as ProtoPropertyFilter, ResolveAllRequest, ResolveStartRequest,
    Value as ProtoValue,
};
use graph_query::{AntiJoinStep, Binding, LocalExecutor, PlanStep, SimpleLocalExecutor};
use graph_schema::{EdgeId, NodeId};
use graph_storage::PropertyValue;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tonic::{Request, Response, Status};

type BindingStream = Pin<Box<dyn futures::Stream<Item = Result<ProtoBinding, Status>> + Send>>;

pub struct PartitionServiceImpl {
    generation: Arc<GenerationHandle>,
    partition: PartitionId,
}

impl PartitionServiceImpl {
    pub fn new(generation: Arc<GenerationHandle>, partition: PartitionId) -> Self {
        Self {
            generation,
            partition,
        }
    }
}

#[tonic::async_trait]
impl PartitionService for PartitionServiceImpl {
    type ResolveStartStream = BindingStream;
    type ResolveAllStream = BindingStream;
    type ExpandHopStream = BindingStream;

    /// *(task PN3)* Delegates to `LocalExecutor::resolve_start`.
    async fn resolve_start(
        &self,
        request: Request<ResolveStartRequest>,
    ) -> Result<Response<Self::ResolveStartStream>, Status> {
        let req = request.into_inner();
        let key = proto_value_to_property_key(
            req.key
                .ok_or_else(|| Status::invalid_argument("missing key"))?,
        )?;
        let step = PlanStep::ResolveStart {
            alias: req.alias,
            label_or_type: req.label_or_type,
            property: req.property,
            key,
        };

        let bindings = SimpleLocalExecutor
            .resolve_start(&self.generation, &step)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(bindings_to_stream(bindings)))
    }

    /// *(least-privilege-via-telemetry use case)* Delegates to
    /// `LocalExecutor::resolve_all`.
    async fn resolve_all(
        &self,
        request: Request<ResolveAllRequest>,
    ) -> Result<Response<Self::ResolveAllStream>, Status> {
        let req = request.into_inner();
        let step = PlanStep::ResolveAll {
            alias: req.alias,
            label: req.label,
        };

        let bindings = SimpleLocalExecutor
            .resolve_all(&self.generation, &step)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(bindings_to_stream(bindings)))
    }

    /// *(task PN4)* Delegates to `LocalExecutor::expand_hop`.
    async fn expand_hop(
        &self,
        request: Request<ExpandHopRequest>,
    ) -> Result<Response<Self::ExpandHopStream>, Status> {
        let req = request.into_inner();
        let frontier: Vec<Binding> = req.frontier.iter().map(proto_binding_to_binding).collect();
        let filters = req
            .filters
            .into_iter()
            .map(proto_filter_to_property_filter)
            .collect::<Result<Vec<_>, _>>()?;

        let step = PlanStep::ExpandHop {
            from_alias: req.from_alias,
            to_alias: req.to_alias,
            to_label: (!req.to_label.is_empty()).then_some(req.to_label),
            edge_type: (!req.edge_type.is_empty()).then_some(req.edge_type),
            direction: if req.incoming {
                Direction::Incoming
            } else {
                Direction::Outgoing
            },
            hops: HopRange {
                min: req.hop_min,
                max: req.hop_max,
            },
            filters,
            edge_alias: (!req.edge_alias.is_empty()).then_some(req.edge_alias),
        };

        let start = Instant::now();
        let result = SimpleLocalExecutor
            .expand_hop(&self.generation, &step, &frontier)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        metrics::HOP_LATENCY
            .with_label_values(&[&self.partition.0.to_string()])
            .observe(start.elapsed().as_secs_f64());

        // *(task CO5)* `result.remote` (task IX3/Q6's cross-partition
        // continuations) is merged into the same wire stream as
        // `result.local` rather than getting its own message shape — the
        // coordinator's `ScatterGatherExecutor::one_round` re-derives
        // which partition owns each returned binding via
        // `PartitionHasher` on *every* round regardless of how the
        // binding got here, so it never actually needs to know which of
        // these two Rust-side buckets a binding came from once it's back
        // on the wire. Flattening here avoids a proto/wire contract
        // change entirely. In mono-partition mode (`n_partitions: 1`,
        // spec §12 Phase 1) `result.remote` is always empty (IX3 never
        // populates a `RemoteRef` when every node hashes to the same
        // partition it's already local to), so this is a no-op for CO2's
        // existing mono-partition path.
        let bindings: Vec<Binding> = result
            .local
            .into_iter()
            .chain(result.remote.into_iter().map(|(_, binding)| binding))
            .collect();
        Ok(Response::new(bindings_to_stream(bindings)))
    }

    /// *(least-privilege-via-telemetry use case)* Delegates to
    /// `LocalExecutor::check_anti_join` — `req.conditions` are always
    /// already fully resolved literals by the time they arrive here (see
    /// `graph_query::distributed_executor`'s module doc comment).
    /// Unary, not streaming: the response is just which of the request's
    /// `frontier` entries survived, by index, not a re-serialization of
    /// the bindings themselves.
    async fn check_anti_join(
        &self,
        request: Request<CheckAntiJoinRequest>,
    ) -> Result<Response<CheckAntiJoinResponse>, Status> {
        let req = request.into_inner();
        let frontier: Vec<Binding> = req.frontier.iter().map(proto_binding_to_binding).collect();
        let conditions = req
            .conditions
            .into_iter()
            .map(proto_filter_to_property_filter)
            .collect::<Result<Vec<_>, _>>()?;

        let step = AntiJoinStep {
            from_alias: req.from_alias,
            to_alias: req.to_alias,
            direction: if req.incoming {
                Direction::Incoming
            } else {
                Direction::Outgoing
            },
            edge_alias: (!req.edge_alias.is_empty()).then_some(req.edge_alias),
            edge_type: (!req.edge_type.is_empty()).then_some(req.edge_type),
            // Unused by `LocalExecutor::check_anti_join` (it takes the
            // already-resolved `conditions` slice as its own parameter,
            // see this RPC's doc comment) — only the routing fields
            // above matter for this step value.
            literal_conditions: Vec::new(),
            outer_conditions: Vec::new(),
        };

        let survivors = SimpleLocalExecutor
            .check_anti_join(&self.generation, &step, &conditions, &frontier)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Indices into the *request's* frontier, per the RPC's contract
        // (`CheckAntiJoinResponse.surviving_indices`'s doc comment) —
        // `SimpleLocalExecutor::check_anti_join` returns the surviving
        // `Binding`s themselves, not indices, so this maps back by exact
        // equality (`Binding: PartialEq`). Frontier entries are never
        // literally duplicated within one request (each corresponds to a
        // distinct upstream match), so this is unambiguous.
        let surviving_indices = frontier
            .iter()
            .enumerate()
            .filter_map(|(i, b)| survivors.contains(b).then_some(i as u32))
            .collect();

        Ok(Response::new(CheckAntiJoinResponse { surviving_indices }))
    }

    async fn get_index_status(
        &self,
        _request: Request<GetIndexStatusRequest>,
    ) -> Result<Response<IndexStatusResponse>, Status> {
        let generation = self.generation.acquire();
        let pinned_snapshot_by_table = generation
            .meta
            .snapshot_by_table
            .iter()
            .map(|(table, snapshot)| (table.clone(), snapshot.0))
            .collect();

        Ok(Response::new(IndexStatusResponse {
            pinned_snapshot_by_table,
            last_rebuilt_unix_ms: generation.meta.built_at_unix_ms,
            node_count: generation.meta.node_count,
            edge_count: generation.meta.edge_count,
        }))
    }

    /// Point lookup used by the coordinator to hydrate a query's results
    /// with real property data once traversal has resolved which
    /// `NodeId`s matched — silently skips any id this generation doesn't
    /// know (e.g. one from a since-rebuilt older generation) rather than
    /// erroring the whole batch.
    async fn get_node_properties(
        &self,
        request: Request<GetNodePropertiesRequest>,
    ) -> Result<Response<GetNodePropertiesResponse>, Status> {
        let generation = self.generation.acquire();
        let req = request.into_inner();

        let properties = req
            .node_ids
            .into_iter()
            .filter_map(|id| {
                generation
                    .node_records
                    .get(&NodeId(id))
                    .map(|record| (id, node_record_to_proto(record)))
            })
            .collect();

        Ok(Response::new(GetNodePropertiesResponse { properties }))
    }

    /// Edge-alias counterpart of `get_node_properties` (least-privilege
    /// use case's `g.action`) — same point-lookup-by-id shape, against
    /// `edge_records` instead of `node_records`. Only ever finds an edge
    /// whose *source* this partition owns (`graph-index`'s builder) —
    /// silently skipped otherwise, same "not every id resolves, that's
    /// fine" posture as `get_node_properties`.
    async fn get_edge_properties(
        &self,
        request: Request<GetEdgePropertiesRequest>,
    ) -> Result<Response<GetEdgePropertiesResponse>, Status> {
        let generation = self.generation.acquire();
        let req = request.into_inner();

        let properties = req
            .edge_ids
            .into_iter()
            .filter_map(|id| {
                generation
                    .edge_records
                    .get(&EdgeId(id))
                    .map(|record| (id, edge_record_to_proto(record)))
            })
            .collect();

        Ok(Response::new(GetEdgePropertiesResponse { properties }))
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

fn node_record_to_proto(record: &NodeRecord) -> NodeProperties {
    NodeProperties {
        label: record.label.0.clone(),
        fields: record
            .properties
            .iter()
            .filter_map(|(name, value)| property_value_to_proto(value).map(|v| (name.clone(), v)))
            .collect(),
    }
}

fn edge_record_to_proto(record: &EdgeRecord) -> EdgeProperties {
    EdgeProperties {
        edge_type: record.edge_type.0.clone(),
        fields: record
            .properties
            .iter()
            .filter_map(|(name, value)| property_value_to_proto(value).map(|v| (name.clone(), v)))
            .collect(),
    }
}

/// `List`/`Null` have no `Value` oneof variant (same gap `PropertyKey`
/// has, for the same reason — see its doc comment) — dropped from the
/// projected field set rather than erroring the whole record over one
/// unrepresentable property.
fn property_value_to_proto(value: &PropertyValue) -> Option<ProtoValue> {
    let kind = match value {
        PropertyValue::Int64(v) => Kind::Int64Value(*v),
        PropertyValue::Float64(v) => Kind::Float64Value(*v),
        PropertyValue::Bool(v) => Kind::BoolValue(*v),
        PropertyValue::String(v) => Kind::StringValue(v.clone()),
        PropertyValue::Timestamp(v) => Kind::TimestampValue(*v),
        PropertyValue::Bytes(v) => Kind::BytesValue(v.clone()),
        PropertyValue::List(_) | PropertyValue::Null => return None,
    };
    Some(ProtoValue { kind: Some(kind) })
}

fn bindings_to_stream(bindings: Vec<Binding>) -> BindingStream {
    let items: Vec<Result<ProtoBinding, Status>> =
        bindings.iter().map(|b| Ok(binding_to_proto(b))).collect();
    Box::pin(futures::stream::iter(items))
}

fn binding_to_proto(binding: &Binding) -> ProtoBinding {
    ProtoBinding {
        node_ids_by_alias: binding
            .nodes
            .iter()
            .map(|(alias, id)| (alias.clone(), id.0))
            .collect(),
        edge_ids_by_alias: binding
            .edges
            .iter()
            .map(|(alias, id)| (alias.clone(), id.0))
            .collect(),
    }
}

fn proto_binding_to_binding(proto: &ProtoBinding) -> Binding {
    Binding {
        nodes: proto
            .node_ids_by_alias
            .iter()
            .map(|(alias, id)| (alias.clone(), NodeId(*id)))
            .collect::<HashMap<_, _>>(),
        edges: proto
            .edge_ids_by_alias
            .iter()
            .map(|(alias, id)| (alias.clone(), EdgeId(*id)))
            .collect::<HashMap<_, _>>(),
    }
}

fn proto_value_to_property_key(value: ProtoValue) -> Result<PropertyKey, Status> {
    match value.kind {
        Some(Kind::Int64Value(v)) => Ok(PropertyKey::Int64(v)),
        Some(Kind::BoolValue(v)) => Ok(PropertyKey::Bool(v)),
        Some(Kind::StringValue(v)) => Ok(PropertyKey::String(v)),
        Some(Kind::TimestampValue(v)) => Ok(PropertyKey::Timestamp(v)),
        Some(Kind::Float64Value(_)) | Some(Kind::BytesValue(_)) => Err(Status::invalid_argument(
            "PropertyIndex has no key variant for float/bytes values",
        )),
        None => Err(Status::invalid_argument("missing Value.kind")),
    }
}

fn proto_value_to_literal(value: ProtoValue) -> Result<Literal, Status> {
    match value.kind {
        Some(Kind::Int64Value(v)) => Ok(Literal::Int64(v)),
        Some(Kind::Float64Value(v)) => Ok(Literal::Float64(v)),
        Some(Kind::BoolValue(v)) => Ok(Literal::Bool(v)),
        Some(Kind::StringValue(v)) => Ok(Literal::String(v)),
        Some(Kind::TimestampValue(_)) | Some(Kind::BytesValue(_)) => Err(Status::invalid_argument(
            "WHERE filters don't support timestamp/bytes literals in v1",
        )),
        None => Err(Status::invalid_argument("missing Value.kind")),
    }
}

fn proto_comparison_op_to_op(op: i32) -> Result<ComparisonOp, Status> {
    match ProtoComparisonOp::try_from(op)
        .map_err(|_| Status::invalid_argument("unknown ComparisonOp"))?
    {
        ProtoComparisonOp::Eq => Ok(ComparisonOp::Eq),
        ProtoComparisonOp::Ne => Ok(ComparisonOp::Ne),
        ProtoComparisonOp::Lt => Ok(ComparisonOp::Lt),
        ProtoComparisonOp::Lte => Ok(ComparisonOp::Lte),
        ProtoComparisonOp::Gt => Ok(ComparisonOp::Gt),
        ProtoComparisonOp::Gte => Ok(ComparisonOp::Gte),
    }
}

fn proto_filter_to_property_filter(proto: ProtoPropertyFilter) -> Result<PropertyFilter, Status> {
    Ok(PropertyFilter {
        property: proto.property,
        op: proto_comparison_op_to_op(proto.op)?,
        value: proto_value_to_literal(
            proto
                .value
                .ok_or_else(|| Status::invalid_argument("missing filter value"))?,
        )?,
    })
}
