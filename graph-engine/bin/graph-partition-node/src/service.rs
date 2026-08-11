//! `PartitionServiceImpl` (tasks PN3, PN4): the gRPC surface a
//! `graph-coordinator` drives one hop at a time (spec §7.4). Pure
//! translation layer — the actual traversal logic lives in
//! `graph_query::SimpleLocalExecutor`, this just marshals wire types to
//! and from it.

// tonic::Status (176 bytes) is the framework's standard RPC error type -
// every handler in a tonic service returns Result<_, Status> by
// convention, so this lint would need boxing Status everywhere rather
// than flagging a real problem here.
#![allow(clippy::result_large_err)]

use graph_dsl::{ComparisonOp, Direction, HopRange, Literal, PropertyFilter};
use graph_index::{GenerationHandle, PropertyKey};
use graph_proto::v1::partition_service_server::PartitionService;
use graph_proto::v1::value::Kind;
use graph_proto::v1::{
    health_check_response, Binding as ProtoBinding, ComparisonOp as ProtoComparisonOp,
    ExpandHopRequest, GetIndexStatusRequest, HealthCheckRequest, HealthCheckResponse,
    IndexStatusResponse, PropertyFilter as ProtoPropertyFilter, ResolveStartRequest,
    Value as ProtoValue,
};
use graph_query::{Binding, LocalExecutor, PlanStep, SimpleLocalExecutor};
use graph_schema::NodeId;
use std::pin::Pin;
use std::sync::Arc;
use tonic::{Request, Response, Status};

type BindingStream = Pin<Box<dyn futures::Stream<Item = Result<ProtoBinding, Status>> + Send>>;

pub struct PartitionServiceImpl {
    generation: Arc<GenerationHandle>,
}

impl PartitionServiceImpl {
    pub fn new(generation: Arc<GenerationHandle>) -> Self {
        Self { generation }
    }
}

#[tonic::async_trait]
impl PartitionService for PartitionServiceImpl {
    type ResolveStartStream = BindingStream;
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
        };

        let result = SimpleLocalExecutor
            .expand_hop(&self.generation, &step, &frontier)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(bindings_to_stream(result.local)))
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

    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        Ok(Response::new(HealthCheckResponse {
            status: health_check_response::Status::Serving as i32,
        }))
    }
}

fn bindings_to_stream(bindings: Vec<Binding>) -> BindingStream {
    let items: Vec<Result<ProtoBinding, Status>> =
        bindings.iter().map(|b| Ok(binding_to_proto(b))).collect();
    Box::pin(futures::stream::iter(items))
}

fn binding_to_proto(binding: &Binding) -> ProtoBinding {
    ProtoBinding {
        node_ids_by_alias: binding
            .iter()
            .map(|(alias, id)| (alias.clone(), id.0))
            .collect(),
    }
}

fn proto_binding_to_binding(proto: &ProtoBinding) -> Binding {
    proto
        .node_ids_by_alias
        .iter()
        .map(|(alias, id)| (alias.clone(), NodeId(*id)))
        .collect()
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
