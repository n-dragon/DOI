//! *(task CO2)* Turns a [`QueryPlan`] into RPC calls against the
//! partition node — the coordinator side of the wire protocol
//! `graph-partition-node::service::PartitionServiceImpl` implements.
//! Mono-partition v1 calls the single configured replica directly for
//! every step; there's no scatter-gather (Q5-Q8/CO5) until
//! `graph-cluster` can route across more than one partition.

use graph_dsl::{ComparisonOp, Direction, Literal, PropertyFilter};
use graph_index::PropertyKey;
use graph_proto::v1::partition_service_client::PartitionServiceClient;
use graph_proto::v1::value::Kind;
use graph_proto::v1::{
    Binding as ProtoBinding, ComparisonOp as ProtoComparisonOp, ExpandHopRequest,
    PropertyFilter as ProtoPropertyFilter, ResolveStartRequest, Value as ProtoValue,
};
use graph_query::{PlanStep, QueryPlan};
use tonic::transport::Channel;
use tonic::Status;

pub async fn execute(
    client: &mut PartitionServiceClient<Channel>,
    plan: &QueryPlan,
) -> Result<Vec<ProtoBinding>, Status> {
    let mut steps = plan.steps.iter();
    let first = steps
        .next()
        .ok_or_else(|| Status::internal("a plan always has at least a ResolveStart step"))?;
    let PlanStep::ResolveStart {
        alias,
        label_or_type,
        property,
        key,
    } = first
    else {
        return Err(Status::internal("plan must start with ResolveStart"));
    };

    let request = ResolveStartRequest {
        alias: alias.clone(),
        label_or_type: label_or_type.clone(),
        property: property.clone(),
        key: Some(property_key_to_proto(key)),
    };
    let mut bindings = collect(client.resolve_start(request).await?).await?;

    for step in steps {
        let PlanStep::ExpandHop {
            from_alias,
            to_alias,
            to_label,
            edge_type,
            direction,
            hops,
            filters,
        } = step
        else {
            return Err(Status::internal(
                "every plan step after the first must be ExpandHop",
            ));
        };

        let request = ExpandHopRequest {
            frontier: bindings,
            from_alias: from_alias.clone(),
            to_alias: to_alias.clone(),
            to_label: to_label.clone().unwrap_or_default(),
            edge_type: edge_type.clone().unwrap_or_default(),
            incoming: matches!(direction, Direction::Incoming),
            hop_min: hops.min,
            hop_max: hops.max,
            filters: filters
                .iter()
                .map(property_filter_to_proto)
                .collect::<Vec<_>>(),
        };
        bindings = collect(client.expand_hop(request).await?).await?;
    }

    Ok(bindings)
}

async fn collect(
    response: tonic::Response<tonic::codec::Streaming<ProtoBinding>>,
) -> Result<Vec<ProtoBinding>, Status> {
    use futures::StreamExt;
    let mut stream = response.into_inner();
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        out.push(item?);
    }
    Ok(out)
}

fn property_key_to_proto(key: &PropertyKey) -> ProtoValue {
    let kind = match key {
        PropertyKey::Int64(v) => Kind::Int64Value(*v),
        PropertyKey::Bool(v) => Kind::BoolValue(*v),
        PropertyKey::String(v) => Kind::StringValue(v.clone()),
        PropertyKey::Timestamp(v) => Kind::TimestampValue(*v),
    };
    ProtoValue { kind: Some(kind) }
}

fn literal_to_proto(literal: &Literal) -> ProtoValue {
    let kind = match literal {
        Literal::Int64(v) => Kind::Int64Value(*v),
        Literal::Float64(v) => Kind::Float64Value(*v),
        Literal::Bool(v) => Kind::BoolValue(*v),
        Literal::String(v) => Kind::StringValue(v.clone()),
    };
    ProtoValue { kind: Some(kind) }
}

fn comparison_op_to_proto(op: ComparisonOp) -> ProtoComparisonOp {
    match op {
        ComparisonOp::Eq => ProtoComparisonOp::Eq,
        ComparisonOp::Ne => ProtoComparisonOp::Ne,
        ComparisonOp::Lt => ProtoComparisonOp::Lt,
        ComparisonOp::Lte => ProtoComparisonOp::Lte,
        ComparisonOp::Gt => ProtoComparisonOp::Gt,
        ComparisonOp::Gte => ProtoComparisonOp::Gte,
    }
}

fn property_filter_to_proto(filter: &PropertyFilter) -> ProtoPropertyFilter {
    ProtoPropertyFilter {
        property: filter.property.clone(),
        op: comparison_op_to_proto(filter.op) as i32,
        value: Some(literal_to_proto(&filter.value)),
    }
}
