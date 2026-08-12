//! *(task CO5)* Real [`graph_query::PartitionRpc`] implementation against
//! `graph-proto`'s generated `PartitionServiceClient` — the concrete
//! transport `graph-query::ScatterGatherExecutor` (tasks Q5-Q7) is
//! generic over. Everything upstream of this file (`graph-query`) has no
//! idea gRPC exists (see `distributed_executor.rs`'s doc comment); this
//! is the one place that translates the crate-agnostic `PlanStep`/
//! `Binding`/`Frontier` types to and from `graph-proto`'s wire messages,
//! same job `remote_executor.rs` did for the mono-partition-only CO2
//! path this supersedes.

use async_trait::async_trait;
use graph_cluster::ReplicaEndpoint;
use graph_dsl::{ComparisonOp, Direction, Literal, PropertyFilter};
use graph_index::{NodeRecord, PropertyKey};
use graph_proto::v1::partition_service_client::PartitionServiceClient;
use graph_proto::v1::value::Kind;
use graph_proto::v1::{
    Binding as ProtoBinding, ComparisonOp as ProtoComparisonOp, ExpandHopRequest,
    GetNodePropertiesRequest, PropertyFilter as ProtoPropertyFilter, ResolveStartRequest,
    Value as ProtoValue,
};
use graph_query::{Binding, ExecutionError, Frontier, PartitionRpc, PlanStep};
use graph_schema::NodeId;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use tokio::sync::Mutex;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

type InterceptorFn = fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>;
type Client = PartitionServiceClient<InterceptedService<Channel, InterceptorFn>>;

/// One lazily-connected, cached gRPC client per replica address. A
/// `graph-partition-node` replica's address only changes when
/// `graph-cluster::Discovery` reports a rebalance/failover (§6.2/§6.4) —
/// far less often than queries execute — so reconnecting on every call
/// would be pure overhead; this caches indefinitely for the process
/// lifetime rather than evicting stale entries, since a v1 without
/// `Discovery`-driven rebalancing wired all the way through
/// (`bin/graph-coordinator`'s own polling loop, not this RPC layer)
/// doesn't yet have a signal for "this address is gone for good" versus
/// "transiently unhealthy" to act on.
pub struct GrpcPartitionRpc {
    clients: Mutex<HashMap<SocketAddr, Client>>,
}

impl GrpcPartitionRpc {
    pub fn new() -> Self {
        Self {
            clients: Mutex::new(HashMap::new()),
        }
    }

    async fn client(&self, replica: &ReplicaEndpoint) -> Result<Client, ExecutionError> {
        let mut clients = self.clients.lock().await;
        if let Some(client) = clients.get(&replica.address) {
            return Ok(client.clone());
        }

        let channel = Channel::from_shared(format!("http://{}", replica.address))
            .map_err(|e| {
                ExecutionError::Rpc(format!("invalid replica address {}: {e}", replica.address))
            })?
            .connect()
            .await
            .map_err(|e| {
                ExecutionError::Rpc(format!(
                    "failed to connect to replica {}: {e}",
                    replica.address
                ))
            })?;

        // *(task OB3)* Every outgoing hop carries the current span's
        // trace context, so a query's fan-out across partitions stays
        // one connected trace in whatever OTLP collector is configured
        // (spec §9.2).
        let client = PartitionServiceClient::with_interceptor(
            channel,
            graph_observability::inject_trace_context as InterceptorFn,
        );
        clients.insert(replica.address, client.clone());
        Ok(client)
    }
}

impl Default for GrpcPartitionRpc {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PartitionRpc for GrpcPartitionRpc {
    async fn resolve_start(
        &self,
        replica: &ReplicaEndpoint,
        step: &PlanStep,
    ) -> Result<Vec<Binding>, ExecutionError> {
        let PlanStep::ResolveStart {
            alias,
            label_or_type,
            property,
            key,
        } = step
        else {
            return Err(ExecutionError::Rpc(
                "resolve_start called with a non-ResolveStart step".to_string(),
            ));
        };

        let mut client = self.client(replica).await?;
        let request = ResolveStartRequest {
            alias: alias.clone(),
            label_or_type: label_or_type.clone(),
            property: property.clone(),
            key: Some(property_key_to_proto(key)),
        };
        let stream = client
            .resolve_start(request)
            .await
            .map_err(|e| ExecutionError::Rpc(e.to_string()))?;
        let bindings = collect(stream).await?;
        Ok(bindings.iter().map(proto_binding_to_binding).collect())
    }

    async fn expand_hop(
        &self,
        replica: &ReplicaEndpoint,
        step: &PlanStep,
        frontier: &[Binding],
    ) -> Result<Frontier, ExecutionError> {
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
            return Err(ExecutionError::Rpc(
                "expand_hop called with a non-ExpandHop step".to_string(),
            ));
        };

        let mut client = self.client(replica).await?;
        let request = ExpandHopRequest {
            frontier: frontier.iter().map(binding_to_proto).collect(),
            from_alias: from_alias.clone(),
            to_alias: to_alias.clone(),
            to_label: to_label.clone().unwrap_or_default(),
            edge_type: edge_type.clone().unwrap_or_default(),
            incoming: matches!(direction, Direction::Incoming),
            hop_min: hops.min,
            hop_max: hops.max,
            filters: filters.iter().map(property_filter_to_proto).collect(),
        };
        let stream = client
            .expand_hop(request)
            .await
            .map_err(|e| ExecutionError::Rpc(e.to_string()))?;
        let bindings = collect(stream).await?;

        // *(task CO5)* `PartitionServiceImpl::expand_hop` (in
        // `bin/graph-partition-node`) already flattens its own
        // `Frontier.local`/`.remote` into one wire stream — see that
        // handler's doc comment for why the distinction doesn't need to
        // survive the RPC boundary. Everything comes back into `local`
        // here; `remote` stays empty since there's nothing left to route
        // any differently.
        Ok(Frontier {
            local: bindings.iter().map(proto_binding_to_binding).collect(),
            remote: Vec::new(),
        })
    }

    async fn get_node_properties(
        &self,
        replica: &ReplicaEndpoint,
        ids: &[NodeId],
    ) -> Result<HashMap<NodeId, NodeRecord>, ExecutionError> {
        let mut client = self.client(replica).await?;
        let response = client
            .get_node_properties(GetNodePropertiesRequest {
                node_ids: ids.iter().map(|id| id.0).collect(),
            })
            .await
            .map_err(|e| ExecutionError::Rpc(e.to_string()))?
            .into_inner();

        Ok(response
            .properties
            .into_iter()
            .map(|(id, props)| (NodeId(id), proto_node_properties_to_record(props)))
            .collect())
    }
}

async fn collect(
    response: tonic::Response<tonic::codec::Streaming<ProtoBinding>>,
) -> Result<Vec<ProtoBinding>, ExecutionError> {
    use futures::StreamExt;
    let mut stream = response.into_inner();
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        out.push(item.map_err(|e| ExecutionError::Rpc(e.to_string()))?);
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

/// Mirrors `bin/graph-partition-node/src/service.rs`'s
/// `property_value_to_proto` in reverse — `List`/`Null` have no `Value`
/// oneof variant on either side, so a property of one of those types
/// simply never round-trips through this RPC (same gap noted there).
fn proto_node_properties_to_record(proto: graph_proto::v1::NodeProperties) -> NodeRecord {
    let properties: BTreeMap<String, graph_storage::PropertyValue> = proto
        .fields
        .into_iter()
        .filter_map(|(name, value)| proto_value_to_property_value(value).map(|v| (name, v)))
        .collect();
    NodeRecord {
        label: graph_schema::Label(proto.label),
        properties,
    }
}

fn proto_value_to_property_value(value: ProtoValue) -> Option<graph_storage::PropertyValue> {
    use graph_storage::PropertyValue;
    match value.kind {
        Some(Kind::Int64Value(v)) => Some(PropertyValue::Int64(v)),
        Some(Kind::Float64Value(v)) => Some(PropertyValue::Float64(v)),
        Some(Kind::BoolValue(v)) => Some(PropertyValue::Bool(v)),
        Some(Kind::StringValue(v)) => Some(PropertyValue::String(v)),
        Some(Kind::TimestampValue(v)) => Some(PropertyValue::Timestamp(v)),
        Some(Kind::BytesValue(v)) => Some(PropertyValue::Bytes(v)),
        None => None,
    }
}
