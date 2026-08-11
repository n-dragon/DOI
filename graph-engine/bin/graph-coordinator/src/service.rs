//! `GraphServiceImpl` (tasks CO1-CO3): the client-facing surface (spec
//! §8.2) — parses/validates/plans a DSL query, drives it against the
//! partition node via [`crate::remote_executor`], and streams projected
//! results.

// tonic::Status (176 bytes) is the framework's standard RPC error type -
// every handler in a tonic service returns Result<_, Status> by
// convention, so this lint would need boxing Status everywhere rather
// than flagging a real problem here.
#![allow(clippy::result_large_err)]

use crate::remote_executor;
use graph_dsl::{Parser as DslParser, PestParser, SchemaValidator, Validator};
use graph_proto::v1::graph_service_server::GraphService;
use graph_proto::v1::partition_service_client::PartitionServiceClient;
use graph_proto::v1::value::Kind as ValueKind;
use graph_proto::v1::{
    health_check_response, ExecuteQueryRequest, GetIndexStatusRequest, GetNodePropertiesRequest,
    GetSchemaRequest, HealthCheckRequest, HealthCheckResponse, IndexStatusResponse, QueryResult,
    SchemaResponse, Value as ProtoValue,
};
use graph_query::{NaivePlanner, Planner};
use graph_schema::Schema;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

type QueryResultStream = Pin<Box<dyn futures::Stream<Item = Result<QueryResult, Status>> + Send>>;

pub struct GraphServiceImpl {
    schema: Schema,
    schema_idl: String,
    partition_client: PartitionServiceClient<Channel>,
}

impl GraphServiceImpl {
    pub fn new(
        schema: Schema,
        schema_idl: String,
        partition_client: PartitionServiceClient<Channel>,
    ) -> Self {
        Self {
            schema,
            schema_idl,
            partition_client,
        }
    }
}

#[tonic::async_trait]
impl GraphService for GraphServiceImpl {
    type ExecuteQueryStream = QueryResultStream;

    /// *(task CO2)* parse -> validate (D7-D9) -> plan (Q1) -> execute,
    /// mono-partition, via `remote_executor` -> project `RETURN` ->
    /// stream. Fail-fast: a syntax or validation error is returned
    /// before any RPC to the partition node is made (spec §7.2).
    async fn execute_query(
        &self,
        request: Request<ExecuteQueryRequest>,
    ) -> Result<Response<Self::ExecuteQueryStream>, Status> {
        let dsl = request.into_inner().dsl;

        let query = PestParser
            .parse(&dsl)
            .map_err(|e| Status::invalid_argument(format!("syntax error: {e}")))?;
        SchemaValidator
            .validate(&query, &self.schema)
            .map_err(|errors| {
                Status::invalid_argument(format!("query failed validation: {errors:?}"))
            })?;

        let plan = NaivePlanner.plan(&query);
        let mut client = self.partition_client.clone();
        let bindings = remote_executor::execute(&mut client, &plan).await?;

        let project = plan.project;

        // Every distinct NodeId that will end up in some row's
        // projection, deduped once up front so a result set with
        // repeated nodes across rows still costs one GetNodeProperties
        // round-trip rather than one per row.
        let needed_ids: Vec<u64> = bindings
            .iter()
            .flat_map(|binding| {
                project
                    .iter()
                    .filter_map(|alias| binding.node_ids_by_alias.get(alias).copied())
            })
            .collect::<HashSet<u64>>()
            .into_iter()
            .collect();

        let properties_by_id: HashMap<u64, graph_proto::v1::NodeProperties> = if needed_ids
            .is_empty()
        {
            HashMap::new()
        } else {
            client
                .get_node_properties(GetNodePropertiesRequest {
                    node_ids: needed_ids,
                })
                .await?
                .into_inner()
                .properties
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
                        binding.node_ids_by_alias.get(alias).map(|&id| {
                            (
                                alias.clone(),
                                ProtoValue {
                                    kind: Some(ValueKind::Int64Value(id as i64)),
                                },
                            )
                        })
                    })
                    .collect();
                let properties = project
                    .iter()
                    .filter_map(|alias| {
                        let id = *binding.node_ids_by_alias.get(alias)?;
                        let record = properties_by_id.get(&id)?;
                        Some((alias.clone(), record.clone()))
                    })
                    .collect();
                Ok(QueryResult {
                    projection,
                    properties,
                })
            })
            .collect();

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

    /// *(task CO3)* Relays the partition node's own `GetIndexStatus` -
    /// mono-partition v1 has exactly one replica to relay.
    async fn get_index_status(
        &self,
        _request: Request<GetIndexStatusRequest>,
    ) -> Result<Response<IndexStatusResponse>, Status> {
        let mut client = self.partition_client.clone();
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
