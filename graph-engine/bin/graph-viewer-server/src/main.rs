//! Thin HTTP bridge for `graph-engine/viewer`: serves the static viewer
//! page and proxies its query panel to a running `graph-coordinator` over
//! real gRPC — the browser never re-implements query evaluation, it just
//! calls this server, exactly like `examples/query-client` does from the
//! CLI.
//!
//! Run with (from `graph-engine/`):
//!
//!   cargo run -p graph-viewer-server
//!
//! Env vars (all optional):
//!   GRAPH_COORDINATOR_ADDR    default `http://127.0.0.1:7000`
//!   GRAPH_VIEWER_STATIC_DIR   default `viewer` (relative to cwd)
//!   GRAPH_VIEWER_LISTEN_ADDR  default `0.0.0.0:8080`

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use axum::routing::{get_service, post};
use axum::Router;
use futures::StreamExt;
use graph_proto::v1::graph_service_client::GraphServiceClient;
use graph_proto::v1::value::Kind as ValueKind;
use graph_proto::v1::{ExecuteQueryRequest, NodeProperties, Value as ProtoValue};
use serde::Deserialize;
use serde_json::{json, Map, Value as JsonValue};
use std::net::SocketAddr;
use tower_http::services::ServeDir;

#[derive(Clone)]
struct AppState {
    coordinator_addr: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let coordinator_addr =
        std::env::var("GRAPH_COORDINATOR_ADDR").unwrap_or_else(|_| "http://127.0.0.1:7000".to_string());
    let static_dir = std::env::var("GRAPH_VIEWER_STATIC_DIR").unwrap_or_else(|_| "viewer".to_string());
    let listen_addr: SocketAddr = std::env::var("GRAPH_VIEWER_LISTEN_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
        .parse()?;

    let state = AppState { coordinator_addr };

    let app = Router::new()
        .route("/api/query", post(run_query))
        .fallback_service(get_service(ServeDir::new(&static_dir)))
        .with_state(state.clone());

    println!("graph-viewer-server listening on http://{listen_addr}");
    println!("  static files from ./{static_dir}");
    println!("  /api/query -> {}", state.coordinator_addr);

    let listener = tokio::net::TcpListener::bind(listen_addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[derive(Deserialize)]
struct QueryRequest {
    dsl: String,
}

async fn run_query(State(state): State<AppState>, Json(req): Json<QueryRequest>) -> impl IntoResponse {
    match execute(&state.coordinator_addr, &req.dsl).await {
        Ok(rows) => (StatusCode::OK, Json(json!({ "rows": rows }))).into_response(),
        Err(QueryError::Connect(err)) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({ "error": format!("couldn't reach graph-coordinator at {}: {err}", state.coordinator_addr) })),
        )
            .into_response(),
        Err(QueryError::Grpc(status)) => {
            let code = match status.code() {
                tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
                tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => StatusCode::BAD_GATEWAY,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (code, Json(json!({ "error": status.message() }))).into_response()
        }
    }
}

enum QueryError {
    Connect(tonic::transport::Error),
    Grpc(tonic::Status),
}

/// Runs `dsl` against the coordinator's real `ExecuteQuery` RPC and
/// reshapes each streamed `QueryResult` into one JSON row: `{alias:
/// {label, properties: {...}}}`, falling back to `{alias: {node_id}}` for
/// the (normal-path-unreachable in this dataset, but not impossible)
/// case where a matched node's properties didn't come back from
/// `GetNodeProperties` — e.g. a rebuild swapped generations mid-query.
async fn execute(addr: &str, dsl: &str) -> Result<Vec<JsonValue>, QueryError> {
    let mut client = GraphServiceClient::connect(addr.to_string())
        .await
        .map_err(QueryError::Connect)?;

    let mut stream = client
        .execute_query(ExecuteQueryRequest { dsl: dsl.to_string() })
        .await
        .map_err(QueryError::Grpc)?
        .into_inner();

    let mut rows = Vec::new();
    while let Some(result) = stream.next().await {
        let result = result.map_err(QueryError::Grpc)?;

        let mut row = Map::new();
        for (alias, value) in &result.projection {
            let entry = match result.properties.get(alias) {
                Some(props) => node_properties_to_json(props),
                None => json!({ "node_id": value_to_json(value) }),
            };
            row.insert(alias.clone(), entry);
        }
        rows.push(JsonValue::Object(row));
    }

    Ok(rows)
}

fn node_properties_to_json(props: &NodeProperties) -> JsonValue {
    let mut fields = Map::new();
    for (name, value) in &props.fields {
        fields.insert(name.clone(), value_to_json(value));
    }
    json!({ "label": props.label, "properties": fields })
}

fn value_to_json(value: &ProtoValue) -> JsonValue {
    match &value.kind {
        Some(ValueKind::Int64Value(v)) => json!(v),
        Some(ValueKind::Float64Value(v)) => json!(v),
        Some(ValueKind::BoolValue(v)) => json!(v),
        Some(ValueKind::StringValue(v)) => json!(v),
        Some(ValueKind::TimestampValue(v)) => json!(v),
        Some(ValueKind::BytesValue(v)) => json!(v),
        None => JsonValue::Null,
    }
}
