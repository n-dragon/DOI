//! Minimal CLI for talking to a running `graph-coordinator` — the thing
//! you'd actually run against a deployed engine to prove it works, rather
//! than re-reading Rust test assertions.
//!
//! Run with (from `graph-engine/`):
//!
//!   cargo run -p query-client -- 'MATCH (p:Person {name: "Alice"}) RETURN p'
//!
//! `GRAPH_COORDINATOR_ADDR` (default `http://127.0.0.1:7000`) picks which
//! coordinator to talk to.

use futures::StreamExt;
use graph_proto::v1::graph_service_client::GraphServiceClient;
use graph_proto::v1::value::Kind;
use graph_proto::v1::ExecuteQueryRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("GRAPH_COORDINATOR_ADDR")
        .unwrap_or_else(|_| "http://127.0.0.1:7000".to_string());
    let dsl = std::env::args()
        .nth(1)
        .ok_or("usage: query-client '<DSL query>'")?;

    println!("Connecting to {addr}...");
    let mut client = GraphServiceClient::connect(addr).await?;

    println!("Query:\n  {dsl}\n");
    let mut stream = client
        .execute_query(ExecuteQueryRequest { dsl })
        .await?
        .into_inner();

    let mut count = 0;
    while let Some(result) = stream.next().await {
        let result = result?;
        count += 1;
        let projection: Vec<String> = result
            .projection
            .iter()
            .map(|(alias, value)| format!("{alias} = {}", format_value(value)))
            .collect();
        println!("  [{count}] {}", projection.join(", "));
    }

    println!("\n{count} result(s).");
    Ok(())
}

fn format_value(value: &graph_proto::v1::Value) -> String {
    match &value.kind {
        Some(Kind::Int64Value(v)) => v.to_string(),
        Some(Kind::Float64Value(v)) => v.to_string(),
        Some(Kind::BoolValue(v)) => v.to_string(),
        Some(Kind::StringValue(v)) => v.clone(),
        Some(Kind::TimestampValue(v)) => v.to_string(),
        Some(Kind::BytesValue(v)) => format!("{v:?}"),
        None => "<none>".to_string(),
    }
}
