# Graph viewer

`index.html` — no build step, no client-side dataset, no query logic in
the browser — visualizing the `examples/ingest-cloud-cost` dataset and
running the spec §7.1-style example query (`OWNS`/`CONTAINS*1..3`/
`HAS_COST` join, filtered on cost) against a **live**
`graph-coordinator`, the same one `examples/query-client` talks to.

It's served by `graph-viewer-server` (`bin/graph-viewer-server`), a thin
HTTP↔gRPC bridge: it serves this static page and proxies the query panel's
`POST /api/query` to `GraphService::ExecuteQuery` on a running
coordinator. Nothing here re-implements traversal or filtering — the
DSL text you type goes to the real parser/planner/executor and comes
back as real results (via the coordinator's `GetNodeProperties` RPC,
which hydrates each matched `NodeId` with its actual property record).

## Running it

From `graph-engine/`, four terminals:

```sh
# 1. ingest the demo dataset into a persistent Iceberg catalog
#    (creates ./warehouse and ./catalog.sqlite in the cwd)
cargo run -p ingest-cloud-cost

# 2. the partition node — needs the schema both it and the coordinator
#    parse (schema/cloud_cost.graphidl matches examples/ingest-cloud-cost
#    exactly; it's what the WHERE/RETURN clauses get validated against)
GRAPH_SCHEMA_PATH=schema/cloud_cost.graphidl cargo run -p graph-partition-node

# 3. the coordinator — same schema path
GRAPH_SCHEMA_PATH=schema/cloud_cost.graphidl cargo run -p graph-coordinator

# 4. the viewer's HTTP bridge (serves viewer/ + proxies queries to the coordinator)
cargo run -p graph-viewer-server
```

Run each from `graph-engine/` (relative paths like `./warehouse` and
`schema/cloud_cost.graphidl` are resolved from the current directory).
Then open <http://localhost:8080>. `GRAPH_COORDINATOR_ADDR`,
`GRAPH_VIEWER_STATIC_DIR`, `GRAPH_VIEWER_LISTEN_ADDR`,
`GRAPH_PARTITION_NODE_ADDR`, `GRAPH_WAREHOUSE_PATH`, and
`GRAPH_CATALOG_DB_PATH` override the defaults if you're not running
everything on localhost from the same working directory.

Toggle between "full graph" (every node/edge, colored by type) and
"query result" (the traversal highlighted, each excluded node annotated
with why — cost too low, or never reached by the pattern at all). Edit
the query and click "Run query" — that's a real HTTP round trip to
`graph-viewer-server`, which is a real gRPC round trip to
`graph-coordinator`. A syntax or validation error from the DSL parser
comes back and shows inline, same as it would from `query-client`.

The palette is a visual homage to Datadog's brand purple
(`#632CA6` / `#8000FF`) — chosen because this dataset mirrors
Datadog's Cloud Cost Management product, not because this is an
official Datadog asset.
