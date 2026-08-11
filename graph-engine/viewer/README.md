# Graph viewer

`index.html` — no build step, no client-side dataset, no query logic in
the browser. Two tabs, two questions asked of the **same** graph:

- **Cloud cost** — the spec §7.1-style example query
  (`OWNS`/`CONTAINS*1..3`/`HAS_COST` join, filtered on cost).
- **Attack paths** — a security-posture traversal: is there a chain from
  an internet-facing, vulnerable resource, through the IAM role it runs
  as, through up to three `CAN_ASSUME` hops, to a role that can read a
  store classified `pii`?

Both traverse the same `Resource` nodes — the billing feed and the
posture feed are joined to the inventory by edges, not by a runtime join
between two systems. That sharing is the point of the demo.

The attack-path tab is the one that fits the engine's shape best: the
answer is the *path itself*, so the deliberate absence of aggregation
(§1.4) never bites, and bounded role-assumption hops are exactly the
priority primitive of §7.1. The cost tab is honest about the opposite —
"how much does the team spend" is a `SUM`, which this engine leaves to
whatever consumes the streamed rows.

Everything is served by `graph-viewer-server` (`bin/graph-viewer-server`),
a thin HTTP↔gRPC bridge: it serves this static page and proxies each
query panel's `POST /api/query` to `GraphService::ExecuteQuery` on a
running coordinator. Nothing here re-implements traversal or filtering —
the DSL text you type goes to the real parser/planner/executor and comes
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
#    exactly; it's what the WHERE/RETURN clauses get validated against,
#    and it now carries both lenses: CostRecord *and* IAMRole/DataStore)
GRAPH_SCHEMA_PATH=schema/cloud_cost.graphidl cargo run -p graph-partition-node

# 3. the coordinator — same schema path
GRAPH_SCHEMA_PATH=schema/cloud_cost.graphidl cargo run -p graph-coordinator

# 4. the viewer's HTTP bridge (serves viewer/ + proxies queries to the coordinator)
cargo run -p graph-viewer-server
```

Run each from `graph-engine/` (relative paths like `./warehouse` and
`schema/cloud_cost.graphidl` are resolved from the current directory).
Then open <http://localhost:8080> — or <http://localhost:8080#security>
to land straight on the attack-path tab. `GRAPH_COORDINATOR_ADDR`,
`GRAPH_VIEWER_STATIC_DIR`, `GRAPH_VIEWER_LISTEN_ADDR`,
`GRAPH_PARTITION_NODE_ADDR`, `GRAPH_WAREHOUSE_PATH`, and
`GRAPH_CATALOG_DB_PATH` override the defaults if you're not running
everything on localhost from the same working directory.

Toggle between "full graph" (every node/edge, colored by type) and
"query result" (the traversal highlighted, each excluded node annotated
with why). Edit the query and click "Run query" — that's a real HTTP
round trip to `graph-viewer-server`, which is a real gRPC round trip to
`graph-coordinator`. A syntax or validation error from the DSL parser
comes back and shows inline, same as it would from `query-client`.

### What the highlighting means

Each run issues more than one real query, so the diagram can distinguish
three different reasons a node isn't in your result:

| State | Meaning |
|---|---|
| solid ring + badge | matched and projected by your `RETURN` |
| dashed ring, "traversed, not projected" | the traversal walked through it, but no returned alias binds it — the middle hop of a `*1..3` range |
| greyed, "reached — filtered out by `WHERE`" | your pattern reached it; the `WHERE` clause rejected it |
| greyed, "not reached by this pattern" | the traversal never got there at all |

The second row is why the attack-path tab issues a third query (the
`CAN_ASSUME` chain on its own): `RETURN` only *projects*, so without it
the pivot role would be drawn as unreachable even though the traversal
went straight through it. The third row comes from re-running your own
pattern with its `WHERE` clause stripped. Both are best-effort — if
either fails, the primary result still stands.

### The dataset is built to have decoys

The attack-path query returns exactly one of three near-identical-looking
resources. The other two are there to be rejected, for two different
reasons — same CVE but not internet-facing, versus internet-facing but
whose role reaches nothing classified. A rule-per-resource scanner raises
all three; only the path tells them apart.

The palette is a visual homage to Datadog's brand purple
(`#632CA6` / `#8000FF`) — chosen because this dataset mirrors
Datadog's Cloud Cost Management and Cloud Security products, not because
this is an official Datadog asset.
