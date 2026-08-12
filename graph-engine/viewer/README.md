# Graph viewer

`index.html` — no client-side dataset for the live tab, no query logic in
the browser. Three tabs:

- **Unused permissions** — the least-privilege-via-telemetry use case:
  cross-references declared IAM access (`RUNS_AS`/`CAN_READ`) against
  observed access telemetry (`ACCESSED`) via a correlated `NOT EXISTS`
  anti-join, live against a running coordinator.
- **Architecture** — static reference content (API/storage/index/engine
  choices), no live query surface.
- **Datadog fleet** — the 1000-node synthetic dataset from
  `examples/datadog-dataset`, rendered in 3D, static (no coordinator
  needed). See "The diagrams" below.

Everything on the Unused permissions tab is served by `graph-viewer-server`
(`bin/graph-viewer-server`), a thin HTTP↔gRPC bridge: it serves this
static page and proxies the query panel's `POST /api/query` to
`GraphService::ExecuteQuery` on a running coordinator. Nothing here
re-implements traversal or filtering — the DSL text you type goes to the
real parser/planner/executor and comes back as real results (via the
coordinator's `GetNodeProperties`/`GetEdgeProperties` RPCs, which hydrate
each matched id with its actual property record).

The client-side dependency is the diagrams themselves:
[react-force-graph](https://github.com/vasturiano/react-force-graph) (2D
for Unused permissions, 3D for Datadog fleet), bundled locally into
`vendor/` — see "The diagrams" below — so the page still has zero CDN
dependency at runtime, just like the query path.

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
with why). Edit the query and click "Run query" — that's a real HTTP
round trip to `graph-viewer-server`, which is a real gRPC round trip to
`graph-coordinator`. A syntax or validation error from the DSL parser
comes back and shows inline, same as it would from `query-client`.

### What the highlighting means

Each run issues more than one real query, so the diagram can distinguish
different reasons a node isn't in your result:

| State | Meaning |
|---|---|
| purple ring + badge | matched and projected by your `RETURN` |
| greyed, "reached — filtered out by `WHERE`" | your pattern reached it; the `WHERE` clause rejected it |
| greyed, "not reached by this pattern" | the traversal never got there at all |

The second query is your own pattern re-run with its `WHERE` clause
stripped, so the diagram can tell those two greyed-out states apart.
Best-effort — if it fails, the primary result still stands.

### The dataset is built to have a decoy

`i-checkout-api-1` and `i-checkout-api-2` run as the same role and hold
the same declared `CAN_READ` grant on the PII store — identical on paper.
Only one of them has an `ACCESSED` edge into that store; the other is the
finding. A rule-per-resource scanner (or a CSPM that only reads declared
policy) can't tell them apart — the `NOT EXISTS` anti-join is what does.

The palette is a visual homage to Datadog's brand purple
(`#632CA6` / `#8000FF`) — chosen because this dataset mirrors Datadog's
Cloud Security posture products, not because this is an official Datadog
asset.

## The diagrams

Both force-directed diagrams are React components
([react-force-graph](https://github.com/vasturiano/react-force-graph))
mounted imperatively from the page's plain-JS logic — only the diagrams
themselves are React; the rest of the page (tabs, query editor, results
panel) stays vanilla JS/DOM.

**Unused permissions** (2D, `force-graph-widget-src/entry.js` →
`vendor/force-graph-widget.js`) — custom canvas node/link rendering that
reproduces this page's status language (included/excluded/dimmed,
badges, the always-on `ACCESSED` edge), reading colors from the page's
CSS custom properties so light/dark theming stays centralized in
`index.html`. Exposes `window.DOIForceGraph.render(container, {nodes,
links, mode})`. Loaded eagerly (it's the default tab).

**Datadog fleet** (3D, `force-graph-widget-src/entry-3d.js` →
`vendor/force-graph-3d-widget.js`) — deliberately the opposite approach:
no custom rendering at all, just `graphData` + `nodeAutoColorBy` +
`linkDirectionalParticles`, the same three props upstream's own
[large-graph example](https://github.com/vasturiano/react-force-graph/tree/master/example/large-graph)
uses to make the point that a graph too big to hand-tune still reads
fine off the library's defaults. Exposes
`window.DOIForceGraph3D.render(container, {graphData, height})`. Bundles
three.js (`3d-force-graph`), so unlike the 2D widget it's ~1.5MB —
loaded lazily, only when the Datadog fleet tab is first opened
(`loadFleet()` in `index.html`), not on every page load.

Both bundles are checked in like a compiled binary would be — there's no
build step in CI/deploy for this page, so the artifacts have to be ready
to serve as-is. `vendor/datadog-graph.json` (the fleet tab's dataset,
flattened from `examples/datadog-dataset/out/*.csv` by
`examples/datadog-dataset/to_graph_json.py`) is checked in the same way.

To change a diagram, edit the matching `entry*.js`, then:

```sh
cd force-graph-widget-src
npm install   # first time only
./build.sh    # rewrites both vendor/force-graph-widget.js and vendor/force-graph-3d-widget.js
```
