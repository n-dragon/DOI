# Graph viewer

A single self-contained `index.html` — no build step, no dependencies —
visualizing the `examples/ingest-cloud-cost` dataset and walking through
the spec §7.1-style example query (`OWNS`/`CONTAINS*1..3`/`HAS_COST`
join, filtered on cost) that `examples/query-client` actually runs
against a live `graph-coordinator`.

It's a static snapshot of that same dataset/query/result, not a live
client — there's no backend call. Open it directly in a browser:

```sh
open graph-engine/viewer/index.html   # or just double-click it
```

Toggle between "full graph" (every node/edge, colored by type) and
"query result" (the traversal highlighted, each excluded node annotated
with why — cost too low, or too many hops).
