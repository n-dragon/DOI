# Graph viewer

A single self-contained `index.html` — no build step, no dependencies —
visualizing the `examples/ingest-cloud-cost` dataset and walking through
the spec §7.1-style example query (`OWNS`/`CONTAINS*1..3`/`HAS_COST`
join, filtered on cost) that `examples/query-client` actually runs
against a live `graph-coordinator`.

It's a static snapshot of that dataset — no backend call, no live
`graph-coordinator` — but the query panel is *not* decorative: it
re-evaluates the `CONTAINS*min..max` hop range and the
`WHERE c.amount_usd` threshold against the six resources embedded in
the page and recomputes the highlighting. The rest of the pattern
(the account/VPC match, edge types) is fixed to this demo's dataset.
Open it directly in a browser:

```sh
open graph-engine/viewer/index.html   # or just double-click it
```

Toggle between "full graph" (every node/edge, colored by type) and
"query result" (the traversal highlighted, each excluded node annotated
with why — cost too low, or too many hops). Edit the hop range or
threshold in the query box and click "Run query" to see the result
change live.

The palette is a visual homage to Datadog's brand purple
(`#632CA6` / `#8000FF`) — chosen because this dataset mirrors
Datadog's Cloud Cost Management product, not because this is an
official Datadog asset.
