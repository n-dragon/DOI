# datadog-dataset

A synthetic, Datadog-shaped infrastructure graph — 1000 nodes, 1771 edges —
generated for exercising the engine at a size larger than the ~20-node
viewer fixtures. Schema: `schema/datadog_infra.graphidl`.

## Shape

One inventory, the ownership/runtime hierarchy real infra actually has —
not a flat split across labels. Containers dominate the node count the way
they do in a real fleet (many short-lived containers per host, many
containers per service):

| Label        | Count | Notes |
|--------------|------:|-------|
| CloudAccount |     3 | aws / gcp / azure |
| Team         |    12 | owns 1..N services |
| Service      |    80 | the layer everything else attaches to |
| Host         |   150 | VMs/nodes, tagged by provider/region/az |
| Container    |   600 | 4 per host on average |
| Monitor      |   120 | metric alert / apm / log / synthetic / process |
| Dashboard    |    35 | 1-2 services each |
| **Total**    **1000** | |

| Edge         | Count | From → To |
|--------------|------:|-----------|
| CONTAINS     |   150 | CloudAccount → Host |
| OWNS         |    80 | Team → Service |
| RUNS_ON      |   600 | Container → Host |
| PART_OF      |   600 | Container → Service |
| MONITORS     |   120 | Monitor → Service |
| DEPENDS_ON   |  ~172 | Service → Service (service map; not acyclic) |
| DISPLAYS     |   ~49 | Dashboard → Service |
| **Total**    **~1771** | |

Every node id referenced by an edge exists (checked, zero dangling
references); node ids are dense `1..1000`, generated in label order
(CloudAccount, Team, Service, Host, Container, Monitor, Dashboard).

## Regenerating

```
python3 generate_dataset.py [output_dir]   # defaults to ./out
```

Deterministic — fixed seed, so a re-run produces byte-identical CSVs. Pure
Python stdlib (`csv`, `random`), no dependencies.

## Viewing it

`python3 to_graph_json.py` flattens `out/*.csv` into one `{nodes, links}`
JSON file (`viewer/vendor/datadog-graph.json` by default) — the shape
`react-force-graph` takes directly as `graphData`. That's what the
viewer's "Datadog fleet" tab renders in 3D (`viewer/README.md`'s "The
diagrams" section). Re-run both scripts in sequence after changing
`generate_dataset.py`.

## Loading it into the engine

This script writes CSV, not Iceberg tables — it's a data-shape reference,
not a drop-in ingester. To actually serve this from `graph-partition-node`,
port the row-generation logic into a Rust ingester following the
`edge_schema_with` / `write_table` pattern in
`examples/ingest-cloud-cost/src/main.rs`: one Arrow `RecordBatch` per
node/edge table, written through `graph_storage::open_sql_catalog`.
