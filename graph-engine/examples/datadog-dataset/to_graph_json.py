#!/usr/bin/env python3
"""Flattens out/*.csv (see generate_dataset.py) into one {nodes, links}
JSON file — the shape react-force-graph(-3d) expects directly as
graphData, and the same shape the upstream large-graph example fetches
(example/large-graph/index.html: `fetch('../datasets/blocks.json')`).

Usage:
    python3 to_graph_json.py [csv_dir] [out_file]
    # defaults: ./out -> ../../viewer/vendor/datadog-graph.json
"""
import csv
import json
import os
import sys

CSV_DIR = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "out")
OUT_FILE = (
    sys.argv[2]
    if len(sys.argv) > 2
    else os.path.join(os.path.dirname(__file__), "..", "..", "viewer", "vendor", "datadog-graph.json")
)

NODE_FILES = {
    "CloudAccount": ("nodes_cloudaccount.csv", "name"),
    "Team": ("nodes_team.csv", "name"),
    "Service": ("nodes_service.csv", "name"),
    "Host": ("nodes_host.csv", "hostname"),
    "Container": ("nodes_container.csv", "name"),
    "Monitor": ("nodes_monitor.csv", "name"),
    "Dashboard": ("nodes_dashboard.csv", "name"),
}

EDGE_FILES = {
    "CONTAINS": "edges_contains.csv",
    "OWNS": "edges_owns.csv",
    "RUNS_ON": "edges_runs_on.csv",
    "PART_OF": "edges_part_of.csv",
    "MONITORS": "edges_monitors.csv",
    "DEPENDS_ON": "edges_depends_on.csv",
    "DISPLAYS": "edges_displays.csv",
}


def main():
    nodes = []
    for label, (filename, name_field) in NODE_FILES.items():
        with open(os.path.join(CSV_DIR, filename)) as f:
            for row in csv.DictReader(f):
                nodes.append(
                    {
                        "id": int(row["node_id"]),
                        "label": label,
                        "name": row[name_field],
                    }
                )

    links = []
    for edge_type, filename in EDGE_FILES.items():
        with open(os.path.join(CSV_DIR, filename)) as f:
            for row in csv.DictReader(f):
                links.append(
                    {
                        "source": int(row["src_node_id"]),
                        "target": int(row["dst_node_id"]),
                        "type": edge_type,
                    }
                )

    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w") as f:
        json.dump({"nodes": nodes, "links": links}, f, separators=(",", ":"))

    print(f"wrote {OUT_FILE} ({len(nodes)} nodes, {len(links)} links)")


if __name__ == "__main__":
    main()
