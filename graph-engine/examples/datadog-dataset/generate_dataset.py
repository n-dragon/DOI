#!/usr/bin/env python3
"""Synthetic, Datadog-shaped infrastructure dataset generator.

Produces ~1000 nodes across the labels in `schema/datadog_infra.graphidl`
(CloudAccount, Team, Service, Host, Container, Monitor, Dashboard) plus
their edges, as CSV — one file per node label / edge type, so the shape
is inspectable without a database. Deterministic (fixed seed) so re-runs
are diffable.

This writes CSV, not Iceberg tables: it is a data-shape reference, not a
drop-in replacement for `examples/ingest-cloud-cost`. To actually load
this into a running `graph-partition-node`, port the row generation below
into a Rust ingester following the `edge_schema_with`/`write_table`
pattern in `examples/ingest-cloud-cost/src/main.rs`.

Usage:
    python3 generate_dataset.py [output_dir]   # defaults to ./out
"""
import csv
import os
import random
import sys

SEED = 20260812
random.seed(SEED)

OUT_DIR = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "out")

# ---------------------------------------------------------------------
# Node counts. Deliberately not a flat split across labels: containers
# vastly outnumber hosts and services the way they do in a real fleet
# (many short-lived containers per host, many containers per service).
# Sums to exactly 1000.
# ---------------------------------------------------------------------
N_CLOUD_ACCOUNTS = 3
N_TEAMS = 12
N_SERVICES = 80
N_HOSTS = 150
N_CONTAINERS = 600
N_MONITORS = 120
N_DASHBOARDS = 35
assert (
    N_CLOUD_ACCOUNTS + N_TEAMS + N_SERVICES + N_HOSTS + N_CONTAINERS + N_MONITORS + N_DASHBOARDS
    == 1000
)

TEAM_NAMES = [
    "checkout", "payments-platform", "identity", "catalog", "search",
    "fulfillment", "notifications", "growth", "data-platform", "ml-ranking",
    "observability", "core-infra",
]

SERVICE_WORDS_A = [
    "checkout", "payments", "auth", "catalog", "search", "cart", "orders",
    "shipping", "inventory", "pricing", "recommendations", "notifications",
    "email", "sms", "push", "user-profile", "session", "billing", "invoicing",
    "tax", "fraud", "risk", "ledger", "reporting", "analytics", "ingestion",
    "etl", "feature-store", "ranking", "personalization", "review", "rating",
    "media", "image-resize", "video-transcode", "cdn-edge", "gateway",
    "graphql", "bff", "web-frontend", "mobile-bff",
]
SERVICE_WORDS_B = ["api", "service", "worker", "consumer", "scheduler"]

LANGUAGES = ["go", "python", "java", "node", "ruby", "rust"]
ENVS = ["prod"] * 6 + ["staging"] * 3 + ["dev"] * 1  # weighted: mostly prod
PROVIDERS = ["aws", "gcp", "azure"]
REGIONS_BY_PROVIDER = {
    "aws": ["us-east-1", "us-west-2", "eu-west-1"],
    "gcp": ["us-central1", "europe-west1"],
    "azure": ["eastus", "westeurope"],
}
INSTANCE_TYPES_BY_PROVIDER = {
    "aws": ["m5.large", "m5.xlarge", "c5.2xlarge", "r5.large"],
    "gcp": ["n2-standard-4", "n2-standard-8", "e2-medium"],
    "azure": ["Standard_D4s_v5", "Standard_D2s_v5"],
}
OS_CHOICES = ["ubuntu-22.04", "amazon-linux-2023", "debian-12"]

MONITOR_TYPES = ["metric alert", "apm", "log", "synthetic", "process"]
MONITOR_STATUS = ["OK"] * 7 + ["Warn"] * 2 + ["Alert"] * 1  # mostly healthy
PRIORITIES = ["P1", "P2", "P3", "P4", "P5"]
MONITOR_TEMPLATES = [
    "High error rate",
    "Elevated p99 latency",
    "CPU throttling",
    "Memory usage above threshold",
    "Pod restarts",
    "Queue depth growing",
    "Disk usage above 85%",
    "5xx rate spike",
    "Synthetic check failing",
    "No data received",
]

DASHBOARD_TEMPLATES = ["{svc} — Service Overview", "{svc} — SLOs", "{svc} — On-call"]


def slug(*parts):
    return "-".join(parts)


def unique_service_names(n):
    names = set()
    while len(names) < n:
        a = random.choice(SERVICE_WORDS_A)
        b = random.choice(SERVICE_WORDS_B)
        names.add(f"{a}-{b}")
    return sorted(names)


def write_csv(path, header, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"  wrote {path} ({len(rows)} rows)")


def main():
    node_id = 1
    ids = {}  # label -> list of node ids, in generation order

    def next_id():
        nonlocal node_id
        i = node_id
        node_id += 1
        return i

    # -- CloudAccount --------------------------------------------------
    cloud_accounts = []
    ids["CloudAccount"] = []
    for i in range(N_CLOUD_ACCOUNTS):
        provider = PROVIDERS[i % len(PROVIDERS)]
        nid = next_id()
        ids["CloudAccount"].append(nid)
        cloud_accounts.append((nid, f"{provider}-prod-{i+1}", provider, f"{100000000000 + i}"))

    # -- Team ------------------------------------------------------------
    teams = []
    ids["Team"] = []
    for name in TEAM_NAMES[:N_TEAMS]:
        nid = next_id()
        ids["Team"].append(nid)
        teams.append((nid, name, f"#team-{name}"))

    # -- Service -----------------------------------------------------
    services = []
    ids["Service"] = []
    for name in unique_service_names(N_SERVICES):
        nid = next_id()
        ids["Service"].append(nid)
        env = random.choice(ENVS)
        services.append(
            (
                nid,
                name,
                env,
                random.choice(LANGUAGES),
                random.choices([1, 2, 3, 4], weights=[1, 3, 4, 2])[0],
                f"v{random.randint(1, 9)}.{random.randint(0, 40)}.{random.randint(0, 20)}",
            )
        )

    # -- Host ----------------------------------------------------------
    hosts = []
    ids["Host"] = []
    host_account = {}
    HOSTNAME_FORMAT_BY_PROVIDER = {
        "aws": lambda: f"i-{random.getrandbits(32):08x}",
        "gcp": lambda: f"gke-node-{random.getrandbits(24):06x}",
        "azure": lambda: f"aks-node{random.randint(100000, 999999)}",
    }
    for i in range(N_HOSTS):
        nid = next_id()
        ids["Host"].append(nid)
        account_idx = i % N_CLOUD_ACCOUNTS
        provider = cloud_accounts[account_idx][2]
        region = random.choice(REGIONS_BY_PROVIDER[provider])
        az = f"{region}-{random.choice(['a', 'b', 'c'])}"
        hosts.append(
            (
                nid,
                HOSTNAME_FORMAT_BY_PROVIDER[provider](),
                random.choice(INSTANCE_TYPES_BY_PROVIDER[provider]),
                region,
                az,
                random.choice(OS_CHOICES),
            )
        )
        host_account[nid] = cloud_accounts[account_idx][0]

    # -- Container -----------------------------------------------------
    containers = []
    ids["Container"] = []
    container_host = {}
    container_service = {}
    for i in range(N_CONTAINERS):
        nid = next_id()
        ids["Container"].append(nid)
        svc = services[i % N_SERVICES]
        svc_name = svc[1]
        host_id = ids["Host"][i % N_HOSTS]
        pod_suffix = f"{random.getrandbits(24):06x}-{random.choice('abcdefghijklmnopqrstuvwxyz')}{random.choice('abcdefghijklmnopqrstuvwxyz')}{random.randint(10,99)}"
        containers.append(
            (
                nid,
                f"{svc_name}-{pod_suffix}",
                f"registry.internal/{svc_name}:{svc[5]}",
                random.choice([100, 250, 500, 1000, 2000]),
                random.choice([256, 512, 1024, 2048, 4096]),
            )
        )
        container_host[nid] = host_id
        container_service[nid] = svc[0]

    # -- Monitor ---------------------------------------------------------
    monitors = []
    ids["Monitor"] = []
    monitor_service = {}
    for i in range(N_MONITORS):
        nid = next_id()
        ids["Monitor"].append(nid)
        svc = services[i % N_SERVICES]
        template = MONITOR_TEMPLATES[i % len(MONITOR_TEMPLATES)]
        monitors.append(
            (
                nid,
                f"[{svc[1]}] {template}",
                random.choice(MONITOR_TYPES),
                random.choice(MONITOR_STATUS),
                random.choice(PRIORITIES),
            )
        )
        monitor_service[nid] = svc[0]

    # -- Dashboard -------------------------------------------------------
    dashboards = []
    ids["Dashboard"] = []
    dashboard_services = {}
    for i in range(N_DASHBOARDS):
        nid = next_id()
        ids["Dashboard"].append(nid)
        svc = services[i % N_SERVICES]
        template = DASHBOARD_TEMPLATES[i % len(DASHBOARD_TEMPLATES)]
        dashboards.append(
            (
                nid,
                template.format(svc=svc[1]),
                "timeboard" if i % 3 else "screenboard",
            )
        )
        linked = {svc[0]}
        if random.random() < 0.4:
            linked.add(random.choice(services)[0])
        dashboard_services[nid] = linked

    # -- Edges -----------------------------------------------------------
    edge_id = 1

    def next_edge():
        nonlocal edge_id
        i = edge_id
        edge_id += 1
        return i

    contains_edges = [(next_edge(), host_account[h[0]], h[0]) for h in hosts]

    team_owns_edges = []
    for i, svc in enumerate(services):
        team_id = ids["Team"][i % N_TEAMS]
        team_owns_edges.append((next_edge(), team_id, svc[0]))

    runs_on_edges = [(next_edge(), c[0], container_host[c[0]]) for c in containers]
    part_of_edges = [(next_edge(), c[0], container_service[c[0]]) for c in containers]
    monitors_edges = [(next_edge(), m[0], monitor_service[m[0]]) for m in monitors]

    displays_edges = []
    for d in dashboards:
        for svc_id in dashboard_services[d[0]]:
            displays_edges.append((next_edge(), d[0], svc_id))

    # Service dependency map: each service calls 1-4 others, no self-loops,
    # no duplicate pairs. Not filtered for cycles — real service graphs have
    # them (a callback/webhook calling back into the caller).
    depends_on_edges = []
    seen_pairs = set()
    for svc in services:
        n_deps = random.choices([1, 2, 3, 4], weights=[3, 4, 2, 1])[0]
        candidates = [s for s in services if s[0] != svc[0]]
        random.shuffle(candidates)
        picked = 0
        for cand in candidates:
            if picked >= n_deps:
                break
            pair = (svc[0], cand[0])
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            depends_on_edges.append((next_edge(), svc[0], cand[0]))
            picked += 1

    # -- Write CSVs --------------------------------------------------------
    print(f"Writing dataset to {OUT_DIR}")

    write_csv(
        os.path.join(OUT_DIR, "nodes_cloudaccount.csv"),
        ["node_id", "name", "provider", "account_id"],
        cloud_accounts,
    )
    write_csv(os.path.join(OUT_DIR, "nodes_team.csv"), ["node_id", "name", "slack_channel"], teams)
    write_csv(
        os.path.join(OUT_DIR, "nodes_service.csv"),
        ["node_id", "name", "env", "language", "tier", "version"],
        services,
    )
    write_csv(
        os.path.join(OUT_DIR, "nodes_host.csv"),
        ["node_id", "hostname", "instance_type", "region", "az", "os"],
        hosts,
    )
    write_csv(
        os.path.join(OUT_DIR, "nodes_container.csv"),
        ["node_id", "name", "image", "cpu_limit_m", "memory_limit_mb"],
        containers,
    )
    write_csv(
        os.path.join(OUT_DIR, "nodes_monitor.csv"),
        ["node_id", "name", "monitor_type", "status", "priority"],
        monitors,
    )
    write_csv(
        os.path.join(OUT_DIR, "nodes_dashboard.csv"),
        ["node_id", "name", "dashboard_type"],
        dashboards,
    )

    write_csv(
        os.path.join(OUT_DIR, "edges_contains.csv"),
        ["edge_id", "src_node_id", "dst_node_id"],
        contains_edges,
    )
    write_csv(
        os.path.join(OUT_DIR, "edges_owns.csv"),
        ["edge_id", "src_node_id", "dst_node_id"],
        team_owns_edges,
    )
    write_csv(
        os.path.join(OUT_DIR, "edges_runs_on.csv"),
        ["edge_id", "src_node_id", "dst_node_id"],
        runs_on_edges,
    )
    write_csv(
        os.path.join(OUT_DIR, "edges_part_of.csv"),
        ["edge_id", "src_node_id", "dst_node_id"],
        part_of_edges,
    )
    write_csv(
        os.path.join(OUT_DIR, "edges_monitors.csv"),
        ["edge_id", "src_node_id", "dst_node_id"],
        monitors_edges,
    )
    write_csv(
        os.path.join(OUT_DIR, "edges_depends_on.csv"),
        ["edge_id", "src_node_id", "dst_node_id"],
        depends_on_edges,
    )
    write_csv(
        os.path.join(OUT_DIR, "edges_displays.csv"),
        ["edge_id", "src_node_id", "dst_node_id"],
        displays_edges,
    )

    total_nodes = (
        len(cloud_accounts)
        + len(teams)
        + len(services)
        + len(hosts)
        + len(containers)
        + len(monitors)
        + len(dashboards)
    )
    total_edges = (
        len(contains_edges)
        + len(team_owns_edges)
        + len(runs_on_edges)
        + len(part_of_edges)
        + len(monitors_edges)
        + len(depends_on_edges)
        + len(displays_edges)
    )
    print(f"Done: {total_nodes} nodes, {total_edges} edges.")


if __name__ == "__main__":
    main()
