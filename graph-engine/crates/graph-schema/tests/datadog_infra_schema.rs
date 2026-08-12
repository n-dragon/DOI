//! Regression test for `schema/datadog_infra.graphidl` — the synthetic
//! Datadog-shaped infrastructure dataset (`examples/datadog-dataset`).
//! Parses the shipped file directly (`include_str!`) so an edit that
//! breaks parsing fails here instead of only being caught downstream.

use graph_schema::{EdgeType, Label, PestSchemaParser, SchemaParser};

const SCHEMA_IDL: &str = include_str!("../../../schema/datadog_infra.graphidl");

#[test]
fn datadog_infra_schema_parses() {
    let schema = PestSchemaParser
        .parse(SCHEMA_IDL)
        .expect("schema/datadog_infra.graphidl should be valid IDL");

    for label in [
        "CloudAccount",
        "Team",
        "Service",
        "Host",
        "Container",
        "Monitor",
        "Dashboard",
    ] {
        assert!(
            schema.node_def(&Label(label.to_string())).is_some(),
            "missing node {label}"
        );
    }
    for edge_type in [
        "CONTAINS",
        "OWNS",
        "RUNS_ON",
        "PART_OF",
        "MONITORS",
        "DEPENDS_ON",
        "DISPLAYS",
    ] {
        assert!(
            schema.edges.contains_key(&EdgeType(edge_type.to_string())),
            "missing edge type {edge_type}"
        );
    }
}
