//! Regression test for `schema/least_privilege.graphidl` — the "least
//! privilege proven by telemetry" use case's data model (declared vs.
//! observed access as distinct edge types, cf. that file's own doc
//! comment). Parses the shipped file directly (`include_str!`) rather
//! than a copy pasted into the test, so an edit to the schema that
//! breaks parsing fails here instead of only being caught by whoever
//! next runs `graph-partition-node` against it.

use graph_schema::{EdgeType, Label, PestSchemaParser, SchemaParser};

const SCHEMA_IDL: &str = include_str!("../../../schema/least_privilege.graphidl");

#[test]
fn least_privilege_schema_parses() {
    let schema = PestSchemaParser
        .parse(SCHEMA_IDL)
        .expect("schema/least_privilege.graphidl should be valid IDL");

    for label in ["Workload", "IAMRole", "DataStore"] {
        assert!(
            schema.node_def(&Label(label.to_string())).is_some(),
            "missing node {label}"
        );
    }
    for edge_type in ["ASSUMES", "GRANTS", "RAN_AS", "ACCESSED"] {
        assert!(
            schema.edges.contains_key(&EdgeType(edge_type.to_string())),
            "missing edge type {edge_type}"
        );
    }
}

/// The two decisions the schema's doc comment calls "structuring":
/// declared (`ASSUMES`/`GRANTS`) and observed (`RAN_AS`/`ACCESSED`)
/// access never share an edge type, and `ACCESSED` originates at
/// `Workload`, not `IAMRole` — pinned as a test so a future edit can't
/// silently collapse them into an "effective access" edge, which would
/// destroy the whole point of the model (the design note is explicit:
/// "si tu les collapses en une seule arête effective, tu perds le
/// livrable").
#[test]
fn declared_and_observed_access_stay_on_separate_edge_types() {
    let schema = PestSchemaParser.parse(SCHEMA_IDL).expect("valid IDL");

    let grants = schema
        .edges
        .get(&EdgeType("GRANTS".to_string()))
        .expect("GRANTS edge");
    assert_eq!(grants.from.0, "IAMRole");
    assert_eq!(grants.to.0, "DataStore");

    let accessed = schema
        .edges
        .get(&EdgeType("ACCESSED".to_string()))
        .expect("ACCESSED edge");
    assert_eq!(
        accessed.from.0, "Workload",
        "ACCESSED must start at the actor (Workload), not the vehicle (IAMRole)"
    );
    assert_eq!(accessed.to.0, "DataStore");

    assert_ne!(
        (grants.from.0.as_str(), grants.to.0.as_str()),
        (accessed.from.0.as_str(), accessed.to.0.as_str()),
        "GRANTS (declared) and ACCESSED (observed) must not share an endpoint shape that could tempt collapsing them"
    );
}
