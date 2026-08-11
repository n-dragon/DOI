//! [`SchemaEvolution`] (tasks S5/S6): classifies the change from one
//! [`Schema`] version to another per spec §3.5 — compatible changes are
//! applied without a blocking migration (mirroring Iceberg's native
//! column-add evolution); incompatible ones require the explicit,
//! versioned migration process left `TBD` in spec §3.5/§13.
//!
//! Only the two rules the spec actually states are checked (§3.5):
//! compatible = new optional property, or new node label/edge type.
//! Incompatible = a property removed, or a property's type changed.
//! Renaming is not distinguished from remove-then-add — the IDL has no
//! rename marker, so a rename is observationally a removal (already
//! caught) plus an addition.

use crate::{EvolutionKind, Schema, SchemaEvolution};

pub struct SpecSchemaEvolution;

impl SchemaEvolution for SpecSchemaEvolution {
    fn diff(&self, from: &Schema, to: &Schema) -> EvolutionKind {
        let mut reasons = Vec::new();

        for (label, from_node) in &from.nodes {
            match to.nodes.get(label) {
                Some(to_node) => diff_properties(
                    &format!("node {label}"),
                    &from_node.properties,
                    &to_node.properties,
                    &mut reasons,
                ),
                None => reasons.push(format!("node label {label} was removed")),
            }
        }

        for (edge_type, from_edge) in &from.edges {
            match to.edges.get(edge_type) {
                Some(to_edge) => diff_properties(
                    &format!("edge {edge_type}"),
                    &from_edge.properties,
                    &to_edge.properties,
                    &mut reasons,
                ),
                None => reasons.push(format!("edge type {edge_type} was removed")),
            }
        }

        if reasons.is_empty() {
            EvolutionKind::Compatible
        } else {
            EvolutionKind::Incompatible { reasons }
        }
    }
}

fn diff_properties(
    owner: &str,
    from: &[crate::PropertyDef],
    to: &[crate::PropertyDef],
    reasons: &mut Vec<String>,
) {
    for from_prop in from {
        match to.iter().find(|p| p.name == from_prop.name) {
            Some(to_prop) if to_prop.ty != from_prop.ty => reasons.push(format!(
                "{owner}: property {} changed type from {:?} to {:?}",
                from_prop.name, from_prop.ty, to_prop.ty
            )),
            Some(_) => {}
            None => reasons.push(format!("{owner}: property {} was removed", from_prop.name)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PestSchemaParser;
    use crate::SchemaParser;

    fn schema(source: &str) -> Schema {
        PestSchemaParser.parse(source).expect("valid IDL")
    }

    /// *(task S7)* Identical schemas are trivially compatible.
    #[test]
    fn identical_schemas_are_compatible() {
        let s = schema(
            r#"schema graph_v1 { node Person { id: NodeId name: String } }"#,
        );
        assert_eq!(SpecSchemaEvolution.diff(&s, &s), EvolutionKind::Compatible);
    }

    /// *(task S5)* Adding an optional property is compatible.
    #[test]
    fn adding_an_optional_property_is_compatible() {
        let from = schema(r#"schema graph_v1 { node Person { id: NodeId } }"#);
        let to = schema(
            r#"schema graph_v2 { node Person { id: NodeId nickname: String? } }"#,
        );
        assert_eq!(
            SpecSchemaEvolution.diff(&from, &to),
            EvolutionKind::Compatible
        );
    }

    /// *(task S5)* Adding a new node label is compatible.
    #[test]
    fn adding_a_node_label_is_compatible() {
        let from = schema(r#"schema graph_v1 { node Person { id: NodeId } }"#);
        let to = schema(
            r#"
            schema graph_v2 {
              node Person { id: NodeId }
              node Organization { id: NodeId }
            }
            "#,
        );
        assert_eq!(
            SpecSchemaEvolution.diff(&from, &to),
            EvolutionKind::Compatible
        );
    }

    /// *(task S5)* Adding a new edge type is compatible.
    #[test]
    fn adding_an_edge_type_is_compatible() {
        let from = schema(
            r#"schema graph_v1 { node Person { id: NodeId } }"#,
        );
        let to = schema(
            r#"
            schema graph_v2 {
              node Person { id: NodeId }
              edge KNOWS { from: Person to: Person }
            }
            "#,
        );
        assert_eq!(
            SpecSchemaEvolution.diff(&from, &to),
            EvolutionKind::Compatible
        );
    }

    /// *(task S6)* Removing a property is incompatible.
    #[test]
    fn removing_a_property_is_incompatible() {
        let from = schema(
            r#"schema graph_v1 { node Person { id: NodeId name: String } }"#,
        );
        let to = schema(r#"schema graph_v2 { node Person { id: NodeId } }"#);

        match SpecSchemaEvolution.diff(&from, &to) {
            EvolutionKind::Incompatible { reasons } => {
                assert_eq!(reasons.len(), 1);
                assert!(reasons[0].contains("name"));
            }
            other => panic!("expected Incompatible, got {other:?}"),
        }
    }

    /// *(task S6)* Changing a property's type is incompatible.
    #[test]
    fn changing_a_property_type_is_incompatible() {
        let from = schema(
            r#"schema graph_v1 { node Person { id: NodeId age: Int64 } }"#,
        );
        let to = schema(
            r#"schema graph_v2 { node Person { id: NodeId age: String } }"#,
        );

        match SpecSchemaEvolution.diff(&from, &to) {
            EvolutionKind::Incompatible { reasons } => {
                assert_eq!(reasons.len(), 1);
                assert!(reasons[0].contains("age"));
            }
            other => panic!("expected Incompatible, got {other:?}"),
        }
    }

    /// *(task S6)* Removing a node label (and everything on it) is
    /// incompatible.
    #[test]
    fn removing_a_node_label_is_incompatible() {
        let from = schema(
            r#"
            schema graph_v1 {
              node Person { id: NodeId }
              node Organization { id: NodeId }
            }
            "#,
        );
        let to = schema(r#"schema graph_v2 { node Person { id: NodeId } }"#);

        match SpecSchemaEvolution.diff(&from, &to) {
            EvolutionKind::Incompatible { reasons } => {
                assert_eq!(reasons.len(), 1);
                assert!(reasons[0].contains("Organization"));
            }
            other => panic!("expected Incompatible, got {other:?}"),
        }
    }

    /// *(task S6)* A rename (old name gone, new name present) is
    /// observationally a removal — reported the same way, since the IDL
    /// has no explicit rename marker to distinguish the two.
    #[test]
    fn renaming_a_property_is_reported_as_a_removal() {
        let from = schema(
            r#"schema graph_v1 { node Person { id: NodeId name: String } }"#,
        );
        let to = schema(
            r#"schema graph_v2 { node Person { id: NodeId full_name: String } }"#,
        );

        match SpecSchemaEvolution.diff(&from, &to) {
            EvolutionKind::Incompatible { reasons } => {
                assert_eq!(reasons.len(), 1);
                assert!(reasons[0].contains("name") && !reasons[0].contains("full_name"));
            }
            other => panic!("expected Incompatible, got {other:?}"),
        }
    }

    /// *(task S6)* Multiple independent incompatibilities are all
    /// reported, not just the first one found.
    #[test]
    fn accumulates_multiple_incompatible_reasons() {
        let from = schema(
            r#"
            schema graph_v1 {
              node Person { id: NodeId name: String age: Int64 }
            }
            "#,
        );
        let to = schema(
            r#"
            schema graph_v2 {
              node Person { id: NodeId age: String }
            }
            "#,
        );

        match SpecSchemaEvolution.diff(&from, &to) {
            EvolutionKind::Incompatible { reasons } => assert_eq!(reasons.len(), 2),
            other => panic!("expected Incompatible, got {other:?}"),
        }
    }
}
