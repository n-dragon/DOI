//! Walks `grammar::SchemaGrammar` output (task S1) into a [`crate::Schema`]
//! (task S2). Semantic checks that don't require comparing two schemas
//! (duplicate labels/edge types, edge endpoints referencing a known label)
//! are folded into `SchemaError::Parse` here, per spec §3.4 — schema
//! construction is all-or-nothing, there's no partial/"schema with
//! warnings" state.

use crate::grammar::{Rule, SchemaGrammar};
use crate::{
    EdgeDef, EdgeType, Label, NodeDef, PropertyDef, ScalarType, Schema, SchemaError, SchemaParser,
};
use pest::iterators::Pair;
use pest::Parser;
use std::collections::BTreeMap;

/// [`SchemaParser`] backed by the `pest` grammar in `schema.pest`.
pub struct PestSchemaParser;

impl SchemaParser for PestSchemaParser {
    fn parse(&self, idl_source: &str) -> Result<Schema, SchemaError> {
        let mut pairs = SchemaGrammar::parse(Rule::schema_file, idl_source)
            .map_err(|e| SchemaError::Parse(e.to_string()))?;

        let schema_file = pairs
            .next()
            .expect("schema_file is the grammar's only top-level rule");
        let schema_decl = schema_file
            .into_inner()
            .find(|p| p.as_rule() == Rule::schema_decl)
            .expect("schema_file always contains exactly one schema_decl");

        let mut inner = schema_decl.into_inner();
        let version = inner
            .next()
            .expect("schema_decl always starts with its version ident")
            .as_str()
            .to_string();

        let mut nodes = BTreeMap::new();
        let mut edges = BTreeMap::new();

        for decl in inner {
            match decl.as_rule() {
                Rule::node_decl => {
                    let node = parse_node_decl(decl);
                    if nodes.contains_key(&node.label) {
                        return Err(SchemaError::Parse(format!(
                            "duplicate node label: {}",
                            node.label
                        )));
                    }
                    nodes.insert(node.label.clone(), node);
                }
                Rule::edge_decl => {
                    let edge = parse_edge_decl(decl);
                    if edges.contains_key(&edge.edge_type) {
                        return Err(SchemaError::Parse(format!(
                            "duplicate edge type: {}",
                            edge.edge_type
                        )));
                    }
                    edges.insert(edge.edge_type.clone(), edge);
                }
                other => unreachable!("schema_decl only yields node_decl/edge_decl, got {other:?}"),
            }
        }

        for edge in edges.values() {
            if !nodes.contains_key(&edge.from) {
                return Err(SchemaError::Parse(format!(
                    "edge {} references unknown source label {}",
                    edge.edge_type, edge.from
                )));
            }
            if !nodes.contains_key(&edge.to) {
                return Err(SchemaError::Parse(format!(
                    "edge {} references unknown target label {}",
                    edge.edge_type, edge.to
                )));
            }
        }

        Ok(Schema {
            version,
            nodes,
            edges,
        })
    }
}

fn parse_node_decl(pair: Pair<Rule>) -> NodeDef {
    let mut inner = pair.into_inner();
    let label = Label(
        inner
            .next()
            .expect("node_decl always starts with its label ident")
            .as_str()
            .to_string(),
    );

    let mut properties = Vec::new();
    for p in inner {
        match p.as_rule() {
            Rule::id_field => {}
            Rule::property_decl => properties.push(parse_property_decl(p)),
            other => {
                unreachable!("node_decl body only yields id_field/property_decl, got {other:?}")
            }
        }
    }

    NodeDef { label, properties }
}

fn parse_edge_decl(pair: Pair<Rule>) -> EdgeDef {
    let mut inner = pair.into_inner();
    let edge_type = EdgeType(
        inner
            .next()
            .expect("edge_decl always starts with its type ident")
            .as_str()
            .to_string(),
    );

    let mut from = None;
    let mut to = None;
    let mut properties = Vec::new();
    for p in inner {
        match p.as_rule() {
            Rule::from_field => from = Some(field_label(p)),
            Rule::to_field => to = Some(field_label(p)),
            Rule::property_decl => properties.push(parse_property_decl(p)),
            other => unreachable!(
                "edge_decl body only yields from_field/to_field/property_decl, got {other:?}"
            ),
        }
    }

    EdgeDef {
        edge_type,
        from: from.expect("grammar requires a from_field"),
        to: to.expect("grammar requires a to_field"),
        properties,
    }
}

fn field_label(pair: Pair<Rule>) -> Label {
    Label(
        pair.into_inner()
            .next()
            .expect("from_field/to_field always wrap a label ident")
            .as_str()
            .to_string(),
    )
}

fn parse_property_decl(pair: Pair<Rule>) -> PropertyDef {
    let mut indexed = false;
    let mut name = None;
    let mut ty = None;
    let mut optional = false;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::indexed_marker => indexed = true,
            Rule::ident => name = Some(p.as_str().to_string()),
            Rule::scalar_type => ty = Some(parse_scalar_type(p)),
            Rule::optional_marker => optional = true,
            other => unreachable!(
                "property_decl only yields indexed_marker/ident/scalar_type/optional_marker, got {other:?}"
            ),
        }
    }

    PropertyDef {
        name: name.expect("grammar requires a property name"),
        ty: ty.expect("grammar requires a property type"),
        optional,
        indexed,
    }
}

fn parse_scalar_type(pair: Pair<Rule>) -> ScalarType {
    let inner = pair
        .into_inner()
        .next()
        .expect("scalar_type always wraps exactly one alternative");

    match inner.as_rule() {
        Rule::simple_type => match inner.as_str() {
            "Int64" => ScalarType::Int64,
            "Float64" => ScalarType::Float64,
            "Bool" => ScalarType::Bool,
            "String" => ScalarType::String,
            "Timestamp" => ScalarType::Timestamp,
            "Bytes" => ScalarType::Bytes,
            other => unreachable!("grammar's simple_type alternatives are exhaustive, got {other:?}"),
        },
        Rule::list_type => {
            let elem = inner
                .into_inner()
                .next()
                .expect("list_type always wraps its element scalar_type");
            ScalarType::List(Box::new(parse_scalar_type(elem)))
        }
        Rule::vector_type => {
            let dim = inner
                .into_inner()
                .next()
                .expect("vector_type always wraps its dimension number")
                .as_str()
                .parse::<u32>()
                .expect("the `number` rule only matches ASCII digits");
            ScalarType::Vector { dim }
        }
        other => unreachable!("scalar_type only wraps simple_type/list_type/vector_type, got {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The spec §3.4 example, multi-node/multi-edge, must round-trip into
    /// the `Schema` it visibly describes. *(task S3)*
    #[test]
    fn parses_the_spec_example_into_the_expected_schema() {
        let source = r#"
            schema graph_v1 {
              node Person {
                id: NodeId
                @indexed name: String
                birth_year: Int64?
              }

              node Organization {
                id: NodeId
                name: String
              }

              edge WORKS_AT {
                from: Person
                to: Organization
                since: Timestamp?
              }

              edge KNOWS {
                from: Person
                to: Person
                since: Timestamp?
              }
            }
        "#;

        let schema = PestSchemaParser
            .parse(source)
            .expect("the spec §3.4 example should parse");

        assert_eq!(schema.version, "graph_v1");
        assert_eq!(schema.nodes.len(), 2);
        assert_eq!(schema.edges.len(), 2);

        let person = schema
            .node_def(&Label("Person".into()))
            .expect("Person node should be present");
        assert_eq!(person.properties.len(), 2);

        let name = person
            .properties
            .iter()
            .find(|p| p.name == "name")
            .expect("name property");
        assert_eq!(name.ty, ScalarType::String);
        assert!(name.indexed);
        assert!(!name.optional);

        let birth_year = person
            .properties
            .iter()
            .find(|p| p.name == "birth_year")
            .expect("birth_year property");
        assert_eq!(birth_year.ty, ScalarType::Int64);
        assert!(!birth_year.indexed);
        assert!(birth_year.optional);

        let works_at = schema
            .edge_def(&EdgeType("WORKS_AT".into()))
            .expect("WORKS_AT edge should be present");
        assert_eq!(works_at.from, Label("Person".into()));
        assert_eq!(works_at.to, Label("Organization".into()));
        assert_eq!(works_at.properties.len(), 1);

        let knows = schema
            .edge_def(&EdgeType("KNOWS".into()))
            .expect("KNOWS edge should be present");
        assert_eq!(knows.from, Label("Person".into()));
        assert_eq!(knows.to, Label("Person".into()));
    }

    /// The two composite scalar types (§3.2) must also round-trip through
    /// the parser, not just through the grammar (already covered by
    /// `grammar::tests::accepts_list_and_vector_properties`).
    #[test]
    fn parses_list_and_vector_property_types() {
        let source = r#"
            schema graph_v1 {
              node Document {
                id: NodeId
                tags: List<String>
                embedding: Vector<Float32, 768>
              }
            }
        "#;

        let schema = PestSchemaParser
            .parse(source)
            .expect("List<T> and Vector<Float32, N> properties should parse");
        let doc = schema.node_def(&Label("Document".into())).unwrap();

        let tags = doc.properties.iter().find(|p| p.name == "tags").unwrap();
        assert_eq!(tags.ty, ScalarType::List(Box::new(ScalarType::String)));

        let embedding = doc
            .properties
            .iter()
            .find(|p| p.name == "embedding")
            .unwrap();
        assert_eq!(embedding.ty, ScalarType::Vector { dim: 768 });
    }

    /// Syntax errors surface as `SchemaError::Parse`, via the underlying
    /// grammar (task S1) rejecting the input outright. *(task S4)*
    #[test]
    fn rejects_a_property_without_a_type() {
        let source = r#"
            schema graph_v1 {
              node Person {
                id: NodeId
                name:
              }
            }
        "#;

        let err = PestSchemaParser.parse(source).unwrap_err();
        assert!(matches!(err, SchemaError::Parse(_)));
    }

    /// Duplicate labels are a semantic error the grammar can't catch on
    /// its own (both declarations are individually well-formed).
    /// *(task S4)*
    #[test]
    fn rejects_a_duplicate_node_label() {
        let source = r#"
            schema graph_v1 {
              node Person { id: NodeId }
              node Person { id: NodeId }
            }
        "#;

        let err = PestSchemaParser.parse(source).unwrap_err();
        assert!(matches!(err, SchemaError::Parse(_)));
    }

    /// *(task S4)*
    #[test]
    fn rejects_a_duplicate_edge_type() {
        let source = r#"
            schema graph_v1 {
              node Person { id: NodeId }
              edge KNOWS { from: Person to: Person }
              edge KNOWS { from: Person to: Person }
            }
        "#;

        let err = PestSchemaParser.parse(source).unwrap_err();
        assert!(matches!(err, SchemaError::Parse(_)));
    }

    /// An edge type whose source label was never declared as a node.
    /// *(task S4)*
    #[test]
    fn rejects_an_edge_type_with_unknown_source_label() {
        let source = r#"
            schema graph_v1 {
              node Organization { id: NodeId }
              edge WORKS_AT {
                from: Person
                to: Organization
              }
            }
        "#;

        let err = PestSchemaParser.parse(source).unwrap_err();
        assert!(matches!(err, SchemaError::Parse(_)));
    }

    /// An edge type whose target label was never declared as a node.
    /// *(task S4)*
    #[test]
    fn rejects_an_edge_type_with_unknown_target_label() {
        let source = r#"
            schema graph_v1 {
              node Person { id: NodeId }
              edge WORKS_AT {
                from: Person
                to: Organization
              }
            }
        "#;

        let err = PestSchemaParser.parse(source).unwrap_err();
        assert!(matches!(err, SchemaError::Parse(_)));
    }
}
