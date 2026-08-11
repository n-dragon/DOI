//! `Validator` (tasks D7-D9): static validation of a parsed [`Query`]
//! against the active [`Schema`] (spec §7.2). Every violation found is
//! collected and returned together — fail-fast means "before execution
//! starts", not "stop at the first error".

use crate::{
    ComparisonOp, EdgePattern, NodePattern, PatternStep, PropertyFilter, Query, ValidationError,
    Validator, WhereCondition,
};
use graph_schema::{Label, ScalarType, Schema};
use std::collections::HashMap;

/// [`Validator`] backed directly by a [`Schema`] — no external state.
pub struct SchemaValidator;

impl Validator for SchemaValidator {
    fn validate(&self, query: &Query, schema: &Schema) -> Result<(), Vec<ValidationError>> {
        let mut errors = Vec::new();
        let mut alias_labels: HashMap<&str, &Label> = HashMap::new();

        for step in &query.pattern.steps {
            match step {
                PatternStep::Node(node) => {
                    validate_node_pattern(node, schema, &mut errors);
                    if let Some(label) = &node.label {
                        alias_labels.insert(node.alias.as_str(), label);
                    }
                }
                PatternStep::Edge(edge) => validate_edge_pattern(edge, schema, &mut errors),
            }
        }

        for condition in &query.where_conditions {
            if let WhereCondition::Property(alias, filter) = condition {
                if let Some(&label) = alias_labels.get(alias.as_str()) {
                    validate_property_filter(label, filter, schema, &mut errors);
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

fn validate_node_pattern(node: &NodePattern, schema: &Schema, errors: &mut Vec<ValidationError>) {
    let Some(label) = &node.label else { return };

    match schema.node_def(label) {
        Some(_) => {
            for filter in &node.property_filters {
                validate_property_filter(label, filter, schema, errors);
            }
        }
        None => errors.push(ValidationError::UnknownLabel(label.0.clone())),
    }
}

fn validate_edge_pattern(edge: &EdgePattern, schema: &Schema, errors: &mut Vec<ValidationError>) {
    let Some(edge_type) = &edge.edge_type else {
        return;
    };

    if schema.edge_def(edge_type).is_none() {
        errors.push(ValidationError::UnknownEdgeType(edge_type.0.clone()));
    }
}

/// Assumes `label` itself is already known to exist — callers only reach
/// here after `schema.node_def(label)` succeeded, so a missing property
/// is reported without re-litigating the label.
fn validate_property_filter(
    label: &Label,
    filter: &PropertyFilter,
    schema: &Schema,
    errors: &mut Vec<ValidationError>,
) {
    let Some(node_def) = schema.node_def(label) else {
        return;
    };

    match node_def
        .properties
        .iter()
        .find(|p| p.name == filter.property)
    {
        Some(prop) => {
            if !operator_compatible(&prop.ty, filter.op) {
                errors.push(ValidationError::IncompatibleOperator {
                    property: filter.property.clone(),
                });
            }
        }
        None => errors.push(ValidationError::UnknownProperty {
            label: label.0.clone(),
            property: filter.property.clone(),
        }),
    }
}

/// Equality/inequality is valid for any scalar except the two composite
/// types (§3.2); ordering operators are valid only where "greater than"
/// has an unambiguous meaning — numeric and timestamp properties.
fn operator_compatible(ty: &ScalarType, op: ComparisonOp) -> bool {
    match op {
        ComparisonOp::Eq | ComparisonOp::Ne => {
            !matches!(ty, ScalarType::List(_) | ScalarType::Vector { .. })
        }
        ComparisonOp::Lt | ComparisonOp::Lte | ComparisonOp::Gt | ComparisonOp::Gte => {
            matches!(
                ty,
                ScalarType::Int64 | ScalarType::Float64 | ScalarType::Timestamp
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Parser as DslParser, PestParser};
    use graph_schema::{PestSchemaParser, SchemaParser};

    fn schema() -> Schema {
        PestSchemaParser
            .parse(
                r#"
                schema graph_v1 {
                  node Person {
                    id: NodeId
                    name: String
                    birth_year: Int64?
                  }
                  node Organization {
                    id: NodeId
                    name: String
                  }
                  edge WORKS_AT {
                    from: Person
                    to: Organization
                  }
                  edge KNOWS {
                    from: Person
                    to: Person
                  }
                }
                "#,
            )
            .expect("valid IDL")
    }

    fn query(dsl: &str) -> Query {
        PestParser.parse(dsl).expect("valid DSL")
    }

    /// *(task D9)* A query whose labels, edge types and WHERE property
    /// usage are all valid against the schema passes.
    #[test]
    fn a_valid_query_passes() {
        let q = query(
            r#"
            MATCH (p:Person {name: "Alice"})-[:KNOWS*1..3]->(friend:Person)
            WHERE friend.birth_year > 1990
            RETURN friend
            "#,
        );

        assert_eq!(SchemaValidator.validate(&q, &schema()), Ok(()));
    }

    /// *(task D9)* An unknown node label is reported.
    #[test]
    fn an_unknown_label_fails() {
        let q = query("MATCH (p:Robot) RETURN p");

        let errors = SchemaValidator.validate(&q, &schema()).unwrap_err();
        assert_eq!(errors, vec![ValidationError::UnknownLabel("Robot".into())]);
    }

    /// *(task D9)* An unknown edge type is reported.
    #[test]
    fn an_unknown_edge_type_fails() {
        let q = query("MATCH (p:Person)-[:LIKES]->(o:Organization) RETURN p");

        let errors = SchemaValidator.validate(&q, &schema()).unwrap_err();
        assert_eq!(
            errors,
            vec![ValidationError::UnknownEdgeType("LIKES".into())]
        );
    }

    /// *(task D9)* An ordering operator on a `String` property is
    /// incompatible.
    #[test]
    fn an_incompatible_operator_fails() {
        let q = query(
            r#"
            MATCH (p:Person)
            WHERE p.name > "Alice"
            RETURN p
            "#,
        );

        let errors = SchemaValidator.validate(&q, &schema()).unwrap_err();
        assert_eq!(
            errors,
            vec![ValidationError::IncompatibleOperator {
                property: "name".into()
            }]
        );
    }

    /// An unknown property on a known label is reported.
    #[test]
    fn an_unknown_property_fails() {
        let q = query(
            r#"
            MATCH (p:Person)
            WHERE p.nickname = "Al"
            RETURN p
            "#,
        );

        let errors = SchemaValidator.validate(&q, &schema()).unwrap_err();
        assert_eq!(
            errors,
            vec![ValidationError::UnknownProperty {
                label: "Person".into(),
                property: "nickname".into(),
            }]
        );
    }

    /// Errors accumulate — an unknown label and an unrelated unknown
    /// edge type in the same query are both reported.
    #[test]
    fn accumulates_multiple_errors() {
        let q = query("MATCH (p:Robot)-[:LIKES]->(o:Organization) RETURN p");

        let errors = SchemaValidator.validate(&q, &schema()).unwrap_err();
        assert_eq!(errors.len(), 2);
    }
}
