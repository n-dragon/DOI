//! `Validator` (tasks D7-D9, extended for the least-privilege-via-
//! telemetry use case): static validation of a parsed [`Query`] against
//! the active [`Schema`] (spec §7.2). Every violation found is collected
//! and returned together — fail-fast means "before execution starts",
//! not "stop at the first error".

use crate::{
    ComparisonOp, EdgePattern, ExistsSubquery, NodePattern, PatternStep, PropertyFilter, Query,
    ReturnItem, ValidationError, Validator, WhereCondition,
};
use graph_schema::{EdgeType, Label, ScalarType, Schema};
use std::collections::{HashMap, HashSet};

/// What's bound in a pattern (or a `NOT EXISTS` subquery's own scope,
/// which extends the outer one) at the point a `WHERE`/`RETURN` item is
/// checked. Node and edge aliases are tracked separately: a node alias
/// might be *bound but unlabeled* (e.g. `(w)` re-anchoring an outer
/// binding, or a genuinely untyped node in a plain pattern) — present
/// in `bound_nodes` but absent from `node_labels` — which is enough for
/// `AliasComparison`/`NOT EXISTS` anchor checks (identity only) but not
/// for property validation (needs a schema to check against).
#[derive(Default, Clone)]
struct Scope<'a> {
    bound_nodes: HashSet<&'a str>,
    node_labels: HashMap<&'a str, &'a Label>,
    edge_types: HashMap<&'a str, &'a EdgeType>,
}

/// [`Validator`] backed directly by a [`Schema`] — no external state.
pub struct SchemaValidator;

impl Validator for SchemaValidator {
    fn validate(&self, query: &Query, schema: &Schema) -> Result<(), Vec<ValidationError>> {
        let mut errors = Vec::new();
        let mut scope = Scope::default();

        for step in &query.pattern.steps {
            match step {
                PatternStep::Node(node) => {
                    validate_node_pattern(node, schema, &mut errors);
                    scope.bound_nodes.insert(node.alias.as_str());
                    if let Some(label) = &node.label {
                        scope.node_labels.insert(node.alias.as_str(), label);
                    }
                }
                PatternStep::Edge(edge) => {
                    validate_edge_pattern(edge, schema, &mut errors);
                    if let (Some(alias), Some(edge_type)) = (&edge.alias, &edge.edge_type) {
                        scope.edge_types.insert(alias.as_str(), edge_type);
                    }
                }
            }
        }

        for condition in &query.where_conditions {
            validate_where_condition(condition, schema, &scope, true, &mut errors);
        }

        for item in &query.returns {
            validate_return_item(item, schema, &scope, &mut errors);
        }

        if let Some(order_by) = &query.order_by {
            validate_order_by(order_by, schema, &scope, &mut errors);
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
                validate_node_property_filter(label, filter, schema, errors);
            }
        }
        None => errors.push(ValidationError::UnknownLabel(label.0.clone())),
    }
}

/// *(edge alias extension)* Also enforces the scoping decision recorded
/// in `dsl.pest`'s `edge_detail` comment: an aliased edge must be a
/// single hop — which of a `*1..3` chain's several edges the alias
/// would refer to is ambiguous, and no v1 use case needs it.
fn validate_edge_pattern(edge: &EdgePattern, schema: &Schema, errors: &mut Vec<ValidationError>) {
    if let Some(edge_type) = &edge.edge_type {
        if schema.edge_def(edge_type).is_none() {
            errors.push(ValidationError::UnknownEdgeType(edge_type.0.clone()));
        }
    }

    if let Some(alias) = &edge.alias {
        if edge.hops.min != 1 || edge.hops.max != 1 {
            errors.push(ValidationError::EdgeAliasOnVariableLengthHop {
                alias: alias.clone(),
                min: edge.hops.min,
                max: edge.hops.max,
            });
        }
    }
}

fn validate_where_condition(
    condition: &WhereCondition,
    schema: &Schema,
    scope: &Scope,
    allow_not_exists: bool,
    errors: &mut Vec<ValidationError>,
) {
    match condition {
        WhereCondition::Property(alias, filter) => {
            validate_property_ref(scope, schema, alias, &filter.property, filter.op, errors);
        }
        WhereCondition::AliasComparison { left, right, .. } => {
            for alias in [left, right] {
                if !scope.bound_nodes.contains(alias.as_str()) {
                    errors.push(ValidationError::UnknownAlias(alias.clone()));
                }
            }
        }
        WhereCondition::PropertyComparison { left, op, right } => {
            validate_property_ref(scope, schema, &left.alias, &left.property, *op, errors);
            // The right-hand side's own operator compatibility only
            // needs checking once — `op` is shared, and checking it
            // against both sides independently would either duplicate
            // the same error twice (for `Eq`/`Ne`, valid on nearly every
            // type) or double-report a real mismatch. Using `Eq` here
            // for the right side's check keeps this a pure "does this
            // property exist, with this alias resolved" check without
            // asserting the two sides must share a scalar type — v1
            // doesn't attempt cross-type unification (documented gap:
            // `g.action = w.observed_since`, comparing a String to a
            // Timestamp, isn't rejected here — a naive-but-correct
            // baseline, same posture as the rest of this validator).
            validate_property_ref(
                scope,
                schema,
                &right.alias,
                &right.property,
                ComparisonOp::Eq,
                errors,
            );
        }
        WhereCondition::NotExists(subquery) => {
            if allow_not_exists {
                validate_not_exists(subquery, schema, scope, errors);
            } else {
                errors.push(ValidationError::NestedNotExists);
            }
        }
    }
}

/// *(task D-not-exists)* Validates the scoping decision recorded on
/// `ExistsSubquery` itself: exactly one edge between two node aliases
/// that must already be bound (and unlabeled/unfiltered, i.e. genuinely
/// re-anchoring rather than redeclaring) by the *outer* pattern.
fn validate_not_exists(
    subquery: &ExistsSubquery,
    schema: &Schema,
    outer: &Scope,
    errors: &mut Vec<ValidationError>,
) {
    let [PatternStep::Node(from_node), PatternStep::Edge(edge), PatternStep::Node(to_node)] =
        subquery.pattern.steps.as_slice()
    else {
        errors.push(ValidationError::NotExistsWrongShape(
            subquery.pattern.steps.len(),
        ));
        return;
    };

    for anchor in [from_node, to_node] {
        let pre_bound = outer.bound_nodes.contains(anchor.alias.as_str());
        if !pre_bound || anchor.label.is_some() || !anchor.property_filters.is_empty() {
            errors.push(ValidationError::NotExistsAnchorNotPreBound(
                anchor.alias.clone(),
            ));
        }
    }

    validate_edge_pattern(edge, schema, errors);
    if edge.hops.min != 1 || edge.hops.max != 1 {
        errors.push(ValidationError::EdgeAliasOnVariableLengthHop {
            alias: edge
                .alias
                .clone()
                .unwrap_or_else(|| "<unaliased>".to_string()),
            min: edge.hops.min,
            max: edge.hops.max,
        });
    }
    if edge.direction != crate::Direction::Outgoing {
        errors.push(ValidationError::NotExistsMustBeOutgoing);
    }

    // Inner scope for the subquery's own WHERE: the outer bindings
    // (so `g.action` on the outer GRANTS alias still resolves) plus this
    // edge's own alias, if any. The two re-anchored node aliases are
    // already in `outer.bound_nodes`/`node_labels` — nothing to add.
    let mut inner = outer.clone();
    if let (Some(alias), Some(edge_type)) = (&edge.alias, &edge.edge_type) {
        inner.edge_types.insert(alias.as_str(), edge_type);
    }

    for condition in &subquery.where_conditions {
        validate_where_condition(condition, schema, &inner, false, errors);
    }
}

fn validate_return_item(
    item: &ReturnItem,
    schema: &Schema,
    scope: &Scope,
    errors: &mut Vec<ValidationError>,
) {
    let Some(property) = &item.property else {
        if !scope.bound_nodes.contains(item.alias.as_str())
            && !scope.edge_types.contains_key(item.alias.as_str())
        {
            errors.push(ValidationError::UnknownAlias(item.alias.clone()));
        }
        return;
    };
    // Eq is a harmless placeholder operator here: `validate_property_ref`
    // only uses `op` for the operator-compatibility check, which a bare
    // `RETURN alias.property` (no comparison at all) has no opinion on —
    // property existence is what's actually being checked.
    validate_property_ref(
        scope,
        schema,
        &item.alias,
        property,
        ComparisonOp::Eq,
        errors,
    );
}

/// *(task D14)* `alias.property` must resolve the same way a `RETURN
/// alias.property` item does — same helper, same `Eq`-as-placeholder
/// trick as `validate_return_item` (existence is what matters here, not
/// operator compatibility) which also, as a side effect, rejects a
/// `List`/`Vector` property (§3.2's two composite types have no defined
/// ordering — `operator_compatible` already excludes them from `Eq`/`Ne`,
/// the same exclusion an ordering comparison would need).
fn validate_order_by(
    order_by: &crate::OrderBy,
    schema: &Schema,
    scope: &Scope,
    errors: &mut Vec<ValidationError>,
) {
    validate_property_ref(
        scope,
        schema,
        &order_by.alias,
        &order_by.property,
        ComparisonOp::Eq,
        errors,
    );
}

/// Resolves `alias.property` against whichever of `node_labels`/
/// `edge_types` `alias` is bound in, checks the property exists there,
/// and checks `op` against its declared type. Reports `UnknownAlias` if
/// `alias` isn't bound with enough type information to check at all
/// (unbound entirely, or a node bound without a label — see `Scope`'s
/// doc comment).
fn validate_property_ref(
    scope: &Scope,
    schema: &Schema,
    alias: &str,
    property: &str,
    op: ComparisonOp,
    errors: &mut Vec<ValidationError>,
) {
    if let Some(&label) = scope.node_labels.get(alias) {
        validate_node_property_filter(
            label,
            &PropertyFilter {
                property: property.to_string(),
                op,
                value: crate::Literal::Bool(false), // unused by this check
            },
            schema,
            errors,
        );
        return;
    }
    if let Some(&edge_type) = scope.edge_types.get(alias) {
        validate_edge_property(edge_type, property, op, schema, errors);
        return;
    }
    errors.push(ValidationError::UnknownAlias(alias.to_string()));
}

/// Assumes `label` itself is already known to exist — callers only reach
/// here after `schema.node_def(label)` succeeded, so a missing property
/// is reported without re-litigating the label.
fn validate_node_property_filter(
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

/// Edge-property counterpart of `validate_node_property_filter` — edge
/// aliases (`g.action`, least-privilege use case) resolve against
/// `EdgeDef::properties` instead of `NodeDef::properties`. Unlike node
/// properties (§5.2), edge properties are never indexed by `graph-index`
/// (see its builder's doc comment) — that's an *execution-time*
/// consequence (how `graph-query` evaluates the condition, client-side
/// via `GetEdgeProperties` rather than an index lookup), not a reason to
/// skip static validation here.
fn validate_edge_property(
    edge_type: &EdgeType,
    property: &str,
    op: ComparisonOp,
    schema: &Schema,
    errors: &mut Vec<ValidationError>,
) {
    let Some(edge_def) = schema.edge_def(edge_type) else {
        return;
    };

    match edge_def.properties.iter().find(|p| p.name == property) {
        Some(prop) => {
            if !operator_compatible(&prop.ty, op) {
                errors.push(ValidationError::IncompatibleOperator {
                    property: property.to_string(),
                });
            }
        }
        None => errors.push(ValidationError::UnknownProperty {
            label: edge_type.0.clone(),
            property: property.to_string(),
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

    fn least_privilege_schema() -> Schema {
        PestSchemaParser
            .parse(std::include_str!(
                "../../../schema/least_privilege.graphidl"
            ))
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

    /// A `WHERE`/`AliasComparison` referencing an alias the pattern never
    /// bound is reported (previously silently ignored — see this
    /// module's `validate_where_condition` doc comment history).
    #[test]
    fn an_unbound_alias_in_a_comparison_fails() {
        let q = query("MATCH (p:Person) WHERE p <> ghost RETURN p");

        let errors = SchemaValidator.validate(&q, &schema()).unwrap_err();
        assert_eq!(errors, vec![ValidationError::UnknownAlias("ghost".into())]);
    }

    /// The least-privilege-via-telemetry use case's central query
    /// validates cleanly against its own schema.
    #[test]
    fn the_least_privilege_example_validates() {
        let q = query(
            r#"
            MATCH (w:Workload)-[:ASSUMES]->(r:IAMRole)-[g:GRANTS]->(d:DataStore)
            WHERE d.data_class = "sensitive"
              AND NOT EXISTS {
                MATCH (w)-[a:ACCESSED]->(d)
                WHERE a.action = g.action AND a.last_seen >= "2024-05-01T00:00:00Z"
              }
            RETURN w.service, w.team, r.arn, g.action, d.arn
            "#,
        );

        assert_eq!(
            SchemaValidator.validate(&q, &least_privilege_schema()),
            Ok(())
        );
    }

    /// An edge alias on a variable-length hop is rejected — see
    /// `dsl.pest`'s `edge_detail` doc comment for why.
    #[test]
    fn an_edge_alias_on_a_variable_length_hop_fails() {
        let q = query("MATCH (w:Workload)-[g:GRANTS*1..3]->(d:DataStore) RETURN g");

        let errors = SchemaValidator
            .validate(&q, &least_privilege_schema())
            .unwrap_err();
        assert_eq!(
            errors,
            vec![ValidationError::EdgeAliasOnVariableLengthHop {
                alias: "g".into(),
                min: 1,
                max: 3,
            }]
        );
    }

    /// A `NOT EXISTS` anchor that isn't already bound by the outer
    /// pattern is rejected.
    #[test]
    fn a_not_exists_anchor_not_bound_outside_fails() {
        let q = query(
            r#"
            MATCH (w:Workload)
            WHERE NOT EXISTS { MATCH (w)-[a:ACCESSED]->(ghost) }
            RETURN w
            "#,
        );

        let errors = SchemaValidator
            .validate(&q, &least_privilege_schema())
            .unwrap_err();
        assert_eq!(
            errors,
            vec![ValidationError::NotExistsAnchorNotPreBound("ghost".into())]
        );
    }

    /// A `NOT EXISTS` anchor that re-declares a label (instead of bare
    /// re-referencing) is rejected — it isn't the "re-anchor an outer
    /// binding" shape the scoping decision requires.
    #[test]
    fn a_not_exists_anchor_redeclaring_a_label_fails() {
        let q = query(
            r#"
            MATCH (w:Workload)-[:ASSUMES]->(r:IAMRole)-[g:GRANTS]->(d:DataStore)
            WHERE NOT EXISTS { MATCH (w:Workload)-[a:ACCESSED]->(d) }
            RETURN w
            "#,
        );

        let errors = SchemaValidator
            .validate(&q, &least_privilege_schema())
            .unwrap_err();
        assert_eq!(
            errors,
            vec![ValidationError::NotExistsAnchorNotPreBound("w".into())]
        );
    }

    /// An incoming edge inside `NOT EXISTS` is rejected — see
    /// `ValidationError::NotExistsMustBeOutgoing`'s doc comment for why.
    #[test]
    fn a_not_exists_incoming_edge_fails() {
        let q = query(
            r#"
            MATCH (w:Workload)-[:ASSUMES]->(r:IAMRole)-[g:GRANTS]->(d:DataStore)
            WHERE NOT EXISTS { MATCH (d)<-[a:ACCESSED]-(w) }
            RETURN w
            "#,
        );

        let errors = SchemaValidator
            .validate(&q, &least_privilege_schema())
            .unwrap_err();
        assert!(errors.contains(&ValidationError::NotExistsMustBeOutgoing));
    }

    /// `NOT EXISTS` nested inside another `NOT EXISTS`'s `WHERE` is
    /// rejected outright — no v1 use case needs it (see `ExistsSubquery`'s
    /// doc comment).
    #[test]
    fn a_nested_not_exists_fails() {
        let q = query(
            r#"
            MATCH (w:Workload)-[:ASSUMES]->(r:IAMRole)-[g:GRANTS]->(d:DataStore)
            WHERE NOT EXISTS {
              MATCH (w)-[a:ACCESSED]->(d)
              WHERE NOT EXISTS { MATCH (w)-[a2:RAN_AS]->(r) }
            }
            RETURN w
            "#,
        );

        let errors = SchemaValidator
            .validate(&q, &least_privilege_schema())
            .unwrap_err();
        assert!(errors.contains(&ValidationError::NestedNotExists));
    }

    /// *(task D14)* A valid `ORDER BY alias.property` passes.
    #[test]
    fn a_valid_order_by_passes() {
        let q = query(
            r#"
            MATCH (p:Person)-[:KNOWS*1..3]->(friend:Person)
            RETURN friend ORDER BY friend.birth_year DESC
            "#,
        );

        assert_eq!(SchemaValidator.validate(&q, &schema()), Ok(()));
    }

    /// *(task D14)* `ORDER BY` on an alias the pattern never bound is
    /// reported the same way an unbound `RETURN`/`WHERE` alias is.
    #[test]
    fn an_order_by_on_an_unbound_alias_fails() {
        let q = query("MATCH (p:Person) RETURN p ORDER BY ghost.name");

        let errors = SchemaValidator.validate(&q, &schema()).unwrap_err();
        assert_eq!(errors, vec![ValidationError::UnknownAlias("ghost".into())]);
    }

    /// *(task D14)* `ORDER BY` on a property the label doesn't declare is
    /// reported.
    #[test]
    fn an_order_by_on_an_unknown_property_fails() {
        let q = query("MATCH (p:Person) RETURN p ORDER BY p.nickname");

        let errors = SchemaValidator.validate(&q, &schema()).unwrap_err();
        assert_eq!(
            errors,
            vec![ValidationError::UnknownProperty {
                label: "Person".into(),
                property: "nickname".into(),
            }]
        );
    }

    /// `RETURN g.action` (edge alias property) validates against the
    /// edge type's own declared properties.
    #[test]
    fn a_return_item_on_an_unknown_edge_property_fails() {
        let q = query(
            r#"
            MATCH (r:IAMRole)-[g:GRANTS]->(d:DataStore)
            RETURN g.nonexistent
            "#,
        );

        let errors = SchemaValidator
            .validate(&q, &least_privilege_schema())
            .unwrap_err();
        assert_eq!(
            errors,
            vec![ValidationError::UnknownProperty {
                label: "GRANTS".into(),
                property: "nonexistent".into(),
            }]
        );
    }
}
