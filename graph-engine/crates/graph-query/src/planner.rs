//! [`Planner`] (task Q1): naive lowering of a validated [`Query`] into a
//! [`QueryPlan`], in pattern order (spec §7.3 — cost-based choices are
//! explicitly deferred).
//!
//! v1's planner only handles what the spec's two priority query shapes
//! (§7.1) actually need: a pattern that starts with a single labeled node
//! carrying at most one equality property filter (the `MATCH (p:Person
//! {name: "Alice"})` shape — no filter at all lowers to
//! `PlanStep::ResolveAll` instead, least-privilege-via-telemetry use
//! case). Anything else panics with a clear message rather than silently
//! producing a wrong plan — `Planner::plan` has no `Result` in its
//! signature (an earlier scaffolding decision this task doesn't
//! revisit), so there's no error channel to report a shape v1 doesn't
//! support. `WHERE` conditions comparing two aliases to each other
//! (`colleague <> p`, spec §7.1's pattern-matching example), and any
//! top-level `PropertyComparison` outside a `NOT EXISTS` subquery, are
//! silently dropped rather than enforced — full support is future work,
//! tracked as a known gap rather than implemented halfway (same posture
//! this doc comment already took for `AliasComparison`).

use crate::{AntiJoinStep, PlanStep, Planner, QueryPlan};
use graph_dsl::{
    ComparisonOp, ExistsSubquery, Literal, NodePattern, PatternStep, PropertyFilter, Query,
    WhereCondition,
};
use graph_index::PropertyKey;

pub struct NaivePlanner;

impl Planner for NaivePlanner {
    fn plan(&self, query: &Query) -> QueryPlan {
        let steps = &query.pattern.steps;
        let PatternStep::Node(first) = steps
            .first()
            .expect("a validated pattern always has at least one node")
        else {
            panic!("a pattern's first step is always a node");
        };

        // A WHERE condition targeting the start alias itself
        // (`MATCH (role:IAMRole) WHERE role.is_admin = true`, no hop at
        // all) used to be silently dropped here — only a hop-target's
        // WHERE conditions were ever routed anywhere (the loop below,
        // building each ExpandHop's `filters`). Same extraction, applied
        // to `first` instead of `to_node`, so the start node gets a
        // chance at its own WHERE conditions too.
        let start_where_filters = query
            .where_conditions
            .iter()
            .filter_map(|c| match c {
                WhereCondition::Property(alias, filter) if alias == &first.alias => {
                    Some(filter.clone())
                }
                _ => None,
            })
            .collect::<Vec<_>>();

        let mut plan_steps = vec![resolve_start_or_all_step(first, &start_where_filters)];
        let mut from_alias = first.alias.clone();
        let mut i = 1;

        while i < steps.len() {
            let PatternStep::Edge(edge) = &steps[i] else {
                panic!("expected an edge step at pattern position {i}");
            };
            let PatternStep::Node(to_node) = steps
                .get(i + 1)
                .unwrap_or_else(|| panic!("edge step at position {i} has no node after it"))
            else {
                panic!("expected a node step at pattern position {}", i + 1);
            };

            let to_label = to_node.label.as_ref().map(|l| l.0.clone());
            let filters = query
                .where_conditions
                .iter()
                .filter_map(|c| match c {
                    WhereCondition::Property(alias, filter) if alias == &to_node.alias => {
                        Some(filter.clone())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            // A filter with nowhere to route (unlabeled target) can't be
            // enforced — drop it rather than plan a lookup that would
            // panic at execution time.
            let filters = if to_label.is_some() {
                filters
            } else {
                Vec::new()
            };

            plan_steps.push(PlanStep::ExpandHop {
                from_alias: from_alias.clone(),
                to_alias: to_node.alias.clone(),
                to_label,
                edge_type: edge.edge_type.as_ref().map(|t| t.0.clone()),
                direction: edge.direction,
                hops: edge.hops,
                filters,
                edge_alias: edge.alias.clone(),
            });

            from_alias = to_node.alias.clone();
            i += 2;
        }

        // *(least-privilege-via-telemetry use case)* `NOT EXISTS`
        // clauses compile to `AntiJoin` steps appended after the whole
        // main pattern chain — every alias a `NOT EXISTS` re-anchors on
        // (D7-D9 requires them already bound by the outer `MATCH`) is
        // only guaranteed resolved once the loop above has run to
        // completion.
        for condition in &query.where_conditions {
            if let WhereCondition::NotExists(subquery) = condition {
                plan_steps.push(compile_anti_join(subquery));
            }
        }

        QueryPlan {
            steps: plan_steps,
            project: query.returns.clone(),
        }
    }
}

fn resolve_start_or_all_step(node: &NodePattern, where_filters: &[PropertyFilter]) -> PlanStep {
    let label = node
        .label
        .as_ref()
        .expect("v1's naive planner requires a labeled start node");

    // Inline map filter (`{property: value}`) wins if present — same
    // equality-only shape `ResolveStart` already supports, and it was
    // already here before `where_filters` existed. Otherwise fall back
    // to a WHERE condition on the start alias, but only when it's a
    // single equality check: `ResolveStart` is one indexed point lookup,
    // it has no post-filter step the way `ExpandHop` does (`filters` +
    // `evaluate_filter`) — a non-equality op or more than one condition
    // can't be represented by it. Panic rather than silently planning a
    // `ResolveAll` that drops the condition and returns every node of
    // the label, same posture this module already takes for shapes v1
    // doesn't support (see the module doc comment).
    let filter = node.property_filters.first().or_else(|| {
        match where_filters {
            [] => None,
            [single] => Some(single),
            multiple => panic!(
                "v1's ResolveStart supports at most one WHERE condition on the pattern's \
                 start node ({count} target {alias}.*) — split into a chained MATCH instead",
                count = multiple.len(),
                alias = node.alias
            ),
        }
    });

    match filter {
        Some(filter) if filter.op == ComparisonOp::Eq => PlanStep::ResolveStart {
            alias: node.alias.clone(),
            label_or_type: label.0.clone(),
            property: filter.property.clone(),
            key: literal_to_property_key(&filter.value),
        },
        Some(filter) => panic!(
            "v1's ResolveStart only supports an equality filter on the pattern's start node \
             ({op:?} on {alias}.{property} isn't an index lookup key)",
            op = filter.op,
            alias = node.alias,
            property = filter.property
        ),
        // *(least-privilege-via-telemetry use case)* No filter at all —
        // `MATCH (w:Workload)` — resolve every node of the label rather
        // than requiring a lookup key that doesn't exist for this shape.
        None => PlanStep::ResolveAll {
            alias: node.alias.clone(),
            label: label.0.clone(),
        },
    }
}

/// *(least-privilege-via-telemetry use case)* Compiles one
/// `NOT EXISTS { MATCH (from)-[edge]->(to) WHERE ... }` into an
/// [`AntiJoinStep`]. The validator (D7-D9) already guarantees the
/// subquery's pattern is exactly `[node, edge, node]` with both nodes
/// pre-bound and the edge outgoing — this only has to sort each
/// `WHERE` condition into `literal_conditions` (a plain property filter
/// on the subquery's own edge alias) or `outer_conditions` (comparing
/// that edge's property against a *different*, outer-bound alias's
/// property, e.g. `a.action = g.action`); anything else (an
/// `AliasComparison`, or a `PropertyComparison` naming neither side as
/// the inner edge alias) is dropped, same "documented gap" posture as
/// the module doc comment's `AliasComparison` note.
fn compile_anti_join(subquery: &ExistsSubquery) -> PlanStep {
    let [PatternStep::Node(from_node), PatternStep::Edge(edge), PatternStep::Node(to_node)] =
        subquery.pattern.steps.as_slice()
    else {
        panic!("the validator guarantees a NOT EXISTS pattern is exactly [node, edge, node]");
    };

    let mut literal_conditions = Vec::new();
    let mut outer_conditions = Vec::new();
    let inner_alias = edge.alias.as_deref();

    for condition in &subquery.where_conditions {
        match condition {
            WhereCondition::Property(alias, filter) if Some(alias.as_str()) == inner_alias => {
                literal_conditions.push(filter.clone());
            }
            WhereCondition::PropertyComparison { left, op, right }
                if Some(left.alias.as_str()) == inner_alias =>
            {
                outer_conditions.push((left.property.clone(), *op, right.clone()));
            }
            WhereCondition::PropertyComparison { left, op, right }
                if Some(right.alias.as_str()) == inner_alias =>
            {
                // The inner edge alias is on the right — flip both
                // sides *and* the operator (`a > b` becomes `b < a`,
                // not `b > a`) so `outer_conditions` can assume
                // "inner OP outer" uniformly everywhere else.
                outer_conditions.push((right.property.clone(), flip_op(*op), left.clone()));
            }
            _ => {}
        }
    }

    PlanStep::AntiJoin(AntiJoinStep {
        from_alias: from_node.alias.clone(),
        to_alias: to_node.alias.clone(),
        direction: edge.direction,
        edge_alias: edge.alias.clone(),
        edge_type: edge.edge_type.as_ref().map(|t| t.0.clone()),
        literal_conditions,
        outer_conditions,
    })
}

fn flip_op(op: ComparisonOp) -> ComparisonOp {
    match op {
        ComparisonOp::Eq => ComparisonOp::Eq,
        ComparisonOp::Ne => ComparisonOp::Ne,
        ComparisonOp::Lt => ComparisonOp::Gt,
        ComparisonOp::Lte => ComparisonOp::Gte,
        ComparisonOp::Gt => ComparisonOp::Lt,
        ComparisonOp::Gte => ComparisonOp::Lte,
    }
}

pub(crate) fn literal_to_property_key(literal: &Literal) -> PropertyKey {
    match literal {
        Literal::Int64(v) => PropertyKey::Int64(*v),
        Literal::Bool(v) => PropertyKey::Bool(*v),
        Literal::String(v) => PropertyKey::String(v.clone()),
        Literal::Float64(_) => panic!(
            "PropertyIndex has no PropertyKey variant for Float64 (see its doc comment) - \
             a float-valued equality filter can't be planned in v1"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graph_dsl::{Parser as DslParser, PestParser};

    fn plan_of(dsl: &str) -> QueryPlan {
        let query = PestParser.parse(dsl).expect("valid DSL");
        NaivePlanner.plan(&query)
    }

    /// *(task Q1)* The spec §7.1 k-hop example lowers to exactly
    /// ResolveStart + one hop-ranged ExpandHop.
    #[test]
    fn plans_the_k_hop_example() {
        let plan = plan_of(
            r#"
            MATCH (p:Person {name: "Alice"})-[:KNOWS*1..3]->(friend:Person)
            WHERE friend.birth_year > 1990
            RETURN friend
            "#,
        );

        assert_eq!(plan.steps.len(), 2);
        match &plan.steps[0] {
            PlanStep::ResolveStart {
                alias,
                label_or_type,
                property,
                key,
            } => {
                assert_eq!(alias, "p");
                assert_eq!(label_or_type, "Person");
                assert_eq!(property, "name");
                assert_eq!(*key, PropertyKey::String("Alice".to_string()));
            }
            other => panic!("expected ResolveStart, got {other:?}"),
        }

        match &plan.steps[1] {
            PlanStep::ExpandHop {
                from_alias,
                to_alias,
                to_label,
                edge_type,
                hops,
                filters,
                edge_alias,
                ..
            } => {
                assert_eq!(from_alias, "p");
                assert_eq!(to_alias, "friend");
                assert_eq!(to_label.as_deref(), Some("Person"));
                assert_eq!(edge_type.as_deref(), Some("KNOWS"));
                assert_eq!(hops.min, 1);
                assert_eq!(hops.max, 3);
                assert_eq!(filters.len(), 1);
                assert_eq!(filters[0].property, "birth_year");
                assert_eq!(*edge_alias, None);
            }
            other => panic!("expected ExpandHop, got {other:?}"),
        }

        assert_eq!(plan.project.len(), 1);
        assert_eq!(plan.project[0].alias, "friend");
        assert_eq!(plan.project[0].property, None);
    }

    /// *(task Q1)* A multi-hop pattern (two edges) lowers to two
    /// ExpandHop steps, each carrying its own direction.
    #[test]
    fn plans_a_multi_edge_pattern() {
        let plan = plan_of(
            "MATCH (p:Person {name: \"Alice\"})-[:WORKS_AT]->(o:Organization)<-[:WORKS_AT]-(c:Person) RETURN c, o",
        );

        assert_eq!(plan.steps.len(), 3);
        let PlanStep::ExpandHop { direction: d1, .. } = &plan.steps[1] else {
            panic!("expected ExpandHop")
        };
        let PlanStep::ExpandHop { direction: d2, .. } = &plan.steps[2] else {
            panic!("expected ExpandHop")
        };
        assert_eq!(*d1, graph_dsl::Direction::Outgoing);
        assert_eq!(*d2, graph_dsl::Direction::Incoming);
    }

    /// *(least-privilege-via-telemetry use case)* A start node with no
    /// inline filter lowers to `ResolveAll`, not `ResolveStart`.
    #[test]
    fn plans_a_filterless_start_as_resolve_all() {
        let plan = plan_of("MATCH (w:Workload) RETURN w");

        match &plan.steps[0] {
            PlanStep::ResolveAll { alias, label } => {
                assert_eq!(alias, "w");
                assert_eq!(label, "Workload");
            }
            other => panic!("expected ResolveAll, got {other:?}"),
        }
    }

    /// Regression test: a `WHERE` equality condition on the pattern's
    /// *start* alias (`MATCH (role:IAMRole) WHERE role.is_admin = true`,
    /// no hop at all) used to be silently dropped — nothing ever routed
    /// it anywhere, so the planner fell through to `ResolveAll` and the
    /// query returned every `IAMRole` regardless of `is_admin`. It must
    /// plan the same `ResolveStart` an equivalent inline filter would.
    #[test]
    fn routes_a_where_condition_on_the_start_alias_into_resolve_start() {
        let plan = plan_of(r#"MATCH (role:IAMRole) WHERE role.is_admin = true RETURN role"#);

        assert_eq!(plan.steps.len(), 1);
        match &plan.steps[0] {
            PlanStep::ResolveStart {
                alias,
                label_or_type,
                property,
                key,
            } => {
                assert_eq!(alias, "role");
                assert_eq!(label_or_type, "IAMRole");
                assert_eq!(property, "is_admin");
                assert_eq!(*key, PropertyKey::Bool(true));
            }
            other => panic!("expected ResolveStart, got {other:?}"),
        }
    }

    /// A non-equality `WHERE` condition on the start alias can't be
    /// represented by `ResolveStart` (one indexed point lookup, no
    /// post-filter step) — this must panic rather than silently plan a
    /// `ResolveAll` that drops the condition.
    #[test]
    #[should_panic(expected = "only supports an equality filter")]
    fn panics_on_a_non_equality_where_condition_on_the_start_alias() {
        plan_of(r#"MATCH (d:DataStore) WHERE d.name > "m" RETURN d"#);
    }

    /// Same posture for more than one `WHERE` condition on the start
    /// alias — still one indexed point lookup, still no post-filter step.
    #[test]
    #[should_panic(expected = "at most one WHERE condition")]
    fn panics_on_multiple_where_conditions_on_the_start_alias() {
        plan_of(
            r#"MATCH (d:DataStore) WHERE d.name = "s3-ops-logs" AND d.classification = "internal" RETURN d"#,
        );
    }

    /// The least-privilege-via-telemetry use case's central query
    /// compiles into the expected plan shape: ResolveAll, two
    /// ExpandHops (the second carrying an edge alias), and a trailing
    /// AntiJoin with one literal and one outer-resolved condition.
    #[test]
    fn plans_the_least_privilege_example() {
        let plan = plan_of(
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

        assert_eq!(plan.steps.len(), 4);
        assert!(matches!(plan.steps[0], PlanStep::ResolveAll { .. }));

        let PlanStep::ExpandHop {
            edge_alias: grants_alias,
            ..
        } = &plan.steps[2]
        else {
            panic!("expected the GRANTS ExpandHop");
        };
        assert_eq!(grants_alias.as_deref(), Some("g"));

        let PlanStep::AntiJoin(anti_join) = &plan.steps[3] else {
            panic!("expected a trailing AntiJoin step");
        };
        assert_eq!(anti_join.from_alias, "w");
        assert_eq!(anti_join.to_alias, "d");
        assert_eq!(anti_join.edge_alias.as_deref(), Some("a"));
        assert_eq!(anti_join.edge_type.as_deref(), Some("ACCESSED"));
        assert_eq!(anti_join.direction, graph_dsl::Direction::Outgoing);

        assert_eq!(anti_join.literal_conditions.len(), 1);
        assert_eq!(anti_join.literal_conditions[0].property, "last_seen");

        assert_eq!(anti_join.outer_conditions.len(), 1);
        let (inner_property, op, outer) = &anti_join.outer_conditions[0];
        assert_eq!(inner_property, "action");
        assert_eq!(*op, graph_dsl::ComparisonOp::Eq);
        assert_eq!(outer.alias, "g");
        assert_eq!(outer.property, "action");

        assert_eq!(plan.project.len(), 5);
        assert_eq!(plan.project[3].alias, "g");
        assert_eq!(plan.project[3].property.as_deref(), Some("action"));
    }
}
