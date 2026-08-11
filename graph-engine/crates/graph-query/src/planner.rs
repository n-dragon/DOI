//! [`Planner`] (task Q1): naive lowering of a validated [`Query`] into a
//! [`QueryPlan`], in pattern order (spec §7.3 — cost-based choices are
//! explicitly deferred).
//!
//! v1's planner only handles what the spec's two priority query shapes
//! (§7.1) actually need: a pattern that starts with a single labeled node
//! carrying exactly one equality property filter (the `MATCH (p:Person
//! {name: "Alice"})` shape). Anything else panics with a clear message
//! rather than silently producing a wrong plan — `Planner::plan` has no
//! `Result` in its signature (an earlier scaffolding decision this task
//! doesn't revisit), so there's no error channel to report a shape v1
//! doesn't support. `WHERE` conditions comparing two aliases to each
//! other (`colleague <> p`, spec §7.1's pattern-matching example) are
//! silently dropped rather than enforced — full support is future work,
//! tracked as a known gap rather than implemented halfway.

use crate::{PlanStep, Planner, QueryPlan};
use graph_dsl::{Literal, NodePattern, PatternStep, Query, WhereCondition};
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

        let mut plan_steps = vec![resolve_start_step(first)];
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
            });

            from_alias = to_node.alias.clone();
            i += 2;
        }

        QueryPlan {
            steps: plan_steps,
            project: query.returns.clone(),
        }
    }
}

fn resolve_start_step(node: &NodePattern) -> PlanStep {
    let label = node
        .label
        .as_ref()
        .expect("v1's naive planner requires a labeled start node");
    let filter = node.property_filters.first().unwrap_or_else(|| {
        panic!(
            "v1's naive planner requires exactly one equality property filter on the start node \
             (e.g. {{name: \"Alice\"}}), found none on alias {:?}",
            node.alias
        )
    });

    PlanStep::ResolveStart {
        alias: node.alias.clone(),
        label_or_type: label.0.clone(),
        property: filter.property.clone(),
        key: literal_to_property_key(&filter.value),
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
            }
            other => panic!("expected ExpandHop, got {other:?}"),
        }

        assert_eq!(plan.project, vec!["friend".to_string()]);
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
}
