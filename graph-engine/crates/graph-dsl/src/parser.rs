//! Walks `grammar::DslGrammar` output (task D1) into a [`crate::Query`]
//! (tasks D2-D4).

use crate::grammar::{DslGrammar, Rule};
use crate::{
    ComparisonOp, Direction, DslError, EdgePattern, HopRange, Literal, NodePattern,
    Parser as DslParser, Pattern, PatternStep, PropertyFilter, Query, WhereCondition,
};
use graph_schema::{EdgeType, Label};
use pest::error::InputLocation;
use pest::iterators::Pair;
use pest::Parser;

/// [`DslParser`] backed by the `pest` grammar in `dsl.pest`.
pub struct PestParser;

impl DslParser for PestParser {
    fn parse(&self, source: &str) -> Result<Query, DslError> {
        let mut pairs = DslGrammar::parse(Rule::query, source).map_err(|e| {
            let pos = match e.location {
                InputLocation::Pos(p) => p,
                InputLocation::Span((start, _)) => start,
            };
            DslError::Syntax {
                pos,
                message: e.to_string(),
            }
        })?;

        let query_pair = pairs
            .next()
            .expect("query is the grammar's only top-level rule");

        let mut pattern = None;
        let mut where_conditions = Vec::new();
        let mut returns = Vec::new();

        for p in query_pair.into_inner() {
            match p.as_rule() {
                Rule::pattern => pattern = Some(parse_pattern(p)),
                Rule::where_clause => where_conditions = parse_where_clause(p),
                Rule::return_clause => returns = parse_return_clause(p),
                Rule::EOI => {}
                other => unreachable!(
                    "query only yields pattern/where_clause/return_clause/EOI, got {other:?}"
                ),
            }
        }

        Ok(Query {
            pattern: pattern.expect("grammar requires a pattern after MATCH"),
            where_conditions,
            returns,
        })
    }
}

fn parse_pattern(pair: Pair<Rule>) -> Pattern {
    let steps = pair
        .into_inner()
        .map(|p| match p.as_rule() {
            Rule::node_pattern => PatternStep::Node(parse_node_pattern(p)),
            Rule::edge_pattern => PatternStep::Edge(parse_edge_pattern(p)),
            other => unreachable!("pattern only yields node_pattern/edge_pattern, got {other:?}"),
        })
        .collect();

    Pattern { steps }
}

fn parse_node_pattern(pair: Pair<Rule>) -> NodePattern {
    let mut inner = pair.into_inner();
    let alias = inner
        .next()
        .expect("node_pattern always starts with its alias ident")
        .as_str()
        .to_string();

    let mut label = None;
    let mut property_filters = Vec::new();

    for p in inner {
        match p.as_rule() {
            Rule::ident => label = Some(Label(p.as_str().to_string())),
            Rule::property_map => property_filters = parse_property_map(p),
            other => {
                unreachable!("node_pattern body only yields ident/property_map, got {other:?}")
            }
        }
    }

    NodePattern {
        alias,
        label,
        property_filters,
    }
}

fn parse_property_map(pair: Pair<Rule>) -> Vec<PropertyFilter> {
    pair.into_inner()
        .map(|kv| {
            let mut inner = kv.into_inner();
            let property = inner
                .next()
                .expect("property_kv always starts with its key ident")
                .as_str()
                .to_string();
            let value = parse_literal(
                inner
                    .next()
                    .expect("property_kv always has a literal value"),
            );
            PropertyFilter {
                property,
                op: ComparisonOp::Eq,
                value,
            }
        })
        .collect()
}

fn parse_edge_pattern(pair: Pair<Rule>) -> EdgePattern {
    let directed = pair
        .into_inner()
        .next()
        .expect("edge_pattern always wraps outgoing_edge/incoming_edge");

    let direction = match directed.as_rule() {
        Rule::outgoing_edge => Direction::Outgoing,
        Rule::incoming_edge => Direction::Incoming,
        other => unreachable!("edge_pattern only wraps outgoing_edge/incoming_edge, got {other:?}"),
    };

    let mut edge_type = None;
    let mut hops = HopRange { min: 1, max: 1 };

    for detail in directed.into_inner() {
        match detail.as_rule() {
            Rule::edge_detail => {
                for d in detail.into_inner() {
                    match d.as_rule() {
                        Rule::ident => edge_type = Some(EdgeType(d.as_str().to_string())),
                        Rule::hop_range => hops = parse_hop_range(d),
                        other => {
                            unreachable!("edge_detail only yields ident/hop_range, got {other:?}")
                        }
                    }
                }
            }
            other => {
                unreachable!("outgoing_edge/incoming_edge only yield edge_detail, got {other:?}")
            }
        }
    }

    EdgePattern {
        edge_type,
        direction,
        hops,
    }
}

fn parse_hop_range(pair: Pair<Rule>) -> HopRange {
    let mut inner = pair.into_inner();
    let min = inner
        .next()
        .expect("hop_range always has a min number")
        .as_str()
        .parse()
        .expect("the `number` rule only matches ASCII digits");
    let max = inner
        .next()
        .expect("hop_range always has a max number")
        .as_str()
        .parse()
        .expect("the `number` rule only matches ASCII digits");

    HopRange { min, max }
}

fn parse_where_clause(pair: Pair<Rule>) -> Vec<WhereCondition> {
    pair.into_inner().map(parse_where_condition).collect()
}

fn parse_where_condition(pair: Pair<Rule>) -> WhereCondition {
    let inner = pair
        .into_inner()
        .next()
        .expect("where_condition always wraps property_condition/alias_comparison");

    match inner.as_rule() {
        Rule::property_condition => parse_property_condition(inner),
        Rule::alias_comparison => parse_alias_comparison(inner),
        other => unreachable!(
            "where_condition only wraps property_condition/alias_comparison, got {other:?}"
        ),
    }
}

fn parse_property_condition(pair: Pair<Rule>) -> WhereCondition {
    let mut inner = pair.into_inner();
    let alias = inner
        .next()
        .expect("property_condition always starts with its alias ident")
        .as_str()
        .to_string();
    let property = inner
        .next()
        .expect("property_condition always has a property ident")
        .as_str()
        .to_string();
    let op = parse_comparison_op(
        inner
            .next()
            .expect("property_condition always has a comparison_op"),
    );
    let value = parse_literal(
        inner
            .next()
            .expect("property_condition always has a literal value"),
    );

    WhereCondition::Property(
        alias,
        PropertyFilter {
            property,
            op,
            value,
        },
    )
}

fn parse_alias_comparison(pair: Pair<Rule>) -> WhereCondition {
    let mut inner = pair.into_inner();
    let left = inner
        .next()
        .expect("alias_comparison always starts with its left ident")
        .as_str()
        .to_string();
    let op = parse_comparison_op(
        inner
            .next()
            .expect("alias_comparison always has a comparison_op"),
    );
    let right = inner
        .next()
        .expect("alias_comparison always has a right ident")
        .as_str()
        .to_string();

    WhereCondition::AliasComparison { left, op, right }
}

fn parse_comparison_op(pair: Pair<Rule>) -> ComparisonOp {
    match pair.as_str() {
        "=" => ComparisonOp::Eq,
        "<>" => ComparisonOp::Ne,
        "<" => ComparisonOp::Lt,
        "<=" => ComparisonOp::Lte,
        ">" => ComparisonOp::Gt,
        ">=" => ComparisonOp::Gte,
        other => unreachable!("grammar's comparison_op alternatives are exhaustive, got {other:?}"),
    }
}

fn parse_return_clause(pair: Pair<Rule>) -> Vec<String> {
    pair.into_inner().map(|p| p.as_str().to_string()).collect()
}

fn parse_literal(pair: Pair<Rule>) -> Literal {
    let inner = pair
        .into_inner()
        .next()
        .expect("literal always wraps exactly one alternative");

    match inner.as_rule() {
        Rule::string_literal => {
            let s = inner.as_str();
            Literal::String(s[1..s.len() - 1].to_string())
        }
        Rule::float_literal => Literal::Float64(
            inner
                .as_str()
                .parse()
                .expect("float_literal only matches valid floats"),
        ),
        Rule::int_literal => Literal::Int64(
            inner
                .as_str()
                .parse()
                .expect("int_literal only matches valid integers"),
        ),
        Rule::bool_literal => Literal::Bool(inner.as_str() == "true"),
        other => {
            unreachable!("literal only wraps string/float/int/bool_literal, got {other:?}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// *(task D2)* A lone node pattern with an inline property filter.
    #[test]
    fn parses_a_single_node_pattern() {
        let query = PestParser
            .parse(r#"MATCH (p:Person {name: "Alice"}) RETURN p"#)
            .expect("valid DSL should parse");

        assert_eq!(query.pattern.steps.len(), 1);
        let PatternStep::Node(node) = &query.pattern.steps[0] else {
            panic!("expected a single node step");
        };
        assert_eq!(node.alias, "p");
        assert_eq!(node.label, Some(Label("Person".into())));
        assert_eq!(
            node.property_filters,
            vec![PropertyFilter {
                property: "name".into(),
                op: ComparisonOp::Eq,
                value: Literal::String("Alice".into()),
            }]
        );
        assert_eq!(query.returns, vec!["p".to_string()]);
    }

    /// *(task D3)* A full chain: typed node, hop-ranged outgoing edge,
    /// typed node.
    #[test]
    fn parses_a_full_pattern_chain_with_a_hop_range() {
        let query = PestParser
            .parse("MATCH (p:Person)-[:KNOWS*1..3]->(friend:Person) RETURN friend")
            .expect("valid DSL should parse");

        assert_eq!(query.pattern.steps.len(), 3);

        let PatternStep::Edge(edge) = &query.pattern.steps[1] else {
            panic!("expected an edge step in the middle");
        };
        assert_eq!(edge.edge_type, Some(EdgeType("KNOWS".into())));
        assert_eq!(edge.direction, Direction::Outgoing);
        assert_eq!(edge.hops, HopRange { min: 1, max: 3 });
    }

    /// *(task D3)* An incoming, untyped, single-hop edge (no `[...]` at
    /// all) defaults to `HopRange { min: 1, max: 1 }`.
    #[test]
    fn parses_an_untyped_incoming_edge_with_default_hop_range() {
        let query = PestParser
            .parse("MATCH (a:Person)<--(b:Person) RETURN a")
            .expect("valid DSL should parse");

        let PatternStep::Edge(edge) = &query.pattern.steps[1] else {
            panic!("expected an edge step in the middle");
        };
        assert_eq!(edge.edge_type, None);
        assert_eq!(edge.direction, Direction::Incoming);
        assert_eq!(edge.hops, HopRange { min: 1, max: 1 });
    }

    /// *(task D4)* `WHERE property.op.literal` clause.
    #[test]
    fn parses_a_where_clause_property_condition() {
        let query = PestParser
            .parse(
                r#"
                MATCH (p:Person)-[:KNOWS*1..3]->(friend:Person)
                WHERE friend.birth_year > 1990
                RETURN friend
                "#,
            )
            .expect("valid DSL should parse");

        assert_eq!(
            query.where_conditions,
            vec![WhereCondition::Property(
                "friend".into(),
                PropertyFilter {
                    property: "birth_year".into(),
                    op: ComparisonOp::Gt,
                    value: Literal::Int64(1990),
                }
            )]
        );
    }

    /// *(task D4)* `WHERE a AND b` combines a property condition and an
    /// alias (in)equality comparison.
    #[test]
    fn parses_a_where_clause_with_a_property_condition_and_an_alias_comparison() {
        let query = PestParser
            .parse(
                r#"
                MATCH (p:Person)-[:WORKS_AT]->(o:Organization)<-[:WORKS_AT]-(colleague:Person)
                WHERE p.name = "Alice" AND colleague <> p
                RETURN colleague, o
                "#,
            )
            .expect("valid DSL should parse");

        assert_eq!(
            query.where_conditions,
            vec![
                WhereCondition::Property(
                    "p".into(),
                    PropertyFilter {
                        property: "name".into(),
                        op: ComparisonOp::Eq,
                        value: Literal::String("Alice".into()),
                    }
                ),
                WhereCondition::AliasComparison {
                    left: "colleague".into(),
                    op: ComparisonOp::Ne,
                    right: "p".into(),
                },
            ]
        );
        assert_eq!(
            query.returns,
            vec!["colleague".to_string(), "o".to_string()]
        );
    }

    /// *(task D6)* Syntax errors surface as `DslError::Syntax`.
    #[test]
    fn rejects_malformed_dsl_with_a_syntax_error() {
        let err = PestParser.parse("MATCH (p:Person RETURN p").unwrap_err();
        assert!(matches!(err, DslError::Syntax { .. }));
    }

    /// *(task D6)* A query missing its mandatory `RETURN` clause.
    #[test]
    fn rejects_a_query_missing_return() {
        let err = PestParser.parse("MATCH (p:Person)").unwrap_err();
        assert!(matches!(err, DslError::Syntax { .. }));
    }
}
