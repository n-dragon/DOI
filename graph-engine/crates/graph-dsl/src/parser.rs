//! Walks `grammar::DslGrammar` output (task D1) into a [`crate::Query`]
//! (tasks D2-D4).

use crate::grammar::{DslGrammar, Rule};
use crate::{
    ComparisonOp, Direction, DslError, EdgePattern, ExistsSubquery, HopRange, Literal, NodePattern,
    OrderBy, Parser as DslParser, Pattern, PatternStep, PropertyFilter, PropertyRef, Query,
    ReturnItem, SortDirection, WhereCondition,
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
        let mut order_by = None;
        let mut limit = None;

        for p in query_pair.into_inner() {
            match p.as_rule() {
                Rule::pattern => pattern = Some(parse_pattern(p)),
                Rule::where_clause => where_conditions = parse_where_clause(p),
                Rule::return_clause => returns = parse_return_clause(p),
                Rule::order_by_clause => order_by = Some(parse_order_by_clause(p)),
                Rule::limit_clause => limit = Some(parse_limit_clause(p)),
                Rule::EOI => {}
                other => unreachable!(
                    "query only yields pattern/where_clause/return_clause/order_by_clause/\
                     limit_clause/EOI, got {other:?}"
                ),
            }
        }

        Ok(Query {
            pattern: pattern.expect("grammar requires a pattern after MATCH"),
            where_conditions,
            returns,
            order_by,
            limit,
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

    let mut alias = None;
    let mut edge_type = None;
    let mut hops = HopRange { min: 1, max: 1 };

    for detail in directed.into_inner() {
        match detail.as_rule() {
            Rule::edge_detail => {
                for d in detail.into_inner() {
                    match d.as_rule() {
                        Rule::edge_alias => alias = Some(d.as_str().to_string()),
                        Rule::ident => edge_type = Some(EdgeType(d.as_str().to_string())),
                        Rule::hop_range => hops = parse_hop_range(d),
                        other => unreachable!(
                            "edge_detail only yields edge_alias/ident/hop_range, got {other:?}"
                        ),
                    }
                }
            }
            other => {
                unreachable!("outgoing_edge/incoming_edge only yield edge_detail, got {other:?}")
            }
        }
    }

    EdgePattern {
        alias,
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
    let inner = pair.into_inner().next().expect(
        "where_condition always wraps not_exists_clause/property_property_comparison/\
         property_condition/alias_comparison",
    );

    match inner.as_rule() {
        Rule::not_exists_clause => parse_not_exists_clause(inner),
        Rule::property_property_comparison => parse_property_property_comparison(inner),
        Rule::property_condition => parse_property_condition(inner),
        Rule::alias_comparison => parse_alias_comparison(inner),
        other => unreachable!(
            "where_condition only wraps not_exists_clause/property_property_comparison/\
             property_condition/alias_comparison, got {other:?}"
        ),
    }
}

fn parse_property_property_comparison(pair: Pair<Rule>) -> WhereCondition {
    let mut inner = pair.into_inner();
    let left_alias = inner
        .next()
        .expect("property_property_comparison always starts with the left alias ident")
        .as_str()
        .to_string();
    let left_property = inner
        .next()
        .expect("property_property_comparison always has a left property ident")
        .as_str()
        .to_string();
    let op = parse_comparison_op(
        inner
            .next()
            .expect("property_property_comparison always has a comparison_op"),
    );
    let right_alias = inner
        .next()
        .expect("property_property_comparison always has a right alias ident")
        .as_str()
        .to_string();
    let right_property = inner
        .next()
        .expect("property_property_comparison always has a right property ident")
        .as_str()
        .to_string();

    WhereCondition::PropertyComparison {
        left: PropertyRef {
            alias: left_alias,
            property: left_property,
        },
        op,
        right: PropertyRef {
            alias: right_alias,
            property: right_property,
        },
    }
}

fn parse_not_exists_clause(pair: Pair<Rule>) -> WhereCondition {
    let mut pattern = None;
    let mut where_conditions = Vec::new();

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::pattern => pattern = Some(parse_pattern(p)),
            Rule::where_clause => where_conditions = parse_where_clause(p),
            other => {
                unreachable!("not_exists_clause only yields pattern/where_clause, got {other:?}")
            }
        }
    }

    WhereCondition::NotExists(ExistsSubquery {
        pattern: pattern.expect("grammar requires a pattern after NOT EXISTS { MATCH"),
        where_conditions,
    })
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

fn parse_return_clause(pair: Pair<Rule>) -> Vec<ReturnItem> {
    pair.into_inner().map(parse_return_item).collect()
}

fn parse_return_item(pair: Pair<Rule>) -> ReturnItem {
    let mut inner = pair.into_inner();
    let alias = inner
        .next()
        .expect("return_item always starts with its alias ident")
        .as_str()
        .to_string();
    let property = inner.next().map(|p| p.as_str().to_string());

    ReturnItem { alias, property }
}

fn parse_order_by_clause(pair: Pair<Rule>) -> OrderBy {
    let mut inner = pair.into_inner();
    let alias = inner
        .next()
        .expect("order_by_clause always starts with its alias ident")
        .as_str()
        .to_string();
    let property = inner
        .next()
        .expect("order_by_clause always has a property ident")
        .as_str()
        .to_string();
    let direction = match inner.next() {
        Some(d) => match d.as_str() {
            "ASC" => SortDirection::Asc,
            "DESC" => SortDirection::Desc,
            other => unreachable!("sort_direction alternatives are exhaustive, got {other:?}"),
        },
        None => SortDirection::Asc, // omitted direction defaults to ASC, same as SQL
    };

    OrderBy {
        alias,
        property,
        direction,
    }
}

fn parse_limit_clause(pair: Pair<Rule>) -> u64 {
    pair.into_inner()
        .next()
        .expect("limit_clause always has a number")
        .as_str()
        .parse()
        .expect("the `number` rule only matches ASCII digits")
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
        assert_eq!(
            query.returns,
            vec![ReturnItem {
                alias: "p".into(),
                property: None
            }]
        );
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
            vec![
                ReturnItem {
                    alias: "colleague".into(),
                    property: None
                },
                ReturnItem {
                    alias: "o".into(),
                    property: None
                },
            ]
        );
    }

    /// Edge alias (`[g:GRANTS]`) parses into `EdgePattern.alias`.
    #[test]
    fn parses_an_edge_alias() {
        let query = PestParser
            .parse("MATCH (r:IAMRole)-[g:GRANTS]->(d:DataStore) RETURN g")
            .expect("valid DSL should parse");

        let PatternStep::Edge(edge) = &query.pattern.steps[1] else {
            panic!("expected an edge step in the middle");
        };
        assert_eq!(edge.alias.as_deref(), Some("g"));
        assert_eq!(edge.edge_type, Some(EdgeType("GRANTS".into())));
    }

    /// `RETURN alias.property` parses into a `ReturnItem` with
    /// `property: Some(..)`, distinct from a bare alias.
    #[test]
    fn parses_return_items_with_and_without_a_property() {
        let query = PestParser
            .parse("MATCH (w:Workload)-[g:GRANTS]->(d:DataStore) RETURN w, g.action, d.arn")
            .expect("valid DSL should parse");

        assert_eq!(
            query.returns,
            vec![
                ReturnItem {
                    alias: "w".into(),
                    property: None
                },
                ReturnItem {
                    alias: "g".into(),
                    property: Some("action".into())
                },
                ReturnItem {
                    alias: "d".into(),
                    property: Some("arn".into())
                },
            ]
        );
    }

    /// `a.action = g.action` (least-privilege use case) parses as a
    /// `PropertyComparison`, not a `Property`/`AliasComparison` — both of
    /// which would be a plausible-but-wrong reading of the same tokens.
    #[test]
    fn parses_a_property_to_property_comparison() {
        let query = PestParser
            .parse(
                r#"
                MATCH (w:Workload)-[a:ACCESSED]->(d:DataStore)
                WHERE a.action = w.kind
                RETURN a
                "#,
            )
            .expect("valid DSL should parse");

        assert_eq!(
            query.where_conditions,
            vec![WhereCondition::PropertyComparison {
                left: PropertyRef {
                    alias: "a".into(),
                    property: "action".into()
                },
                op: ComparisonOp::Eq,
                right: PropertyRef {
                    alias: "w".into(),
                    property: "kind".into()
                },
            }]
        );
    }

    /// The least-privilege use case's central query, checked all the way
    /// down to AST shape: edge aliases on both `GRANTS` and `ACCESSED`,
    /// a correlated `NOT EXISTS` whose inner pattern re-anchors on the
    /// outer `w`/`d` node aliases (no label/properties on them), and a
    /// `RETURN` mixing bare aliases with `alias.property` items.
    #[test]
    fn parses_the_least_privilege_example_into_the_expected_ast() {
        let query = PestParser
            .parse(
                r#"
                MATCH (w:Workload)-[:ASSUMES]->(r:IAMRole)-[g:GRANTS]->(d:DataStore)
                WHERE d.data_class = "sensitive"
                  AND NOT EXISTS {
                    MATCH (w)-[a:ACCESSED]->(d)
                    WHERE a.action = g.action AND a.last_seen >= "2024-05-01T00:00:00Z"
                  }
                RETURN w.service, w.team, r.arn, g.action, d.arn
                "#,
            )
            .expect("valid DSL should parse");

        assert_eq!(query.pattern.steps.len(), 5);
        let PatternStep::Edge(grants) = &query.pattern.steps[3] else {
            panic!("expected the GRANTS edge step");
        };
        assert_eq!(grants.alias.as_deref(), Some("g"));

        assert_eq!(query.where_conditions.len(), 2);
        assert_eq!(
            query.where_conditions[0],
            WhereCondition::Property(
                "d".into(),
                PropertyFilter {
                    property: "data_class".into(),
                    op: ComparisonOp::Eq,
                    value: Literal::String("sensitive".into()),
                }
            )
        );

        let WhereCondition::NotExists(subquery) = &query.where_conditions[1] else {
            panic!("expected a NOT EXISTS condition");
        };
        assert_eq!(subquery.pattern.steps.len(), 3);
        let PatternStep::Node(anchor_w) = &subquery.pattern.steps[0] else {
            panic!("expected the (w) anchor node");
        };
        assert_eq!(anchor_w.alias, "w");
        assert_eq!(anchor_w.label, None, "re-anchored alias carries no label");
        assert!(anchor_w.property_filters.is_empty());
        let PatternStep::Edge(accessed) = &subquery.pattern.steps[1] else {
            panic!("expected the ACCESSED edge step");
        };
        assert_eq!(accessed.alias.as_deref(), Some("a"));
        assert_eq!(accessed.edge_type, Some(EdgeType("ACCESSED".into())));

        assert_eq!(
            subquery.where_conditions,
            vec![
                WhereCondition::PropertyComparison {
                    left: PropertyRef {
                        alias: "a".into(),
                        property: "action".into()
                    },
                    op: ComparisonOp::Eq,
                    right: PropertyRef {
                        alias: "g".into(),
                        property: "action".into()
                    },
                },
                WhereCondition::Property(
                    "a".into(),
                    PropertyFilter {
                        property: "last_seen".into(),
                        op: ComparisonOp::Gte,
                        value: Literal::String("2024-05-01T00:00:00Z".into()),
                    }
                ),
            ]
        );

        assert_eq!(
            query.returns,
            vec![
                ReturnItem {
                    alias: "w".into(),
                    property: Some("service".into())
                },
                ReturnItem {
                    alias: "w".into(),
                    property: Some("team".into())
                },
                ReturnItem {
                    alias: "r".into(),
                    property: Some("arn".into())
                },
                ReturnItem {
                    alias: "g".into(),
                    property: Some("action".into())
                },
                ReturnItem {
                    alias: "d".into(),
                    property: Some("arn".into())
                },
            ]
        );
    }

    /// *(task D14/D15)* `ORDER BY alias.property DESC LIMIT n` parses
    /// into `Query::order_by`/`Query::limit`.
    #[test]
    fn parses_order_by_and_limit() {
        let query = PestParser
            .parse(
                "MATCH (p:Person)-[:KNOWS*1..3]->(friend:Person) \
                 RETURN friend ORDER BY friend.birth_year DESC LIMIT 10",
            )
            .expect("valid DSL should parse");

        assert_eq!(
            query.order_by,
            Some(OrderBy {
                alias: "friend".into(),
                property: "birth_year".into(),
                direction: SortDirection::Desc,
            })
        );
        assert_eq!(query.limit, Some(10));
    }

    /// *(task D14)* An omitted sort direction defaults to `ASC`; `LIMIT`
    /// is independently optional (`ORDER BY` with no `LIMIT` at all).
    #[test]
    fn order_by_direction_defaults_to_ascending_and_limit_is_independent() {
        let query = PestParser
            .parse("MATCH (p:Person) RETURN p ORDER BY p.name")
            .expect("valid DSL should parse");

        assert_eq!(
            query.order_by,
            Some(OrderBy {
                alias: "p".into(),
                property: "name".into(),
                direction: SortDirection::Asc,
            })
        );
        assert_eq!(query.limit, None);
    }

    /// *(task D15)* `LIMIT` alone, with no `ORDER BY` at all, also parses.
    #[test]
    fn parses_a_bare_limit_with_no_order_by() {
        let query = PestParser
            .parse("MATCH (p:Person) RETURN p LIMIT 5")
            .expect("valid DSL should parse");

        assert_eq!(query.order_by, None);
        assert_eq!(query.limit, Some(5));
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
