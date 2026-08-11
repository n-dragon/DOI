//! Pest grammar for the query DSL (`dsl.pest`, spec §7.1). Only the
//! grammar itself lives here — walking the resulting `Pairs` into a
//! [`crate::Query`] is `Parser`'s job (tasks D2-D4), not this crate's
//! internals. Kept `pub(crate)` so callers always go through the trait.

#[allow(dead_code)]
#[derive(pest_derive::Parser)]
#[grammar = "dsl.pest"]
pub(crate) struct DslGrammar;

#[cfg(test)]
mod tests {
    use super::*;
    use pest::Parser;

    /// The k-hop filtered neighborhood example (spec §7.1).
    #[test]
    fn parses_the_k_hop_example() {
        let source = r#"
            MATCH (p:Person {name: "Alice"})-[:KNOWS*1..3]->(friend:Person)
            WHERE friend.birth_year > 1990
            RETURN friend
        "#;

        DslGrammar::parse(Rule::query, source).expect("the spec §7.1 k-hop example should parse");
    }

    /// The subgraph pattern-matching example (spec §7.1), including the
    /// `colleague <> p` alias (in)equality comparison.
    #[test]
    fn parses_the_pattern_matching_example() {
        let source = r#"
            MATCH (p:Person)-[:WORKS_AT]->(o:Organization)<-[:WORKS_AT]-(colleague:Person)
            WHERE p.name = "Alice" AND colleague <> p
            RETURN colleague, o
        "#;

        DslGrammar::parse(Rule::query, source)
            .expect("the spec §7.1 pattern-matching example should parse");
    }

    #[test]
    fn accepts_an_untyped_single_hop_edge() {
        let source = "MATCH (a)-->(b) RETURN a";

        DslGrammar::parse(Rule::query, source)
            .expect("a bare edge with no type or hop range should parse");
    }

    #[test]
    fn rejects_a_query_missing_return() {
        let source = "MATCH (p:Person)";

        assert!(DslGrammar::parse(Rule::query, source).is_err());
    }

    #[test]
    fn rejects_a_query_not_starting_with_match() {
        let source = "RETURN p";

        assert!(DslGrammar::parse(Rule::query, source).is_err());
    }

    #[test]
    fn rejects_a_where_clause_with_a_dangling_and() {
        let source = r#"
            MATCH (p:Person)
            WHERE p.name = "Alice" AND
            RETURN p
        "#;

        assert!(DslGrammar::parse(Rule::query, source).is_err());
    }
}
