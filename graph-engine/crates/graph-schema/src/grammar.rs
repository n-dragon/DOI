//! Pest grammar for the schema IDL (`schema.pest`, spec §3.4). Only the
//! grammar itself lives here — walking the resulting `Pairs` into a
//! [`crate::Schema`] is `SchemaParser`'s job (task S2), not this crate's
//! internals. Kept `pub(crate)` so callers always go through the trait.

// Not yet constructed outside its own tests — `SchemaParser` (task S2)
// will be the first real caller.
#[allow(dead_code)]
#[derive(pest_derive::Parser)]
#[grammar = "schema.pest"]
pub(crate) struct SchemaGrammar;

#[cfg(test)]
mod tests {
    use super::*;
    use pest::Parser;

    /// Confirms the grammar itself is well-formed and accepts the
    /// illustrative example from spec §3.4. Tree-walking into a `Schema`
    /// is exercised separately once `SchemaParser` exists (S2/S3).
    #[test]
    fn parses_the_spec_example() {
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

        SchemaGrammar::parse(Rule::schema_file, source)
            .expect("the spec §3.4 example should parse");
    }

    #[test]
    fn rejects_a_property_missing_a_type() {
        let source = r#"
            schema graph_v1 {
              node Person {
                id: NodeId
                name:
              }
            }
        "#;

        assert!(SchemaGrammar::parse(Rule::schema_file, source).is_err());
    }
}
