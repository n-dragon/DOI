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

    /// A node with only the mandatory `id` field and no other properties
    /// is legal — `property_decl*` allows zero.
    #[test]
    fn accepts_a_minimal_node_with_no_properties() {
        let source = r#"
            schema graph_v1 {
              node Person {
                id: NodeId
              }
            }
        "#;

        SchemaGrammar::parse(Rule::schema_file, source)
            .expect("a node with just an id field should parse");
    }

    /// Covers the two composite scalar types from §3.2 that the spec
    /// example doesn't exercise: `List<T>` and `Vector<Float32, N>`
    /// (reserved for future embeddings use, still part of the grammar).
    #[test]
    fn accepts_list_and_vector_properties() {
        let source = r#"
            schema graph_v1 {
              node Document {
                id: NodeId
                tags: List<String>
                embedding: Vector<Float32, 768>
              }
            }
        "#;

        SchemaGrammar::parse(Rule::schema_file, source)
            .expect("List<T> and Vector<Float32, N> properties should parse");
    }

    /// `//` line comments (the `COMMENT` rule) must be skippable anywhere
    /// whitespace is allowed, not just between top-level declarations.
    #[test]
    fn skips_comments_interleaved_with_declarations() {
        let source = r#"
            // top-level comment
            schema graph_v1 {
              node Person { // trailing comment on the same line
                id: NodeId
                // a property comment
                @indexed name: String
              }
            }
        "#;

        SchemaGrammar::parse(Rule::schema_file, source)
            .expect("comments should be skipped like other whitespace");
    }

    #[test]
    fn rejects_a_node_missing_the_id_field() {
        let source = r#"
            schema graph_v1 {
              node Person {
                name: String
              }
            }
        "#;

        assert!(SchemaGrammar::parse(Rule::schema_file, source).is_err());
    }

    #[test]
    fn rejects_an_edge_missing_the_to_field() {
        let source = r#"
            schema graph_v1 {
              node Person { id: NodeId }

              edge KNOWS {
                from: Person
              }
            }
        "#;

        assert!(SchemaGrammar::parse(Rule::schema_file, source).is_err());
    }

    #[test]
    fn rejects_a_scalar_type_not_in_the_grammar() {
        let source = r#"
            schema graph_v1 {
              node Person {
                id: NodeId
                age: Int32
              }
            }
        "#;

        assert!(SchemaGrammar::parse(Rule::schema_file, source).is_err());
    }

    #[test]
    fn rejects_an_empty_schema_body_without_a_closing_brace() {
        let source = "schema graph_v1 {";

        assert!(SchemaGrammar::parse(Rule::schema_file, source).is_err());
    }
}
