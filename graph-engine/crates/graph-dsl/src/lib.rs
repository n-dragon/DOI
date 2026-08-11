//! The traversal / pattern-matching query language (spec §7.1): AST,
//! parser contract and static validation against the active schema (§7.2).
//!
//! Deliberately excludes aggregation (`COUNT`, `SUM`, `GROUP BY`, ...) —
//! `RETURN` only projects matched nodes/edges/properties, it never reduces
//! them (§1.3, §7.1). Full grammar (aliases, `ORDER BY`, `LIMIT`,
//! pagination) is still `TBD` — this AST covers the two prioritized shapes
//! only: filtered k-hop neighborhood and pattern matching.

mod grammar;
mod parser;
mod validator;

pub use parser::PestParser;
pub use validator::SchemaValidator;

use graph_schema::{EdgeType, Label};

/// One `MATCH (...)`-style pattern: a chain of node/edge steps, e.g.
/// `(p:Person {name: "Alice"})-[:KNOWS*1..3]->(friend:Person)`.
#[derive(Debug, Clone, PartialEq)]
pub struct Pattern {
    pub steps: Vec<PatternStep>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PatternStep {
    Node(NodePattern),
    Edge(EdgePattern),
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodePattern {
    pub alias: String,
    pub label: Option<Label>,
    pub property_filters: Vec<PropertyFilter>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EdgePattern {
    pub edge_type: Option<EdgeType>,
    pub direction: Direction,
    /// Hop-count range for variable-length traversal, e.g. `*1..3` (§7.1).
    pub hops: HopRange,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Direction {
    Outgoing,
    Incoming,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HopRange {
    pub min: u32,
    pub max: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PropertyFilter {
    pub property: String,
    pub op: ComparisonOp,
    pub value: Literal,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ComparisonOp {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Int64(i64),
    Float64(f64),
    Bool(bool),
    String(String),
}

/// One `WHERE`-clause condition (spec §7.1). Either a property lookup
/// compared against a literal (`friend.birth_year > 1990`) or two bound
/// aliases compared against each other (`colleague <> p`) — the latter
/// has no property to look up, so it can't reuse `PropertyFilter`.
/// Conditions are implicitly AND-combined: the spec's examples never use
/// `OR`, and the grammar doesn't accept it.
#[derive(Debug, Clone, PartialEq)]
pub enum WhereCondition {
    Property(String, PropertyFilter), // (alias, filter)
    AliasComparison {
        left: String,
        op: ComparisonOp,
        right: String,
    },
}

/// A full query: `MATCH <pattern> [WHERE <conditions>] RETURN <projection>`.
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub pattern: Pattern,
    pub where_conditions: Vec<WhereCondition>,
    pub returns: Vec<String>, // aliases to project — no aggregation (§7.1)
}

#[derive(Debug, thiserror::Error)]
pub enum DslError {
    #[error("syntax error at position {pos}: {message}")]
    Syntax { pos: usize, message: String },
}

/// Parses DSL source text into a [`Query`] AST. See [`PestParser`] for the
/// concrete `pest`-backed implementation.
pub trait Parser {
    fn parse(&self, source: &str) -> Result<Query, DslError>;
}

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum ValidationError {
    #[error("unknown label: {0}")]
    UnknownLabel(String),
    #[error("unknown edge type: {0}")]
    UnknownEdgeType(String),
    #[error("unknown property {property} on {label}")]
    UnknownProperty { label: String, property: String },
    #[error("comparison operator not valid for property type: {property}")]
    IncompatibleOperator { property: String },
}

/// Validates a parsed [`Query`] against the active [`graph_schema::Schema`]
/// before any execution is attempted — labels/edge types must exist,
/// property types must be compatible with the operators used in `WHERE`.
/// Fail-fast: errors are returned to the client before any distributed
/// work starts (§7.2). See [`SchemaValidator`] for the concrete
/// implementation.
pub trait Validator {
    fn validate(
        &self,
        query: &Query,
        schema: &graph_schema::Schema,
    ) -> Result<(), Vec<ValidationError>>;
}
