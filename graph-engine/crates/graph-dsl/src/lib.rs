//! The traversal / pattern-matching query language (spec §7.1): AST,
//! parser contract and static validation against the active schema (§7.2).
//!
//! Deliberately excludes aggregation (`COUNT`, `SUM`, `GROUP BY`, ...) —
//! `RETURN` only projects matched nodes/edges/properties, it never reduces
//! them (§1.3, §7.1). Full grammar (aliases, `ORDER BY`, `LIMIT`,
//! pagination) is still `TBD` — this AST covers the two prioritized shapes
//! only: filtered k-hop neighborhood and pattern matching.

use graph_schema::{EdgeType, Label};

/// One `MATCH (...)`-style pattern: a chain of node/edge steps, e.g.
/// `(p:Person {name: "Alice"})-[:KNOWS*1..3]->(friend:Person)`.
#[derive(Debug, Clone)]
pub struct Pattern {
    pub steps: Vec<PatternStep>,
}

#[derive(Debug, Clone)]
pub enum PatternStep {
    Node(NodePattern),
    Edge(EdgePattern),
}

#[derive(Debug, Clone)]
pub struct NodePattern {
    pub alias: String,
    pub label: Option<Label>,
    pub property_filters: Vec<PropertyFilter>,
}

#[derive(Debug, Clone)]
pub struct EdgePattern {
    pub edge_type: Option<EdgeType>,
    pub direction: Direction,
    /// Hop-count range for variable-length traversal, e.g. `*1..3` (§7.1).
    pub hops: HopRange,
}

#[derive(Debug, Clone, Copy)]
pub enum Direction {
    Outgoing,
    Incoming,
}

#[derive(Debug, Clone, Copy)]
pub struct HopRange {
    pub min: u32,
    pub max: u32,
}

#[derive(Debug, Clone)]
pub struct PropertyFilter {
    pub property: String,
    pub op: ComparisonOp,
    pub value: Literal,
}

#[derive(Debug, Clone, Copy)]
pub enum ComparisonOp {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Debug, Clone)]
pub enum Literal {
    Int64(i64),
    Float64(f64),
    Bool(bool),
    String(String),
}

/// A full query: `MATCH <pattern> [WHERE <filters>] RETURN <projection>`.
#[derive(Debug, Clone)]
pub struct Query {
    pub pattern: Pattern,
    pub where_filters: Vec<(String, PropertyFilter)>, // (alias, filter)
    pub returns: Vec<String>,                         // aliases to project — no aggregation (§7.1)
}

#[derive(Debug, thiserror::Error)]
pub enum DslError {
    #[error("syntax error at position {pos}: {message}")]
    Syntax { pos: usize, message: String },
}

/// Parses DSL source text into a [`Query`] AST. Grammar not yet finalized
/// (spec §7.1 "TBD") — this trait fixes the contract so graph-query can be
/// built against it before the parser itself lands.
pub trait Parser {
    fn parse(&self, source: &str) -> Result<Query, DslError>;
}

#[derive(Debug, thiserror::Error)]
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
/// work starts (§7.2).
pub trait Validator {
    fn validate(&self, query: &Query, schema: &graph_schema::Schema) -> Result<(), Vec<ValidationError>>;
}
