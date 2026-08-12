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
    /// Binds the specific edge instance traversed (e.g. `[g:GRANTS]`) for
    /// later `alias.property` reference — least-privilege-via-telemetry
    /// use case. Restricted to single hops (`hops.min == hops.max == 1`)
    /// by the validator: see `dsl.pest`'s `edge_detail` doc comment for
    /// why a variable-length hop can't sensibly bind one alias.
    pub alias: Option<String>,
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

/// `alias.property` — one operand of a [`WhereCondition::PropertyComparison`]
/// or a [`ReturnItem`]. `alias` may name a node *or* an edge alias; which
/// one it is isn't decidable from syntax alone (both are bare `ident`s),
/// so it's resolved against the enclosing pattern by the validator
/// (D7-D9) and the planner, not here.
#[derive(Debug, Clone, PartialEq)]
pub struct PropertyRef {
    pub alias: String,
    pub property: String,
}

/// One `WHERE`-clause condition (spec §7.1, extended for the
/// least-privilege-via-telemetry use case). Four shapes:
/// - a property lookup against a literal (`friend.birth_year > 1990`);
/// - two bound aliases compared against each other (`colleague <> p`) —
///   no property to look up, so it can't reuse `PropertyFilter`;
/// - two *properties*, each on a (possibly different) bound alias,
///   compared against each other (`a.action = g.action`) — distinct from
///   the above since neither side is a literal nor a bare alias;
/// - a correlated existence check (`NOT EXISTS { ... }`) — see
///   [`ExistsSubquery`] for the scoping this is deliberately narrowed to.
///
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
    PropertyComparison {
        left: PropertyRef,
        op: ComparisonOp,
        right: PropertyRef,
    },
    NotExists(ExistsSubquery),
}

/// `NOT EXISTS { MATCH <pattern> [WHERE <conditions>] }` — a *correlated*
/// existence check, not a general subquery. Deliberately scoped (see
/// `dsl.pest`'s `not_exists_clause` doc comment) to: the pattern's node
/// aliases must already be bound by the enclosing `MATCH` (no fresh node
/// bindings inside), and the pattern is exactly one edge — one
/// additional hop between two already-known nodes, asking "does an edge
/// like *this* exist between them". Nesting a second `NotExists` inside
/// `where_conditions` is syntactically representable but rejected by the
/// validator (§D7-D9 extension) — no v1 use case needs it, and it would
/// require genuine recursive subquery planning rather than the single
/// correlated anti-join `graph-query`'s planner compiles this into.
#[derive(Debug, Clone, PartialEq)]
pub struct ExistsSubquery {
    pub pattern: Pattern,
    pub where_conditions: Vec<WhereCondition>,
}

/// One `RETURN` projection item: a bare alias (`RETURN friend`, existing
/// behavior — projects the whole matched node) or `alias.property`
/// (`RETURN g.action`, least-privilege use case — projects one property
/// of a node *or* an edge alias).
#[derive(Debug, Clone, PartialEq)]
pub struct ReturnItem {
    pub alias: String,
    pub property: Option<String>,
}

/// A full query: `MATCH <pattern> [WHERE <conditions>] RETURN <projection>`.
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub pattern: Pattern,
    pub where_conditions: Vec<WhereCondition>,
    pub returns: Vec<ReturnItem>, // no aggregation (§7.1)
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
    #[error("alias {0} is not bound by this pattern")]
    UnknownAlias(String),
    #[error("edge alias {alias} is on a variable-length hop (*{min}..{max}) — edge aliases are only valid on a single hop")]
    EdgeAliasOnVariableLengthHop { alias: String, min: u32, max: u32 },
    #[error("NOT EXISTS {{ ... }} anchor {0} must already be bound by the outer MATCH (no label/properties on it here)")]
    NotExistsAnchorNotPreBound(String),
    #[error("NOT EXISTS {{ ... }} pattern must be exactly one edge between two already-bound nodes, got {0} pattern step(s)")]
    NotExistsWrongShape(usize),
    #[error("NOT EXISTS {{ ... }} cannot be nested inside another NOT EXISTS")]
    NestedNotExists,
    /// *(scoping decision, `graph-query`'s anti-join)* An edge's property
    /// record is owned by whichever partition owns its *source* node
    /// (`graph-index`'s `edge_records`) — so a `NOT EXISTS`'s single-hop
    /// check only ever needs the partition already handling `from_alias`
    /// (the source). An `<-` (incoming) edge inside `NOT EXISTS` would
    /// make `from_alias` the *destination*, whose partition may not be
    /// the one holding the edge record at all — not unsolvable, just not
    /// needed by any v1 use case, so rejected rather than half-supported.
    #[error("NOT EXISTS {{ ... }} edge must be outgoing (`->`), not incoming (`<-`)")]
    NotExistsMustBeOutgoing,
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
