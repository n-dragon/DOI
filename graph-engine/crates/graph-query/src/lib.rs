//! Query planning and execution (spec §7.3, §7.4): turns a validated
//! [`graph_dsl::Query`] into a plan, then drives it hop-by-hop.
//!
//! Two execution contexts share this crate's types:
//! - **Partition-local** (`LocalExecutor`): resolve start nodes via the
//!   property index, expand one hop via the topological index — this is
//!   the unit of work a `graph-partition-node` process performs, and it's
//!   also the whole story for the single-node MVP (spec §12 Phase 1).
//! - **Distributed** (`ScatterGatherExecutor`, tasks Q5-Q7, spec §7.4):
//!   the coordinator's scatter-gather loop — send the current frontier to
//!   every partition it touches, collect responses, re-dispatch for the
//!   next hop. No peer-to-peer message passing (decision recorded in
//!   spec §7.4). See `distributed_executor`'s doc comment for the
//!   concrete design and the decisions taken while implementing it.
//!
//! Extended for the least-privilege-via-telemetry use case with: edge
//! aliases (a `Binding` now carries `EdgeId`s as well as `NodeId`s),
//! label-wide start resolution with no filter (`PlanStep::ResolveAll`),
//! and a correlated anti-join (`PlanStep::AntiJoin`, compiling
//! `graph_dsl::WhereCondition::NotExists`) — see each type's doc comment
//! for the scoping decisions made while adding them.

mod distributed_executor;
mod filter_eval;
mod local_executor;
mod planner;

pub use distributed_executor::{PartitionRpc, ScatterGatherExecutor};
pub use local_executor::SimpleLocalExecutor;
pub use planner::NaivePlanner;

use async_trait::async_trait;
use graph_dsl::{ComparisonOp, Direction, HopRange, PropertyFilter, Query, ReturnItem};
use graph_index::{GenerationHandle, PropertyKey, RemoteRef};
use graph_schema::{EdgeId, NodeId};
use std::collections::HashMap;

/// A query lowered into an ordered sequence of steps (§7.3). Planning is
/// naive in v1 — filter/index-choice optimization is explicitly `TBD`
/// (§7.3) once this baseline plan is validated end to end.
#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub steps: Vec<PlanStep>,
    /// What to project per matched row (`RETURN`, §7.1) — a bare alias
    /// (whole node/edge) or `alias.property` (least-privilege use case).
    /// No reduction, no `GROUP BY`: aggregation is out of scope for this
    /// engine (§1.3).
    pub project: Vec<ReturnItem>,
}

#[derive(Debug, Clone)]
pub enum PlanStep {
    /// Resolve the initial binding set via the property index (§5.2),
    /// e.g. `MATCH (p:Person {name: "Alice"})`.
    ResolveStart {
        alias: String,
        label_or_type: String,
        property: String,
        key: PropertyKey,
    },
    /// *(least-privilege-via-telemetry use case)* Resolve the initial
    /// binding set with **no** filter at all — every node of `label`,
    /// e.g. `MATCH (w:Workload)` with no `{...}`. The planner (task
    /// Q-resolve-all) emits this instead of `ResolveStart` whenever the
    /// pattern's start node carries a label but no equality filter — a
    /// full local scan (`node_records` filtered by label) rather than a
    /// `PropertyIndex` lookup, since there's no value to look up by.
    ResolveAll { alias: String, label: String },
    /// Expand the pattern via the topological index (§5.1) from
    /// `from_alias` to `to_alias`, `hops.min..=hops.max` hops deep
    /// (§7.1's `*1..3` syntax — a plain `-->` lowers to `min: 1, max: 1`).
    /// `filters` are the `WHERE` conditions that apply to `to_alias`;
    /// `to_label` is `to_alias`'s declared label, needed to route a
    /// filter to the right property index (§5.2). If the pattern left
    /// `to_alias` unlabeled, `to_label` is `None` and the planner (Q1)
    /// drops any filter that would have targeted it — there's no index
    /// to route through without a label, and the validator (D7-D9)
    /// doesn't currently reject that combination itself.
    ExpandHop {
        from_alias: String,
        to_alias: String,
        to_label: Option<String>,
        edge_type: Option<String>,
        direction: Direction,
        hops: HopRange,
        filters: Vec<PropertyFilter>,
        /// *(least-privilege-via-telemetry use case)* Binds the specific
        /// edge traversed (e.g. `[g:GRANTS]`) — only ever `Some` when
        /// `hops == {1,1}` (D7-D9 rejects an alias on a variable-length
        /// hop, see `dsl.pest`'s `edge_detail` doc comment).
        edge_alias: Option<String>,
    },
    /// *(least-privilege-via-telemetry use case)* Compiles
    /// `graph_dsl::WhereCondition::NotExists` — see [`AntiJoinStep`].
    AntiJoin(AntiJoinStep),
}

/// The correlated existence check `NOT EXISTS { MATCH (from)-[edge]->(to)
/// WHERE ... }` compiles into (task Q-not-exists). `from_alias`/
/// `to_alias` must already be bound by prior plan steps (the validator,
/// D7-D9, guarantees this at the DSL level). Filters a frontier down to
/// the bindings for which **no** matching edge satisfies every
/// condition — i.e. the survivors are exactly where `NOT EXISTS` holds.
///
/// Split into two condition buckets rather than one, mirroring the two
/// shapes `graph_dsl::WhereCondition::PropertyComparison` can produce
/// inside a subquery: a literal is already fully resolved and travels
/// with the plan; a reference to an *outer* bound alias's property
/// (`a.action = g.action`) is **not** resolved yet — its value varies
/// per binding (different rows can have bound a different `g` edge with
/// a different `action`), so it can only be resolved once the actual
/// frontier is in hand, right before dispatch
/// (`ScatterGatherExecutor::execute_anti_join`, task Q6-antijoin) —
/// `LocalExecutor`/the wire protocol only ever see the fully-resolved
/// form (`literal_conditions`-shaped).
#[derive(Debug, Clone)]
pub struct AntiJoinStep {
    pub from_alias: String,
    pub to_alias: String,
    /// Always `Outgoing` — the validator rejects `<-` inside
    /// `NOT EXISTS` (see `ValidationError::NotExistsMustBeOutgoing`'s
    /// doc comment for why: edge-record ownership follows the source,
    /// which must be `from_alias` for a purely local check to work).
    /// Kept as a field rather than assumed for symmetry with
    /// `PlanStep::ExpandHop` and in case that restriction is ever lifted.
    pub direction: Direction,
    pub edge_alias: Option<String>,
    pub edge_type: Option<String>,
    pub literal_conditions: Vec<PropertyFilter>,
    /// `(candidate_edge_property, op, outer_alias.outer_property)` —
    /// resolved into `literal_conditions`-shaped filters, per binding,
    /// before any RPC carrying this step is sent.
    pub outer_conditions: Vec<(String, ComparisonOp, graph_dsl::PropertyRef)>,
}

/// Turns a validated [`Query`] into a [`QueryPlan`]. Concrete cost-based
/// choices (which index to start from, filter ordering) are Phase 2+ work
/// (§7.3) — v1 lowers steps in pattern order. See [`NaivePlanner`].
pub trait Planner {
    fn plan(&self, query: &Query) -> QueryPlan;
}

/// One partial match in progress: alias -> bound node/edge, threaded
/// through `ExpandHop`/`AntiJoin` steps and across partition boundaries
/// during scatter-gather. Node and edge aliases are tracked separately
/// (an alias is always exclusively one or the other, fixed by the
/// pattern) rather than in one map of a sum type — most call sites only
/// ever touch `nodes`; `edges` stays empty for the large majority of
/// bindings that never traverse an aliased edge.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Binding {
    pub nodes: HashMap<String, NodeId>,
    pub edges: HashMap<String, EdgeId>,
}

impl Binding {
    /// A binding with exactly one node bound — the common case building
    /// up from `ResolveStart`/`ResolveAll`.
    pub fn from_node(alias: impl Into<String>, node: NodeId) -> Self {
        let mut binding = Binding::default();
        binding.nodes.insert(alias.into(), node);
        binding
    }
}

/// The set of in-progress bindings passed between coordinator and
/// partitions at each hop of a scatter-gather round (§7.4). Bindings whose
/// next hop lands on another partition are represented as
/// [`RemoteRef`]s rather than resolved locally.
#[derive(Debug, Clone, Default)]
pub struct Frontier {
    pub local: Vec<Binding>,
    pub remote: Vec<(RemoteRef, Binding)>,
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    #[error("index generation unavailable")]
    NoIndex,
    #[error("plan step referenced unknown alias: {0}")]
    UnknownAlias(String),
    /// *(task Q5, `PartitionRpc` implementations)* A [`PartitionRpc`]
    /// call failed — connection refused, a partition-node returned an
    /// RPC error, etc. Distinct from `UnknownAlias`/`NoIndex` (which are
    /// planner/executor logic errors local to this process) since a
    /// caller may reasonably want to distinguish "this query is
    /// malformed" from "a partition was unreachable" — the latter is a
    /// transient cluster-health condition, not a query bug.
    #[error("partition RPC failed: {0}")]
    Rpc(String),
}

/// Executes plan steps against a single partition's currently-served index
/// generation (`GenerationHandle::acquire`, spec §5.3). This is the whole
/// execution engine for the single-node MVP (§12 Phase 1); in the
/// distributed setup it's what a `graph-partition-node` runs per RPC from
/// the coordinator. See [`SimpleLocalExecutor`].
#[async_trait]
pub trait LocalExecutor {
    async fn resolve_start(
        &self,
        index: &GenerationHandle,
        step: &PlanStep,
    ) -> Result<Vec<Binding>, ExecutionError>;

    /// *(least-privilege-via-telemetry use case)* Companion to
    /// `resolve_start` for `PlanStep::ResolveAll` — every local node of
    /// a label, no property lookup.
    async fn resolve_all(
        &self,
        index: &GenerationHandle,
        step: &PlanStep,
    ) -> Result<Vec<Binding>, ExecutionError>;

    async fn expand_hop(
        &self,
        index: &GenerationHandle,
        step: &PlanStep,
        frontier: &[Binding],
    ) -> Result<Frontier, ExecutionError>;

    /// *(least-privilege-via-telemetry use case)* Evaluates
    /// `PlanStep::AntiJoin` against a frontier that's already local to
    /// this partition (every binding's `from_alias` node is owned here —
    /// the caller, `ScatterGatherExecutor`, groups by partition the same
    /// way it does for `expand_hop`). `conditions` are always fully
    /// resolved `PropertyFilter`s by this point — see `AntiJoinStep`'s
    /// doc comment for where the resolution happens. Returns the
    /// surviving bindings (those for which `NOT EXISTS` holds).
    async fn check_anti_join(
        &self,
        index: &GenerationHandle,
        step: &AntiJoinStep,
        conditions: &[PropertyFilter],
        frontier: &[Binding],
    ) -> Result<Vec<Binding>, ExecutionError>;
}

// The coordinator's scatter-gather loop (§7.4) is `ScatterGatherExecutor`
// (tasks Q5-Q7, `distributed_executor.rs`) — this used to be a bare trait
// stub (`DistributedExecutor::execute(..) -> Result<(), _>`, no way to
// even get results out) ahead of Phase 2 actually implementing it; now
// superseded by a concrete struct generic over `PartitionRpc`, which
// *does* return the resolved bindings, since a real coordinator needs
// them for `RETURN` projection.
