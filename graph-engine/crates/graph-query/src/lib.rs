//! Query planning and execution (spec §7.3, §7.4): turns a validated
//! [`graph_dsl::Query`] into a plan, then drives it hop-by-hop.
//!
//! Two execution contexts share this crate's types:
//! - **Partition-local** (`LocalExecutor`): resolve start nodes via the
//!   property index, expand one hop via the topological index — this is
//!   the unit of work a `graph-partition-node` process performs, and it's
//!   also the whole story for the single-node MVP (spec §12 Phase 1).
//! - **Distributed** (`DistributedExecutor`, spec §7.4): the coordinator's
//!   scatter-gather loop — send the current frontier to every partition it
//!   touches, collect responses, re-dispatch for the next hop. No
//!   peer-to-peer message passing (decision recorded in spec §7.4).

use async_trait::async_trait;
use graph_dsl::Query;
use graph_index::{GenerationHandle, PartitionId, PropertyKey, RemoteRef};
use graph_schema::NodeId;
use std::collections::HashMap;

/// A query lowered into an ordered sequence of steps (§7.3). Planning is
/// naive in v1 — filter/index-choice optimization is explicitly `TBD`
/// (§7.3) once this baseline plan is validated end to end.
#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub steps: Vec<PlanStep>,
    /// Aliases to keep in the final projection (`RETURN`) — no reduction,
    /// no `GROUP BY`: aggregation is out of scope for this engine (§1.3).
    pub project: Vec<String>,
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
    /// Expand one hop of the pattern via the topological index (§5.1),
    /// pushing `WHERE` filters down onto the new binding as early as
    /// possible to keep intermediate frontiers small.
    ExpandHop {
        from_alias: String,
        to_alias: String,
        edge_type: Option<String>,
        direction: graph_dsl::Direction,
        filters: Vec<graph_dsl::PropertyFilter>,
    },
}

/// Turns a validated [`Query`] into a [`QueryPlan`]. Concrete cost-based
/// choices (which index to start from, filter ordering) are Phase 2+ work
/// (§7.3) — v1 lowers steps in pattern order.
pub trait Planner {
    fn plan(&self, query: &Query) -> QueryPlan;
}

/// One partial match in progress: alias -> bound node, threaded through
/// `ExpandHop` steps and across partition boundaries during scatter-gather.
pub type Binding = HashMap<String, NodeId>;

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
}

/// Executes plan steps against a single partition's currently-served index
/// generation (`GenerationHandle::acquire`, spec §5.3). This is the whole
/// execution engine for the single-node MVP (§12 Phase 1); in the
/// distributed setup it's what a `graph-partition-node` runs per RPC from
/// the coordinator.
#[async_trait]
pub trait LocalExecutor {
    async fn resolve_start(
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
}

/// The coordinator's scatter-gather loop (§7.4): for each plan step, fan
/// the current frontier out to every partition it touches (via
/// `graph-proto`'s partition-facing RPC), await responses, merge
/// (dedup only — no aggregation, §7.1) and move to the next step. Results
/// are streamed to the client as they're finalized (§7.5), not
/// materialized in full.
#[async_trait]
pub trait DistributedExecutor {
    async fn execute(
        &self,
        plan: &QueryPlan,
        target_partitions: &[PartitionId],
    ) -> Result<(), ExecutionError>;
}
