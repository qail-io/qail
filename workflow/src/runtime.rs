//! Runtime safety contracts for workflow execution.
//!
//! These types describe optional production guarantees that app executors can
//! back with database rows, advisory locks, outbox tables, or provider-specific
//! idempotency keys.

use std::time::Duration;

use serde_json::Value;

/// Optional runtime controls for a workflow run/resume/timeout operation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WorkflowRunOptions {
    /// Optional per-workflow lease acquired before execution starts.
    pub lease: Option<WorkflowLeaseOptions>,
    /// Optional idempotency key for the external operation being handled.
    pub idempotency_key: Option<String>,
}

impl WorkflowRunOptions {
    /// Add a per-workflow lease requirement.
    pub fn with_lease(mut self, owner: impl Into<String>, ttl: Duration) -> Self {
        self.lease = Some(WorkflowLeaseOptions {
            owner: owner.into(),
            ttl,
        });
        self
    }

    /// Add an operation idempotency key.
    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }
}

/// Per-workflow lease acquisition request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowLeaseOptions {
    /// Worker/process identity that owns the lease while executing.
    pub owner: String,
    /// Lease time-to-live. Backends should expire abandoned leases after this.
    pub ttl: Duration,
}

/// Lease held for the duration of one workflow operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowLease {
    /// Workflow instance id.
    pub workflow_id: String,
    /// Worker/process identity that owns the lease.
    pub owner: String,
    /// Lease time-to-live.
    pub ttl: Duration,
}

/// High-level workflow operation kind for idempotency ledgers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowOperationKind {
    /// Direct run from the current state.
    Run,
    /// Resume from an external event.
    Resume {
        /// Event name accepted by the wait cursor.
        event: String,
    },
    /// Execute a timed-out wait fallback.
    Timeout,
}

/// Idempotent workflow operation request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowOperation {
    /// Workflow definition name.
    pub workflow_name: String,
    /// Workflow instance id.
    pub workflow_id: String,
    /// Caller-provided idempotency key.
    pub idempotency_key: String,
    /// Operation kind.
    pub kind: WorkflowOperationKind,
}

/// Operation ledger decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowOperationStatus {
    /// This caller owns execution for this idempotency key.
    Started,
    /// Another worker is currently handling this idempotency key.
    InProgress,
    /// This idempotency key already completed with a final state.
    Completed {
        /// Previously completed workflow state.
        state: String,
    },
}

/// Side-effect kind used for idempotent outbox/provider calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowSideEffectKind {
    /// QAIL query execution.
    Query,
    /// External notification delivery.
    Notify,
    /// Payment charge creation.
    Charge,
}

impl WorkflowSideEffectKind {
    /// Stable lowercase identifier for operation keys.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Notify => "notify",
            Self::Charge => "charge",
        }
    }
}

/// Stable side-effect operation identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowSideEffect {
    /// Workflow instance id.
    pub workflow_id: String,
    /// Workflow state that owns this step.
    pub state: String,
    /// Stable nested step path within the workflow definition.
    pub step_path: String,
    /// Side-effect kind.
    pub kind: WorkflowSideEffectKind,
    /// Stable operation id for DB uniqueness/idempotency.
    pub operation_id: String,
}

/// Side-effect ledger decision.
#[derive(Debug, Clone, PartialEq)]
pub enum WorkflowSideEffectStatus {
    /// Execute the side effect now.
    Execute,
    /// The side effect already completed. `result` is used for query/charge
    /// steps that need to restore stored context values.
    AlreadyCompleted {
        /// Previously recorded side-effect result.
        result: Option<Value>,
    },
}

/// Result of one workflow timeout attempt in a timeout drain batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowTimeoutOutcome {
    /// Workflow instance id.
    pub workflow_id: String,
    /// Final/current state if timeout execution succeeded.
    pub state: Option<String>,
    /// Error text if timeout execution failed.
    pub error: Option<String>,
}
