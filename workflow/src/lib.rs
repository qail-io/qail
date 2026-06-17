//! # QAIL Flow Engine
//!
//! Declarative state machine engine for multi-vertical workflows.
//!
//! Provides domain-agnostic primitives for orchestrating sequences of
//! database queries, notifications, and external events. Designed for
//! booking recovery, inventory sync, and operator coordination across
//! ferry, car rental, yacht charter, and fishing boat verticals.
//!
//! ## Architecture
//!
//! ```text
//! your app (domain states + executor impl)
//!       ↓
//! qail-workflow (Flow Engine)
//!       ↓
//! qail-core (AST queries composed into steps)
//!       ↓
//! optional qail-workflow-postgres (Flow Ledger)
//! ```
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use qail_workflow::*;
//! use qail_core::Qail;
//!
//! // Define a workflow
//! let wf = WorkflowDefinition::new("booking_recovery")
//!     .initial_state("operator_declined")
//!     .transition("operator_declined", "recovery_mode", vec![
//!         WorkflowStep::query(
//!             Qail::get("odysseys").limit(5),
//!             Some("alternatives"),
//!         ),
//!         WorkflowStep::for_each("alternatives", vec![
//!             WorkflowStep::notify(ChannelKind::WhatsApp, "opportunity_alert", "item.phone"),
//!         ]),
//!         WorkflowStep::wait("operator_accept", Duration::from_secs(7200)),
//!     ]);
//!
//! // Run it with your executor
//! let result = run_workflow(&my_executor, &wf, &mut ctx).await?;
//! ```

#![deny(warnings)]
#![deny(clippy::all)]
#![deny(unused_imports)]
#![deny(dead_code)]

pub mod channel;
pub mod context;
pub mod engine;
pub mod payment;
pub mod registry;
pub mod runtime;
pub mod state;
pub mod step;

// Re-exports for convenience
pub use channel::{ChannelError, ChannelKind, NotifyChannel};
pub use context::{
    WorkflowBranchCursorSelection, WorkflowContext, WorkflowCursor, WorkflowCursorFrame,
    WorkflowPendingWait,
};
pub use engine::{
    LegacyQueryPayloadIssue, WorkflowError, WorkflowExecutor, collect_legacy_query_payload_issues,
    resume_workflow, resume_workflow_with_event, resume_workflow_with_event_and_options,
    resume_workflow_with_options, run_workflow, run_workflow_with_options, timeout_due_workflows,
    timeout_workflow, timeout_workflow_with_options,
};
pub use payment::{
    ChargeRequest, ChargeResponse, ChargeStatus, Currency, PaymentError, PaymentEvent, PaymentKind,
    PaymentProvider,
};
pub use registry::{StateTransition, WorkflowDefinition};
pub use runtime::{
    WorkflowLease, WorkflowLeaseOptions, WorkflowOperation, WorkflowOperationKind,
    WorkflowOperationStatus, WorkflowRunOptions, WorkflowSideEffect, WorkflowSideEffectKind,
    WorkflowSideEffectStatus, WorkflowTimeoutOutcome,
};
pub use state::State;
pub use step::{WorkflowBranchCondition, WorkflowStep};
