//! # QAIL Workflow
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
//! example-engine (domain states + executor impl)
//!       ↓
//! qail-workflow (state machine engine)
//!       ↓
//! qail-core (AST queries composed into steps)
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
pub mod registry;
pub mod state;
pub mod step;

// Re-exports for convenience
pub use channel::{ChannelKind, NotifyChannel, ChannelError};
pub use context::WorkflowContext;
pub use engine::{WorkflowExecutor, WorkflowError, run_workflow};
pub use registry::{WorkflowDefinition, StateTransition};
pub use state::State;
pub use step::WorkflowStep;
