//! State trait — the contract domain states must implement.
//!
//! Each vertical defines its own state enum (e.g. `BookingState`, `RentalState`)
//! and implements this trait. `qail-workflow` never knows about specific states.

use serde::{Deserialize, Serialize};

/// Trait for workflow states. Each vertical defines its own states.
///
/// # Example
///
/// ```rust
/// use qail_workflow::State;
///
/// #[derive(Debug, Clone)]
/// enum BookingState {
///     Created,
///     PaidEscrow,
///     OperatorPending,
///     OperatorDeclined,
///     RecoveryMode,
///     GuestSelecting,
///     Fulfilled,
///     Cancelled,
/// }
///
/// impl State for BookingState {
///     fn name(&self) -> &str {
///         match self {
///             Self::Created => "created",
///             Self::PaidEscrow => "paid_escrow",
///             Self::OperatorPending => "operator_pending",
///             Self::OperatorDeclined => "operator_declined",
///             Self::RecoveryMode => "recovery_mode",
///             Self::GuestSelecting => "guest_selecting",
///             Self::Fulfilled => "fulfilled",
///             Self::Cancelled => "cancelled",
///         }
///     }
///
///     fn is_terminal(&self) -> bool {
///         matches!(self, Self::Fulfilled | Self::Cancelled)
///     }
/// }
/// ```
pub trait State: std::fmt::Debug + Clone + Send + Sync + 'static {
    /// Human-readable state name (for logging/persistence).
    /// Should be snake_case for database storage.
    fn name(&self) -> &str;

    /// Whether this is a terminal state (workflow complete).
    fn is_terminal(&self) -> bool;
}

/// A simple string-based state for workflows that don't need typed states.
///
/// Useful for quick prototyping or dynamic workflow definitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleState {
    name: String,
    terminal: bool,
}

impl SimpleState {
    /// Create a non-terminal state.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            terminal: false,
        }
    }

    /// Create a terminal state.
    pub fn terminal(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            terminal: true,
        }
    }
}

impl State for SimpleState {
    fn name(&self) -> &str {
        &self.name
    }

    fn is_terminal(&self) -> bool {
        self.terminal
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_state() {
        let s = SimpleState::new("pending");
        assert_eq!(s.name(), "pending");
        assert!(!s.is_terminal());
    }

    #[test]
    fn test_terminal_state() {
        let s = SimpleState::terminal("cancelled");
        assert_eq!(s.name(), "cancelled");
        assert!(s.is_terminal());
    }
}
