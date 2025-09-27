//! Workflow steps — the composable building blocks of a workflow.
//!
//! Each step represents a single atomic action: execute a query,
//! send a notification, wait for an event, or branch on a condition.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::channel::ChannelKind;

/// A single step in a workflow execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkflowStep {
    /// Execute a QAIL query against the database.
    /// The `cmd_sql` holds the transpiled SQL (since Qail AST isn't serializable by default).
    /// The `store_as` key saves the result into WorkflowContext for later steps.
    Query {
        /// QAIL AST command (serialized as JSON)
        cmd_json: String,
        /// Optional key to store query results in context
        store_as: Option<String>,
    },

    /// Send a notification via a channel (WhatsApp, Email, SMS, Push).
    Notify {
        /// Which channel to send through
        channel: ChannelKind,
        /// Template name (e.g., "opportunity_alert", "booking_confirmed")
        template: String,
        /// Context key that resolves to the recipient address
        recipient_key: String,
        /// Optional extra payload (template variables)
        payload_key: Option<String>,
    },

    /// Wait for an external event (webhook callback, user action, timeout).
    Wait {
        /// Event key to listen for (matched against incoming webhook/callback)
        event: String,
        /// Maximum time to wait before triggering on_timeout
        timeout: Duration,
        /// Steps to execute if timeout is reached
        on_timeout: Vec<WorkflowStep>,
    },

    /// Conditional branching based on a context value.
    Branch {
        /// Context key to evaluate
        condition_key: String,
        /// Map of value → steps to execute
        branches: Vec<(String, Vec<WorkflowStep>)>,
        /// Default steps if no branch matches
        default: Vec<WorkflowStep>,
    },

    /// Execute steps for each item in a context list.
    ForEach {
        /// Context key containing a JSON array
        list_key: String,
        /// Steps to execute per item (item available as "item" in context)
        steps: Vec<WorkflowStep>,
    },

    /// Transition to a new workflow state.
    Transition {
        /// Target state name
        to: String,
    },

    /// Log a message (useful for debugging/audit trails).
    Log {
        /// Message template (can reference context keys with {key})
        message: String,
    },
}

impl WorkflowStep {
    /// Create a Query step from a Qail command.
    pub fn query(cmd: &qail_core::Qail, store_as: Option<&str>) -> Self {
        // Serialize the Qail AST to JSON for persistence
        let cmd_json = serde_json::to_string(cmd).unwrap_or_default();
        WorkflowStep::Query {
            cmd_json,
            store_as: store_as.map(String::from),
        }
    }

    /// Create a Notify step.
    pub fn notify(channel: ChannelKind, template: &str, recipient_key: &str) -> Self {
        WorkflowStep::Notify {
            channel,
            template: template.into(),
            recipient_key: recipient_key.into(),
            payload_key: None,
        }
    }

    /// Create a Notify step with payload.
    pub fn notify_with_payload(
        channel: ChannelKind,
        template: &str,
        recipient_key: &str,
        payload_key: &str,
    ) -> Self {
        WorkflowStep::Notify {
            channel,
            template: template.into(),
            recipient_key: recipient_key.into(),
            payload_key: Some(payload_key.into()),
        }
    }

    /// Create a Wait step.
    pub fn wait(event: &str, timeout: Duration) -> Self {
        WorkflowStep::Wait {
            event: event.into(),
            timeout,
            on_timeout: vec![],
        }
    }

    /// Create a Wait step with timeout fallback.
    pub fn wait_or(event: &str, timeout: Duration, on_timeout: Vec<WorkflowStep>) -> Self {
        WorkflowStep::Wait {
            event: event.into(),
            timeout,
            on_timeout,
        }
    }

    /// Create a ForEach step.
    pub fn for_each(list_key: &str, steps: Vec<WorkflowStep>) -> Self {
        WorkflowStep::ForEach {
            list_key: list_key.into(),
            steps,
        }
    }

    /// Create a Transition step.
    pub fn transition(to: &str) -> Self {
        WorkflowStep::Transition { to: to.into() }
    }

    /// Create a Log step.
    pub fn log(message: &str) -> Self {
        WorkflowStep::Log {
            message: message.into(),
        }
    }

    /// Create a Branch step.
    pub fn branch(
        condition_key: &str,
        branches: Vec<(&str, Vec<WorkflowStep>)>,
        default: Vec<WorkflowStep>,
    ) -> Self {
        WorkflowStep::Branch {
            condition_key: condition_key.into(),
            branches: branches
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            default,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_notify_step() {
        let step = WorkflowStep::notify(ChannelKind::WhatsApp, "booking_confirm", "guest.phone");
        match step {
            WorkflowStep::Notify {
                channel,
                template,
                recipient_key,
                payload_key,
            } => {
                assert!(matches!(channel, ChannelKind::WhatsApp));
                assert_eq!(template, "booking_confirm");
                assert_eq!(recipient_key, "guest.phone");
                assert!(payload_key.is_none());
            }
            _ => panic!("Expected Notify step"),
        }
    }

    #[test]
    fn test_wait_with_timeout() {
        let step = WorkflowStep::wait_or(
            "operator_accept",
            Duration::from_secs(3600),
            vec![WorkflowStep::transition("cancelled")],
        );
        match step {
            WorkflowStep::Wait {
                event,
                timeout,
                on_timeout,
            } => {
                assert_eq!(event, "operator_accept");
                assert_eq!(timeout.as_secs(), 3600);
                assert_eq!(on_timeout.len(), 1);
            }
            _ => panic!("Expected Wait step"),
        }
    }

    #[test]
    fn test_branch_step() {
        let step = WorkflowStep::branch(
            "decline_reason",
            vec![
                (
                    "full",
                    vec![WorkflowStep::log("Boat is full")],
                ),
                (
                    "maintenance",
                    vec![WorkflowStep::log("Boat under maintenance")],
                ),
            ],
            vec![WorkflowStep::log("Unknown reason")],
        );
        match step {
            WorkflowStep::Branch {
                condition_key,
                branches,
                default,
            } => {
                assert_eq!(condition_key, "decline_reason");
                assert_eq!(branches.len(), 2);
                assert_eq!(default.len(), 1);
            }
            _ => panic!("Expected Branch step"),
        }
    }

    #[test]
    fn test_for_each_step() {
        let step = WorkflowStep::for_each(
            "alternatives",
            vec![WorkflowStep::notify(
                ChannelKind::WhatsApp,
                "opportunity",
                "item.phone",
            )],
        );
        match step {
            WorkflowStep::ForEach { list_key, steps } => {
                assert_eq!(list_key, "alternatives");
                assert_eq!(steps.len(), 1);
            }
            _ => panic!("Expected ForEach step"),
        }
    }

    #[test]
    fn test_step_serialization() {
        let step = WorkflowStep::transition("fulfilled");
        let json = serde_json::to_string(&step).unwrap();
        let _: WorkflowStep = serde_json::from_str(&json).unwrap();
    }
}
