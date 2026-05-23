//! Workflow execution context — carries data between steps.
//!
//! The context is a typed key-value bag that steps read from and write to.
//! Query results are stored here, and Notify steps resolve recipient
//! addresses from context keys.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::step::WorkflowStep;

/// Workflow execution context.
///
/// Persisted between steps so workflows survive process restarts.
/// Each step can read from and write to the context.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowContext {
    /// Unique identifier for this workflow instance
    pub workflow_id: String,
    /// Current state name
    pub current_state: String,
    /// Key-value data bag (query results, user inputs, etc.)
    pub data: HashMap<String, Value>,
    /// When this workflow instance was created
    pub created_at: DateTime<Utc>,
    /// When this context was last updated
    pub updated_at: DateTime<Utc>,
    /// Audit trail of state transitions
    pub history: Vec<StateChange>,
    /// Cursor used to resume inside nested workflow blocks after a Wait.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<WorkflowCursor>,
}

/// Persisted execution cursor for resuming after a Wait.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowCursor {
    /// State whose transition owns the cursor frames.
    pub state: String,
    /// Nested frame path to the next executable step.
    pub frames: Vec<WorkflowCursorFrame>,
    /// External event currently required before this cursor may resume.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait: Option<WorkflowPendingWait>,
}

/// Persisted metadata for a paused Wait step.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowPendingWait {
    /// Event name required to resume this workflow.
    pub event: String,
    /// Wall-clock deadline after which timeout fallback is eligible.
    pub deadline_at: DateTime<Utc>,
    /// Steps to execute if the wait times out.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on_timeout: Vec<WorkflowStep>,
}

/// One level in a persisted workflow execution cursor.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WorkflowCursorFrame {
    /// Top-level transition step list.
    Steps {
        /// Next top-level step index.
        index: usize,
    },
    /// Selected branch step list.
    Branch {
        /// Branch arm that was active when execution paused.
        selection: WorkflowBranchCursorSelection,
        /// Next step index inside the selected branch.
        index: usize,
    },
    /// Active ForEach item step list.
    ForEach {
        /// Current array item index.
        item_index: usize,
        /// Next step index inside the item block.
        index: usize,
    },
}

/// Persisted branch arm selection for workflow resume.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WorkflowBranchCursorSelection {
    /// A concrete branch index from `WorkflowStep::Branch::branches`.
    Branch(usize),
    /// The default branch arm.
    Default,
}

/// Record of a state transition for audit trail.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateChange {
    /// Previous state name.
    pub from: String,
    /// New state name.
    pub to: String,
    /// When the transition occurred.
    pub at: DateTime<Utc>,
    /// Optional human-readable reason for the transition.
    pub reason: Option<String>,
}

impl WorkflowContext {
    /// Create a new context for a workflow instance.
    pub fn new(workflow_id: impl Into<String>, initial_state: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            workflow_id: workflow_id.into(),
            current_state: initial_state.into(),
            data: HashMap::new(),
            created_at: now,
            updated_at: now,
            history: Vec::new(),
            cursor: None,
        }
    }

    /// Store a value in the context.
    pub fn set(&mut self, key: impl Into<String>, value: Value) {
        self.data.insert(key.into(), value);
        self.updated_at = Utc::now();
    }

    /// Get a value from the context.
    pub fn get(&self, key: &str) -> Option<&Value> {
        // Support dot-notation: "item.phone" → data["item"]["phone"]
        if key.contains('.') {
            let parts: Vec<&str> = key.splitn(2, '.').collect();
            if let Some(parent) = self.data.get(parts[0]) {
                return get_nested(parent, parts[1]);
            }
            return None;
        }
        self.data.get(key)
    }

    /// Get a string value from the context.
    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(|v| v.as_str())
    }

    /// Transition to a new state, recording the change.
    pub fn transition_to(&mut self, new_state: impl Into<String>, reason: Option<String>) {
        let new = new_state.into();
        self.history.push(StateChange {
            from: self.current_state.clone(),
            to: new.clone(),
            at: Utc::now(),
            reason,
        });
        self.current_state = new;
        self.cursor = None;
        self.updated_at = Utc::now();
    }

    /// Store a resume cursor for the next run.
    pub fn set_cursor(&mut self, cursor: WorkflowCursor) {
        self.cursor = Some(cursor);
        self.updated_at = Utc::now();
    }

    /// Remove and return the current resume cursor.
    pub fn take_cursor(&mut self) -> Option<WorkflowCursor> {
        let cursor = self.cursor.take();
        if cursor.is_some() {
            self.updated_at = Utc::now();
        }
        cursor
    }

    /// Clear any saved resume cursor.
    pub fn clear_cursor(&mut self) {
        if self.cursor.take().is_some() {
            self.updated_at = Utc::now();
        }
    }

    /// Get the number of state transitions that have occurred.
    pub fn transition_count(&self) -> usize {
        self.history.len()
    }
}

/// Resolve a dot-notation path within a JSON value.
fn get_nested<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    if path.contains('.') {
        let parts: Vec<&str> = path.splitn(2, '.').collect();
        match value {
            Value::Object(map) => map.get(parts[0]).and_then(|v| get_nested(v, parts[1])),
            _ => None,
        }
    } else {
        match value {
            Value::Object(map) => map.get(path),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_context() {
        let ctx = WorkflowContext::new("wf-001", "created");
        assert_eq!(ctx.workflow_id, "wf-001");
        assert_eq!(ctx.current_state, "created");
        assert!(ctx.data.is_empty());
        assert!(ctx.history.is_empty());
        assert!(ctx.cursor.is_none());
    }

    #[test]
    fn test_set_and_get() {
        let mut ctx = WorkflowContext::new("wf-001", "created");
        ctx.set("name", Value::String("test".into()));
        assert_eq!(ctx.get_str("name"), Some("test"));
        assert!(ctx.get("missing").is_none());
    }

    #[test]
    fn test_dot_notation() {
        let mut ctx = WorkflowContext::new("wf-001", "created");
        ctx.set(
            "guest",
            serde_json::json!({
                "name": "John",
                "phone": "+628123456",
                "address": { "city": "Bali" }
            }),
        );
        assert_eq!(ctx.get_str("guest.name"), Some("John"));
        assert_eq!(ctx.get_str("guest.phone"), Some("+628123456"));
        assert_eq!(ctx.get_str("guest.address.city"), Some("Bali"));
        assert!(ctx.get("guest.missing").is_none());
    }

    #[test]
    fn test_state_transition() {
        let mut ctx = WorkflowContext::new("wf-001", "created");
        ctx.transition_to("paid_escrow", Some("Payment received".into()));
        assert_eq!(ctx.current_state, "paid_escrow");
        assert_eq!(ctx.transition_count(), 1);
        assert_eq!(ctx.history[0].from, "created");
        assert_eq!(ctx.history[0].to, "paid_escrow");
    }

    #[test]
    fn test_context_serialization() {
        let mut ctx = WorkflowContext::new("wf-001", "created");
        ctx.set("booking_id", Value::String("b-123".into()));
        ctx.transition_to("pending", None);

        let json = serde_json::to_string(&ctx).unwrap();
        let restored: WorkflowContext = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.workflow_id, "wf-001");
        assert_eq!(restored.current_state, "pending");
        assert_eq!(restored.get_str("booking_id"), Some("b-123"));
        assert!(restored.cursor.is_none());
    }

    #[test]
    fn test_context_resume_cursor_round_trip() {
        let mut ctx = WorkflowContext::new("wf-001", "created");
        ctx.set_cursor(WorkflowCursor {
            state: "created".to_string(),
            frames: vec![
                WorkflowCursorFrame::Steps { index: 0 },
                WorkflowCursorFrame::ForEach {
                    item_index: 1,
                    index: 2,
                },
            ],
            wait: Some(WorkflowPendingWait {
                event: "operator_accept".to_string(),
                deadline_at: Utc::now(),
                on_timeout: vec![],
            }),
        });

        let json = serde_json::to_string(&ctx).unwrap();
        let restored: WorkflowContext = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.cursor, ctx.cursor);
    }

    #[test]
    fn test_context_deserializes_cursor_without_wait_metadata() {
        let json = r#"{
            "workflow_id":"wf-001",
            "current_state":"created",
            "data":{},
            "created_at":"2026-01-01T00:00:00Z",
            "updated_at":"2026-01-01T00:00:00Z",
            "history":[],
            "cursor":{
                "state":"created",
                "frames":[{"Steps":{"index":1}}]
            }
        }"#;

        let restored: WorkflowContext = serde_json::from_str(json).unwrap();
        let cursor = restored.cursor.expect("cursor should deserialize");
        assert!(cursor.wait.is_none());
    }

    #[test]
    fn test_context_deserializes_without_cursor() {
        let json = r#"{
            "workflow_id":"wf-001",
            "current_state":"created",
            "data":{},
            "created_at":"2026-01-01T00:00:00Z",
            "updated_at":"2026-01-01T00:00:00Z",
            "history":[]
        }"#;

        let restored: WorkflowContext = serde_json::from_str(json).unwrap();
        assert!(restored.cursor.is_none());
    }
}
