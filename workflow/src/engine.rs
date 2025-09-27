//! Workflow engine — the executor trait and step runner.
//!
//! Consumers implement `WorkflowExecutor` to connect the engine
//! to their database driver and notification channels.

use async_trait::async_trait;
use serde_json::Value;

use crate::channel::ChannelKind;
use crate::context::WorkflowContext;
use crate::registry::WorkflowDefinition;
use crate::step::WorkflowStep;

/// Errors that can occur during workflow execution.
#[derive(Debug)]
pub enum WorkflowError {
    /// Query execution failed
    QueryFailed(String),
    /// Notification delivery failed
    NotifyFailed(String),
    /// No transition found for current state
    NoTransition { state: String, workflow: String },
    /// Context key not found
    MissingContextKey(String),
    /// Workflow has reached a terminal state
    AlreadyTerminal(String),
    /// State persistence failed
    PersistenceFailed(String),
    /// Timeout reached while waiting
    Timeout { event: String },
    /// Generic error
    Other(String),
}

impl std::fmt::Display for WorkflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkflowError::QueryFailed(e) => write!(f, "Query failed: {}", e),
            WorkflowError::NotifyFailed(e) => write!(f, "Notification failed: {}", e),
            WorkflowError::NoTransition { state, workflow } => {
                write!(f, "No transition from '{}' in workflow '{}'", state, workflow)
            }
            WorkflowError::MissingContextKey(k) => write!(f, "Context key not found: {}", k),
            WorkflowError::AlreadyTerminal(s) => write!(f, "Workflow already terminal: {}", s),
            WorkflowError::PersistenceFailed(e) => write!(f, "Persistence failed: {}", e),
            WorkflowError::Timeout { event } => write!(f, "Timeout waiting for: {}", event),
            WorkflowError::Other(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for WorkflowError {}

/// Executor trait — consumers implement this to connect workflows to their infrastructure.
///
/// This is the bridge between `qail-workflow` (domain-agnostic) and your app
/// (domain-specific). Your app provides the database driver, notification channels,
/// and state persistence.
///
/// # Example
///
/// ```rust,ignore
/// struct MyExecutor { pg: PgDriver, waba: WabaClient }
///
/// #[async_trait]
/// impl WorkflowExecutor for MyExecutor {
///     async fn execute_query(&self, cmd_json: &str) -> Result<Value, WorkflowError> {
///         let cmd: Qail = serde_json::from_str(cmd_json)?;
///         let rows = self.pg.fetch_all(&cmd).await?;
///         Ok(rows_to_json(rows))
///     }
///
///     async fn send_notification(
///         &self, channel: &ChannelKind, recipient: &str,
///         template: &str, params: &Value,
///     ) -> Result<(), WorkflowError> {
///         self.waba.send_template(recipient, template, params).await
///     }
///
///     async fn save_state(&self, ctx: &WorkflowContext) -> Result<(), WorkflowError> {
///         // Persist to _qail_workflow_states table
///     }
///
///     async fn load_state(&self, workflow_id: &str) -> Result<Option<WorkflowContext>, WorkflowError> {
///         // Load from _qail_workflow_states table
///     }
/// }
/// ```
#[async_trait]
pub trait WorkflowExecutor: Send + Sync {
    /// Execute a QAIL query (serialized as JSON) and return results.
    async fn execute_query(&self, cmd_json: &str) -> Result<Value, WorkflowError>;

    /// Send a notification through a channel.
    async fn send_notification(
        &self,
        channel: &ChannelKind,
        recipient: &str,
        template: &str,
        params: &Value,
    ) -> Result<(), WorkflowError>;

    /// Persist the workflow state (for crash recovery).
    async fn save_state(&self, ctx: &WorkflowContext) -> Result<(), WorkflowError>;

    /// Load a previously persisted workflow state.
    async fn load_state(
        &self,
        workflow_id: &str,
    ) -> Result<Option<WorkflowContext>, WorkflowError>;
}

/// Execute a single workflow step.
fn execute_step<'a, E: WorkflowExecutor>(
    executor: &'a E,
    step: &'a WorkflowStep,
    ctx: &'a mut WorkflowContext,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), WorkflowError>> + Send + 'a>> {
    Box::pin(async move {
        match step {
            WorkflowStep::Query { cmd_json, store_as } => {
                let result = executor.execute_query(cmd_json).await?;
                if let Some(key) = store_as {
                    ctx.set(key, result);
                }
            }

            WorkflowStep::Notify {
                channel,
                template,
                recipient_key,
                payload_key,
            } => {
                let recipient = ctx
                    .get_str(recipient_key)
                    .ok_or_else(|| WorkflowError::MissingContextKey(recipient_key.clone()))?
                    .to_string();

                let params = payload_key
                    .as_ref()
                    .and_then(|k| ctx.get(k).cloned())
                    .unwrap_or(Value::Object(serde_json::Map::new()));

                executor
                    .send_notification(channel, &recipient, template, &params)
                    .await?;
            }

            WorkflowStep::Wait {
                event: _,
                timeout: _,
                on_timeout: _,
            } => {
                // Wait steps are handled externally:
                // The engine pauses here and persists state.
                // When the event arrives (via webhook), the workflow resumes.
                // Timeout is managed by the consumer's scheduler/cron.
                //
                // This is intentionally a no-op in the synchronous runner.
                // The consumer calls `resume_workflow()` when the event fires.
            }

            WorkflowStep::Branch {
                condition_key,
                branches,
                default,
            } => {
                let value = ctx
                    .get_str(condition_key)
                    .unwrap_or("")
                    .to_string();

                let steps = branches
                    .iter()
                    .find(|(k, _)| k == &value)
                    .map(|(_, steps)| steps)
                    .unwrap_or(default);

                for s in steps {
                    execute_step(executor, s, ctx).await?;
                }
            }

            WorkflowStep::ForEach { list_key, steps } => {
                let list = ctx
                    .get(list_key)
                    .cloned()
                    .ok_or_else(|| WorkflowError::MissingContextKey(list_key.clone()))?;

                if let Value::Array(items) = list {
                    for item in items {
                        // Make item available as "item" in context
                        ctx.set("item", item.clone());
                        for s in steps {
                            execute_step(executor, s, ctx).await?;
                        }
                    }
                    // Clean up
                    ctx.data.remove("item");
                }
            }

            WorkflowStep::Transition { to } => {
                ctx.transition_to(to, None);
            }

            WorkflowStep::Log { message } => {
                // Replace {key} placeholders with context values
                let mut resolved = message.clone();
                for (key, value) in &ctx.data {
                    let placeholder = format!("{{{}}}", key);
                    if let Some(s) = value.as_str() {
                        resolved = resolved.replace(&placeholder, s);
                    }
                }
                // Engine logging — consumers can override via tracing subscriber
                eprintln!("[workflow:{}] {}", ctx.workflow_id, resolved);
            }
        }

        Ok(())
    })
}

/// Run a workflow from its current state until it reaches a Wait or terminal state.
///
/// Returns the final state name after execution.
///
/// # Behavior
///
/// 1. Finds the transition matching `ctx.current_state`
/// 2. Executes each step in the transition
/// 3. If a `Wait` step is encountered, persists state and returns
/// 4. If a `Transition` step moves to a new state, looks for the next transition
/// 5. Continues until no more transitions or a terminal state is reached
///
/// # Example
///
/// ```rust,ignore
/// let mut ctx = WorkflowContext::new("booking-123", "operator_declined");
/// let final_state = run_workflow(&executor, &recovery_workflow, &mut ctx).await?;
/// // final_state might be "recovery_mode" (paused at Wait)
/// // or "cancelled" (if timeout was immediate)
/// ```
pub async fn run_workflow<E: WorkflowExecutor>(
    executor: &E,
    definition: &WorkflowDefinition,
    ctx: &mut WorkflowContext,
) -> Result<String, WorkflowError> {
    loop {
        // Find transition from current state
        let transition = definition.find_transition(&ctx.current_state);

        let transition = match transition {
            Some(t) => t,
            None => {
                // No transition = terminal state or waiting
                executor.save_state(ctx).await?;
                return Ok(ctx.current_state.clone());
            }
        };

        // Execute each step
        for step in &transition.steps {
            // If we hit a Wait, persist and return
            if matches!(step, WorkflowStep::Wait { .. }) {
                executor.save_state(ctx).await?;
                return Ok(ctx.current_state.clone());
            }

            execute_step(executor, step, ctx).await?;
        }

        // After executing all steps, if a Transition step changed the state,
        // loop again to check for chained transitions.
        // If state didn't change (steps didn't include Transition), we're done.
        if ctx.current_state == transition.from {
            // No transition happened, move to `to` state
            ctx.transition_to(&transition.to, None);
        }

        // Persist after each state change
        executor.save_state(ctx).await?;

        // Safety: prevent infinite loops (max 50 transitions per run)
        if ctx.transition_count() > 50 {
            return Err(WorkflowError::Other(
                "Maximum transition count exceeded (50)".into(),
            ));
        }
    }
}

/// Resume a workflow after a Wait event was received.
///
/// Call this when a webhook/callback arrives that matches the Wait event.
/// The workflow continues from where it paused.
pub async fn resume_workflow<E: WorkflowExecutor>(
    executor: &E,
    definition: &WorkflowDefinition,
    workflow_id: &str,
    event_data: Value,
) -> Result<String, WorkflowError> {
    // Load persisted state
    let mut ctx = executor
        .load_state(workflow_id)
        .await?
        .ok_or_else(|| WorkflowError::Other(format!("Workflow not found: {}", workflow_id)))?;

    // Store the event data in context
    ctx.set("event", event_data);

    // Continue running from current state
    run_workflow(executor, definition, &mut ctx).await
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockExecutor {
        queries: std::sync::Mutex<Vec<String>>,
        notifications: std::sync::Mutex<Vec<(String, String)>>,
    }

    impl MockExecutor {
        fn new() -> Self {
            Self {
                queries: std::sync::Mutex::new(Vec::new()),
                notifications: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl WorkflowExecutor for MockExecutor {
        async fn execute_query(&self, cmd_json: &str) -> Result<Value, WorkflowError> {
            self.queries.lock().unwrap().push(cmd_json.to_string());
            // Return mock results
            Ok(serde_json::json!([
                {"id": "op-1", "phone": "+628111"},
                {"id": "op-2", "phone": "+628222"},
            ]))
        }

        async fn send_notification(
            &self,
            _channel: &ChannelKind,
            recipient: &str,
            template: &str,
            _params: &Value,
        ) -> Result<(), WorkflowError> {
            self.notifications
                .lock()
                .unwrap()
                .push((recipient.to_string(), template.to_string()));
            Ok(())
        }

        async fn save_state(&self, _ctx: &WorkflowContext) -> Result<(), WorkflowError> {
            Ok(())
        }

        async fn load_state(
            &self,
            _workflow_id: &str,
        ) -> Result<Option<WorkflowContext>, WorkflowError> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn test_simple_workflow() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("test")
            .initial_state("start")
            .transition(
                "start",
                "done",
                vec![
                    WorkflowStep::log("Starting workflow"),
                    WorkflowStep::transition("done"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-001", "start");
        let result = run_workflow(&executor, &wf, &mut ctx).await.unwrap();
        assert_eq!(result, "done");
        assert_eq!(ctx.transition_count(), 1);
    }

    #[tokio::test]
    async fn test_workflow_with_branch() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("branching")
            .initial_state("pending")
            .transition(
                "pending",
                "resolved",
                vec![
                    WorkflowStep::branch(
                        "reason",
                        vec![
                            ("full", vec![WorkflowStep::log("Fully booked")]),
                            ("maintenance", vec![WorkflowStep::log("Under maintenance")]),
                        ],
                        vec![WorkflowStep::log("Unknown reason")],
                    ),
                    WorkflowStep::transition("resolved"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-002", "pending");
        ctx.set("reason", Value::String("maintenance".into()));

        let result = run_workflow(&executor, &wf, &mut ctx).await.unwrap();
        assert_eq!(result, "resolved");
    }

    #[tokio::test]
    async fn test_workflow_for_each_notify() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("broadcast")
            .initial_state("broadcasting")
            .transition(
                "broadcasting",
                "waiting",
                vec![WorkflowStep::for_each(
                    "operators",
                    vec![WorkflowStep::notify(
                        ChannelKind::WhatsApp,
                        "opportunity",
                        "item.phone",
                    )],
                )],
            );

        let mut ctx = WorkflowContext::new("wf-003", "broadcasting");
        ctx.set(
            "operators",
            serde_json::json!([
                {"name": "Captain A", "phone": "+628111"},
                {"name": "Captain B", "phone": "+628222"},
                {"name": "Captain C", "phone": "+628333"},
            ]),
        );

        let result = run_workflow(&executor, &wf, &mut ctx).await.unwrap();
        assert_eq!(result, "waiting");

        // Verify all 3 captains were notified
        let notifs = executor.notifications.lock().unwrap();
        assert_eq!(notifs.len(), 3);
        assert_eq!(notifs[0].0, "+628111");
        assert_eq!(notifs[1].0, "+628222");
        assert_eq!(notifs[2].0, "+628333");
    }

    #[tokio::test]
    async fn test_workflow_pauses_at_wait() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("wait_test")
            .initial_state("active")
            .transition(
                "active",
                "resolved",
                vec![
                    WorkflowStep::log("Before wait"),
                    WorkflowStep::wait("user_response", std::time::Duration::from_secs(3600)),
                    // This should NOT execute (paused at Wait)
                    WorkflowStep::log("After wait"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-004", "active");
        let result = run_workflow(&executor, &wf, &mut ctx).await.unwrap();

        // Should pause at "active" (Wait encountered before Transition)
        assert_eq!(result, "active");
    }
}
