//! Workflow engine — the executor trait and step runner.
//!
//! Consumers implement `WorkflowExecutor` to connect the engine
//! to their database driver and notification channels.

use async_trait::async_trait;
use serde_json::Value;

use crate::channel::ChannelKind;
use crate::context::WorkflowContext;
use crate::payment::{ChargeRequest, ChargeResponse, Currency, PaymentKind};
use crate::registry::WorkflowDefinition;
use crate::step::WorkflowStep;

const WORKFLOW_QUERY_WIRE_MAGIC: &str = "QAIL-CMD/1\n";

/// A single legacy query payload detected in a workflow definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyQueryPayloadIssue {
    /// Transition source state containing the step.
    pub transition_from: String,
    /// Transition destination state containing the step.
    pub transition_to: String,
    /// Path to the offending step inside the transition.
    pub step_path: String,
    /// Short preview of the legacy payload (escaped, truncated).
    pub payload_preview: String,
}

/// Return legacy query payload issues found in a workflow definition.
///
/// A query payload is considered legacy when `cmd_json` does not start with
/// QAIL wire text magic (`QAIL-CMD/1\n`).
///
/// This helper is intended for cutover audits before loading persisted workflows
/// into runtime execution.
pub fn collect_legacy_query_payload_issues(
    definition: &WorkflowDefinition,
) -> Vec<LegacyQueryPayloadIssue> {
    let mut out = Vec::new();
    for transition in &definition.transitions {
        collect_legacy_query_payload_issues_in_steps(
            &transition.from,
            &transition.to,
            &transition.steps,
            "",
            &mut out,
        );
    }
    out
}

fn collect_legacy_query_payload_issues_in_steps(
    transition_from: &str,
    transition_to: &str,
    steps: &[WorkflowStep],
    path_prefix: &str,
    out: &mut Vec<LegacyQueryPayloadIssue>,
) {
    for (idx, step) in steps.iter().enumerate() {
        let step_path = if path_prefix.is_empty() {
            format!("steps[{idx}]")
        } else {
            format!("{path_prefix}.steps[{idx}]")
        };

        match step {
            WorkflowStep::Query { cmd_json, .. } => {
                if !is_current_workflow_query_wire(cmd_json) {
                    out.push(LegacyQueryPayloadIssue {
                        transition_from: transition_from.to_string(),
                        transition_to: transition_to.to_string(),
                        step_path,
                        payload_preview: summarize_payload_preview(cmd_json),
                    });
                }
            }
            WorkflowStep::Wait { on_timeout, .. } => {
                collect_legacy_query_payload_issues_in_steps(
                    transition_from,
                    transition_to,
                    on_timeout,
                    &format!("{step_path}.on_timeout"),
                    out,
                );
            }
            WorkflowStep::Branch {
                branches, default, ..
            } => {
                for (branch_idx, (branch_value, branch_steps)) in branches.iter().enumerate() {
                    collect_legacy_query_payload_issues_in_steps(
                        transition_from,
                        transition_to,
                        branch_steps,
                        &format!("{step_path}.branches[{branch_idx}:{branch_value}]"),
                        out,
                    );
                }
                collect_legacy_query_payload_issues_in_steps(
                    transition_from,
                    transition_to,
                    default,
                    &format!("{step_path}.default"),
                    out,
                );
            }
            WorkflowStep::ForEach { steps: nested, .. } => {
                collect_legacy_query_payload_issues_in_steps(
                    transition_from,
                    transition_to,
                    nested,
                    &format!("{step_path}.for_each"),
                    out,
                );
            }
            WorkflowStep::Notify { .. }
            | WorkflowStep::Transition { .. }
            | WorkflowStep::Log { .. }
            | WorkflowStep::Charge { .. } => {}
        }
    }
}

fn summarize_payload_preview(cmd_json: &str) -> String {
    const MAX_CHARS: usize = 64;
    let escaped = cmd_json.replace('\n', "\\n");
    let mut preview = escaped.chars().take(MAX_CHARS).collect::<String>();
    if escaped.chars().count() > MAX_CHARS {
        preview.push_str("...");
    }
    preview
}

fn is_current_workflow_query_wire(cmd_json: &str) -> bool {
    cmd_json.starts_with(WORKFLOW_QUERY_WIRE_MAGIC)
}

/// Errors that can occur during workflow execution.
#[derive(Debug)]
pub enum WorkflowError {
    /// Query execution failed
    QueryFailed(String),
    /// Notification delivery failed
    NotifyFailed(String),
    /// No transition found for current state
    NoTransition {
        /// Current state that has no outgoing transition.
        state: String,
        /// Workflow definition name.
        workflow: String,
    },
    /// Context key not found
    MissingContextKey(String),
    /// Workflow has reached a terminal state
    AlreadyTerminal(String),
    /// State persistence failed
    PersistenceFailed(String),
    /// Timeout reached while waiting
    Timeout {
        /// Name of the event that timed out.
        event: String,
    },
    /// Payment charge creation failed
    ChargeFailed(String),
    /// Generic error
    Other(String),
}

impl std::fmt::Display for WorkflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkflowError::QueryFailed(e) => write!(f, "Query failed: {}", e),
            WorkflowError::NotifyFailed(e) => write!(f, "Notification failed: {}", e),
            WorkflowError::NoTransition { state, workflow } => {
                write!(
                    f,
                    "No transition from '{}' in workflow '{}'",
                    state, workflow
                )
            }
            WorkflowError::MissingContextKey(k) => write!(f, "Context key not found: {}", k),
            WorkflowError::AlreadyTerminal(s) => write!(f, "Workflow already terminal: {}", s),
            WorkflowError::PersistenceFailed(e) => write!(f, "Persistence failed: {}", e),
            WorkflowError::Timeout { event } => write!(f, "Timeout waiting for: {}", event),
            WorkflowError::ChargeFailed(e) => write!(f, "Charge failed: {}", e),
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
///         let cmd: Qail = qail_core::wire::decode_cmd_text(cmd_json)
///             .map_err(WorkflowError::QueryFailed)?;
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
    /// Execute a QAIL query (QAIL wire text) and return results.
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
    async fn load_state(&self, workflow_id: &str)
    -> Result<Option<WorkflowContext>, WorkflowError>;

    /// Create a payment charge via the appropriate provider.
    ///
    /// The engine resolves the charge parameters from context and
    /// delegates to the provider matching `provider_kind`.
    /// Implementations should look up the registered `PaymentProvider`
    /// and call `create_charge()` on it.
    async fn create_charge(
        &self,
        provider: &PaymentKind,
        request: ChargeRequest,
    ) -> Result<ChargeResponse, WorkflowError>;
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
                let cmd_wire = normalize_query_wire_for_execution(cmd_json)?;
                let result = executor.execute_query(&cmd_wire).await?;
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
                let value = ctx.get_str(condition_key).unwrap_or("").to_string();

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

            WorkflowStep::Charge {
                provider,
                amount_key,
                reference_key,
                description_key,
                payment_method_key,
                store_as,
            } => {
                // Resolve amount from context (supports i64 or f64)
                let amount = ctx
                    .get(amount_key)
                    .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)))
                    .ok_or_else(|| {
                        WorkflowError::MissingContextKey(format!(
                            "{} (expected numeric amount)",
                            amount_key
                        ))
                    })?;

                let reference_id = ctx
                    .get_str(reference_key)
                    .ok_or_else(|| WorkflowError::MissingContextKey(reference_key.clone()))?
                    .to_string();

                let description = description_key
                    .as_ref()
                    .and_then(|k| ctx.get_str(k))
                    .map(String::from);

                let payment_method = payment_method_key
                    .as_ref()
                    .and_then(|k| ctx.get_str(k))
                    .map(String::from);

                let request = ChargeRequest {
                    amount,
                    currency: Currency::default(),
                    reference_id,
                    description,
                    payment_method,
                    return_url: None,
                    metadata: None,
                };

                let response = executor.create_charge(provider, request).await?;

                if let Some(key) = store_as {
                    let response_json = serde_json::to_value(&response)
                        .map_err(|e| WorkflowError::Other(e.to_string()))?;
                    ctx.set(key, response_json);
                }
            }
        }

        Ok(())
    })
}

fn normalize_query_wire_for_execution(cmd_json: &str) -> Result<String, WorkflowError> {
    if !is_current_workflow_query_wire(cmd_json) {
        return Err(WorkflowError::QueryFailed(
            "Legacy workflow query payload detected: cmd_json must use QAIL wire text \
             (QAIL-CMD/1). Migrate persisted workflow rows to wire text or purge/restart pending workflows."
                .to_string(),
        ));
    }

    let cmd = qail_core::wire::decode_cmd_text(cmd_json).map_err(|e| {
        WorkflowError::QueryFailed(format!(
            "Invalid workflow query wire payload (expected QAIL-CMD/1): {}",
            e
        ))
    })?;

    // Canonicalize payload before handing it to the executor.
    Ok(qail_core::wire::encode_cmd_text(&cmd))
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
    use crate::payment::{ChargeResponse, ChargeStatus, PaymentKind};

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

        async fn create_charge(
            &self,
            _provider: &PaymentKind,
            request: ChargeRequest,
        ) -> Result<ChargeResponse, WorkflowError> {
            Ok(ChargeResponse {
                charge_id: format!("mock-charge-{}", request.reference_id),
                status: ChargeStatus::Pending,
                redirect_url: None,
                qr_code: Some("00020101021226610014ID.CO.MOCK".into()),
                payment_code: None,
                expires_at: Some("2026-02-13T16:00:00Z".into()),
                raw: None,
            })
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

    #[tokio::test]
    async fn test_workflow_with_charge() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("booking_payment")
            .initial_state("created")
            .transition(
                "created",
                "awaiting_payment",
                vec![
                    WorkflowStep::charge(
                        PaymentKind::Xendit,
                        "order.total",
                        "order.id",
                        Some("charge"),
                    ),
                    WorkflowStep::wait("payment.success", std::time::Duration::from_secs(3600)),
                ],
            )
            .transition(
                "awaiting_payment",
                "confirmed",
                vec![
                    WorkflowStep::notify(ChannelKind::Email, "booking_confirmed", "customer.email"),
                    WorkflowStep::transition("confirmed"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-payment-001", "created");
        ctx.set(
            "order",
            serde_json::json!({
                "id": "booking-789",
                "total": 150000
            }),
        );
        ctx.set(
            "customer",
            serde_json::json!({
                "email": "guest@example.com"
            }),
        );

        let result = run_workflow(&executor, &wf, &mut ctx).await.unwrap();

        // Should pause at Wait step (awaiting_payment webhook)
        assert_eq!(result, "created");

        // Verify charge was stored in context
        let charge = ctx.get("charge").expect("charge should be in context");
        assert_eq!(
            charge.get("charge_id").and_then(|v| v.as_str()),
            Some("mock-charge-booking-789")
        );
        assert_eq!(
            charge.get("status").and_then(|v| v.as_str()),
            Some("Pending")
        );
        assert!(charge.get("qr_code").is_some());
    }

    #[tokio::test]
    async fn test_query_step_rejects_legacy_non_wire_payload() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("legacy_query")
            .initial_state("start")
            .transition(
                "start",
                "done",
                vec![
                    WorkflowStep::Query {
                        cmd_json: "get users limit 1".to_string(),
                        store_as: Some("rows".to_string()),
                    },
                    WorkflowStep::transition("done"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-legacy-query-001", "start");
        let err = run_workflow(&executor, &wf, &mut ctx)
            .await
            .expect_err("legacy non-wire query payload must fail");

        match err {
            WorkflowError::QueryFailed(msg) => {
                assert!(
                    msg.contains("QAIL-CMD/1"),
                    "error should mention required wire magic"
                );
                assert!(
                    msg.contains("purge/restart pending workflows"),
                    "error should include cutover guidance"
                );
            }
            other => panic!("expected QueryFailed, got: {other}"),
        }

        assert!(
            executor.queries.lock().unwrap().is_empty(),
            "legacy payload must fail before executor query is invoked"
        );
    }

    #[tokio::test]
    async fn test_query_step_accepts_wire_payload_and_executes() {
        let executor = MockExecutor::new();
        let cmd = qail_core::Qail::get("users").columns(["id"]).limit(1);
        let wire = qail_core::wire::encode_cmd_text(&cmd);

        let wf = WorkflowDefinition::new("wire_query")
            .initial_state("start")
            .transition(
                "start",
                "done",
                vec![
                    WorkflowStep::Query {
                        cmd_json: wire.clone(),
                        store_as: Some("rows".to_string()),
                    },
                    WorkflowStep::transition("done"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-wire-query-001", "start");
        let result = run_workflow(&executor, &wf, &mut ctx)
            .await
            .expect("wire payload should execute");
        assert_eq!(result, "done");

        let queries = executor.queries.lock().unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(
            queries[0], wire,
            "executor should receive canonical wire payload"
        );
    }

    #[test]
    fn test_collect_legacy_query_payload_issues_reports_nested_paths() {
        let wire_cmd = qail_core::wire::encode_cmd_text(&qail_core::Qail::get("users").limit(1));
        let wf = WorkflowDefinition::new("legacy_audit")
            .initial_state("start")
            .transition(
                "start",
                "done",
                vec![
                    WorkflowStep::Query {
                        cmd_json: "get users limit 1".to_string(),
                        store_as: Some("rows".to_string()),
                    },
                    WorkflowStep::Wait {
                        event: "timeout".to_string(),
                        timeout: std::time::Duration::from_secs(10),
                        on_timeout: vec![WorkflowStep::Query {
                            cmd_json: "{\"legacy\":true}".to_string(),
                            store_as: None,
                        }],
                    },
                    WorkflowStep::branch(
                        "kind",
                        vec![(
                            "wire_ok",
                            vec![WorkflowStep::Query {
                                cmd_json: wire_cmd,
                                store_as: None,
                            }],
                        )],
                        vec![WorkflowStep::ForEach {
                            list_key: "items".to_string(),
                            steps: vec![WorkflowStep::Query {
                                cmd_json: "select * from legacy".to_string(),
                                store_as: None,
                            }],
                        }],
                    ),
                ],
            );

        let issues = collect_legacy_query_payload_issues(&wf);
        assert_eq!(
            issues.len(),
            3,
            "expected all legacy payloads to be detected"
        );

        let paths: Vec<String> = issues.iter().map(|i| i.step_path.clone()).collect();
        assert!(
            paths.iter().any(|p| p == "steps[0]"),
            "top-level legacy query should be reported"
        );
        assert!(
            paths.iter().any(|p| p == "steps[1].on_timeout.steps[0]"),
            "nested wait/on_timeout legacy query should be reported"
        );
        assert!(
            paths
                .iter()
                .any(|p| p == "steps[2].default.steps[0].for_each.steps[0]"),
            "nested branch/default/foreach legacy query should be reported"
        );
    }
}
