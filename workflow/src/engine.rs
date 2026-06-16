//! Workflow engine — the executor trait and step runner.
//!
//! Consumers implement `WorkflowExecutor` to connect the engine
//! to their database driver and notification channels.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::channel::ChannelKind;
use crate::context::{
    WorkflowBranchCursorSelection, WorkflowContext, WorkflowCursor, WorkflowCursorFrame,
    WorkflowPendingWait,
};
use crate::payment::{ChargeRequest, ChargeResponse, Currency, PaymentKind};
use crate::registry::{StateTransition, WorkflowDefinition};
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum StepOutcome {
    Continue,
    Paused(StepPause),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StepPause {
    frames: Vec<WorkflowCursorFrame>,
    wait: WorkflowPendingWait,
}

#[derive(Debug, Clone)]
enum StepListCursorKind {
    Steps,
    Branch {
        selection: WorkflowBranchCursorSelection,
    },
    ForEach {
        item_index: usize,
        item: Value,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunMode<'a> {
    Normal,
    EventResume { event: &'a str },
}

const FOR_EACH_ITEM_KEY: &str = "item";
const RESUME_EVENT_KEY: &str = "event";
const TIMEOUT_FALLBACK_KEY: &str = "__qail_timeout_fallback";
const RESERVED_CONTEXT_KEYS: &[&str] = &[FOR_EACH_ITEM_KEY, RESUME_EVENT_KEY, TIMEOUT_FALLBACK_KEY];

#[derive(Debug, Clone)]
struct StepExecutionScope<'a> {
    state: &'a str,
    path_prefix: Vec<WorkflowCursorFrame>,
    list_kind: StepListCursorKind,
    checkpoint_steps: bool,
    checkpoint_timeout: bool,
}

impl<'a> StepExecutionScope<'a> {
    fn new(state: &'a str, list_kind: StepListCursorKind, checkpoint_steps: bool) -> Self {
        Self {
            state,
            path_prefix: Vec::new(),
            list_kind,
            checkpoint_steps,
            checkpoint_timeout: false,
        }
    }

    fn with_timeout_checkpoint(mut self) -> Self {
        self.checkpoint_timeout = true;
        self
    }

    fn child(&self, step_index: usize, list_kind: StepListCursorKind) -> Self {
        Self {
            state: self.state,
            path_prefix: cursor_frames_for_index(&self.path_prefix, &self.list_kind, step_index),
            list_kind,
            checkpoint_steps: self.checkpoint_steps,
            checkpoint_timeout: self.checkpoint_timeout,
        }
    }
}

fn invalid_cursor(message: impl Into<String>) -> WorkflowError {
    WorkflowError::Other(format!(
        "Invalid workflow resume cursor: {}",
        message.into()
    ))
}

fn set_timeout_fallback(
    ctx: &mut WorkflowContext,
    wait: &WorkflowPendingWait,
) -> Result<(), WorkflowError> {
    let value = serde_json::to_value(wait)
        .map_err(|e| WorkflowError::Other(format!("Failed to serialize timeout cursor: {e}")))?;
    ctx.set(TIMEOUT_FALLBACK_KEY, value);
    Ok(())
}

fn timeout_fallback_from_context(
    ctx: &WorkflowContext,
) -> Result<Option<WorkflowPendingWait>, WorkflowError> {
    let Some(value) = ctx.get(TIMEOUT_FALLBACK_KEY) else {
        return Ok(None);
    };
    serde_json::from_value(value.clone())
        .map(Some)
        .map_err(|e| invalid_cursor(format!("timeout fallback metadata is invalid: {e}")))
}

fn clear_timeout_fallback(ctx: &mut WorkflowContext) {
    if ctx.data.remove(TIMEOUT_FALLBACK_KEY).is_some() {
        ctx.updated_at = Utc::now();
    }
}

fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn ensure_no_child_cursor(
    step_name: &str,
    cursor_frames: &[WorkflowCursorFrame],
) -> Result<(), WorkflowError> {
    if cursor_frames.is_empty() {
        Ok(())
    } else {
        Err(invalid_cursor(format!(
            "{step_name} cannot resume nested cursor frames"
        )))
    }
}

fn ensure_step_position(
    step: &WorkflowStep,
    idx: usize,
    step_count: usize,
) -> Result<(), WorkflowError> {
    if matches!(step, WorkflowStep::Transition { .. }) && idx + 1 < step_count {
        return Err(WorkflowError::Other(
            "Transition steps must be the final step in their block; move follow-up work before the Transition or into the target state's transition".to_string(),
        ));
    }
    Ok(())
}

fn ensure_wait_event_name(event: &str) -> Result<(), WorkflowError> {
    if event.trim().is_empty() {
        return Err(WorkflowError::Other(
            "Wait event name must not be empty".to_string(),
        ));
    }
    if event.trim() != event {
        return Err(WorkflowError::Other(
            "Wait event name must not have leading or trailing whitespace".to_string(),
        ));
    }
    Ok(())
}

fn ensure_user_context_key(key: &str, usage: &str) -> Result<(), WorkflowError> {
    if RESERVED_CONTEXT_KEYS.contains(&key) {
        return Err(WorkflowError::Other(format!(
            "Workflow {usage} uses reserved context key '{key}'"
        )));
    }
    Ok(())
}

fn ensure_optional_user_context_key(
    key: Option<&String>,
    usage: &str,
) -> Result<(), WorkflowError> {
    if let Some(key) = key {
        ensure_user_context_key(key, usage)?;
    }
    Ok(())
}

fn ensure_cursor_has_frames(cursor: &WorkflowCursor) -> Result<(), WorkflowError> {
    if cursor.frames.is_empty() {
        return Err(invalid_cursor("resume cursor has no frames"));
    }
    Ok(())
}

fn ensure_unique_branch_values(
    condition_key: &str,
    branches: &[(String, Vec<WorkflowStep>)],
) -> Result<(), WorkflowError> {
    let mut seen = std::collections::HashSet::new();
    for (branch_value, _) in branches {
        if !seen.insert(branch_value.as_str()) {
            return Err(WorkflowError::Other(format!(
                "Ambiguous workflow branch for '{condition_key}': duplicate value '{branch_value}'"
            )));
        }
    }
    Ok(())
}

fn find_single_transition<'a>(
    definition: &'a WorkflowDefinition,
    state: &str,
) -> Result<Option<&'a StateTransition>, WorkflowError> {
    let mut matches = definition.transitions.iter().filter(|t| t.from == state);
    let Some(first) = matches.next() else {
        return Ok(None);
    };
    if matches.next().is_some() {
        let count = definition.transitions_from(state).len();
        return Err(WorkflowError::Other(format!(
            "Ambiguous workflow definition '{}': state '{state}' has {count} outgoing transitions; use a Branch step or split states explicitly",
            definition.name
        )));
    }
    Ok(Some(first))
}

fn validate_workflow_definition(definition: &WorkflowDefinition) -> Result<(), WorkflowError> {
    let mut seen_states = std::collections::HashMap::<&str, usize>::new();
    for transition in &definition.transitions {
        *seen_states.entry(&transition.from).or_default() += 1;
        validate_workflow_steps(&transition.steps)?;
    }

    if let Some((state, count)) = seen_states.into_iter().find(|(_, count)| *count > 1) {
        return Err(WorkflowError::Other(format!(
            "Ambiguous workflow definition '{}': state '{state}' has {count} outgoing transitions; use a Branch step or split states explicitly",
            definition.name
        )));
    }

    Ok(())
}

fn validate_workflow_steps(steps: &[WorkflowStep]) -> Result<(), WorkflowError> {
    for (idx, step) in steps.iter().enumerate() {
        ensure_step_position(step, idx, steps.len())?;
        match step {
            WorkflowStep::Query { cmd_json, store_as } => {
                normalize_query_wire_for_execution(cmd_json)?;
                ensure_optional_user_context_key(store_as.as_ref(), "Query store_as")?;
            }
            WorkflowStep::Wait {
                event, on_timeout, ..
            } => {
                ensure_wait_event_name(event)?;
                if steps_contain_wait(on_timeout) {
                    return Err(WorkflowError::Other(
                        "Wait steps inside on_timeout fallback are not supported".to_string(),
                    ));
                }
                validate_workflow_steps(on_timeout)?;
            }
            WorkflowStep::Branch {
                condition_key,
                branches,
                default,
            } => {
                ensure_unique_branch_values(condition_key, branches)?;
                for (_, branch_steps) in branches {
                    validate_workflow_steps(branch_steps)?;
                }
                validate_workflow_steps(default)?;
            }
            WorkflowStep::ForEach { steps, .. } => {
                validate_workflow_steps(steps)?;
            }
            WorkflowStep::Charge { store_as, .. } => {
                ensure_optional_user_context_key(store_as.as_ref(), "Charge store_as")?;
            }
            WorkflowStep::Notify { .. }
            | WorkflowStep::Transition { .. }
            | WorkflowStep::Log { .. } => {}
        }
    }

    Ok(())
}

fn deadline_from_timeout(timeout: &std::time::Duration) -> Result<DateTime<Utc>, WorkflowError> {
    let timeout = chrono::Duration::from_std(*timeout).map_err(|_| {
        WorkflowError::Other("Wait timeout duration is too large to represent".to_string())
    })?;
    Utc::now()
        .checked_add_signed(timeout)
        .ok_or_else(|| WorkflowError::Other("Wait timeout deadline overflowed".to_string()))
}

fn cursor_frame_for_index(kind: &StepListCursorKind, index: usize) -> WorkflowCursorFrame {
    match kind {
        StepListCursorKind::Steps => WorkflowCursorFrame::Steps { index },
        StepListCursorKind::Branch { selection } => WorkflowCursorFrame::Branch {
            selection: selection.clone(),
            index,
        },
        StepListCursorKind::ForEach { item_index, item } => WorkflowCursorFrame::ForEachItem {
            item_index: *item_index,
            index,
            item: item.clone(),
        },
    }
}

fn cursor_frames_for_index(
    path_prefix: &[WorkflowCursorFrame],
    kind: &StepListCursorKind,
    index: usize,
) -> Vec<WorkflowCursorFrame> {
    let mut frames = Vec::with_capacity(path_prefix.len() + 1);
    frames.extend_from_slice(path_prefix);
    frames.push(cursor_frame_for_index(kind, index));
    frames
}

async fn checkpoint_cursor<E: WorkflowExecutor>(
    executor: &E,
    ctx: &mut WorkflowContext,
    state: &str,
    frames: Vec<WorkflowCursorFrame>,
    wait: Option<WorkflowPendingWait>,
    timeout_fallback: bool,
) -> Result<(), WorkflowError> {
    if !timeout_fallback {
        clear_timeout_fallback(ctx);
    }
    if ctx.current_state == state {
        ctx.set_cursor(WorkflowCursor {
            state: state.to_string(),
            frames,
            wait,
        });
    } else {
        ctx.clear_cursor();
        if timeout_fallback {
            clear_timeout_fallback(ctx);
        }
    }
    executor.save_state(ctx).await
}

async fn checkpoint_completed_step<E: WorkflowExecutor>(
    executor: &E,
    ctx: &mut WorkflowContext,
    state: &str,
    path_prefix: &[WorkflowCursorFrame],
    kind: &StepListCursorKind,
    next_index: usize,
    timeout_fallback: bool,
) -> Result<(), WorkflowError> {
    let frames = cursor_frames_for_index(path_prefix, kind, next_index);
    checkpoint_cursor(executor, ctx, state, frames, None, timeout_fallback).await
}

fn restore_for_each_item(ctx: &mut WorkflowContext, previous_item: Option<Value>) {
    match previous_item {
        Some(item) => {
            ctx.data.insert(FOR_EACH_ITEM_KEY.to_string(), item);
        }
        None => {
            ctx.data.remove(FOR_EACH_ITEM_KEY);
        }
    }
    ctx.updated_at = Utc::now();
}

fn log_value_to_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        _ => value.to_string(),
    }
}

fn resolve_log_message(ctx: &WorkflowContext, message: &str) -> String {
    let mut resolved = String::with_capacity(message.len());
    let mut rest = message;

    while let Some(start) = rest.find('{') {
        resolved.push_str(&rest[..start]);
        let after_open = &rest[start + 1..];
        let Some(end) = after_open.find('}') else {
            resolved.push_str(&rest[start..]);
            return resolved;
        };

        let key = &after_open[..end];
        if !key.is_empty()
            && let Some(value) = ctx.get(key)
        {
            resolved.push_str(&log_value_to_string(value));
        } else {
            resolved.push('{');
            resolved.push_str(key);
            resolved.push('}');
        }
        rest = &after_open[end + 1..];
    }

    resolved.push_str(rest);
    resolved
}

fn execute_steps<'a, E: WorkflowExecutor>(
    executor: &'a E,
    steps: &'a [WorkflowStep],
    ctx: &'a mut WorkflowContext,
    start_index: usize,
    cursor_frames: &'a [WorkflowCursorFrame],
    scope: StepExecutionScope<'a>,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Option<StepPause>, WorkflowError>> + Send + 'a>,
> {
    Box::pin(async move {
        if start_index > steps.len() {
            return Err(invalid_cursor(format!(
                "step index {start_index} is past step count {}",
                steps.len()
            )));
        }
        if start_index == steps.len() && !cursor_frames.is_empty() {
            return Err(invalid_cursor(
                "cursor contains child frames after the step list ended",
            ));
        }

        for (idx, step) in steps.iter().enumerate().skip(start_index) {
            ensure_step_position(step, idx, steps.len())?;
            let step_cursor = if idx == start_index {
                cursor_frames
            } else {
                &[]
            };
            match execute_step(executor, step, ctx, idx, step_cursor, &scope).await? {
                StepOutcome::Continue => {
                    if scope.checkpoint_steps {
                        checkpoint_completed_step(
                            executor,
                            ctx,
                            scope.state,
                            &scope.path_prefix,
                            &scope.list_kind,
                            idx + 1,
                            scope.checkpoint_timeout,
                        )
                        .await?;
                    }
                    if ctx.current_state != scope.state {
                        return Ok(None);
                    }
                }
                StepOutcome::Paused(pause) => return Ok(Some(pause)),
            }
        }

        Ok(None)
    })
}

fn selected_branch_steps<'a>(
    branches: &'a [(String, Vec<WorkflowStep>)],
    default: &'a [WorkflowStep],
    selection: &WorkflowBranchCursorSelection,
) -> Result<&'a [WorkflowStep], WorkflowError> {
    match selection {
        WorkflowBranchCursorSelection::Branch(idx) => branches
            .get(*idx)
            .map(|(_, steps)| steps.as_slice())
            .ok_or_else(|| invalid_cursor(format!("branch index {idx} no longer exists"))),
        WorkflowBranchCursorSelection::Default => Ok(default),
    }
}

fn validate_branch_resume_selection(
    branches: &[(String, Vec<WorkflowStep>)],
    selection: &WorkflowBranchCursorSelection,
    condition_key: &str,
    condition_value: &str,
) -> Result<(), WorkflowError> {
    match selection {
        WorkflowBranchCursorSelection::Branch(idx) => {
            let Some((branch_value, _)) = branches.get(*idx) else {
                return Err(invalid_cursor(format!(
                    "branch index {idx} no longer exists"
                )));
            };
            if branch_value != condition_value {
                return Err(invalid_cursor(format!(
                    "branch cursor selected index {idx} for value '{branch_value}', \
                     but current condition '{condition_key}' is '{condition_value}'"
                )));
            }
        }
        WorkflowBranchCursorSelection::Default => {
            if branches
                .iter()
                .any(|(branch_value, _)| branch_value == condition_value)
            {
                return Err(invalid_cursor(format!(
                    "default branch cursor no longer matches condition '{condition_key}' \
                     value '{condition_value}'"
                )));
            }
        }
    }
    Ok(())
}

/// Execute a single workflow step.
fn execute_step<'a, E: WorkflowExecutor>(
    executor: &'a E,
    step: &'a WorkflowStep,
    ctx: &'a mut WorkflowContext,
    step_index: usize,
    cursor_frames: &'a [WorkflowCursorFrame],
    scope: &'a StepExecutionScope<'a>,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<StepOutcome, WorkflowError>> + Send + 'a>,
> {
    Box::pin(async move {
        match step {
            WorkflowStep::Query { cmd_json, store_as } => {
                ensure_no_child_cursor("Query", cursor_frames)?;
                ensure_optional_user_context_key(store_as.as_ref(), "Query store_as")?;
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
                ensure_no_child_cursor("Notify", cursor_frames)?;
                let recipient = ctx
                    .get_str(recipient_key)
                    .ok_or_else(|| WorkflowError::MissingContextKey(recipient_key.clone()))?
                    .to_string();

                let params = payload_key
                    .as_ref()
                    .map(|key| {
                        ctx.get(key)
                            .cloned()
                            .ok_or_else(|| WorkflowError::MissingContextKey(key.clone()))
                    })
                    .transpose()?
                    .unwrap_or(Value::Object(serde_json::Map::new()));

                executor
                    .send_notification(channel, &recipient, template, &params)
                    .await?;
            }

            WorkflowStep::Wait {
                event,
                timeout,
                on_timeout,
            } => {
                ensure_no_child_cursor("Wait", cursor_frames)?;
                ensure_wait_event_name(event)?;
                // Wait steps are handled by the outer runner so state can be
                // persisted before returning to the caller.
                return Ok(StepOutcome::Paused(StepPause {
                    frames: cursor_frames_for_index(
                        &scope.path_prefix,
                        &scope.list_kind,
                        step_index + 1,
                    ),
                    wait: WorkflowPendingWait {
                        event: event.clone(),
                        deadline_at: deadline_from_timeout(timeout)?,
                        on_timeout: on_timeout.clone(),
                    },
                }));
            }

            WorkflowStep::Branch {
                condition_key,
                branches,
                default,
            } => {
                ensure_unique_branch_values(condition_key, branches)?;
                let (selection, selected_steps, start_index, nested_cursor) = match cursor_frames
                    .first()
                {
                    Some(WorkflowCursorFrame::Branch { selection, index }) => {
                        let condition_value = ctx
                            .get_str(condition_key)
                            .ok_or_else(|| WorkflowError::MissingContextKey(condition_key.clone()))?
                            .to_string();
                        validate_branch_resume_selection(
                            branches,
                            selection,
                            condition_key,
                            &condition_value,
                        )?;
                        (
                            selection.clone(),
                            selected_branch_steps(branches, default, selection)?,
                            *index,
                            &cursor_frames[1..],
                        )
                    }
                    Some(_) => {
                        return Err(invalid_cursor(
                            "expected Branch frame for nested branch resume",
                        ));
                    }
                    None => {
                        let value = ctx
                            .get_str(condition_key)
                            .ok_or_else(|| WorkflowError::MissingContextKey(condition_key.clone()))?
                            .to_string();
                        match branches.iter().enumerate().find(|(_, (k, _))| k == &value) {
                            Some((idx, (_, steps))) => (
                                WorkflowBranchCursorSelection::Branch(idx),
                                steps.as_slice(),
                                0,
                                &[][..],
                            ),
                            None => (
                                WorkflowBranchCursorSelection::Default,
                                default.as_slice(),
                                0,
                                &[][..],
                            ),
                        }
                    }
                };

                if let Some(pause) = execute_steps(
                    executor,
                    selected_steps,
                    ctx,
                    start_index,
                    nested_cursor,
                    scope.child(step_index, StepListCursorKind::Branch { selection }),
                )
                .await?
                {
                    return Ok(StepOutcome::Paused(pause));
                }
            }

            WorkflowStep::ForEach { list_key, steps } => {
                let list = ctx
                    .get(list_key)
                    .cloned()
                    .ok_or_else(|| WorkflowError::MissingContextKey(list_key.clone()))?;

                let Value::Array(items) = list else {
                    return Err(WorkflowError::Other(format!(
                        "Expected JSON array for ForEach list '{}', got {}",
                        list_key,
                        json_type_name(&list)
                    )));
                };

                let (start_item_index, start_step_index, nested_cursor) = match cursor_frames
                    .first()
                {
                    Some(WorkflowCursorFrame::ForEachItem {
                        item_index,
                        index,
                        item,
                    }) => {
                        if *item_index >= items.len() {
                            return Err(invalid_cursor(format!(
                                "for_each item index {item_index} is past item count {}",
                                items.len()
                            )));
                        }
                        if items.get(*item_index) != Some(item) {
                            return Err(invalid_cursor(format!(
                                "for_each cursor item at index {item_index} changed while the workflow was paused"
                            )));
                        }
                        (*item_index, *index, &cursor_frames[1..])
                    }
                    Some(WorkflowCursorFrame::ForEach { item_index, index }) => {
                        if *item_index >= items.len() {
                            return Err(invalid_cursor(format!(
                                "for_each item index {item_index} is past item count {}",
                                items.len()
                            )));
                        }
                        (*item_index, *index, &cursor_frames[1..])
                    }
                    Some(_) => {
                        return Err(invalid_cursor(
                            "expected ForEach frame for nested loop resume",
                        ));
                    }
                    None => (0, 0, &[][..]),
                };

                for (item_index, item) in items.into_iter().enumerate().skip(start_item_index) {
                    let item_snapshot = item.clone();
                    let previous_item = ctx.data.insert(FOR_EACH_ITEM_KEY.to_string(), item);
                    ctx.updated_at = Utc::now();
                    let item_step_start = if item_index == start_item_index {
                        start_step_index
                    } else {
                        0
                    };
                    let item_cursor = if item_index == start_item_index {
                        nested_cursor
                    } else {
                        &[]
                    };

                    match execute_steps(
                        executor,
                        steps,
                        ctx,
                        item_step_start,
                        item_cursor,
                        scope.child(
                            step_index,
                            StepListCursorKind::ForEach {
                                item_index,
                                item: item_snapshot,
                            },
                        ),
                    )
                    .await
                    {
                        Ok(Some(pause)) => {
                            restore_for_each_item(ctx, previous_item);
                            return Ok(StepOutcome::Paused(pause));
                        }
                        Ok(None) => {
                            restore_for_each_item(ctx, previous_item);
                        }
                        Err(err) => {
                            restore_for_each_item(ctx, previous_item);
                            return Err(err);
                        }
                    }
                }
            }

            WorkflowStep::Transition { to } => {
                ensure_no_child_cursor("Transition", cursor_frames)?;
                ctx.transition_to(to, None);
            }

            WorkflowStep::Log { message } => {
                ensure_no_child_cursor("Log", cursor_frames)?;
                let resolved = resolve_log_message(ctx, message);
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
                ensure_no_child_cursor("Charge", cursor_frames)?;
                ensure_optional_user_context_key(store_as.as_ref(), "Charge store_as")?;
                let amount = resolve_charge_amount(ctx, amount_key)?;

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

        Ok(StepOutcome::Continue)
    })
}

fn resolve_charge_amount(ctx: &WorkflowContext, amount_key: &str) -> Result<i64, WorkflowError> {
    let value = ctx.get(amount_key).ok_or_else(|| {
        WorkflowError::MissingContextKey(format!("{amount_key} (expected numeric amount)"))
    })?;

    let amount = match value {
        Value::Number(number) => charge_amount_from_number(number, amount_key)?,
        _ => {
            return Err(WorkflowError::MissingContextKey(format!(
                "{amount_key} (expected numeric amount)"
            )));
        }
    };

    if amount <= 0 {
        return Err(invalid_charge_amount(
            amount_key,
            "amount must be greater than zero",
        ));
    }

    Ok(amount)
}

fn charge_amount_from_number(
    number: &serde_json::Number,
    amount_key: &str,
) -> Result<i64, WorkflowError> {
    if let Some(value) = number.as_i64() {
        return Ok(value);
    }

    if let Some(value) = number.as_u64() {
        return i64::try_from(value).map_err(|_| {
            invalid_charge_amount(amount_key, "integer amount must fit in signed 64-bit range")
        });
    }

    if let Some(value) = number.as_f64() {
        const MAX_SAFE_JSON_INTEGER: f64 = 9_007_199_254_740_991.0;

        if !value.is_finite() {
            return Err(invalid_charge_amount(
                amount_key,
                "float amount must be finite",
            ));
        }

        if value.fract() != 0.0 {
            return Err(invalid_charge_amount(
                amount_key,
                "float amount must be an integer in the smallest currency unit",
            ));
        }

        if !(-MAX_SAFE_JSON_INTEGER..=MAX_SAFE_JSON_INTEGER).contains(&value) {
            return Err(invalid_charge_amount(
                amount_key,
                "float amount exceeds JSON safe integer range",
            ));
        }

        return Ok(value as i64);
    }

    Err(invalid_charge_amount(
        amount_key,
        "amount number is not representable",
    ))
}

fn invalid_charge_amount(amount_key: &str, message: &str) -> WorkflowError {
    WorkflowError::Other(format!(
        "Invalid charge amount at '{amount_key}': {message}"
    ))
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
    run_workflow_inner(executor, definition, ctx, RunMode::Normal).await
}

async fn run_workflow_inner<'a, E: WorkflowExecutor>(
    executor: &E,
    definition: &WorkflowDefinition,
    ctx: &mut WorkflowContext,
    mode: RunMode<'a>,
) -> Result<String, WorkflowError> {
    validate_workflow_definition(definition)?;
    let run_start_transition_count = ctx.transition_count();
    let mut pending_cursor_frames = match ctx.cursor.clone() {
        Some(cursor) if cursor.state == ctx.current_state => {
            ensure_cursor_has_frames(&cursor)?;
            if timeout_fallback_from_context(ctx)?.is_some() {
                return Err(WorkflowError::Other(
                    "Workflow is executing a timeout fallback; resume with timeout_workflow"
                        .to_string(),
                ));
            }
            match (&cursor.wait, mode) {
                (Some(wait), RunMode::Normal) => {
                    return Err(WorkflowError::Other(format!(
                        "Workflow is paused awaiting event '{}'; resume with a matching event",
                        wait.event
                    )));
                }
                (Some(wait), RunMode::EventResume { event }) => {
                    if wait.event != event {
                        return Err(WorkflowError::Other(format!(
                            "Workflow is waiting for event '{}', received '{}'",
                            wait.event, event
                        )));
                    }
                    if Utc::now() > wait.deadline_at {
                        return Err(WorkflowError::Timeout {
                            event: wait.event.clone(),
                        });
                    }
                    ctx.clear_cursor();
                    Some(cursor.frames)
                }
                (None, RunMode::EventResume { .. }) => {
                    return Err(WorkflowError::Other(
                        "Workflow is not waiting for an external event".to_string(),
                    ));
                }
                (None, RunMode::Normal) => {
                    ctx.clear_cursor();
                    Some(cursor.frames)
                }
            }
        }
        Some(_) => match mode {
            RunMode::Normal => {
                ctx.clear_cursor();
                None
            }
            RunMode::EventResume { .. } => {
                return Err(invalid_cursor(
                    "resume cursor state does not match current workflow state",
                ));
            }
        },
        None => match mode {
            RunMode::Normal => None,
            RunMode::EventResume { .. } => {
                return Err(WorkflowError::Other(
                    "Workflow is not waiting for an external event".to_string(),
                ));
            }
        },
    };

    loop {
        // Find transition from current state
        let transition = find_single_transition(definition, &ctx.current_state)?;

        let transition = match transition {
            Some(t) => t,
            None => {
                // No transition = terminal state or waiting
                executor.save_state(ctx).await?;
                return Ok(ctx.current_state.clone());
            }
        };

        // Safety: prevent infinite loops before executing the next transition.
        if ctx
            .transition_count()
            .saturating_sub(run_start_transition_count)
            >= 50
        {
            return Err(WorkflowError::Other(
                "Maximum transition count exceeded (50)".into(),
            ));
        }

        let cursor_frames = pending_cursor_frames.take().unwrap_or_default();
        let (start_index, nested_cursor) = match cursor_frames.first() {
            Some(WorkflowCursorFrame::Steps { index }) => (*index, &cursor_frames[1..]),
            Some(_) => {
                return Err(invalid_cursor(
                    "top-level transition resume must start with a Steps frame",
                ));
            }
            None => (0, &[][..]),
        };

        if let Some(pause) = execute_steps(
            executor,
            &transition.steps,
            ctx,
            start_index,
            nested_cursor,
            StepExecutionScope::new(&transition.from, StepListCursorKind::Steps, true),
        )
        .await?
        {
            clear_timeout_fallback(ctx);
            ctx.set_cursor(WorkflowCursor {
                state: transition.from.clone(),
                frames: pause.frames,
                wait: Some(pause.wait),
            });
            executor.save_state(ctx).await?;
            return Ok(ctx.current_state.clone());
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
    let event_name = extract_resume_event_name(&event_data)?;
    resume_workflow_with_event(executor, definition, workflow_id, &event_name, event_data).await
}

/// Resume a workflow after a named Wait event was received.
pub async fn resume_workflow_with_event<E: WorkflowExecutor>(
    executor: &E,
    definition: &WorkflowDefinition,
    workflow_id: &str,
    event_name: &str,
    event_data: Value,
) -> Result<String, WorkflowError> {
    ensure_wait_event_name(event_name)?;

    // Load persisted state
    let mut ctx = executor
        .load_state(workflow_id)
        .await?
        .ok_or_else(|| WorkflowError::Other(format!("Workflow not found: {}", workflow_id)))?;

    // Store the event data in context
    ctx.set(RESUME_EVENT_KEY, event_data);

    // Continue running from current state
    run_workflow_inner(
        executor,
        definition,
        &mut ctx,
        RunMode::EventResume { event: event_name },
    )
    .await
}

/// Execute the timeout fallback for a workflow currently paused at a Wait step.
pub async fn timeout_workflow<E: WorkflowExecutor>(
    executor: &E,
    definition: &WorkflowDefinition,
    workflow_id: &str,
) -> Result<String, WorkflowError> {
    validate_workflow_definition(definition)?;
    let mut ctx = executor
        .load_state(workflow_id)
        .await?
        .ok_or_else(|| WorkflowError::Other(format!("Workflow not found: {}", workflow_id)))?;

    let cursor = ctx
        .cursor
        .clone()
        .ok_or_else(|| WorkflowError::Other("Workflow is not paused at a Wait step".to_string()))?;
    ensure_cursor_has_frames(&cursor)?;
    if cursor.state != ctx.current_state {
        return Err(invalid_cursor(
            "timeout cursor state does not match current workflow state",
        ));
    }
    let timeout_fallback = timeout_fallback_from_context(&ctx)?;
    if cursor.wait.is_some() && timeout_fallback.is_some() {
        return Err(invalid_cursor(
            "timeout cursor cannot also be waiting for an event",
        ));
    }

    let (wait, cursor_frames) = if let Some(timeout) = timeout_fallback {
        (timeout, cursor.frames.clone())
    } else {
        let wait = cursor.wait.clone().ok_or_else(|| {
            WorkflowError::Other("Workflow is not waiting for a timeout".to_string())
        })?;
        if Utc::now() < wait.deadline_at {
            return Err(WorkflowError::Other(format!(
                "Workflow wait for event '{}' has not timed out",
                wait.event
            )));
        }
        (wait, Vec::new())
    };

    if wait.on_timeout.is_empty() {
        return Err(WorkflowError::Timeout { event: wait.event });
    }
    if steps_contain_wait(&wait.on_timeout) {
        return Err(WorkflowError::Other(
            "Wait steps inside on_timeout fallback are not supported".to_string(),
        ));
    }

    set_timeout_fallback(&mut ctx, &wait)?;
    ctx.clear_cursor();
    ctx.set(
        RESUME_EVENT_KEY,
        serde_json::json!({
            "event": wait.event.clone(),
            "timeout": true,
        }),
    );

    let (start_index, nested_cursor) = match cursor_frames.first() {
        Some(WorkflowCursorFrame::Steps { index }) => (*index, &cursor_frames[1..]),
        Some(_) => {
            return Err(invalid_cursor(
                "timeout fallback resume must start with a Steps frame",
            ));
        }
        None => (0, &[][..]),
    };

    if execute_steps(
        executor,
        &wait.on_timeout,
        &mut ctx,
        start_index,
        nested_cursor,
        StepExecutionScope::new(&cursor.state, StepListCursorKind::Steps, true)
            .with_timeout_checkpoint(),
    )
    .await?
    .is_some()
    {
        return Err(WorkflowError::Other(
            "on_timeout fallback paused unexpectedly".to_string(),
        ));
    }

    clear_timeout_fallback(&mut ctx);
    ctx.clear_cursor();
    if ctx.current_state == cursor.state {
        executor.save_state(&ctx).await?;
        return Ok(ctx.current_state.clone());
    }

    run_workflow_inner(executor, definition, &mut ctx, RunMode::Normal).await
}

fn extract_resume_event_name(event_data: &Value) -> Result<String, WorkflowError> {
    let event = ["event", "event_name", "type"]
        .iter()
        .find_map(|key| event_data.get(*key).and_then(Value::as_str))
        .ok_or_else(|| {
            WorkflowError::Other(
                "Resume event data must include a string 'event' field".to_string(),
            )
        })?;
    ensure_wait_event_name(event)?;
    Ok(event.to_string())
}

fn steps_contain_wait(steps: &[WorkflowStep]) -> bool {
    steps.iter().any(|step| match step {
        WorkflowStep::Wait { .. } => true,
        WorkflowStep::Branch {
            branches, default, ..
        } => {
            branches
                .iter()
                .any(|(_, branch_steps)| steps_contain_wait(branch_steps))
                || steps_contain_wait(default)
        }
        WorkflowStep::ForEach { steps, .. } => steps_contain_wait(steps),
        WorkflowStep::Query { .. }
        | WorkflowStep::Notify { .. }
        | WorkflowStep::Transition { .. }
        | WorkflowStep::Log { .. }
        | WorkflowStep::Charge { .. } => false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::StateChange;
    use crate::payment::{ChargeResponse, ChargeStatus, PaymentKind};

    struct MockExecutor {
        queries: std::sync::Mutex<Vec<String>>,
        query_failures: std::sync::Mutex<std::collections::HashSet<String>>,
        notifications: std::sync::Mutex<Vec<(String, String)>>,
        charges: std::sync::Mutex<Vec<ChargeRequest>>,
        saved_state: std::sync::Mutex<Option<WorkflowContext>>,
    }

    impl MockExecutor {
        fn new() -> Self {
            Self {
                queries: std::sync::Mutex::new(Vec::new()),
                query_failures: std::sync::Mutex::new(std::collections::HashSet::new()),
                notifications: std::sync::Mutex::new(Vec::new()),
                charges: std::sync::Mutex::new(Vec::new()),
                saved_state: std::sync::Mutex::new(None),
            }
        }

        fn fail_query(&self, cmd_wire: impl Into<String>) {
            self.query_failures.lock().unwrap().insert(cmd_wire.into());
        }
    }

    fn charge_only_workflow(name: &str) -> WorkflowDefinition {
        WorkflowDefinition::new(name)
            .initial_state("created")
            .transition(
                "created",
                "awaiting_payment",
                vec![WorkflowStep::charge(
                    PaymentKind::Xendit,
                    "order.total",
                    "order.id",
                    Some("charge"),
                )],
            )
    }

    #[async_trait]
    impl WorkflowExecutor for MockExecutor {
        async fn execute_query(&self, cmd_json: &str) -> Result<Value, WorkflowError> {
            self.queries.lock().unwrap().push(cmd_json.to_string());
            if self.query_failures.lock().unwrap().contains(cmd_json) {
                return Err(WorkflowError::QueryFailed(
                    "forced query failure".to_string(),
                ));
            }
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

        async fn save_state(&self, ctx: &WorkflowContext) -> Result<(), WorkflowError> {
            *self.saved_state.lock().unwrap() = Some(ctx.clone());
            Ok(())
        }

        async fn load_state(
            &self,
            workflow_id: &str,
        ) -> Result<Option<WorkflowContext>, WorkflowError> {
            Ok(self
                .saved_state
                .lock()
                .unwrap()
                .as_ref()
                .filter(|ctx| ctx.workflow_id == workflow_id)
                .cloned())
        }

        async fn create_charge(
            &self,
            _provider: &PaymentKind,
            request: ChargeRequest,
        ) -> Result<ChargeResponse, WorkflowError> {
            self.charges.lock().unwrap().push(request.clone());
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
    async fn transition_limit_counts_only_current_run() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("aged_workflow")
            .initial_state("active")
            .transition("active", "done", vec![WorkflowStep::transition("done")]);

        let mut ctx = WorkflowContext::new("wf-aged-history", "active");
        for idx in 0..51 {
            ctx.history.push(StateChange {
                from: format!("past_{idx}"),
                to: format!("past_{}", idx + 1),
                at: Utc::now(),
                reason: None,
            });
        }

        let result = run_workflow(&executor, &wf, &mut ctx)
            .await
            .expect("old workflow history must not trip per-run loop guard");

        assert_eq!(result, "done");
        assert_eq!(ctx.transition_count(), 52);
    }

    #[tokio::test]
    async fn transition_limit_blocks_next_transition_before_side_effects() {
        let executor = MockExecutor::new();

        let mut wf = WorkflowDefinition::new("runaway_workflow").initial_state("s0");
        for idx in 0..50 {
            wf = wf.transition(
                format!("s{idx}"),
                format!("s{}", idx + 1),
                vec![WorkflowStep::transition(&format!("s{}", idx + 1))],
            );
        }
        wf = wf.transition(
            "s50",
            "s51",
            vec![
                WorkflowStep::notify(ChannelKind::Email, "runaway_side_effect", "ops.email"),
                WorkflowStep::transition("s51"),
            ],
        );

        let mut ctx = WorkflowContext::new("wf-runaway", "s0");
        ctx.set("ops", serde_json::json!({"email": "ops@example.com"}));

        let err = run_workflow(&executor, &wf, &mut ctx)
            .await
            .expect_err("the 51st transition must be blocked before side effects");

        match err {
            WorkflowError::Other(msg) => {
                assert!(
                    msg.contains("Maximum transition count exceeded"),
                    "got: {msg}"
                );
            }
            other => panic!("expected transition limit error, got {other:?}"),
        }
        assert_eq!(ctx.current_state, "s50");
        assert_eq!(ctx.transition_count(), 50);
        assert!(
            executor.notifications.lock().unwrap().is_empty(),
            "blocked transition side effects must not execute"
        );
        let saved = executor
            .saved_state
            .lock()
            .unwrap()
            .clone()
            .expect("state after the last allowed transition should be saved");
        assert_eq!(saved.current_state, "s50");
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
    async fn branch_missing_context_key_errors_instead_of_taking_default() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("branching_missing_key")
            .initial_state("pending")
            .transition(
                "pending",
                "resolved",
                vec![WorkflowStep::branch(
                    "payment.status",
                    vec![("paid", vec![WorkflowStep::transition("resolved")])],
                    vec![WorkflowStep::transition("fallback")],
                )],
            );

        let mut ctx = WorkflowContext::new("wf-missing-branch-key", "pending");
        let err = run_workflow(&executor, &wf, &mut ctx)
            .await
            .expect_err("missing branch context must not route to default");

        match err {
            WorkflowError::MissingContextKey(key) => assert_eq!(key, "payment.status"),
            other => panic!("expected MissingContextKey, got {other:?}"),
        }
        assert_eq!(ctx.current_state, "pending");
    }

    #[tokio::test]
    async fn duplicate_transitions_from_same_state_fail_before_side_effects() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("duplicate_transition_guard")
            .initial_state("active")
            .transition(
                "active",
                "vendor_a",
                vec![WorkflowStep::notify(
                    ChannelKind::WhatsApp,
                    "vendor_a",
                    "vendor.phone",
                )],
            )
            .transition(
                "active",
                "vendor_b",
                vec![WorkflowStep::notify(
                    ChannelKind::WhatsApp,
                    "vendor_b",
                    "vendor.phone",
                )],
            );

        let mut ctx = WorkflowContext::new("wf-duplicate-transition", "active");
        ctx.set("vendor", serde_json::json!({"phone": "+628111"}));

        let err = run_workflow(&executor, &wf, &mut ctx)
            .await
            .expect_err("duplicate transitions must fail closed");

        match err {
            WorkflowError::Other(msg) => {
                assert!(msg.contains("Ambiguous workflow definition"), "got: {msg}");
                assert!(msg.contains("active"), "got: {msg}");
                assert!(msg.contains("2 outgoing transitions"), "got: {msg}");
            }
            other => panic!("expected ambiguous transition error, got {other:?}"),
        }
        assert!(executor.notifications.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn duplicate_future_transitions_fail_before_current_side_effects() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("future_duplicate_transition_guard")
            .initial_state("active")
            .transition(
                "active",
                "routing",
                vec![
                    WorkflowStep::notify(ChannelKind::WhatsApp, "before_bad_state", "vendor.phone"),
                    WorkflowStep::transition("routing"),
                ],
            )
            .transition(
                "routing",
                "vendor_a",
                vec![WorkflowStep::notify(
                    ChannelKind::WhatsApp,
                    "vendor_a",
                    "vendor.phone",
                )],
            )
            .transition(
                "routing",
                "vendor_b",
                vec![WorkflowStep::notify(
                    ChannelKind::WhatsApp,
                    "vendor_b",
                    "vendor.phone",
                )],
            );

        let mut ctx = WorkflowContext::new("wf-future-duplicate-transition", "active");
        ctx.set("vendor", serde_json::json!({"phone": "+628111"}));

        let err = run_workflow(&executor, &wf, &mut ctx)
            .await
            .expect_err("future duplicate transitions must fail before current side effects");

        match err {
            WorkflowError::Other(msg) => {
                assert!(msg.contains("Ambiguous workflow definition"), "got: {msg}");
                assert!(msg.contains("routing"), "got: {msg}");
            }
            other => panic!("expected ambiguous transition error, got {other:?}"),
        }
        assert_eq!(ctx.current_state, "active");
        assert!(
            executor.notifications.lock().unwrap().is_empty(),
            "definition validation must run before the initial notification"
        );
    }

    #[tokio::test]
    async fn duplicate_branch_values_fail_before_side_effects() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("duplicate_branch_guard")
            .initial_state("active")
            .transition(
                "active",
                "done",
                vec![WorkflowStep::branch(
                    "decision",
                    vec![
                        (
                            "accepted",
                            vec![WorkflowStep::notify(
                                ChannelKind::WhatsApp,
                                "accepted_a",
                                "vendor.phone",
                            )],
                        ),
                        (
                            "accepted",
                            vec![WorkflowStep::notify(
                                ChannelKind::WhatsApp,
                                "accepted_b",
                                "vendor.phone",
                            )],
                        ),
                    ],
                    vec![],
                )],
            );

        let mut ctx = WorkflowContext::new("wf-duplicate-branch", "active");
        ctx.set("decision", Value::String("accepted".to_string()));
        ctx.set("vendor", serde_json::json!({"phone": "+628111"}));

        let err = run_workflow(&executor, &wf, &mut ctx)
            .await
            .expect_err("duplicate branch values must fail closed");

        match err {
            WorkflowError::Other(msg) => {
                assert!(msg.contains("Ambiguous workflow branch"), "got: {msg}");
                assert!(msg.contains("accepted"), "got: {msg}");
            }
            other => panic!("expected ambiguous branch error, got {other:?}"),
        }
        assert!(executor.notifications.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn invalid_timeout_fallback_fails_before_pre_wait_side_effects() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("invalid_timeout_fallback_guard")
            .initial_state("active")
            .transition(
                "active",
                "done",
                vec![
                    WorkflowStep::notify(ChannelKind::Email, "before_wait", "customer.email"),
                    WorkflowStep::wait_or(
                        "payment.success",
                        std::time::Duration::from_secs(3600),
                        vec![WorkflowStep::wait(
                            "manual_review",
                            std::time::Duration::from_secs(3600),
                        )],
                    ),
                    WorkflowStep::transition("done"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-invalid-timeout-fallback", "active");
        ctx.set(
            "customer",
            serde_json::json!({"email": "guest@example.com"}),
        );

        let err = run_workflow(&executor, &wf, &mut ctx)
            .await
            .expect_err("unsupported timeout fallback must fail before reaching the Wait");

        match err {
            WorkflowError::Other(msg) => {
                assert!(
                    msg.contains("Wait steps inside on_timeout fallback are not supported"),
                    "got: {msg}"
                );
            }
            other => panic!("expected timeout fallback validation error, got {other:?}"),
        }
        assert!(ctx.cursor.is_none());
        assert!(
            executor.notifications.lock().unwrap().is_empty(),
            "definition validation must run before the pre-wait notification"
        );
    }

    #[tokio::test]
    async fn nested_transition_stops_parent_block_before_follow_up_side_effects() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("nested_transition_guard")
            .initial_state("active")
            .transition(
                "active",
                "fallback",
                vec![
                    WorkflowStep::branch(
                        "decision",
                        vec![("accepted", vec![WorkflowStep::transition("confirmed")])],
                        vec![],
                    ),
                    WorkflowStep::notify(
                        ChannelKind::WhatsApp,
                        "old_state_followup",
                        "vendor.phone",
                    ),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-nested-transition", "active");
        ctx.set("decision", Value::String("accepted".to_string()));
        ctx.set("vendor", serde_json::json!({"phone": "+628111"}));

        let result = run_workflow(&executor, &wf, &mut ctx)
            .await
            .expect("nested transition should finish cleanly");

        assert_eq!(result, "confirmed");
        assert_eq!(ctx.current_state, "confirmed");
        assert!(
            executor.notifications.lock().unwrap().is_empty(),
            "parent block must stop after nested transition changes state"
        );
    }

    #[test]
    fn log_message_resolves_nested_context_placeholders() {
        let mut ctx = WorkflowContext::new("wf-log", "running");
        ctx.set(
            "item",
            serde_json::json!({
                "name": "Captain A",
                "phone": "+628111",
            }),
        );
        ctx.set("attempt", serde_json::json!(2));

        assert_eq!(
            resolve_log_message(
                &ctx,
                "Processing {item.name} via {item.phone} attempt {attempt}"
            ),
            "Processing Captain A via +628111 attempt 2"
        );
        assert_eq!(
            resolve_log_message(&ctx, "Missing {item.email} stays"),
            "Missing {item.email} stays"
        );
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
    async fn notify_with_missing_payload_key_fails_before_send() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("notify_payload_guard")
            .initial_state("active")
            .transition(
                "active",
                "done",
                vec![WorkflowStep::notify_with_payload(
                    ChannelKind::Email,
                    "booking_confirmed",
                    "customer.email",
                    "booking.template_payload",
                )],
            );

        let mut ctx = WorkflowContext::new("wf-notify-payload-guard", "active");
        ctx.set(
            "customer",
            serde_json::json!({"email": "guest@example.com"}),
        );

        let err = run_workflow(&executor, &wf, &mut ctx)
            .await
            .expect_err("missing explicit notification payload must fail closed");

        match err {
            WorkflowError::MissingContextKey(key) => assert_eq!(key, "booking.template_payload"),
            other => panic!("expected MissingContextKey, got {other:?}"),
        }
        assert!(
            executor.notifications.lock().unwrap().is_empty(),
            "notification provider must not receive an empty payload for a missing explicit payload key"
        );
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
    async fn test_workflow_pauses_at_nested_branch_wait() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("nested_branch_wait")
            .initial_state("active")
            .transition(
                "active",
                "resolved",
                vec![
                    WorkflowStep::branch(
                        "customer.tier",
                        vec![(
                            "vip",
                            vec![WorkflowStep::wait(
                                "vip_payment",
                                std::time::Duration::from_secs(7200),
                            )],
                        )],
                        vec![WorkflowStep::log("Standard customer")],
                    ),
                    WorkflowStep::notify(ChannelKind::Email, "booking_confirmed", "customer.email"),
                    WorkflowStep::transition("resolved"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-nested-branch-wait", "active");
        ctx.set(
            "customer",
            serde_json::json!({
                "tier": "vip",
                "email": "guest@example.com"
            }),
        );

        let result = run_workflow(&executor, &wf, &mut ctx).await.unwrap();
        assert_eq!(result, "active");
        assert_eq!(ctx.current_state, "active");

        let notifs = executor.notifications.lock().unwrap();
        assert!(
            notifs.is_empty(),
            "steps after a nested branch Wait must not execute"
        );
    }

    #[tokio::test]
    async fn branch_resume_rejects_reordered_branch_definition() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("branch_reorder_source")
            .initial_state("active")
            .transition(
                "active",
                "done",
                vec![WorkflowStep::branch(
                    "customer.tier",
                    vec![
                        (
                            "vip",
                            vec![
                                WorkflowStep::wait(
                                    "approved",
                                    std::time::Duration::from_secs(3600),
                                ),
                                WorkflowStep::notify(
                                    ChannelKind::Email,
                                    "vip_approved",
                                    "customer.email",
                                ),
                            ],
                        ),
                        (
                            "standard",
                            vec![
                                WorkflowStep::wait(
                                    "approved",
                                    std::time::Duration::from_secs(3600),
                                ),
                                WorkflowStep::notify(
                                    ChannelKind::Email,
                                    "standard_approved",
                                    "customer.email",
                                ),
                            ],
                        ),
                    ],
                    vec![],
                )],
            );

        let mut ctx = WorkflowContext::new("wf-branch-reorder", "active");
        ctx.set(
            "customer",
            serde_json::json!({
                "tier": "vip",
                "email": "guest@example.com"
            }),
        );

        let result = run_workflow(&executor, &wf, &mut ctx).await.unwrap();
        assert_eq!(result, "active");

        let drifted_wf = WorkflowDefinition::new("branch_reorder_drifted")
            .initial_state("active")
            .transition(
                "active",
                "done",
                vec![WorkflowStep::branch(
                    "customer.tier",
                    vec![
                        (
                            "standard",
                            vec![
                                WorkflowStep::wait(
                                    "approved",
                                    std::time::Duration::from_secs(3600),
                                ),
                                WorkflowStep::notify(
                                    ChannelKind::Email,
                                    "standard_approved",
                                    "customer.email",
                                ),
                            ],
                        ),
                        (
                            "vip",
                            vec![
                                WorkflowStep::wait(
                                    "approved",
                                    std::time::Duration::from_secs(3600),
                                ),
                                WorkflowStep::notify(
                                    ChannelKind::Email,
                                    "vip_approved",
                                    "customer.email",
                                ),
                            ],
                        ),
                    ],
                    vec![],
                )],
            );

        let err = resume_workflow(
            &executor,
            &drifted_wf,
            "wf-branch-reorder",
            serde_json::json!({"event": "approved"}),
        )
        .await
        .expect_err("branch definition reorder must not silently resume wrong arm");

        match err {
            WorkflowError::Other(msg) => {
                assert!(msg.contains("Invalid workflow resume cursor"));
                assert!(msg.contains("standard"));
                assert!(msg.contains("vip"));
            }
            other => panic!("expected invalid cursor error, got {other:?}"),
        }
        assert!(
            executor.notifications.lock().unwrap().is_empty(),
            "drifted resume must fail before sending the wrong branch notification"
        );
    }

    #[tokio::test]
    async fn branch_resume_rejects_default_branch_drift() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("branch_default_source")
            .initial_state("active")
            .transition(
                "active",
                "done",
                vec![WorkflowStep::branch(
                    "customer.tier",
                    vec![(
                        "vip",
                        vec![WorkflowStep::notify(
                            ChannelKind::Email,
                            "vip",
                            "customer.email",
                        )],
                    )],
                    vec![
                        WorkflowStep::wait("approved", std::time::Duration::from_secs(3600)),
                        WorkflowStep::notify(ChannelKind::Email, "default", "customer.email"),
                    ],
                )],
            );

        let mut ctx = WorkflowContext::new("wf-branch-default-drift", "active");
        ctx.set(
            "customer",
            serde_json::json!({
                "tier": "gold",
                "email": "guest@example.com"
            }),
        );

        let result = run_workflow(&executor, &wf, &mut ctx).await.unwrap();
        assert_eq!(result, "active");

        let drifted_wf = WorkflowDefinition::new("branch_default_drifted")
            .initial_state("active")
            .transition(
                "active",
                "done",
                vec![WorkflowStep::branch(
                    "customer.tier",
                    vec![
                        (
                            "vip",
                            vec![WorkflowStep::notify(
                                ChannelKind::Email,
                                "vip",
                                "customer.email",
                            )],
                        ),
                        (
                            "gold",
                            vec![WorkflowStep::notify(
                                ChannelKind::Email,
                                "gold",
                                "customer.email",
                            )],
                        ),
                    ],
                    vec![
                        WorkflowStep::wait("approved", std::time::Duration::from_secs(3600)),
                        WorkflowStep::notify(ChannelKind::Email, "default", "customer.email"),
                    ],
                )],
            );

        let err = resume_workflow(
            &executor,
            &drifted_wf,
            "wf-branch-default-drift",
            serde_json::json!({"event": "approved"}),
        )
        .await
        .expect_err("default branch cursor must reject new matching branch arms");

        match err {
            WorkflowError::Other(msg) => {
                assert!(msg.contains("default branch cursor"));
                assert!(msg.contains("gold"));
            }
            other => panic!("expected invalid cursor error, got {other:?}"),
        }
        assert!(executor.notifications.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_workflow_pauses_at_nested_for_each_wait() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("nested_for_each_wait")
            .initial_state("active")
            .transition(
                "active",
                "resolved",
                vec![
                    WorkflowStep::for_each(
                        "operators",
                        vec![
                            WorkflowStep::wait(
                                "operator_accept",
                                std::time::Duration::from_secs(3600),
                            ),
                            WorkflowStep::notify(ChannelKind::WhatsApp, "after_wait", "item.phone"),
                        ],
                    ),
                    WorkflowStep::transition("resolved"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-nested-for-each-wait", "active");
        ctx.set(
            "operators",
            serde_json::json!([
                {"name": "Captain A", "phone": "+628111"},
                {"name": "Captain B", "phone": "+628222"}
            ]),
        );

        let result = run_workflow(&executor, &wf, &mut ctx).await.unwrap();
        assert_eq!(result, "active");
        assert_eq!(ctx.current_state, "active");

        let notifs = executor.notifications.lock().unwrap();
        assert!(
            notifs.is_empty(),
            "steps after a nested for_each Wait must not execute"
        );
    }

    #[tokio::test]
    async fn test_workflow_resumes_for_each_after_wait_without_replaying_items() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("for_each_resume")
            .initial_state("active")
            .transition(
                "active",
                "resolved",
                vec![
                    WorkflowStep::for_each(
                        "operators",
                        vec![
                            WorkflowStep::notify(
                                ChannelKind::WhatsApp,
                                "before_wait",
                                "item.phone",
                            ),
                            WorkflowStep::wait(
                                "operator_accept",
                                std::time::Duration::from_secs(3600),
                            ),
                            WorkflowStep::notify(ChannelKind::WhatsApp, "after_wait", "item.phone"),
                        ],
                    ),
                    WorkflowStep::transition("resolved"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-for-each-resume", "active");
        ctx.set(
            "operators",
            serde_json::json!([
                {"name": "Captain A", "phone": "+628111"},
                {"name": "Captain B", "phone": "+628222"}
            ]),
        );

        let result = run_workflow(&executor, &wf, &mut ctx).await.unwrap();
        assert_eq!(result, "active");
        assert!(
            ctx.cursor.is_some(),
            "first pause should persist a resume cursor"
        );

        {
            let notifs = executor.notifications.lock().unwrap();
            assert_eq!(
                notifs.as_slice(),
                &[("+628111".to_string(), "before_wait".to_string())]
            );
        }

        let result = resume_workflow(
            &executor,
            &wf,
            "wf-for-each-resume",
            serde_json::json!({"event": "operator_accept", "accepted": true}),
        )
        .await
        .unwrap();
        assert_eq!(result, "active");

        {
            let notifs = executor.notifications.lock().unwrap();
            assert_eq!(
                notifs.as_slice(),
                &[
                    ("+628111".to_string(), "before_wait".to_string()),
                    ("+628111".to_string(), "after_wait".to_string()),
                    ("+628222".to_string(), "before_wait".to_string()),
                ],
                "resume must continue within the active item and must not replay item 0"
            );
        }

        let result = resume_workflow(
            &executor,
            &wf,
            "wf-for-each-resume",
            serde_json::json!({"event": "operator_accept", "accepted": true}),
        )
        .await
        .unwrap();
        assert_eq!(result, "resolved");

        let notifs = executor.notifications.lock().unwrap();
        assert_eq!(
            notifs.as_slice(),
            &[
                ("+628111".to_string(), "before_wait".to_string()),
                ("+628111".to_string(), "after_wait".to_string()),
                ("+628222".to_string(), "before_wait".to_string()),
                ("+628222".to_string(), "after_wait".to_string()),
            ],
            "each side effect should run exactly once per operator"
        );

        let saved = executor.saved_state.lock().unwrap();
        let saved = saved.as_ref().expect("final state should be saved");
        assert_eq!(saved.current_state, "resolved");
        assert!(
            saved.cursor.is_none(),
            "completed workflow should not retain a resume cursor"
        );
    }

    #[tokio::test]
    async fn for_each_resume_rejects_reordered_items_before_notification() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("for_each_reorder_guard")
            .initial_state("active")
            .transition(
                "active",
                "resolved",
                vec![
                    WorkflowStep::for_each(
                        "operators",
                        vec![
                            WorkflowStep::wait(
                                "operator_accept",
                                std::time::Duration::from_secs(3600),
                            ),
                            WorkflowStep::notify(ChannelKind::WhatsApp, "accepted", "item.phone"),
                        ],
                    ),
                    WorkflowStep::transition("resolved"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-for-each-reorder", "active");
        ctx.set(
            "operators",
            serde_json::json!([
                {"name": "Captain A", "phone": "+628111"},
                {"name": "Captain B", "phone": "+628222"}
            ]),
        );

        let result = run_workflow(&executor, &wf, &mut ctx).await.unwrap();
        assert_eq!(result, "active");

        {
            let mut saved = executor.saved_state.lock().unwrap();
            let saved = saved.as_mut().expect("paused state should be saved");
            saved.set(
                "operators",
                serde_json::json!([
                    {"name": "Captain B", "phone": "+628222"},
                    {"name": "Captain A", "phone": "+628111"}
                ]),
            );
        }

        let err = resume_workflow(
            &executor,
            &wf,
            "wf-for-each-reorder",
            serde_json::json!({"event": "operator_accept"}),
        )
        .await
        .expect_err("reordered operator list must not resume on the wrong item");

        match err {
            WorkflowError::Other(msg) => {
                assert!(msg.contains("Invalid workflow resume cursor"), "got: {msg}");
                assert!(msg.contains("for_each cursor item"), "got: {msg}");
                assert!(msg.contains("changed"), "got: {msg}");
            }
            other => panic!("expected invalid cursor error, got {other:?}"),
        }
        assert!(
            executor.notifications.lock().unwrap().is_empty(),
            "wrong operator must not receive the post-wait notification"
        );
    }

    #[tokio::test]
    async fn test_nested_for_each_restores_outer_item_binding() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("nested_for_each_items")
            .initial_state("active")
            .transition(
                "active",
                "done",
                vec![
                    WorkflowStep::for_each(
                        "operators",
                        vec![
                            WorkflowStep::for_each(
                                "tasks",
                                vec![WorkflowStep::notify(
                                    ChannelKind::WhatsApp,
                                    "task",
                                    "item.phone",
                                )],
                            ),
                            WorkflowStep::notify(ChannelKind::WhatsApp, "summary", "item.phone"),
                        ],
                    ),
                    WorkflowStep::transition("done"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-nested-items", "active");
        ctx.set(
            "operators",
            serde_json::json!([
                {"name": "Captain A", "phone": "+628111"},
                {"name": "Captain B", "phone": "+628222"}
            ]),
        );
        ctx.set(
            "tasks",
            serde_json::json!([
                {"name": "Task 1", "phone": "+629999"}
            ]),
        );

        let result = run_workflow(&executor, &wf, &mut ctx).await.unwrap();
        assert_eq!(result, "done");

        let notifs = executor.notifications.lock().unwrap();
        assert_eq!(
            notifs.as_slice(),
            &[
                ("+629999".to_string(), "task".to_string()),
                ("+628111".to_string(), "summary".to_string()),
                ("+629999".to_string(), "task".to_string()),
                ("+628222".to_string(), "summary".to_string()),
            ],
            "inner ForEach must not delete the outer item binding"
        );
    }

    #[tokio::test]
    async fn test_nested_for_each_wait_restores_item_binding_across_resume() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("nested_for_each_wait_items")
            .initial_state("active")
            .transition(
                "active",
                "done",
                vec![
                    WorkflowStep::for_each(
                        "operators",
                        vec![
                            WorkflowStep::for_each(
                                "tasks",
                                vec![
                                    WorkflowStep::wait(
                                        "task_done",
                                        std::time::Duration::from_secs(3600),
                                    ),
                                    WorkflowStep::notify(
                                        ChannelKind::WhatsApp,
                                        "task",
                                        "item.phone",
                                    ),
                                ],
                            ),
                            WorkflowStep::notify(ChannelKind::WhatsApp, "summary", "item.phone"),
                        ],
                    ),
                    WorkflowStep::transition("done"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-nested-wait-items", "active");
        ctx.set(
            "operators",
            serde_json::json!([
                {"name": "Captain A", "phone": "+628111"},
                {"name": "Captain B", "phone": "+628222"}
            ]),
        );
        ctx.set(
            "tasks",
            serde_json::json!([
                {"name": "Task 1", "phone": "+629999"}
            ]),
        );

        let result = run_workflow(&executor, &wf, &mut ctx).await.unwrap();
        assert_eq!(result, "active");
        assert!(
            ctx.get("item").is_none(),
            "paused workflow state must not persist an active loop item"
        );

        let result = resume_workflow(
            &executor,
            &wf,
            "wf-nested-wait-items",
            serde_json::json!({"event": "task_done"}),
        )
        .await
        .unwrap();
        assert_eq!(result, "active");
        assert!(
            executor
                .saved_state
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|ctx| ctx.get("item"))
                .is_none(),
            "paused resume state must keep item reconstruction cursor-driven"
        );

        {
            let notifs = executor.notifications.lock().unwrap();
            assert_eq!(
                notifs.as_slice(),
                &[
                    ("+629999".to_string(), "task".to_string()),
                    ("+628111".to_string(), "summary".to_string()),
                ],
                "inner wait resume must restore the outer item before summary"
            );
        }

        let result = resume_workflow(
            &executor,
            &wf,
            "wf-nested-wait-items",
            serde_json::json!({"event": "task_done"}),
        )
        .await
        .unwrap();
        assert_eq!(result, "done");

        let notifs = executor.notifications.lock().unwrap();
        assert_eq!(
            notifs.as_slice(),
            &[
                ("+629999".to_string(), "task".to_string()),
                ("+628111".to_string(), "summary".to_string()),
                ("+629999".to_string(), "task".to_string()),
                ("+628222".to_string(), "summary".to_string()),
            ],
            "nested for_each wait must not leak the inner item into the outer loop"
        );
        assert!(
            executor
                .saved_state
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|ctx| ctx.get("item"))
                .is_none(),
            "completed workflow state must not retain a stale loop item"
        );
    }

    #[tokio::test]
    async fn test_resume_rejects_unexpected_wait_event() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("wait_event_guard")
            .initial_state("active")
            .transition(
                "active",
                "done",
                vec![
                    WorkflowStep::wait("payment.success", std::time::Duration::from_secs(3600)),
                    WorkflowStep::notify(ChannelKind::Email, "paid", "customer.email"),
                    WorkflowStep::transition("done"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-event-guard", "active");
        ctx.set(
            "customer",
            serde_json::json!({"email": "guest@example.com"}),
        );
        let result = run_workflow(&executor, &wf, &mut ctx).await.unwrap();
        assert_eq!(result, "active");

        let wait = ctx
            .cursor
            .as_ref()
            .and_then(|cursor| cursor.wait.as_ref())
            .expect("wait metadata should be persisted");
        assert_eq!(wait.event, "payment.success");

        let err = resume_workflow(
            &executor,
            &wf,
            "wf-event-guard",
            serde_json::json!({"event": "operator.declined"}),
        )
        .await
        .expect_err("wrong event must not resume the workflow");

        match err {
            WorkflowError::Other(msg) => {
                assert!(msg.contains("payment.success"));
                assert!(msg.contains("operator.declined"));
            }
            other => panic!("expected event mismatch error, got: {other}"),
        }
        assert!(executor.notifications.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn resume_event_rejects_padded_event_name_before_replay() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("wait_event_padding_guard")
            .initial_state("active")
            .transition(
                "active",
                "done",
                vec![
                    WorkflowStep::wait("payment.success", std::time::Duration::from_secs(3600)),
                    WorkflowStep::notify(ChannelKind::Email, "paid", "customer.email"),
                    WorkflowStep::transition("done"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-event-padding-guard", "active");
        ctx.set(
            "customer",
            serde_json::json!({"email": "guest@example.com"}),
        );
        let result = run_workflow(&executor, &wf, &mut ctx).await.unwrap();
        assert_eq!(result, "active");

        let err = resume_workflow(
            &executor,
            &wf,
            "wf-event-padding-guard",
            serde_json::json!({"event": " payment.success "}),
        )
        .await
        .expect_err("padded event name must not resume the workflow");

        match err {
            WorkflowError::Other(msg) => {
                assert!(msg.contains("leading or trailing whitespace"), "got: {msg}");
            }
            other => panic!("expected padded event error, got {other:?}"),
        }
        assert!(
            executor.notifications.lock().unwrap().is_empty(),
            "padded resume event must fail before notification replay"
        );
    }

    #[tokio::test]
    async fn resume_event_without_wait_cursor_does_not_run_transition() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("vendor_callback_guard")
            .initial_state("awaiting_vendor")
            .transition(
                "awaiting_vendor",
                "notified",
                vec![
                    WorkflowStep::notify(ChannelKind::WhatsApp, "vendor_ready", "vendor.phone"),
                    WorkflowStep::transition("notified"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-no-wait-cursor", "awaiting_vendor");
        ctx.set("vendor", serde_json::json!({"phone": "+628111"}));
        executor.save_state(&ctx).await.unwrap();

        let err = resume_workflow(
            &executor,
            &wf,
            "wf-no-wait-cursor",
            serde_json::json!({"event": "vendor.ready"}),
        )
        .await
        .expect_err("event resume must not run a workflow with no wait cursor");

        match err {
            WorkflowError::Other(msg) => {
                assert!(
                    msg.contains("not waiting for an external event"),
                    "got: {msg}"
                );
            }
            other => panic!("expected not-waiting error, got {other:?}"),
        }
        assert!(
            executor.notifications.lock().unwrap().is_empty(),
            "webhook resume without a wait cursor must not send vendor notifications"
        );
    }

    #[tokio::test]
    async fn resume_event_rejects_stale_cursor_state_before_transition() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("stale_cursor_guard")
            .initial_state("awaiting_vendor")
            .transition(
                "awaiting_vendor",
                "notified",
                vec![
                    WorkflowStep::notify(ChannelKind::WhatsApp, "vendor_ready", "vendor.phone"),
                    WorkflowStep::transition("notified"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-stale-cursor", "awaiting_vendor");
        ctx.set("vendor", serde_json::json!({"phone": "+628111"}));
        ctx.set_cursor(WorkflowCursor {
            state: "old_state".to_string(),
            frames: vec![WorkflowCursorFrame::Steps { index: 1 }],
            wait: Some(WorkflowPendingWait {
                event: "vendor.ready".to_string(),
                deadline_at: Utc::now() + chrono::Duration::hours(1),
                on_timeout: vec![],
            }),
        });
        executor.save_state(&ctx).await.unwrap();

        let err = resume_workflow(
            &executor,
            &wf,
            "wf-stale-cursor",
            serde_json::json!({"event": "vendor.ready"}),
        )
        .await
        .expect_err("stale cursor state must not resume the current transition");

        match err {
            WorkflowError::Other(msg) => {
                assert!(msg.contains("Invalid workflow resume cursor"), "got: {msg}");
                assert!(msg.contains("state does not match"), "got: {msg}");
            }
            other => panic!("expected invalid cursor error, got {other:?}"),
        }
        assert!(
            executor.notifications.lock().unwrap().is_empty(),
            "stale cursor mismatch must fail before side effects"
        );
    }

    #[tokio::test]
    async fn normal_resume_rejects_empty_cursor_frames_before_replay() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("empty_cursor_guard")
            .initial_state("active")
            .transition(
                "active",
                "done",
                vec![
                    WorkflowStep::notify(ChannelKind::WhatsApp, "vendor_ready", "vendor.phone"),
                    WorkflowStep::transition("done"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-empty-cursor", "active");
        ctx.set("vendor", serde_json::json!({"phone": "+628111"}));
        ctx.set_cursor(WorkflowCursor {
            state: "active".to_string(),
            frames: vec![],
            wait: None,
        });

        let err = run_workflow(&executor, &wf, &mut ctx)
            .await
            .expect_err("empty cursor frames must not replay the transition from step zero");

        match err {
            WorkflowError::Other(msg) => {
                assert!(msg.contains("Invalid workflow resume cursor"), "got: {msg}");
                assert!(msg.contains("no frames"), "got: {msg}");
            }
            other => panic!("expected invalid cursor error, got {other:?}"),
        }
        assert!(
            executor.notifications.lock().unwrap().is_empty(),
            "empty checkpoint cursor must fail before replaying notifications"
        );
    }

    #[tokio::test]
    async fn event_resume_rejects_empty_wait_cursor_frames_before_replay() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("empty_wait_cursor_guard")
            .initial_state("active")
            .transition(
                "active",
                "done",
                vec![
                    WorkflowStep::wait("vendor.ready", std::time::Duration::from_secs(3600)),
                    WorkflowStep::notify(ChannelKind::WhatsApp, "vendor_ready", "vendor.phone"),
                    WorkflowStep::transition("done"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-empty-wait-cursor", "active");
        ctx.set("vendor", serde_json::json!({"phone": "+628111"}));
        ctx.set_cursor(WorkflowCursor {
            state: "active".to_string(),
            frames: vec![],
            wait: Some(WorkflowPendingWait {
                event: "vendor.ready".to_string(),
                deadline_at: Utc::now() + chrono::Duration::hours(1),
                on_timeout: vec![],
            }),
        });
        executor.save_state(&ctx).await.unwrap();

        let err = resume_workflow(
            &executor,
            &wf,
            "wf-empty-wait-cursor",
            serde_json::json!({"event": "vendor.ready"}),
        )
        .await
        .expect_err("empty wait cursor frames must not resume from step zero");

        match err {
            WorkflowError::Other(msg) => {
                assert!(msg.contains("Invalid workflow resume cursor"), "got: {msg}");
                assert!(msg.contains("no frames"), "got: {msg}");
            }
            other => panic!("expected invalid cursor error, got {other:?}"),
        }
        assert!(
            executor.notifications.lock().unwrap().is_empty(),
            "empty wait cursor must fail before replaying notifications"
        );
    }

    #[tokio::test]
    async fn wait_with_empty_event_name_is_rejected_before_pause() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("empty_wait_event")
            .initial_state("active")
            .transition(
                "active",
                "done",
                vec![
                    WorkflowStep::wait("", std::time::Duration::from_secs(3600)),
                    WorkflowStep::notify(ChannelKind::Email, "after_wait", "customer.email"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-empty-wait", "active");
        ctx.set(
            "customer",
            serde_json::json!({"email": "guest@example.com"}),
        );

        let err = run_workflow(&executor, &wf, &mut ctx)
            .await
            .expect_err("empty wait event must fail before persisting a pause");

        match err {
            WorkflowError::Other(msg) => {
                assert!(
                    msg.contains("Wait event name must not be empty"),
                    "got: {msg}"
                );
            }
            other => panic!("expected empty event error, got {other:?}"),
        }
        assert!(ctx.cursor.is_none());
        assert!(executor.notifications.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn wait_with_padded_event_name_is_rejected_before_pause() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("padded_wait_event")
            .initial_state("active")
            .transition(
                "active",
                "done",
                vec![
                    WorkflowStep::wait(" payment.success ", std::time::Duration::from_secs(3600)),
                    WorkflowStep::notify(ChannelKind::Email, "after_wait", "customer.email"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-padded-wait", "active");
        ctx.set(
            "customer",
            serde_json::json!({"email": "guest@example.com"}),
        );

        let err = run_workflow(&executor, &wf, &mut ctx)
            .await
            .expect_err("padded wait event must fail before persisting a pause");

        match err {
            WorkflowError::Other(msg) => {
                assert!(msg.contains("leading or trailing whitespace"), "got: {msg}");
            }
            other => panic!("expected padded event error, got {other:?}"),
        }
        assert!(ctx.cursor.is_none());
        assert!(executor.notifications.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn transition_must_be_final_to_avoid_lost_follow_up_side_effects() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("transition_order_guard")
            .initial_state("active")
            .transition(
                "active",
                "done",
                vec![
                    WorkflowStep::transition("done"),
                    WorkflowStep::notify(ChannelKind::WhatsApp, "after_transition", "vendor.phone"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-transition-order", "active");
        ctx.set("vendor", serde_json::json!({"phone": "+628111"}));

        let err = run_workflow(&executor, &wf, &mut ctx)
            .await
            .expect_err("transition before side effects must fail closed");

        match err {
            WorkflowError::Other(msg) => {
                assert!(
                    msg.contains("Transition steps must be the final step"),
                    "got: {msg}"
                );
            }
            other => panic!("expected transition order error, got {other:?}"),
        }
        assert_eq!(ctx.current_state, "active");
        assert!(executor.notifications.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_timeout_workflow_executes_on_timeout_fallback() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("wait_timeout")
            .initial_state("active")
            .transition(
                "active",
                "done",
                vec![
                    WorkflowStep::wait_or(
                        "payment.success",
                        std::time::Duration::from_secs(0),
                        vec![
                            WorkflowStep::notify(
                                ChannelKind::Email,
                                "payment_timeout",
                                "customer.email",
                            ),
                            WorkflowStep::transition("timed_out"),
                        ],
                    ),
                    WorkflowStep::notify(ChannelKind::Email, "paid", "customer.email"),
                    WorkflowStep::transition("done"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-timeout", "active");
        ctx.set(
            "customer",
            serde_json::json!({"email": "guest@example.com"}),
        );
        let result = run_workflow(&executor, &wf, &mut ctx).await.unwrap();
        assert_eq!(result, "active");

        let result = timeout_workflow(&executor, &wf, "wf-timeout")
            .await
            .unwrap();
        assert_eq!(result, "timed_out");

        let notifs = executor.notifications.lock().unwrap();
        assert_eq!(
            notifs.as_slice(),
            &[(
                "guest@example.com".to_string(),
                "payment_timeout".to_string()
            )]
        );
    }

    #[tokio::test]
    async fn test_timeout_fallback_without_transition_does_not_run_success_path() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("wait_timeout_no_transition")
            .initial_state("active")
            .transition(
                "active",
                "confirmed",
                vec![
                    WorkflowStep::wait_or(
                        "payment.success",
                        std::time::Duration::from_secs(0),
                        vec![WorkflowStep::notify(
                            ChannelKind::Email,
                            "payment_timeout",
                            "customer.email",
                        )],
                    ),
                    WorkflowStep::notify(ChannelKind::Email, "booking_confirmed", "customer.email"),
                    WorkflowStep::transition("confirmed"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-timeout-no-transition", "active");
        ctx.set(
            "customer",
            serde_json::json!({"email": "guest@example.com"}),
        );
        let result = run_workflow(&executor, &wf, &mut ctx).await.unwrap();
        assert_eq!(result, "active");

        let result = timeout_workflow(&executor, &wf, "wf-timeout-no-transition")
            .await
            .unwrap();
        assert_eq!(result, "active");

        let notifs = executor.notifications.lock().unwrap();
        assert_eq!(
            notifs.as_slice(),
            &[(
                "guest@example.com".to_string(),
                "payment_timeout".to_string()
            )],
            "timeout fallback must not fall through into the post-wait success path"
        );
    }

    #[tokio::test]
    async fn test_step_checkpoint_prevents_side_effect_replay_after_failure() {
        let executor = MockExecutor::new();
        let failing_query =
            qail_core::wire::encode_cmd_text(&qail_core::Qail::get("bookings").limit(1));
        executor.fail_query(failing_query.clone());

        let wf = WorkflowDefinition::new("checkpoint_failure")
            .initial_state("active")
            .transition(
                "active",
                "done",
                vec![
                    WorkflowStep::notify(ChannelKind::Email, "charged", "customer.email"),
                    WorkflowStep::Query {
                        cmd_json: failing_query,
                        store_as: None,
                    },
                    WorkflowStep::transition("done"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-checkpoint", "active");
        ctx.set(
            "customer",
            serde_json::json!({"email": "guest@example.com"}),
        );
        let err = run_workflow(&executor, &wf, &mut ctx)
            .await
            .expect_err("legacy query should fail after notification checkpoint");
        assert!(matches!(err, WorkflowError::QueryFailed(_)));
        assert_eq!(executor.notifications.lock().unwrap().len(), 1);

        let mut restored = executor
            .saved_state
            .lock()
            .unwrap()
            .clone()
            .expect("checkpoint should be saved after notification");
        let cursor = restored.cursor.as_ref().expect("checkpoint cursor");
        assert_eq!(cursor.frames, vec![WorkflowCursorFrame::Steps { index: 1 }]);
        assert!(cursor.wait.is_none());
        assert!(restored.get(TIMEOUT_FALLBACK_KEY).is_none());

        let err = run_workflow(&executor, &wf, &mut restored)
            .await
            .expect_err("query should still fail on retry");
        assert!(matches!(err, WorkflowError::QueryFailed(_)));
        assert_eq!(
            executor.notifications.lock().unwrap().len(),
            1,
            "retry must resume after the completed notification step"
        );
    }

    #[tokio::test]
    async fn test_timeout_checkpoint_prevents_side_effect_replay_after_failure() {
        let executor = MockExecutor::new();
        let failing_query =
            qail_core::wire::encode_cmd_text(&qail_core::Qail::get("bookings").limit(1));
        executor.fail_query(failing_query.clone());

        let wf = WorkflowDefinition::new("timeout_checkpoint_failure")
            .initial_state("active")
            .transition(
                "active",
                "done",
                vec![WorkflowStep::wait_or(
                    "payment.success",
                    std::time::Duration::from_secs(0),
                    vec![
                        WorkflowStep::notify(
                            ChannelKind::Email,
                            "payment_timeout",
                            "customer.email",
                        ),
                        WorkflowStep::Query {
                            cmd_json: failing_query,
                            store_as: None,
                        },
                        WorkflowStep::transition("timed_out"),
                    ],
                )],
            );

        let mut ctx = WorkflowContext::new("wf-timeout-checkpoint", "active");
        ctx.set(
            "customer",
            serde_json::json!({"email": "guest@example.com"}),
        );
        let result = run_workflow(&executor, &wf, &mut ctx).await.unwrap();
        assert_eq!(result, "active");

        let err = timeout_workflow(&executor, &wf, "wf-timeout-checkpoint")
            .await
            .expect_err("legacy query should fail after timeout notification checkpoint");
        assert!(matches!(err, WorkflowError::QueryFailed(_)));
        assert_eq!(executor.notifications.lock().unwrap().len(), 1);

        let saved = executor
            .saved_state
            .lock()
            .unwrap()
            .clone()
            .expect("timeout checkpoint should be saved after notification");
        let cursor = saved.cursor.as_ref().expect("timeout checkpoint cursor");
        assert_eq!(cursor.frames, vec![WorkflowCursorFrame::Steps { index: 1 }]);
        assert!(cursor.wait.is_none());
        assert!(
            saved.get(TIMEOUT_FALLBACK_KEY).is_some(),
            "timeout fallback cursor must retain internal on_timeout metadata"
        );

        let err = timeout_workflow(&executor, &wf, "wf-timeout-checkpoint")
            .await
            .expect_err("timeout fallback query should still fail on retry");
        assert!(matches!(err, WorkflowError::QueryFailed(_)));
        assert_eq!(
            executor.notifications.lock().unwrap().len(),
            1,
            "timeout retry must resume after the completed notification step"
        );
    }

    #[tokio::test]
    async fn test_workflow_errors_on_for_each_non_array() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("bad_for_each")
            .initial_state("active")
            .transition(
                "active",
                "resolved",
                vec![WorkflowStep::for_each(
                    "operators",
                    vec![WorkflowStep::notify(
                        ChannelKind::WhatsApp,
                        "opportunity",
                        "item.phone",
                    )],
                )],
            );

        let mut ctx = WorkflowContext::new("wf-bad-for-each", "active");
        ctx.set("operators", serde_json::json!({"phone": "+628111"}));

        let err = run_workflow(&executor, &wf, &mut ctx)
            .await
            .expect_err("ForEach should reject non-array context values");

        match err {
            WorkflowError::Other(msg) => {
                assert!(msg.contains("Expected JSON array for ForEach list 'operators'"));
                assert!(msg.contains("object"));
            }
            other => panic!("expected Other error, got: {other}"),
        }
        assert!(
            executor.notifications.lock().unwrap().is_empty(),
            "ForEach type errors must fail before running nested steps"
        );
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

        let charges = executor.charges.lock().unwrap();
        assert_eq!(charges.len(), 1);
        assert_eq!(charges[0].amount, 150000);
    }

    #[tokio::test]
    async fn charge_rejects_reserved_store_key_before_provider_call() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("charge_reserved_key")
            .initial_state("created")
            .transition(
                "created",
                "awaiting_payment",
                vec![WorkflowStep::charge(
                    PaymentKind::Xendit,
                    "order.total",
                    "order.id",
                    Some("item"),
                )],
            );

        let mut ctx = WorkflowContext::new("wf-charge-reserved-key", "created");
        ctx.set(
            "order",
            serde_json::json!({
                "id": "booking-reserved",
                "total": 150000
            }),
        );

        let err = run_workflow(&executor, &wf, &mut ctx)
            .await
            .expect_err("reserved charge output key must fail before provider call");

        match err {
            WorkflowError::Other(msg) => {
                assert!(msg.contains("reserved context key 'item'"), "got: {msg}");
            }
            other => panic!("expected reserved context key error, got {other:?}"),
        }
        assert!(
            executor.charges.lock().unwrap().is_empty(),
            "charge provider must not be called after reserved key validation fails"
        );
    }

    #[tokio::test]
    async fn charge_accepts_safe_integer_float_amount() {
        let executor = MockExecutor::new();
        let wf = charge_only_workflow("booking_payment_float");

        let mut ctx = WorkflowContext::new("wf-payment-float", "created");
        ctx.set(
            "order",
            serde_json::json!({
                "id": "booking-float",
                "total": 150000.0
            }),
        );

        let result = run_workflow(&executor, &wf, &mut ctx).await.unwrap();

        assert_eq!(result, "awaiting_payment");
        let charges = executor.charges.lock().unwrap();
        assert_eq!(charges.len(), 1);
        assert_eq!(charges[0].amount, 150000);
    }

    #[tokio::test]
    async fn charge_rejects_fractional_amount_before_provider_call() {
        let executor = MockExecutor::new();
        let wf = charge_only_workflow("booking_payment_fractional");

        let mut ctx = WorkflowContext::new("wf-payment-fractional", "created");
        ctx.set(
            "order",
            serde_json::json!({
                "id": "booking-fractional",
                "total": 150000.75
            }),
        );

        let err = run_workflow(&executor, &wf, &mut ctx)
            .await
            .expect_err("fractional charge amount must fail before provider call");

        match err {
            WorkflowError::Other(msg) => {
                assert!(msg.contains("Invalid charge amount"));
                assert!(msg.contains("smallest currency unit"));
            }
            other => panic!("expected invalid amount error, got {other:?}"),
        }
        assert!(executor.charges.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn charge_rejects_non_positive_amount_before_provider_call() {
        let executor = MockExecutor::new();
        let wf = charge_only_workflow("booking_payment_negative");

        let mut ctx = WorkflowContext::new("wf-payment-negative", "created");
        ctx.set(
            "order",
            serde_json::json!({
                "id": "booking-negative",
                "total": -1
            }),
        );

        let err = run_workflow(&executor, &wf, &mut ctx)
            .await
            .expect_err("non-positive charge amount must fail before provider call");

        match err {
            WorkflowError::Other(msg) => {
                assert!(msg.contains("Invalid charge amount"));
                assert!(msg.contains("greater than zero"));
            }
            other => panic!("expected invalid amount error, got {other:?}"),
        }
        assert!(executor.charges.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn charge_rejects_oversized_unsigned_amount_before_provider_call() {
        let executor = MockExecutor::new();
        let wf = charge_only_workflow("booking_payment_oversized");

        let mut ctx = WorkflowContext::new("wf-payment-oversized", "created");
        ctx.set(
            "order",
            serde_json::json!({
                "id": "booking-oversized",
                "total": u64::MAX
            }),
        );

        let err = run_workflow(&executor, &wf, &mut ctx)
            .await
            .expect_err("oversized charge amount must fail before provider call");

        match err {
            WorkflowError::Other(msg) => {
                assert!(msg.contains("Invalid charge amount"));
                assert!(msg.contains("signed 64-bit"));
            }
            other => panic!("expected invalid amount error, got {other:?}"),
        }
        assert!(executor.charges.lock().unwrap().is_empty());
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
    async fn future_legacy_query_payload_fails_before_current_side_effects() {
        let executor = MockExecutor::new();

        let wf = WorkflowDefinition::new("future_legacy_query")
            .initial_state("active")
            .transition(
                "active",
                "query_state",
                vec![
                    WorkflowStep::notify(ChannelKind::Email, "before_bad_query", "customer.email"),
                    WorkflowStep::transition("query_state"),
                ],
            )
            .transition(
                "query_state",
                "done",
                vec![
                    WorkflowStep::Query {
                        cmd_json: "get users limit 1".to_string(),
                        store_as: Some("rows".to_string()),
                    },
                    WorkflowStep::transition("done"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-future-legacy-query", "active");
        ctx.set(
            "customer",
            serde_json::json!({"email": "guest@example.com"}),
        );

        let err = run_workflow(&executor, &wf, &mut ctx)
            .await
            .expect_err("future legacy query payload must fail before current side effects");

        match err {
            WorkflowError::QueryFailed(msg) => {
                assert!(
                    msg.contains("QAIL-CMD/1"),
                    "error should mention required wire magic"
                );
            }
            other => panic!("expected QueryFailed, got: {other}"),
        }
        assert_eq!(ctx.current_state, "active");
        assert!(
            executor.notifications.lock().unwrap().is_empty(),
            "definition validation must run before the current-state notification"
        );
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

    #[tokio::test]
    async fn query_rejects_reserved_store_key_before_execution() {
        let executor = MockExecutor::new();
        let cmd = qail_core::Qail::get("users").columns(["id"]).limit(1);
        let wire = qail_core::wire::encode_cmd_text(&cmd);

        let wf = WorkflowDefinition::new("query_reserved_key")
            .initial_state("start")
            .transition(
                "start",
                "done",
                vec![
                    WorkflowStep::Query {
                        cmd_json: wire,
                        store_as: Some("__qail_timeout_fallback".to_string()),
                    },
                    WorkflowStep::transition("done"),
                ],
            );

        let mut ctx = WorkflowContext::new("wf-query-reserved-key", "start");
        let err = run_workflow(&executor, &wf, &mut ctx)
            .await
            .expect_err("reserved query output key must fail before query execution");

        match err {
            WorkflowError::Other(msg) => {
                assert!(
                    msg.contains("reserved context key '__qail_timeout_fallback'"),
                    "got: {msg}"
                );
            }
            other => panic!("expected reserved context key error, got {other:?}"),
        }
        assert!(
            executor.queries.lock().unwrap().is_empty(),
            "query executor must not be called after reserved key validation fails"
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
