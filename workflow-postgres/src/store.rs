use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use qail_core::ast::{Operator, Qail, Value as QailValue};
use qail_pg::PgDriver;
use qail_workflow::{
    WorkflowContext, WorkflowError, WorkflowLease, WorkflowOperation, WorkflowOperationStatus,
    WorkflowSideEffect, WorkflowSideEffectStatus,
};
use tokio::sync::Mutex;

use crate::tables::PgWorkflowTables;
use crate::util::{
    STATUS_COMPLETED, STATUS_FAILED, STATUS_STARTED, deadline_from_duration, excluded, finish_tx,
    is_duplicate, json_error, missing_started_row, operation_kind_text, option_string,
    optional_json, pg_error, required_string, timestamp,
};

/// PostgreSQL workflow storage using a single `PgDriver`.
#[derive(Clone)]
pub struct PgWorkflowStore {
    driver: Arc<Mutex<PgDriver>>,
    tables: PgWorkflowTables,
    timeout_claim_ttl: Duration,
}

impl PgWorkflowStore {
    /// Create a store around an existing driver.
    pub fn new(driver: PgDriver) -> Self {
        Self::from_shared(Arc::new(Mutex::new(driver)))
    }

    /// Create a store around a shared driver handle.
    pub fn from_shared(driver: Arc<Mutex<PgDriver>>) -> Self {
        Self {
            driver,
            tables: PgWorkflowTables::default(),
            timeout_claim_ttl: Duration::from_secs(60),
        }
    }

    /// Connect a store from a PostgreSQL URL.
    pub async fn connect_url(url: &str) -> Result<Self, WorkflowError> {
        let driver = PgDriver::connect_url(url).await.map_err(pg_error)?;
        Ok(Self::new(driver))
    }

    /// Override storage table names.
    pub fn with_tables(mut self, tables: PgWorkflowTables) -> Self {
        self.tables = tables;
        self
    }

    /// Override how long a timeout drain claim should be held.
    pub fn with_timeout_claim_ttl(mut self, ttl: Duration) -> Self {
        self.timeout_claim_ttl = ttl;
        self
    }

    /// Return the storage table names.
    pub fn tables(&self) -> &PgWorkflowTables {
        &self.tables
    }

    /// Return QAIL AST schema commands for this store.
    pub fn schema_commands(&self) -> Vec<Qail> {
        self.tables.schema_commands()
    }

    /// Execute the schema commands through the AST driver path.
    pub async fn install_schema(&self) -> Result<(), WorkflowError> {
        let mut driver = self.driver.lock().await;
        for cmd in self.schema_commands() {
            driver.execute(&cmd).await.map_err(pg_error)?;
        }
        Ok(())
    }

    pub(crate) async fn save_state(&self, ctx: &WorkflowContext) -> Result<(), WorkflowError> {
        let context_json = serde_json::to_string(ctx).map_err(json_error)?;
        let wait = ctx.cursor.as_ref().and_then(|cursor| cursor.wait.as_ref());
        let wait_event = wait.map(|wait| wait.event.clone());
        let wait_deadline = wait.map(|wait| timestamp(wait.deadline_at));
        let now = timestamp(Utc::now());

        let cmd = Qail::add(&self.tables.states)
            .columns([
                "workflow_id",
                "definition_name",
                "definition_version",
                "current_state",
                "context",
                "wait_event",
                "wait_deadline_at",
                "timeout_claimed_until",
                "created_at",
                "updated_at",
            ])
            .values([
                QailValue::String(ctx.workflow_id.clone()),
                option_string(ctx.definition_name.clone()),
                option_string(ctx.definition_version.clone()),
                QailValue::String(ctx.current_state.clone()),
                QailValue::Json(context_json),
                option_string(wait_event),
                option_string(wait_deadline),
                QailValue::Null,
                QailValue::String(timestamp(ctx.created_at)),
                QailValue::String(now),
            ])
            .on_conflict_update(
                &["workflow_id"],
                &[
                    ("definition_name", excluded("definition_name")),
                    ("definition_version", excluded("definition_version")),
                    ("current_state", excluded("current_state")),
                    ("context", excluded("context")),
                    ("wait_event", excluded("wait_event")),
                    ("wait_deadline_at", excluded("wait_deadline_at")),
                    ("timeout_claimed_until", excluded("timeout_claimed_until")),
                    ("updated_at", excluded("updated_at")),
                ],
            );

        let mut driver = self.driver.lock().await;
        driver.execute(&cmd).await.map_err(pg_error)?;
        Ok(())
    }

    pub(crate) async fn load_state(
        &self,
        workflow_id: &str,
    ) -> Result<Option<WorkflowContext>, WorkflowError> {
        let cmd = Qail::get(&self.tables.states)
            .columns(["context"])
            .eq("workflow_id", workflow_id)
            .limit(1);

        let mut driver = self.driver.lock().await;
        let rows = driver.fetch_all(&cmd).await.map_err(pg_error)?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let context_json = required_string(row, 0, "context")?;
        let ctx = serde_json::from_str(&context_json).map_err(json_error)?;
        Ok(Some(ctx))
    }

    pub(crate) async fn acquire_workflow_lease(
        &self,
        lease: &WorkflowLease,
    ) -> Result<bool, WorkflowError> {
        let mut driver = self.driver.lock().await;
        driver.begin().await.map_err(pg_error)?;

        let result = self.acquire_workflow_lease_tx(&mut driver, lease).await;
        finish_tx(&mut driver, result).await
    }

    async fn acquire_workflow_lease_tx(
        &self,
        driver: &mut PgDriver,
        lease: &WorkflowLease,
    ) -> Result<bool, WorkflowError> {
        let now = timestamp(Utc::now());
        let expires_at = timestamp(deadline_from_duration(lease.ttl)?);
        let existing = Qail::get(&self.tables.leases)
            .columns(["owner", "expires_at"])
            .eq("workflow_id", lease.workflow_id.as_str())
            .for_update()
            .limit(1);

        let rows = driver.fetch_all(&existing).await.map_err(pg_error)?;
        if let Some(row) = rows.first() {
            let expires = required_string(row, 1, "expires_at")?;
            if expires > now {
                return Ok(false);
            }

            let update = Qail::set(&self.tables.leases)
                .set_value("owner", lease.owner.clone())
                .set_value("expires_at", expires_at)
                .set_value("updated_at", now)
                .eq("workflow_id", lease.workflow_id.as_str());
            driver.execute(&update).await.map_err(pg_error)?;
            return Ok(true);
        }

        let insert = Qail::add(&self.tables.leases)
            .columns(["workflow_id", "owner", "expires_at", "updated_at"])
            .values([
                QailValue::String(lease.workflow_id.clone()),
                QailValue::String(lease.owner.clone()),
                QailValue::String(expires_at),
                QailValue::String(now),
            ]);

        match driver.execute(&insert).await {
            Ok(_) => Ok(true),
            Err(err) if is_duplicate(&err) => Ok(false),
            Err(err) => Err(pg_error(err)),
        }
    }

    pub(crate) async fn release_workflow_lease(
        &self,
        lease: &WorkflowLease,
    ) -> Result<(), WorkflowError> {
        let cmd = Qail::del(&self.tables.leases)
            .eq("workflow_id", lease.workflow_id.as_str())
            .eq("owner", lease.owner.as_str());
        let mut driver = self.driver.lock().await;
        driver.execute(&cmd).await.map_err(pg_error)?;
        Ok(())
    }

    pub(crate) async fn begin_workflow_operation(
        &self,
        operation: &WorkflowOperation,
    ) -> Result<WorkflowOperationStatus, WorkflowError> {
        let mut driver = self.driver.lock().await;
        driver.begin().await.map_err(pg_error)?;

        let result = self
            .begin_workflow_operation_tx(&mut driver, operation)
            .await;
        finish_tx(&mut driver, result).await
    }

    async fn begin_workflow_operation_tx(
        &self,
        driver: &mut PgDriver,
        operation: &WorkflowOperation,
    ) -> Result<WorkflowOperationStatus, WorkflowError> {
        let operation_kind = operation_kind_text(&operation.kind);
        let existing = Qail::get(&self.tables.operations)
            .columns(["status", "state", "kind"])
            .eq("workflow_id", operation.workflow_id.as_str())
            .eq("idempotency_key", operation.idempotency_key.as_str())
            .for_update()
            .limit(1);
        let rows = driver.fetch_all(&existing).await.map_err(pg_error)?;

        if let Some(row) = rows.first() {
            let status = required_string(row, 0, "status")?;
            let stored_kind = required_string(row, 2, "kind")?;
            if stored_kind != operation_kind {
                return Err(WorkflowError::Other(format!(
                    "Workflow operation idempotency key '{}' was previously used for kind {}, not {}",
                    operation.idempotency_key, stored_kind, operation_kind
                )));
            }

            return match status.as_str() {
                STATUS_STARTED => Ok(WorkflowOperationStatus::InProgress),
                STATUS_COMPLETED => {
                    let state = required_string(row, 1, "state")?;
                    Ok(WorkflowOperationStatus::Completed { state })
                }
                STATUS_FAILED => {
                    self.mark_workflow_operation_started_tx(driver, operation)
                        .await?;
                    Ok(WorkflowOperationStatus::Started)
                }
                other => Err(WorkflowError::Other(format!(
                    "Unknown workflow operation status '{other}'"
                ))),
            };
        }

        let now = timestamp(Utc::now());
        let insert = Qail::add(&self.tables.operations)
            .columns([
                "workflow_name",
                "workflow_id",
                "idempotency_key",
                "kind",
                "status",
                "state",
                "error",
                "created_at",
                "updated_at",
            ])
            .values([
                QailValue::String(operation.workflow_name.clone()),
                QailValue::String(operation.workflow_id.clone()),
                QailValue::String(operation.idempotency_key.clone()),
                QailValue::String(operation_kind),
                QailValue::String(STATUS_STARTED.to_string()),
                QailValue::Null,
                QailValue::Null,
                QailValue::String(now.clone()),
                QailValue::String(now),
            ]);

        match driver.execute(&insert).await {
            Ok(_) => Ok(WorkflowOperationStatus::Started),
            Err(err) if is_duplicate(&err) => Ok(WorkflowOperationStatus::InProgress),
            Err(err) => Err(pg_error(err)),
        }
    }

    async fn mark_workflow_operation_started_tx(
        &self,
        driver: &mut PgDriver,
        operation: &WorkflowOperation,
    ) -> Result<(), WorkflowError> {
        let cmd = Qail::set(&self.tables.operations)
            .set_value("status", STATUS_STARTED)
            .set_value("state", QailValue::Null)
            .set_value("error", QailValue::Null)
            .set_value("updated_at", timestamp(Utc::now()))
            .eq("workflow_id", operation.workflow_id.as_str())
            .eq("idempotency_key", operation.idempotency_key.as_str());
        driver.execute(&cmd).await.map_err(pg_error)?;
        Ok(())
    }

    pub(crate) async fn complete_workflow_operation(
        &self,
        operation: &WorkflowOperation,
        state: &str,
    ) -> Result<(), WorkflowError> {
        let cmd = Qail::set(&self.tables.operations)
            .set_value("status", STATUS_COMPLETED)
            .set_value("state", state)
            .set_value("error", QailValue::Null)
            .set_value("updated_at", timestamp(Utc::now()))
            .eq("workflow_id", operation.workflow_id.as_str())
            .eq("idempotency_key", operation.idempotency_key.as_str())
            .eq("status", STATUS_STARTED);
        let mut driver = self.driver.lock().await;
        let affected = driver.execute(&cmd).await.map_err(pg_error)?;
        if affected == 0 {
            return Err(missing_started_row(
                "operation",
                operation.idempotency_key.as_str(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn fail_workflow_operation(
        &self,
        operation: &WorkflowOperation,
        error: &str,
    ) -> Result<(), WorkflowError> {
        let cmd = Qail::set(&self.tables.operations)
            .set_value("status", STATUS_FAILED)
            .set_value("error", error)
            .set_value("updated_at", timestamp(Utc::now()))
            .eq("workflow_id", operation.workflow_id.as_str())
            .eq("idempotency_key", operation.idempotency_key.as_str())
            .eq("status", STATUS_STARTED);
        let mut driver = self.driver.lock().await;
        let affected = driver.execute(&cmd).await.map_err(pg_error)?;
        if affected == 0 {
            return Err(missing_started_row(
                "operation",
                operation.idempotency_key.as_str(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn load_due_workflow_timeouts(
        &self,
        workflow_name: &str,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<String>, WorkflowError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        if self.timeout_claim_ttl.is_zero() {
            return Err(WorkflowError::Other(
                "Workflow timeout claim TTL must be greater than zero".to_string(),
            ));
        }

        let mut driver = self.driver.lock().await;
        driver.begin().await.map_err(pg_error)?;

        let result = self
            .load_due_workflow_timeouts_tx(&mut driver, workflow_name, now, limit)
            .await;
        finish_tx(&mut driver, result).await
    }

    async fn load_due_workflow_timeouts_tx(
        &self,
        driver: &mut PgDriver,
        workflow_name: &str,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<String>, WorkflowError> {
        let now = timestamp(now);
        let claim_until = timestamp(deadline_from_duration(self.timeout_claim_ttl)?);
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let due = timeout_due_query(&self.tables, workflow_name, now.as_str(), limit);

        let rows = driver.fetch_all(&due).await.map_err(pg_error)?;
        let mut ids = Vec::with_capacity(rows.len());
        for row in rows {
            let workflow_id = required_string(&row, 0, "workflow_id")?;
            let claim = Qail::set(&self.tables.states)
                .set_value("timeout_claimed_until", claim_until.clone())
                .set_value("updated_at", timestamp(Utc::now()))
                .eq("workflow_id", workflow_id.as_str());
            driver.execute(&claim).await.map_err(pg_error)?;
            ids.push(workflow_id);
        }
        Ok(ids)
    }

    pub(crate) async fn begin_workflow_side_effect(
        &self,
        operation: &WorkflowSideEffect,
    ) -> Result<WorkflowSideEffectStatus, WorkflowError> {
        let mut driver = self.driver.lock().await;
        driver.begin().await.map_err(pg_error)?;

        let result = self
            .begin_workflow_side_effect_tx(&mut driver, operation)
            .await;
        finish_tx(&mut driver, result).await
    }

    async fn begin_workflow_side_effect_tx(
        &self,
        driver: &mut PgDriver,
        operation: &WorkflowSideEffect,
    ) -> Result<WorkflowSideEffectStatus, WorkflowError> {
        let existing = Qail::get(&self.tables.side_effects)
            .columns(["status", "result"])
            .eq("operation_id", operation.operation_id.as_str())
            .for_update()
            .limit(1);
        let rows = driver.fetch_all(&existing).await.map_err(pg_error)?;

        if let Some(row) = rows.first() {
            let status = required_string(row, 0, "status")?;
            return match status.as_str() {
                STATUS_STARTED => Err(WorkflowError::Other(format!(
                    "Workflow side effect '{}' is already in progress",
                    operation.operation_id
                ))),
                STATUS_COMPLETED => Ok(WorkflowSideEffectStatus::AlreadyCompleted {
                    result: optional_json(row, 1)?,
                }),
                other => Err(WorkflowError::Other(format!(
                    "Unknown workflow side effect status '{other}'"
                ))),
            };
        }

        let now = timestamp(Utc::now());
        let insert = Qail::add(&self.tables.side_effects)
            .columns([
                "operation_id",
                "workflow_id",
                "state",
                "step_path",
                "kind",
                "status",
                "result",
                "created_at",
                "updated_at",
            ])
            .values([
                QailValue::String(operation.operation_id.clone()),
                QailValue::String(operation.workflow_id.clone()),
                QailValue::String(operation.state.clone()),
                QailValue::String(operation.step_path.clone()),
                QailValue::String(operation.kind.as_str().to_string()),
                QailValue::String(STATUS_STARTED.to_string()),
                QailValue::Null,
                QailValue::String(now.clone()),
                QailValue::String(now),
            ]);

        match driver.execute(&insert).await {
            Ok(_) => Ok(WorkflowSideEffectStatus::Execute),
            Err(err) if is_duplicate(&err) => Err(WorkflowError::Other(format!(
                "Workflow side effect '{}' is already in progress",
                operation.operation_id
            ))),
            Err(err) => Err(pg_error(err)),
        }
    }

    pub(crate) async fn complete_workflow_side_effect(
        &self,
        operation: &WorkflowSideEffect,
        result: Option<&serde_json::Value>,
    ) -> Result<(), WorkflowError> {
        let result = match result {
            Some(value) => QailValue::Json(serde_json::to_string(value).map_err(json_error)?),
            None => QailValue::Null,
        };
        let cmd = Qail::set(&self.tables.side_effects)
            .set_value("status", STATUS_COMPLETED)
            .set_value("result", result)
            .set_value("updated_at", timestamp(Utc::now()))
            .eq("operation_id", operation.operation_id.as_str())
            .eq("status", STATUS_STARTED);
        let mut driver = self.driver.lock().await;
        let affected = driver.execute(&cmd).await.map_err(pg_error)?;
        if affected == 0 {
            return Err(missing_started_row(
                "side effect",
                operation.operation_id.as_str(),
            ));
        }
        Ok(())
    }
}

fn timeout_due_query(
    tables: &PgWorkflowTables,
    workflow_name: &str,
    now: &str,
    limit: i64,
) -> Qail {
    Qail::get(&tables.states)
        .columns(["workflow_id"])
        .eq("definition_name", workflow_name)
        .lte("wait_deadline_at", now)
        .is_not_null("wait_deadline_at")
        .or_filter("timeout_claimed_until", Operator::IsNull, QailValue::Null)
        .or_filter("timeout_claimed_until", Operator::Lt, now)
        .for_update_skip_locked()
        .limit(limit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn due_timeout_query_uses_row_locking() {
        let tables = PgWorkflowTables::default();
        let cmd = timeout_due_query(&tables, "booking", "2026-01-01T00:00:00.000000Z", 10);

        assert!(cmd.lock_mode.is_some());
        assert!(cmd.skip_locked);
    }
}
