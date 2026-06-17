use async_trait::async_trait;
use chrono::{DateTime, Utc};
use qail_workflow::{
    ChannelKind, ChargeRequest, ChargeResponse, PaymentKind, WorkflowContext, WorkflowError,
    WorkflowExecutor, WorkflowLease, WorkflowOperation, WorkflowOperationStatus,
    WorkflowSideEffect, WorkflowSideEffectStatus,
};

use crate::store::PgWorkflowStore;

/// Workflow executor wrapper that delegates business side effects and stores
/// runtime guarantees in PostgreSQL.
pub struct PgWorkflowExecutor<E> {
    inner: E,
    store: PgWorkflowStore,
}

impl<E> PgWorkflowExecutor<E> {
    /// Wrap an application executor with Postgres-backed workflow storage.
    pub fn new(inner: E, store: PgWorkflowStore) -> Self {
        Self { inner, store }
    }

    /// Return the underlying store.
    pub fn store(&self) -> &PgWorkflowStore {
        &self.store
    }

    /// Return the wrapped application executor.
    pub fn inner(&self) -> &E {
        &self.inner
    }
}

#[async_trait]
impl<E> WorkflowExecutor for PgWorkflowExecutor<E>
where
    E: WorkflowExecutor,
{
    async fn execute_query(&self, cmd_json: &str) -> Result<serde_json::Value, WorkflowError> {
        self.inner.execute_query(cmd_json).await
    }

    async fn send_notification(
        &self,
        channel: &ChannelKind,
        recipient: &str,
        template: &str,
        params: &serde_json::Value,
    ) -> Result<(), WorkflowError> {
        self.inner
            .send_notification(channel, recipient, template, params)
            .await
    }

    async fn save_state(&self, ctx: &WorkflowContext) -> Result<(), WorkflowError> {
        self.store.save_state(ctx).await
    }

    async fn load_state(
        &self,
        workflow_id: &str,
    ) -> Result<Option<WorkflowContext>, WorkflowError> {
        self.store.load_state(workflow_id).await
    }

    async fn create_charge(
        &self,
        provider: &PaymentKind,
        request: ChargeRequest,
    ) -> Result<ChargeResponse, WorkflowError> {
        self.inner.create_charge(provider, request).await
    }

    async fn acquire_workflow_lease(&self, lease: &WorkflowLease) -> Result<bool, WorkflowError> {
        self.store.acquire_workflow_lease(lease).await
    }

    async fn release_workflow_lease(&self, lease: &WorkflowLease) -> Result<(), WorkflowError> {
        self.store.release_workflow_lease(lease).await
    }

    async fn begin_workflow_operation(
        &self,
        operation: &WorkflowOperation,
    ) -> Result<WorkflowOperationStatus, WorkflowError> {
        self.store.begin_workflow_operation(operation).await
    }

    async fn complete_workflow_operation(
        &self,
        operation: &WorkflowOperation,
        state: &str,
    ) -> Result<(), WorkflowError> {
        self.store
            .complete_workflow_operation(operation, state)
            .await
    }

    async fn fail_workflow_operation(
        &self,
        operation: &WorkflowOperation,
        error: &str,
    ) -> Result<(), WorkflowError> {
        self.store.fail_workflow_operation(operation, error).await
    }

    async fn load_due_workflow_timeouts(
        &self,
        workflow_name: &str,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<String>, WorkflowError> {
        self.store
            .load_due_workflow_timeouts(workflow_name, now, limit)
            .await
    }

    async fn begin_workflow_side_effect(
        &self,
        operation: &WorkflowSideEffect,
    ) -> Result<WorkflowSideEffectStatus, WorkflowError> {
        self.store.begin_workflow_side_effect(operation).await
    }

    async fn complete_workflow_side_effect(
        &self,
        operation: &WorkflowSideEffect,
        result: Option<&serde_json::Value>,
    ) -> Result<(), WorkflowError> {
        self.store
            .complete_workflow_side_effect(operation, result)
            .await
    }
}
