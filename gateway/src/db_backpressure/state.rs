use std::time::Instant;

use qail_core::rls::RlsContext;
use qail_pg::PooledConnection;

use crate::GatewayState;
use crate::auth::AuthContext;
use crate::middleware::ApiError;

use super::{backpressure_api_error, record_acquire_outcome, waiter_key_for_auth};

impl GatewayState {
    /// Acquire RLS-scoped connection with queue backpressure guards.
    pub async fn acquire_with_rls_timeouts_guarded(
        &self,
        waiter_key: &str,
        rls_ctx: RlsContext,
        statement_timeout_ms: u32,
        lock_timeout_ms: u32,
        table: Option<&str>,
    ) -> Result<PooledConnection, ApiError> {
        let _waiter = self
            .db_backpressure
            .enter(waiter_key)
            .map_err(backpressure_api_error)?;

        let started = Instant::now();
        let result = self
            .pool
            .acquire_with_rls_timeouts(rls_ctx, statement_timeout_ms, lock_timeout_ms)
            .await;
        record_acquire_outcome(started, &result);
        result.map_err(|e| ApiError::from_pg_driver_error(&e, table))
    }

    /// Acquire RLS-scoped connection (uses gateway default timeouts).
    pub async fn acquire_with_auth_rls_guarded(
        &self,
        auth: &AuthContext,
        table: Option<&str>,
    ) -> Result<PooledConnection, ApiError> {
        self.acquire_with_auth_rls_timeouts_guarded(
            auth,
            self.config.statement_timeout_ms,
            self.config.lock_timeout_ms,
            table,
        )
        .await
    }

    /// Acquire RLS-scoped connection with explicit timeouts and auth-derived waiter key.
    pub async fn acquire_with_auth_rls_timeouts_guarded(
        &self,
        auth: &AuthContext,
        statement_timeout_ms: u32,
        lock_timeout_ms: u32,
        table: Option<&str>,
    ) -> Result<PooledConnection, ApiError> {
        let waiter_key = waiter_key_for_auth(auth);
        self.acquire_with_rls_timeouts_guarded(
            &waiter_key,
            auth.to_rls_context(),
            statement_timeout_ms,
            lock_timeout_ms,
            table,
        )
        .await
    }

    /// Acquire raw connection with backpressure guards.
    pub async fn acquire_raw_with_auth_guarded(
        &self,
        auth: &AuthContext,
        table: Option<&str>,
    ) -> Result<PooledConnection, ApiError> {
        let waiter_key = waiter_key_for_auth(auth);
        let _waiter = self
            .db_backpressure
            .enter(&waiter_key)
            .map_err(backpressure_api_error)?;

        let started = Instant::now();
        let result = self.pool.acquire_raw().await;
        record_acquire_outcome(started, &result);
        result.map_err(|e| ApiError::from_pg_driver_error(&e, table))
    }

    /// Acquire system-scoped connection with backpressure guards.
    pub async fn acquire_system_guarded(
        &self,
        scope: &str,
        table: Option<&str>,
    ) -> Result<PooledConnection, ApiError> {
        let waiter_key = format!("_system:{}", scope);
        let _waiter = self
            .db_backpressure
            .enter(&waiter_key)
            .map_err(backpressure_api_error)?;

        let started = Instant::now();
        let result = self.pool.acquire_system().await;
        record_acquire_outcome(started, &result);
        result.map_err(|e| ApiError::from_pg_driver_error(&e, table))
    }

    /// Create a transaction session with backpressure guarding on initial pool acquire.
    pub async fn create_txn_session_guarded(
        &self,
        auth: &AuthContext,
        tenant_id: String,
        user_id: Option<String>,
        statement_timeout_ms: u32,
        lock_timeout_ms: u32,
    ) -> Result<String, crate::transaction::TransactionError> {
        let waiter_key = waiter_key_for_auth(auth);
        let _waiter = self.db_backpressure.enter(&waiter_key).map_err(|reason| {
            backpressure_api_error(reason);
            crate::transaction::TransactionError::Pool(
                "transaction begin shed by DB backpressure".to_string(),
            )
        })?;

        let started = Instant::now();
        let result = self
            .transaction_manager
            .create_session(
                &self.pool,
                auth.to_rls_context(),
                tenant_id,
                user_id,
                statement_timeout_ms,
                lock_timeout_ms,
            )
            .await;

        let wait_ms = started.elapsed().as_secs_f64() * 1000.0;
        match &result {
            Ok(_) => crate::metrics::record_db_acquire_wait(wait_ms, "success"),
            Err(crate::transaction::TransactionError::Pool(msg))
                if msg.to_ascii_lowercase().contains("timeout") =>
            {
                crate::metrics::record_db_acquire_timeout();
                crate::metrics::record_db_acquire_wait(wait_ms, "timeout");
            }
            Err(crate::transaction::TransactionError::Pool(_)) => {
                crate::metrics::record_db_acquire_wait(wait_ms, "error");
            }
            Err(_) => crate::metrics::record_db_acquire_wait(wait_ms, "error"),
        }
        result
    }
}
