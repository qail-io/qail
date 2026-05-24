use super::super::TransactionError;
use super::{TransactionSession, TransactionSessionManager};
use qail_core::rls::RlsContext;
use qail_pg::PgPool;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

impl TransactionSessionManager {
    /// Create a new transaction session.
    ///
    /// Acquires a connection from the pool. The RLS checkout already opens
    /// the transaction that pins tenant-local settings for the session.
    /// Returns the session ID (UUID v4).
    #[allow(clippy::too_many_arguments)]
    pub async fn create_session(
        &self,
        pool: &PgPool,
        rls_ctx: RlsContext,
        tenant_id: String,
        user_id: Option<String>,
        auth_fingerprint: String,
        statement_timeout_ms: u32,
        lock_timeout_ms: u32,
    ) -> Result<String, TransactionError> {
        {
            let sessions = self.sessions.lock().await;
            if sessions.len() >= self.max_sessions {
                return Err(TransactionError::SessionLimitReached(self.max_sessions));
            }
        }

        let conn = pool
            .acquire_with_rls_timeouts(rls_ctx, statement_timeout_ms, lock_timeout_ms)
            .await
            .map_err(|e| TransactionError::Pool(e.to_string()))?;

        let session_id = uuid::Uuid::new_v4().to_string();
        let now = Instant::now();

        {
            let mut sessions = self.sessions.lock().await;
            if sessions.len() >= self.max_sessions {
                tracing::warn!(
                    reason = "capacity_guard",
                    session_id = %session_id,
                    "Rejecting transaction session after BEGIN due to session cap race"
                );
                crate::metrics::record_txn_forced_rollback("capacity_guard");
                if let Err(e) = conn.rollback_and_release().await {
                    tracing::warn!(
                        reason = "capacity_guard",
                        error = %e,
                        "Rollback/release failed while rejecting transaction session at capacity"
                    );
                }
                return Err(TransactionError::SessionLimitReached(self.max_sessions));
            }

            let session = TransactionSession {
                conn: Some(conn),
                tenant_id,
                user_id,
                auth_fingerprint,
                created_at: now,
                last_used: now,
                closed: false,
                statements_executed: 0,
                pg_aborted: false,
                mutated_tables: std::collections::HashSet::new(),
            };
            sessions.insert(session_id.clone(), Arc::new(Mutex::new(session)));
            crate::metrics::record_txn_active_sessions(sessions.len());
        }
        crate::metrics::record_txn_session_created();

        tracing::info!(session_id = %session_id, "Transaction session created");

        Ok(session_id)
    }
}
