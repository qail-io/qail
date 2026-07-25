use super::super::TransactionError;
use super::{TransactionSession, TransactionSessionManager};
use qail_core::rls::RlsContext;
use qail_pg::PgPool;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

pub(crate) struct TransactionSessionCreate {
    pub rls_ctx: RlsContext,
    pub tenant_id: String,
    pub user_id: Option<String>,
    pub auth_fingerprint: String,
    pub statement_timeout_ms: u32,
    pub lock_timeout_ms: u32,
}

impl TransactionSessionManager {
    /// Create a new transaction session.
    ///
    /// Acquires a connection from the pool. The RLS checkout already opens
    /// the transaction that pins tenant-local settings for the session.
    /// Returns the session ID (UUID v4).
    pub(crate) async fn create_session(
        &self,
        pool: &PgPool,
        request: TransactionSessionCreate,
    ) -> Result<String, TransactionError> {
        let TransactionSessionCreate {
            rls_ctx,
            tenant_id,
            user_id,
            auth_fingerprint,
            statement_timeout_ms,
            lock_timeout_ms,
        } = request;

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

        let mut conn = Some(conn);
        {
            let mut sessions = self.sessions.lock().await;
            let at_capacity = sessions.len() >= self.max_sessions;
            if !at_capacity {
                let session = TransactionSession {
                    conn: conn.take(),
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
        }

        if let Some(conn) = conn {
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

        crate::metrics::record_txn_session_created();

        tracing::info!(session_id = %session_id, "Transaction session created");

        Ok(session_id)
    }
}
