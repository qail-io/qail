use super::{TransactionSession, TransactionSessionManager};
use qail_pg::PooledConnection;
use std::time::Duration;

mod create_session;
mod reap;
mod session_ops;

impl TransactionSessionManager {
    /// Create a new session manager.
    pub fn new(
        max_sessions: usize,
        timeout_secs: u64,
        max_lifetime_secs: u64,
        max_statements_per_session: usize,
    ) -> Self {
        let max_lifetime = if max_lifetime_secs == 0 {
            Duration::MAX
        } else {
            Duration::from_secs(max_lifetime_secs)
        };
        let max_statements_per_session = if max_statements_per_session == 0 {
            usize::MAX
        } else {
            max_statements_per_session
        };
        Self {
            sessions: tokio::sync::Mutex::new(std::collections::HashMap::new()),
            max_sessions,
            timeout_secs,
            max_lifetime,
            max_statements_per_session,
        }
    }

    /// Returns the number of active sessions.
    pub async fn active_count(&self) -> usize {
        self.sessions.lock().await.len()
    }

    #[cfg(test)]
    pub async fn insert_test_session_no_conn(
        &self,
        session_id: &str,
        tenant_id: &str,
        created_ago: Duration,
        last_used_ago: Duration,
        statements_executed: usize,
    ) {
        let now = std::time::Instant::now();
        let session = TransactionSession {
            conn: None,
            tenant_id: tenant_id.to_string(),
            user_id: Some("test-user".to_string()),
            created_at: now - created_ago,
            last_used: now - last_used_ago,
            closed: false,
            statements_executed,
            pg_aborted: false,
        };
        let mut sessions = self.sessions.lock().await;
        sessions.insert(
            session_id.to_string(),
            std::sync::Arc::new(tokio::sync::Mutex::new(session)),
        );
        crate::metrics::record_txn_active_sessions(sessions.len());
    }

    pub(super) async fn rollback_and_release(conn: Option<PooledConnection>) {
        if let Some(mut conn) = conn {
            if let Err(e) = conn.rollback().await {
                tracing::warn!(
                    error = %e,
                    "Rollback failed during session termination"
                );
            }
            conn.release().await;
        }
    }
}
