use super::{TransactionSession, TransactionSessionManager};
use std::sync::Arc;
use tokio::sync::Mutex;

impl TransactionSessionManager {
    /// Reap expired sessions. Called by the background reaper task.
    pub async fn reap_expired(&self) {
        let timeout = std::time::Duration::from_secs(self.timeout_secs);

        let session_refs: Vec<(String, Arc<Mutex<TransactionSession>>)> = {
            let sessions = self.sessions.lock().await;
            sessions
                .iter()
                .map(|(id, session)| (id.clone(), Arc::clone(session)))
                .collect()
        };

        let mut expired_ids = Vec::new();
        for (id, session) in &session_refs {
            if let Ok(guard) = session.try_lock()
                && !guard.closed
                && guard.last_used.elapsed() > timeout
            {
                expired_ids.push(id.clone());
            }
        }

        for id in &expired_ids {
            let session = {
                let mut sessions = self.sessions.lock().await;
                sessions.remove(id)
            };

            if let Some(session) = session {
                let mut session = session.lock().await;
                if session.closed {
                    continue;
                }
                session.closed = true;
                let conn = session.conn.take();

                let tenant_id = session.tenant_id.clone();
                let idle_secs = session.last_used.elapsed().as_secs();
                let age_secs = session.created_at.elapsed().as_secs();
                let statements = session.statements_executed;
                drop(session);
                tracing::warn!(
                    reason = "idle_timeout",
                    session_id = %id,
                    tenant_id = %tenant_id,
                    idle_secs,
                    age_secs,
                    statements,
                    idle_timeout_secs = self.timeout_secs,
                    "Reaping expired transaction session — forcing rollback"
                );
                {
                    let sessions = self.sessions.lock().await;
                    crate::metrics::record_txn_active_sessions(sessions.len());
                }
                crate::metrics::record_txn_forced_rollback("idle_timeout");
                Self::rollback_and_release(conn).await;
            }
        }

        if !expired_ids.is_empty() {
            tracing::info!(
                count = expired_ids.len(),
                "Reaped expired transaction sessions"
            );
        }
    }
}
