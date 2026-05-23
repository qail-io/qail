use super::super::TransactionError;
use super::{TransactionSession, TransactionSessionManager};
use std::time::Instant;

impl TransactionSessionManager {
    async fn with_session_inner<F, R>(
        &self,
        session_id: &str,
        tenant_id: &str,
        user_id: Option<&str>,
        allow_aborted: bool,
        f: F,
    ) -> Result<R, TransactionError>
    where
        F: FnOnce(
            &mut TransactionSession,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<R, TransactionError>> + Send + '_>,
        >,
    {
        let session = {
            let sessions = self.sessions.lock().await;
            sessions
                .get(session_id)
                .cloned()
                .ok_or(TransactionError::SessionNotFound)?
        };

        let mut session = session.lock().await;
        if session.closed {
            return Err(TransactionError::SessionNotFound);
        }

        if session.tenant_id != tenant_id {
            return Err(TransactionError::TenantMismatch);
        }
        if session.user_id.as_deref() != user_id {
            return Err(TransactionError::UserMismatch);
        }

        if session.pg_aborted && !allow_aborted {
            return Err(TransactionError::Aborted);
        }

        if session.created_at.elapsed() > self.max_lifetime {
            let age_secs = session.created_at.elapsed().as_secs();
            let statements = session.statements_executed;
            let tenant_id = session.tenant_id.clone();
            session.closed = true;
            let conn = session.conn.take();
            drop(session);

            {
                let mut sessions = self.sessions.lock().await;
                sessions.remove(session_id);
                crate::metrics::record_txn_active_sessions(sessions.len());
            }
            tracing::warn!(
                reason = "lifetime_limit",
                session_id = %session_id,
                tenant_id = %tenant_id,
                age_secs,
                statements,
                max_lifetime_secs = self.max_lifetime.as_secs(),
                "Terminating transaction session due to max lifetime"
            );
            crate::metrics::record_txn_session_expired();
            crate::metrics::record_txn_forced_rollback("lifetime_limit");
            Self::rollback_and_release(conn).await;
            return Err(TransactionError::SessionLifetimeExceeded(
                self.max_lifetime.as_secs(),
            ));
        }

        if session.statements_executed >= self.max_statements_per_session {
            let age_secs = session.created_at.elapsed().as_secs();
            let statements = session.statements_executed;
            let tenant_id = session.tenant_id.clone();
            session.closed = true;
            let conn = session.conn.take();
            drop(session);

            {
                let mut sessions = self.sessions.lock().await;
                sessions.remove(session_id);
                crate::metrics::record_txn_active_sessions(sessions.len());
            }
            tracing::warn!(
                reason = "statement_limit",
                session_id = %session_id,
                tenant_id = %tenant_id,
                age_secs,
                statements,
                max_statements = self.max_statements_per_session,
                "Terminating transaction session due to statement limit"
            );
            crate::metrics::record_txn_statement_limit_hit();
            crate::metrics::record_txn_forced_rollback("statement_limit");
            Self::rollback_and_release(conn).await;
            return Err(TransactionError::StatementLimitReached(
                self.max_statements_per_session,
            ));
        }

        if session.conn.is_none() {
            return Err(TransactionError::SessionNotFound);
        }

        session.statements_executed = session.statements_executed.saturating_add(1);
        session.last_used = Instant::now();

        let result = f(&mut session).await;
        session.last_used = Instant::now();

        if let Err(TransactionError::Database(_)) = &result {
            session.pg_aborted = true;
        }

        result
    }

    /// Get a mutable reference to a session, validating tenant ownership.
    ///
    /// The returned closure holds only the per-session lock.
    pub async fn with_session<F, R>(
        &self,
        session_id: &str,
        tenant_id: &str,
        user_id: Option<&str>,
        f: F,
    ) -> Result<R, TransactionError>
    where
        F: FnOnce(
            &mut TransactionSession,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<R, TransactionError>> + Send + '_>,
        >,
    {
        self.with_session_inner(session_id, tenant_id, user_id, false, f)
            .await
    }

    /// Like [`Self::with_session`], but allows access when the session is in
    /// aborted-transaction state.
    ///
    /// Intended for abort-safe recovery operations (for example:
    /// `ROLLBACK TO SAVEPOINT`).
    pub async fn with_session_allow_aborted<F, R>(
        &self,
        session_id: &str,
        tenant_id: &str,
        user_id: Option<&str>,
        f: F,
    ) -> Result<R, TransactionError>
    where
        F: FnOnce(
            &mut TransactionSession,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<R, TransactionError>> + Send + '_>,
        >,
    {
        self.with_session_inner(session_id, tenant_id, user_id, true, f)
            .await
    }

    /// Close a session: COMMIT or ROLLBACK, then release connection.
    pub async fn close_session(
        &self,
        session_id: &str,
        tenant_id: &str,
        user_id: Option<&str>,
        commit: bool,
    ) -> Result<Vec<String>, TransactionError> {
        let session = {
            let sessions = self.sessions.lock().await;
            sessions
                .get(session_id)
                .cloned()
                .ok_or(TransactionError::SessionNotFound)?
        };

        let mut session = session.lock().await;
        if session.closed {
            return Err(TransactionError::SessionNotFound);
        }
        if session.tenant_id != tenant_id {
            return Err(TransactionError::TenantMismatch);
        }
        if session.user_id.as_deref() != user_id {
            return Err(TransactionError::UserMismatch);
        }

        let was_aborted = session.pg_aborted;
        let mutated_tables = if commit {
            session.mutated_tables.iter().cloned().collect()
        } else {
            Vec::new()
        };
        session.closed = true;
        let conn = session
            .conn
            .take()
            .ok_or(TransactionError::SessionNotFound)?;
        drop(session);

        {
            let mut sessions = self.sessions.lock().await;
            sessions.remove(session_id);
            crate::metrics::record_txn_active_sessions(sessions.len());
        }

        let result = if commit && was_aborted {
            tracing::warn!(
                session_id = %session_id,
                "Transaction commit requested while PostgreSQL transaction is aborted; rolling back"
            );
            match conn.rollback_and_release().await {
                Ok(()) => Err(TransactionError::Aborted),
                Err(e) => Err(TransactionError::Database(e.to_string())),
            }
        } else if commit {
            conn.release_checked()
                .await
                .map_err(|e| TransactionError::Database(e.to_string()))
        } else {
            conn.rollback_and_release()
                .await
                .map_err(|e| TransactionError::Database(e.to_string()))
        };

        if let Err(e) = &result {
            tracing::error!(
                session_id = %session_id,
                error = %e,
                action = if commit { "COMMIT" } else { "ROLLBACK" },
                "Transaction close failed"
            );
            crate::metrics::record_txn_session_closed("error");
        } else {
            tracing::info!(
                session_id = %session_id,
                action = if commit { "COMMIT" } else { "ROLLBACK" },
                "Transaction session closed"
            );
            crate::metrics::record_txn_session_closed(if commit { "commit" } else { "rollback" });
        }

        result.map(|()| mutated_tables)
    }
}
