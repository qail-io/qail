//! Transaction session management for the QAIL Gateway.
//!
//! Provides multi-statement transaction support via pinned connections.
//! Sessions are identified by UUID and bound to authenticated tenants.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

use qail_core::rls::RlsContext;
use qail_pg::PgPool;
use qail_pg::PooledConnection;

/// A single transaction session holding a pinned database connection.
pub struct TransactionSession {
    /// The pooled connection pinned to this transaction.
    pub conn: Option<PooledConnection>,
    /// Tenant ID that owns this session (for isolation enforcement).
    pub tenant_id: String,
    /// User ID that created this session.
    pub user_id: Option<String>,
    /// When this session was created.
    pub created_at: Instant,
    /// When this session was last used.
    pub last_used: Instant,
    /// Set once close/reap has begun; blocks new operations.
    pub closed: bool,
    /// Number of statements executed in this session.
    pub statements_executed: usize,
    /// Set when a query error puts PG in aborted-transaction state.
    /// Further queries are impossible until ROLLBACK or session close.
    pub pg_aborted: bool,
}

/// Manages active transaction sessions with timeout-based cleanup.
pub struct TransactionSessionManager {
    /// Active sessions keyed by session ID (UUID string).
    sessions: Mutex<HashMap<String, Arc<Mutex<TransactionSession>>>>,
    /// Maximum allowed concurrent sessions (prevents pool exhaustion).
    max_sessions: usize,
    /// Session idle timeout in seconds (sessions idle beyond this are reaped).
    timeout_secs: u64,
    /// Maximum wall-clock lifetime for a transaction session.
    max_lifetime: Duration,
    /// Maximum statements allowed per session.
    max_statements_per_session: usize,
}

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
            sessions: Mutex::new(HashMap::new()),
            max_sessions,
            timeout_secs,
            max_lifetime,
            max_statements_per_session,
        }
    }

    /// Create a new transaction session.
    ///
    /// Acquires a connection from the pool, sets RLS context, and issues BEGIN.
    /// Returns the session ID (UUID v4).
    pub async fn create_session(
        &self,
        pool: &PgPool,
        rls_ctx: RlsContext,
        tenant_id: String,
        user_id: Option<String>,
        statement_timeout_ms: u32,
        lock_timeout_ms: u32,
    ) -> Result<String, TransactionError> {
        // Fast pre-check
        {
            let sessions = self.sessions.lock().await;
            if sessions.len() >= self.max_sessions {
                return Err(TransactionError::SessionLimitReached(self.max_sessions));
            }
        }

        // Acquire connection with RLS
        let mut conn = pool
            .acquire_with_rls_timeouts(rls_ctx, statement_timeout_ms, lock_timeout_ms)
            .await
            .map_err(|e| TransactionError::Pool(e.to_string()))?;

        // BEGIN transaction
        if let Err(e) = conn.begin().await {
            // Avoid pool churn: always release on startup failure paths.
            conn.release().await;
            return Err(TransactionError::Database(e.to_string()));
        }

        let session_id = uuid::Uuid::new_v4().to_string();
        let now = Instant::now();

        {
            let mut sessions = self.sessions.lock().await;
            // Hard-cap race guard: concurrent creates can pass the pre-check.
            if sessions.len() >= self.max_sessions {
                tracing::warn!(
                    reason = "capacity_guard",
                    session_id = %session_id,
                    "Rejecting transaction session after BEGIN due to session cap race"
                );
                crate::metrics::record_txn_forced_rollback("capacity_guard");
                if let Err(e) = conn.rollback().await {
                    tracing::warn!(
                        reason = "capacity_guard",
                        error = %e,
                        "Rollback failed while rejecting transaction session at capacity"
                    );
                }
                conn.release().await;
                return Err(TransactionError::SessionLimitReached(self.max_sessions));
            }

            let session = TransactionSession {
                conn: Some(conn),
                tenant_id,
                user_id,
                created_at: now,
                last_used: now,
                closed: false,
                statements_executed: 0,
                pg_aborted: false,
            };
            sessions.insert(session_id.clone(), Arc::new(Mutex::new(session)));
            crate::metrics::record_txn_active_sessions(sessions.len());
        }
        crate::metrics::record_txn_session_created();

        tracing::info!(session_id = %session_id, "Transaction session created");

        Ok(session_id)
    }

    /// Get a mutable reference to a session, validating tenant ownership.
    ///
    /// The returned closure holds only the per-session lock.
    pub async fn with_session<F, R>(
        &self,
        session_id: &str,
        tenant_id: &str,
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

        // Finding 4: reject queries on connections in PG aborted state
        if session.pg_aborted {
            return Err(TransactionError::Aborted);
        }

        if session.created_at.elapsed() > self.max_lifetime {
            let age_secs = session.created_at.elapsed().as_secs();
            let statements = session.statements_executed;
            let tenant_id = session.tenant_id.clone();
            session.closed = true;
            let conn = session.conn.take();
            // Finding 2: remove from HashMap BEFORE dropping session lock
            // to avoid ghost entries counting toward max_sessions
            {
                let mut sessions = self.sessions.lock().await;
                sessions.remove(session_id);
                crate::metrics::record_txn_active_sessions(sessions.len());
            }
            drop(session);
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
            // Finding 2: remove from HashMap BEFORE dropping session lock
            {
                let mut sessions = self.sessions.lock().await;
                sessions.remove(session_id);
                crate::metrics::record_txn_active_sessions(sessions.len());
            }
            drop(session);
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

        // Finding 4: if the closure returned a Database error, the PG connection
        // is likely in aborted-transaction state. Mark session so subsequent
        // queries get a clear error instead of confusing PG "aborted" messages.
        if let Err(TransactionError::Database(_)) = &result {
            session.pg_aborted = true;
        }

        result
    }

    /// Close a session: COMMIT or ROLLBACK, then release connection.
    ///
    /// Finding 1: If the explicit commit/rollback fails, we issue a recovery
    /// ROLLBACK to exit PG's aborted-transaction state before calling
    /// `release()`. This prevents `release()` from destroying the connection
    /// due to a failed redundant COMMIT.
    ///
    /// Finding 3: Tenant check + HashMap removal happen under a single lock
    /// acquisition to eliminate the TOCTOU race.
    pub async fn close_session(
        &self,
        session_id: &str,
        tenant_id: &str,
        commit: bool,
    ) -> Result<(), TransactionError> {
        // Finding 3: single lock acquisition for lookup + tenant check + removal
        let session = {
            let mut sessions = self.sessions.lock().await;
            let arc = sessions
                .get(session_id)
                .cloned()
                .ok_or(TransactionError::SessionNotFound)?;

            // Tenant check under the HashMap lock — no TOCTOU gap
            {
                let guard = arc.lock().await;
                if guard.closed {
                    return Err(TransactionError::SessionNotFound);
                }
                if guard.tenant_id != tenant_id {
                    return Err(TransactionError::TenantMismatch);
                }
            }

            // Remove atomically with the check
            sessions.remove(session_id);
            crate::metrics::record_txn_active_sessions(sessions.len());
            arc
        };

        let mut session = session.lock().await;
        if session.closed {
            return Err(TransactionError::SessionNotFound);
        }
        session.closed = true;
        let mut conn = session
            .conn
            .take()
            .ok_or(TransactionError::SessionNotFound)?;

        let action = if commit { "COMMIT" } else { "ROLLBACK" };

        let result = if commit {
            conn.commit().await
        } else {
            conn.rollback().await
        };

        if let Err(e) = &result {
            tracing::error!(
                session_id = %session_id,
                action = %action,
                error = %e,
                "Transaction {} failed",
                action
            );
            // Finding 1: recover from PG aborted-transaction state.
            // If commit/rollback failed, PG rejects further commands.
            // Issue a recovery ROLLBACK so release() doesn't destroy
            // the connection when its redundant COMMIT fails.
            if let Err(rb_err) = conn.rollback().await {
                tracing::warn!(
                    session_id = %session_id,
                    error = %rb_err,
                    "Recovery ROLLBACK also failed; connection will be destroyed on release"
                );
            }
            crate::metrics::record_txn_session_closed("error");
        } else {
            tracing::info!(
                session_id = %session_id,
                action = %action,
                "Transaction session closed"
            );
            crate::metrics::record_txn_session_closed(if commit { "commit" } else { "rollback" });
        }

        // Release connection back to pool (always, even if commit/rollback failed)
        conn.release().await;

        result.map_err(|e| TransactionError::Database(e.to_string()))
    }

    /// Reap expired sessions. Called by the background reaper task.
    ///
    /// Sessions idle for longer than `timeout_secs` are rolled back and released.
    pub async fn reap_expired(&self) {
        let timeout = std::time::Duration::from_secs(self.timeout_secs);

        let session_refs: Vec<(String, Arc<Mutex<TransactionSession>>)> = {
            let sessions = self.sessions.lock().await;
            sessions
                .iter()
                .map(|(id, session)| (id.clone(), Arc::clone(session)))
                .collect()
        };

        // Finding 6: use try_lock to avoid blocking on sessions executing queries.
        // Sessions we can't lock are skipped and checked on the next tick.
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
                // Finding 2: update metrics immediately, then do slow rollback
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
        let now = Instant::now();
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
        sessions.insert(session_id.to_string(), Arc::new(Mutex::new(session)));
        crate::metrics::record_txn_active_sessions(sessions.len());
    }

    /// Rollback and release a connection. Used by termination paths after
    /// the session has already been removed from the HashMap and marked closed.
    async fn rollback_and_release(conn: Option<PooledConnection>) {
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

/// Spawn the background reaper task that cleans up expired transaction sessions.
pub fn spawn_reaper(manager: Arc<TransactionSessionManager>) {
    let interval_secs = std::cmp::max(manager.timeout_secs / 2, 5);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
        loop {
            interval.tick().await;
            manager.reap_expired().await;
        }
    });
}

/// Errors returned by transaction session operations.
#[derive(Debug)]
pub enum TransactionError {
    /// Maximum concurrent sessions reached.
    SessionLimitReached(usize),
    /// Session ID not found (expired or invalid).
    SessionNotFound,
    /// Authenticated tenant_id doesn't match session owner.
    TenantMismatch,
    /// Connection pool error.
    Pool(String),
    /// Database query error.
    Database(String),
    /// Query was rejected (dangerous action).
    Rejected(String),
    /// Session exceeded configured wall-clock lifetime.
    SessionLifetimeExceeded(u64),
    /// Session exceeded configured statement count.
    StatementLimitReached(usize),
    /// PG connection is in aborted-transaction state after a query error.
    /// Client must ROLLBACK or close the session.
    Aborted,
}

impl std::fmt::Display for TransactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SessionLimitReached(n) => {
                write!(f, "Transaction session limit reached (max {})", n)
            }
            Self::SessionNotFound => write!(f, "Transaction session not found or expired"),
            Self::TenantMismatch => write!(f, "Transaction session belongs to a different tenant"),
            Self::Pool(e) => write!(f, "Pool error: {}", e),
            Self::Database(e) => write!(f, "Database error: {}", e),
            Self::Rejected(e) => write!(f, "Query rejected: {}", e),
            Self::SessionLifetimeExceeded(secs) => write!(
                f,
                "Transaction session exceeded maximum lifetime ({}s)",
                secs
            ),
            Self::StatementLimitReached(max) => write!(
                f,
                "Transaction session exceeded statement limit (max {})",
                max
            ),
            Self::Aborted => write!(
                f,
                "Transaction is in aborted state due to a previous query error. \
                 Issue /txn/rollback to close the session, or /txn/savepoint \
                 with action 'rollback' to recover to a savepoint."
            ),
        }
    }
}

impl std::error::Error for TransactionError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{reset_txn_test_metrics, txn_test_metrics_snapshot};

    async fn insert_test_session(
        mgr: &TransactionSessionManager,
        id: &str,
        tenant: &str,
        created_ago: Duration,
        last_used_ago: Duration,
        statements_executed: usize,
    ) {
        let now = Instant::now();
        let session = TransactionSession {
            conn: None,
            tenant_id: tenant.to_string(),
            user_id: Some("test-user".to_string()),
            created_at: now - created_ago,
            last_used: now - last_used_ago,
            closed: false,
            statements_executed,
            pg_aborted: false,
        };
        let mut sessions = mgr.sessions.lock().await;
        sessions.insert(id.to_string(), Arc::new(Mutex::new(session)));
        crate::metrics::record_txn_active_sessions(sessions.len());
    }

    #[test]
    fn test_transaction_error_display() {
        let err = TransactionError::SessionLimitReached(10);
        assert!(err.to_string().contains("limit reached"));
        assert!(err.to_string().contains("10"));

        let err = TransactionError::SessionNotFound;
        assert!(err.to_string().contains("not found"));

        let err = TransactionError::TenantMismatch;
        assert!(err.to_string().contains("different tenant"));

        let err = TransactionError::SessionLifetimeExceeded(900);
        assert!(err.to_string().contains("900"));

        let err = TransactionError::StatementLimitReached(1000);
        assert!(err.to_string().contains("1000"));
    }

    #[tokio::test]
    async fn test_session_manager_respects_limit() {
        let mgr = TransactionSessionManager::new(2, 30, 900, 1000);
        assert_eq!(mgr.active_count().await, 0);
    }

    #[tokio::test]
    async fn test_reap_expired_empty() {
        let mgr = TransactionSessionManager::new(10, 30, 900, 1000);
        // Should not panic on empty sessions
        mgr.reap_expired().await;
    }

    #[tokio::test]
    async fn test_with_session_enforces_lifetime_limit_and_records_metrics() {
        let _serial = crate::metrics::txn_test_serial_guard().await;
        reset_txn_test_metrics();
        let mgr = TransactionSessionManager::new(10, 30, 1, 1000);
        insert_test_session(
            &mgr,
            "s_lifetime",
            "tenant_a",
            Duration::from_secs(5),
            Duration::from_secs(0),
            0,
        )
        .await;

        let result = mgr
            .with_session("s_lifetime", "tenant_a", |_session| {
                Box::pin(async move { Ok(()) })
            })
            .await;

        assert!(matches!(
            result,
            Err(TransactionError::SessionLifetimeExceeded(1))
        ));
        assert_eq!(mgr.active_count().await, 0);

        let snapshot = txn_test_metrics_snapshot();
        assert_eq!(snapshot.expired, 1);
        assert_eq!(snapshot.forced_lifetime, 1);
        assert_eq!(snapshot.active, 0);
    }

    #[tokio::test]
    async fn test_with_session_enforces_statement_limit_and_records_metrics() {
        let _serial = crate::metrics::txn_test_serial_guard().await;
        reset_txn_test_metrics();
        let mgr = TransactionSessionManager::new(10, 30, 900, 1);
        insert_test_session(
            &mgr,
            "s_stmt",
            "tenant_b",
            Duration::from_secs(0),
            Duration::from_secs(0),
            1,
        )
        .await;

        let result = mgr
            .with_session("s_stmt", "tenant_b", |_session| {
                Box::pin(async move { Ok(()) })
            })
            .await;

        assert!(matches!(
            result,
            Err(TransactionError::StatementLimitReached(1))
        ));
        assert_eq!(mgr.active_count().await, 0);

        let snapshot = txn_test_metrics_snapshot();
        assert_eq!(snapshot.statement_limit_hit, 1);
        assert_eq!(snapshot.forced_statement, 1);
        assert_eq!(snapshot.active, 0);
    }

    #[tokio::test]
    async fn test_reap_expired_records_idle_timeout_metrics() {
        let _serial = crate::metrics::txn_test_serial_guard().await;
        reset_txn_test_metrics();
        let mgr = TransactionSessionManager::new(10, 1, 900, 1000);
        insert_test_session(
            &mgr,
            "s_idle",
            "tenant_c",
            Duration::from_secs(10),
            Duration::from_secs(5),
            3,
        )
        .await;

        mgr.reap_expired().await;

        assert_eq!(mgr.active_count().await, 0);
        let snapshot = txn_test_metrics_snapshot();
        assert_eq!(snapshot.forced_idle, 1);
        assert_eq!(snapshot.active, 0);
    }
}
