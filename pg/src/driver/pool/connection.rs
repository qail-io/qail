//! Pooled connection wrapper: struct, accessors, RLS cleanup, transaction control,
//! COPY export, pipeline, LISTEN/NOTIFY delegation, and Drop.

use super::churn::{decrement_active_count_saturating, pool_churn_record_destroy};
use super::lifecycle::{PgPoolInner, execute_simple_with_timeout};
use crate::driver::{PgConnection, PgError, PgResult};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

/// A pooled connection with creation timestamp for idle tracking.
pub(super) struct PooledConn {
    pub(super) conn: PgConnection,
    pub(super) created_at: Instant,
    pub(super) last_used: Instant,
}

/// A pooled connection handle.
///
/// Use [`PooledConnection::release`] for deterministic reset+return behavior.
/// If dropped without `release()`, the pool performs best-effort bounded async
/// cleanup; on any uncertainty it destroys the connection (fail-closed).
pub struct PooledConnection {
    pub(super) conn: Option<PgConnection>,
    pub(super) pool: Arc<PgPoolInner>,
    pub(super) rls_dirty: bool,
    pub(super) created_at: Instant,
}

impl PooledConnection {
    /// Get a reference to the underlying connection, returning an error
    /// if the connection has already been released.
    pub(super) fn conn_ref(&self) -> PgResult<&PgConnection> {
        self.conn
            .as_ref()
            .ok_or_else(|| PgError::Connection("Connection already released back to pool".into()))
    }

    /// Get a mutable reference to the underlying connection, returning an error
    /// if the connection has already been released.
    pub(super) fn conn_mut(&mut self) -> PgResult<&mut PgConnection> {
        self.conn
            .as_mut()
            .ok_or_else(|| PgError::Connection("Connection already released back to pool".into()))
    }

    /// Get a shared reference to the underlying connection.
    ///
    /// Returns an error if the connection has already been released.
    pub fn get(&self) -> PgResult<&PgConnection> {
        self.conn_ref()
    }

    /// Get a mutable reference to the underlying connection.
    ///
    /// Returns an error if the connection has already been released.
    pub fn get_mut(&mut self) -> PgResult<&mut PgConnection> {
        self.conn_mut()
    }

    /// Get a token to cancel the currently running query.
    pub fn cancel_token(&self) -> PgResult<crate::driver::CancelToken> {
        let conn = self.conn_ref()?;
        let (process_id, secret_key_bytes) = conn.get_cancel_key_bytes();
        Ok(crate::driver::CancelToken {
            host: self.pool.config.host.clone(),
            port: self.pool.config.port,
            process_id,
            secret_key_bytes: secret_key_bytes.to_vec(),
        })
    }

    /// Deterministic connection cleanup and pool return.
    ///
    /// This is the **correct** way to return a connection to the pool.
    /// COMMITs the transaction (which auto-resets transaction-local RLS
    /// session variables) and returns the connection to the pool with
    /// prepared statement caches intact.
    ///
    /// If cleanup fails, the connection is destroyed (not returned to pool).
    ///
    /// # Usage
    /// ```ignore
    /// let mut conn = pool.acquire_with_rls(ctx).await?;
    /// let result = conn.fetch_all_cached(&cmd).await;
    /// conn.release().await; // COMMIT + return to pool
    /// result
    /// ```
    pub async fn release(mut self) {
        if let Some(mut conn) = self.conn.take() {
            if conn.is_io_desynced() {
                tracing::warn!(
                    host = %self.pool.config.host,
                    port = self.pool.config.port,
                    user = %self.pool.config.user,
                    db = %self.pool.config.database,
                    "pool_release_desynced: dropping connection due to prior I/O/protocol desync"
                );
                decrement_active_count_saturating(&self.pool.active_count);
                self.pool.semaphore.add_permits(1);
                pool_churn_record_destroy(&self.pool.config, "release_desynced");
                return;
            }
            // COMMIT the transaction opened by acquire_with_rls.
            // Transaction-local set_config values auto-reset on COMMIT,
            // so no explicit RLS cleanup is needed.
            // Prepared statements survive — they are NOT transaction-scoped.
            let reset_timeout = self.pool.config.connect_timeout;
            if let Err(e) = execute_simple_with_timeout(
                &mut conn,
                crate::driver::rls::reset_sql(),
                reset_timeout,
                "pool release reset/COMMIT",
            )
            .await
            {
                tracing::error!(
                    host = %self.pool.config.host,
                    port = self.pool.config.port,
                    user = %self.pool.config.user,
                    db = %self.pool.config.database,
                    timeout_ms = reset_timeout.as_millis() as u64,
                    error = %e,
                    "pool_release_failed: reset/COMMIT failed; dropping connection to prevent state leak"
                );
                decrement_active_count_saturating(&self.pool.active_count);
                self.pool.semaphore.add_permits(1);
                pool_churn_record_destroy(&self.pool.config, "release_reset_failed");
                return; // Connection destroyed — not returned to pool
            }

            self.pool.return_connection(conn, self.created_at).await;
        }
    }

    // ==================== TRANSACTION CONTROL ====================

    /// Begin an explicit transaction on this pooled connection.
    ///
    /// Use this when you need multi-statement atomicity beyond the
    /// implicit transaction created by `acquire_with_rls()`.
    ///
    /// # Example
    /// ```ignore
    /// let mut conn = pool.acquire_with_rls(ctx).await?;
    /// conn.begin().await?;
    /// conn.execute(&insert1).await?;
    /// conn.execute(&insert2).await?;
    /// conn.commit().await?;
    /// conn.release().await;
    /// ```
    pub async fn begin(&mut self) -> PgResult<()> {
        self.conn_mut()?.begin_transaction().await
    }

    /// Commit the current transaction.
    /// Makes all changes since `begin()` permanent.
    pub async fn commit(&mut self) -> PgResult<()> {
        self.conn_mut()?.commit().await
    }

    /// Rollback the current transaction.
    /// Discards all changes since `begin()`.
    pub async fn rollback(&mut self) -> PgResult<()> {
        self.conn_mut()?.rollback().await
    }

    /// Create a named savepoint within the current transaction.
    /// Use `rollback_to()` to return to this savepoint.
    pub async fn savepoint(&mut self, name: &str) -> PgResult<()> {
        self.conn_mut()?.savepoint(name).await
    }

    /// Rollback to a previously created savepoint.
    /// Discards changes since the savepoint, but keeps the transaction open.
    pub async fn rollback_to(&mut self, name: &str) -> PgResult<()> {
        self.conn_mut()?.rollback_to(name).await
    }

    /// Release a savepoint (free resources).
    /// After release, the savepoint cannot be rolled back to.
    pub async fn release_savepoint(&mut self, name: &str) -> PgResult<()> {
        self.conn_mut()?.release_savepoint(name).await
    }

    /// Execute multiple QAIL commands in a single PG pipeline round-trip.
    ///
    /// Sends all queries as Parse+Bind+Execute in one write, receives all
    /// responses in one read. Returns raw column data per query per row.
    ///
    /// This is the fastest path for batch operations — amortizes TCP
    /// overhead across N queries into a single syscall pair.
    pub async fn pipeline_execute_rows_ast(
        &mut self,
        cmds: &[qail_core::ast::Qail],
    ) -> PgResult<Vec<Vec<Vec<Option<Vec<u8>>>>>> {
        let conn = self.conn_mut()?;
        conn.pipeline_execute_rows_ast(cmds).await
    }

    /// Run `EXPLAIN (FORMAT JSON)` on a Qail command and return cost estimates.
    ///
    /// Uses `simple_query` under the hood — no additional round-trips beyond
    /// the single EXPLAIN statement. Returns `None` if parsing fails or
    /// the EXPLAIN output is unexpected.
    pub async fn explain_estimate(
        &mut self,
        cmd: &qail_core::ast::Qail,
    ) -> PgResult<Option<crate::driver::explain::ExplainEstimate>> {
        use qail_core::transpiler::ToSql;

        let sql = cmd.to_sql();
        let explain_sql = format!("EXPLAIN (FORMAT JSON) {}", sql);

        let rows = self.conn_mut()?.simple_query(&explain_sql).await?;

        // PostgreSQL returns the JSON plan as a single text column across one or more rows
        let mut json_output = String::new();
        for row in &rows {
            if let Some(Some(val)) = row.columns.first()
                && let Ok(text) = std::str::from_utf8(val)
            {
                json_output.push_str(text);
            }
        }

        Ok(crate::driver::explain::parse_explain_json(&json_output))
    }

    // ─── LISTEN / NOTIFY delegation ─────────────────────────────────

    /// Subscribe to a PostgreSQL notification channel.
    ///
    /// Delegates to [`PgConnection::listen`].
    pub async fn listen(&mut self, channel: &str) -> PgResult<()> {
        self.conn_mut()?.listen(channel).await
    }

    /// Unsubscribe from a PostgreSQL notification channel.
    ///
    /// Delegates to [`PgConnection::unlisten`].
    pub async fn unlisten(&mut self, channel: &str) -> PgResult<()> {
        self.conn_mut()?.unlisten(channel).await
    }

    /// Unsubscribe from all notification channels.
    ///
    /// Delegates to [`PgConnection::unlisten_all`].
    pub async fn unlisten_all(&mut self) -> PgResult<()> {
        self.conn_mut()?.unlisten_all().await
    }

    /// Wait for the next notification, blocking until one arrives.
    ///
    /// Delegates to [`PgConnection::recv_notification`].
    /// Useful for dedicated LISTEN connections in background tasks.
    pub async fn recv_notification(
        &mut self,
    ) -> PgResult<crate::driver::notification::Notification> {
        self.conn_mut()?.recv_notification().await
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(mut conn) = self.conn.take() {
            // Safety net: connection was NOT released via `release()`.
            // Best-effort strategy:
            // 1) If connection is already desynced, destroy immediately.
            // 2) Else, queue bounded async reset+return cleanup.
            // 3) If cleanup queue/runtime unavailable, destroy.
            //
            // This preserves security (fail-closed) while reducing churn under
            // accidental early-returns in handler code.
            tracing::warn!(
                host = %self.pool.config.host,
                port = self.pool.config.port,
                user = %self.pool.config.user,
                db = %self.pool.config.database,
                rls_dirty = self.rls_dirty,
                "pool_connection_leaked: dropped without release()"
            );
            if conn.is_io_desynced() {
                tracing::warn!(
                    host = %self.pool.config.host,
                    port = self.pool.config.port,
                    user = %self.pool.config.user,
                    db = %self.pool.config.database,
                    "pool_connection_leaked_desynced: destroying immediately"
                );
                decrement_active_count_saturating(&self.pool.active_count);
                self.pool.semaphore.add_permits(1);
                pool_churn_record_destroy(&self.pool.config, "dropped_without_release_desynced");
                return;
            }

            let mut inflight = self.pool.leaked_cleanup_inflight.load(Ordering::Relaxed);
            let max_inflight = self.pool.config.leaked_cleanup_queue;
            loop {
                if inflight >= max_inflight {
                    tracing::warn!(
                        host = %self.pool.config.host,
                        port = self.pool.config.port,
                        user = %self.pool.config.user,
                        db = %self.pool.config.database,
                        max_inflight,
                        "pool_connection_leaked_cleanup_queue_full: destroying connection"
                    );
                    decrement_active_count_saturating(&self.pool.active_count);
                    self.pool.semaphore.add_permits(1);
                    pool_churn_record_destroy(
                        &self.pool.config,
                        "dropped_without_release_cleanup_queue_full",
                    );
                    return;
                }

                match self.pool.leaked_cleanup_inflight.compare_exchange_weak(
                    inflight,
                    inflight + 1,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => inflight = actual,
                }
            }

            let pool = std::sync::Arc::clone(&self.pool);
            let created_at = self.created_at;
            let reset_timeout = pool.config.connect_timeout;
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    handle.spawn(async move {
                        let cleanup_ok = execute_simple_with_timeout(
                            &mut conn,
                            crate::driver::rls::reset_sql(),
                            reset_timeout,
                            "pool leaked cleanup reset/COMMIT",
                        )
                        .await
                        .is_ok();

                        if cleanup_ok && !conn.is_io_desynced() {
                            pool.return_connection(conn, created_at).await;
                        } else {
                            tracing::warn!(
                                host = %pool.config.host,
                                port = pool.config.port,
                                user = %pool.config.user,
                                db = %pool.config.database,
                                timeout_ms = reset_timeout.as_millis() as u64,
                                "pool_connection_leaked_cleanup_failed: destroying connection"
                            );
                            decrement_active_count_saturating(&pool.active_count);
                            pool.semaphore.add_permits(1);
                            pool_churn_record_destroy(
                                &pool.config,
                                "dropped_without_release_cleanup_failed",
                            );
                        }

                        pool.leaked_cleanup_inflight.fetch_sub(1, Ordering::AcqRel);
                    });
                }
                Err(_) => {
                    pool.leaked_cleanup_inflight.fetch_sub(1, Ordering::AcqRel);
                    tracing::warn!(
                        host = %pool.config.host,
                        port = pool.config.port,
                        user = %pool.config.user,
                        db = %pool.config.database,
                        "pool_connection_leaked_no_runtime: destroying connection"
                    );
                    decrement_active_count_saturating(&pool.active_count);
                    pool.semaphore.add_permits(1);
                    pool_churn_record_destroy(&pool.config, "dropped_without_release_no_runtime");
                }
            }
        }
    }
}
