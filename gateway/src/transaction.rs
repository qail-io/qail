//! Transaction session management for the QAIL Gateway.
//!
//! Provides multi-statement transaction support via pinned connections.
//! Sessions are identified by UUID and bound to authenticated tenants.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

use qail_pg::PooledConnection;

mod error;
mod manager;
mod reaper;

pub use error::TransactionError;
pub use reaper::spawn_reaper;

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

#[cfg(test)]
mod tests;
