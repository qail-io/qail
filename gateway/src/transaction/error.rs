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
