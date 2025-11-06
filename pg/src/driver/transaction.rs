//! Transaction control methods for PostgreSQL connection.

use super::{PgConnection, PgResult};

/// Quote a SQL identifier (for savepoint names).
/// Wraps in double-quotes and escapes embedded double-quotes and NUL bytes.
fn quote_savepoint_name(name: &str) -> String {
    let clean = name.replace('\0', "").replace('"', "\"\"");
    format!("\"{}\"", clean)
}

impl PgConnection {
    /// Begin a new transaction.
    /// After calling this, all queries run within the transaction
    /// until `commit()` or `rollback()` is called.
    pub async fn begin_transaction(&mut self) -> PgResult<()> {
        self.execute_simple("BEGIN").await
    }

    /// Commit the current transaction.
    /// Makes all changes since `begin_transaction()` permanent.
    pub async fn commit(&mut self) -> PgResult<()> {
        self.execute_simple("COMMIT").await
    }

    /// Rollback the current transaction.
    /// Discards all changes since `begin_transaction()`.
    pub async fn rollback(&mut self) -> PgResult<()> {
        self.execute_simple("ROLLBACK").await
    }

    /// Create a named savepoint within the current transaction.
    /// Savepoints allow partial rollback within a transaction.
    /// Use `rollback_to()` to return to this savepoint.
    pub async fn savepoint(&mut self, name: &str) -> PgResult<()> {
        self.execute_simple(&format!("SAVEPOINT {}", quote_savepoint_name(name))).await
    }

    /// Rollback to a previously created savepoint.
    /// Discards all changes since the named savepoint was created,
    /// but keeps the transaction open.
    pub async fn rollback_to(&mut self, name: &str) -> PgResult<()> {
        self.execute_simple(&format!("ROLLBACK TO SAVEPOINT {}", quote_savepoint_name(name)))
            .await
    }

    /// Release a savepoint (free resources, if no longer needed).
    pub async fn release_savepoint(&mut self, name: &str) -> PgResult<()> {
        self.execute_simple(&format!("RELEASE SAVEPOINT {}", quote_savepoint_name(name)))
            .await
    }
}
