//! Transaction control methods for PostgreSQL connection.

use super::{PgConnection, PgResult};

impl PgConnection {
    /// Begin a new transaction.
    ///
    /// After calling this, all queries run within the transaction
    /// until `commit()` or `rollback()` is called.
    pub async fn begin_transaction(&mut self) -> PgResult<()> {
        self.execute_simple("BEGIN").await
    }

    /// Commit the current transaction.
    ///
    /// Makes all changes since `begin_transaction()` permanent.
    pub async fn commit(&mut self) -> PgResult<()> {
        self.execute_simple("COMMIT").await
    }

    /// Rollback the current transaction.
    ///
    /// Discards all changes since `begin_transaction()`.
    pub async fn rollback(&mut self) -> PgResult<()> {
        self.execute_simple("ROLLBACK").await
    }
}
