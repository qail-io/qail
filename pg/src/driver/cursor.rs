//! Streaming cursor methods for PostgreSQL connection.

use super::{PgConnection, PgResult};

impl PgConnection {
    /// Declare a cursor for streaming large result sets.
    /// This uses PostgreSQL's DECLARE CURSOR to avoid loading all rows into memory.
    ///
    /// Uses Extended Query Protocol when bind parameters are present, so parameterized
    /// queries (with $1, $2, etc.) are correctly resolved.
    pub(crate) async fn declare_cursor(
        &mut self,
        name: &str,
        sql: &str,
        params: &[Option<Vec<u8>>],
    ) -> PgResult<()> {
        let declare_sql = format!("DECLARE {} CURSOR FOR {}", name, sql);
        if params.is_empty() {
            self.execute_simple(&declare_sql).await
        } else {
            // Extended Query Protocol — bind params to the DECLARE CURSOR statement.
            // `query()` sends Parse/Bind/Execute/Sync with binary parameters.
            self.query(&declare_sql, params).await?;
            Ok(())
        }
    }

    /// Fetch rows from a cursor in batches.
    pub(crate) async fn fetch_cursor(
        &mut self,
        name: &str,
        batch_size: usize,
    ) -> PgResult<Option<Vec<Vec<Option<Vec<u8>>>>>> {
        let fetch_sql = format!("FETCH {} FROM {}", batch_size, name);
        let rows = self.query(&fetch_sql, &[]).await?;

        if rows.is_empty() {
            Ok(None)
        } else {
            Ok(Some(rows))
        }
    }

    pub(crate) async fn close_cursor(&mut self, name: &str) -> PgResult<()> {
        let close_sql = format!("CLOSE {}", name);
        self.execute_simple(&close_sql).await
    }
}
