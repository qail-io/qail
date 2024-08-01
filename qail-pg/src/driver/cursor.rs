//! Streaming cursor methods for PostgreSQL connection.

use super::{PgConnection, PgResult};

impl PgConnection {
    /// Declare a cursor for streaming large result sets.
    ///
    /// This uses PostgreSQL's DECLARE CURSOR to avoid loading all rows into memory.
    pub(crate) async fn declare_cursor(&mut self, name: &str, sql: &str) -> PgResult<()> {
        let declare_sql = format!("DECLARE {} CURSOR FOR {}", name, sql);
        self.execute_simple(&declare_sql).await
    }

    /// Fetch rows from a cursor in batches.
    ///
    /// Returns None when cursor is exhausted.
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

    /// Close a cursor and release server resources.
    pub(crate) async fn close_cursor(&mut self, name: &str) -> PgResult<()> {
        let close_sql = format!("CLOSE {}", name);
        self.execute_simple(&close_sql).await
    }
}
