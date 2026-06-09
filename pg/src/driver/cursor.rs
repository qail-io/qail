//! Streaming cursor methods for PostgreSQL connection.

use super::{PgConnection, PgError, PgResult};

fn quote_cursor_name(name: &str) -> PgResult<String> {
    if name.is_empty() {
        return Err(PgError::Query("cursor name must not be empty".to_string()));
    }
    if name.as_bytes().contains(&0) {
        return Err(PgError::Query(
            "cursor name must not contain NUL bytes".to_string(),
        ));
    }
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}

fn validate_cursor_batch_size(batch_size: usize) -> PgResult<()> {
    if batch_size == 0 {
        return Err(PgError::Query(
            "cursor batch_size must be greater than 0".to_string(),
        ));
    }
    Ok(())
}

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
        let name = quote_cursor_name(name)?;
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
        validate_cursor_batch_size(batch_size)?;
        let name = quote_cursor_name(name)?;
        let fetch_sql = format!("FETCH {} FROM {}", batch_size, name);
        let rows = self.query(&fetch_sql, &[]).await?;

        if rows.is_empty() {
            Ok(None)
        } else {
            Ok(Some(rows))
        }
    }

    pub(crate) async fn close_cursor(&mut self, name: &str) -> PgResult<()> {
        let name = quote_cursor_name(name)?;
        let close_sql = format!("CLOSE {}", name);
        self.execute_simple(&close_sql).await
    }
}

#[cfg(test)]
mod tests {
    use super::{quote_cursor_name, validate_cursor_batch_size};

    #[test]
    fn cursor_name_is_quoted_and_quotes_are_escaped() {
        assert_eq!(quote_cursor_name("cur\"one").unwrap(), "\"cur\"\"one\"");
    }

    #[test]
    fn cursor_name_rejects_empty_and_nul() {
        assert!(quote_cursor_name("").is_err());
        assert!(quote_cursor_name("cur\0one").is_err());
    }

    #[test]
    fn cursor_batch_size_zero_is_rejected() {
        assert!(validate_cursor_batch_size(0).is_err());
        assert!(validate_cursor_batch_size(1).is_ok());
    }
}
