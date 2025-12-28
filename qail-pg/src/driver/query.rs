//! Query execution methods for PostgreSQL connection.
//!
//! This module provides query, query_cached, and execute_simple.

use super::{PgConnection, PgError, PgResult};
use crate::protocol::{BackendMessage, PgEncoder};
use bytes::BytesMut;
use tokio::io::AsyncWriteExt;

impl PgConnection {
    /// Execute a query with binary parameters (crate-internal).
    ///
    /// This uses the Extended Query Protocol (Parse/Bind/Execute/Sync):
    /// - Parameters are sent as binary bytes, skipping the string layer
    /// - No SQL injection possible - parameters are never interpolated
    /// - Better performance via prepared statement reuse
    pub(crate) async fn query(
        &mut self,
        sql: &str,
        params: &[Option<Vec<u8>>],
    ) -> PgResult<Vec<Vec<Option<Vec<u8>>>>> {
        // Send Parse + Bind + Execute + Sync as one pipeline
        let bytes = PgEncoder::encode_extended_query(sql, params);
        self.stream.write_all(&bytes).await?;

        let mut rows = Vec::new();

        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::ParseComplete => {}
                BackendMessage::BindComplete => {}
                BackendMessage::RowDescription(_) => {}
                BackendMessage::DataRow(data) => {
                    rows.push(data);
                }
                BackendMessage::CommandComplete(_) => {}
                BackendMessage::NoData => {}
                BackendMessage::ReadyForQuery(_) => {
                    return Ok(rows);
                }
                BackendMessage::ErrorResponse(err) => {
                    return Err(PgError::Query(err.message));
                }
                _ => {}
            }
        }
    }

    /// Execute a query with cached prepared statement.
    ///
    /// Like `query()`, but reuses prepared statements across calls.
    /// The statement name is derived from a hash of the SQL text.
    ///
    /// OPTIMIZED: Pre-allocated buffer + ultra-fast encoders.
    pub async fn query_cached(
        &mut self,
        sql: &str,
        params: &[Option<Vec<u8>>],
    ) -> PgResult<Vec<Vec<Option<Vec<u8>>>>> {
        // Generate statement name from SQL hash
        let stmt_name = Self::sql_to_stmt_name(sql);
        let is_new = !self.prepared_statements.contains_key(&stmt_name);

        // Pre-calculate buffer size for single allocation
        let params_size: usize = params
            .iter()
            .map(|p| 4 + p.as_ref().map_or(0, |v| v.len()))
            .sum();
        
        // Parse: ~25 + sql.len() + stmt.len(), Bind: ~15 + stmt.len() + params_size, Execute: 10, Sync: 5
        let estimated_size = if is_new {
            50 + sql.len() + stmt_name.len() * 2 + params_size
        } else {
            30 + stmt_name.len() + params_size
        };
        
        let mut buf = BytesMut::with_capacity(estimated_size);

        if is_new {
            // Parse statement with name
            buf.extend(PgEncoder::encode_parse(&stmt_name, sql, &[]));
            // Cache the SQL for debugging
            self.prepared_statements.insert(stmt_name.clone(), sql.to_string());
        }

        // Use ULTRA-OPTIMIZED encoders - write directly to buffer
        PgEncoder::encode_bind_to(&mut buf, &stmt_name, params);
        PgEncoder::encode_execute_to(&mut buf);
        PgEncoder::encode_sync_to(&mut buf);

        self.stream.write_all(&buf).await?;

        let mut rows = Vec::new();

        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::ParseComplete => {
                    // Already cached in is_new block above
                }
                BackendMessage::BindComplete => {}
                BackendMessage::RowDescription(_) => {}
                BackendMessage::DataRow(data) => {
                    rows.push(data);
                }
                BackendMessage::CommandComplete(_) => {}
                BackendMessage::NoData => {}
                BackendMessage::ReadyForQuery(_) => {
                    return Ok(rows);
                }
                BackendMessage::ErrorResponse(err) => {
                    return Err(PgError::Query(err.message));
                }
                _ => {}
            }
        }
    }

    /// Generate a statement name from SQL hash.
    /// Uses a simple hash to create a unique name like "stmt_12345abc".
    pub(crate) fn sql_to_stmt_name(sql: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        sql.hash(&mut hasher);
        format!("s{:016x}", hasher.finish())
    }

    /// Execute a simple SQL statement (no parameters).
    /// Used internally for transaction control.
    pub(crate) async fn execute_simple(&mut self, sql: &str) -> PgResult<()> {
        let bytes = PgEncoder::encode_query_string(sql);
        self.stream.write_all(&bytes).await?;

        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::CommandComplete(_) => {}
                BackendMessage::ReadyForQuery(_) => {
                    return Ok(());
                }
                BackendMessage::ErrorResponse(err) => {
                    return Err(PgError::Query(err.message));
                }
                _ => {}
            }
        }
    }

    /// ZERO-HASH sequential query using pre-computed PreparedStatement.
    ///
    /// This is the FASTEST sequential path because it skips:
    /// - SQL generation from AST (done once outside loop)
    /// - Hash computation for statement name (pre-computed in PreparedStatement)
    /// - HashMap lookup for is_new check (statement already prepared)
    ///
    /// # Example
    /// ```ignore
    /// let stmt = conn.prepare("SELECT * FROM users WHERE id = $1").await?;
    /// for id in 1..10000 {
    ///     let rows = conn.query_prepared_single(&stmt, &[Some(id.to_string().into_bytes())]).await?;
    /// }
    /// ```
    #[inline]
    pub async fn query_prepared_single(
        &mut self,
        stmt: &super::PreparedStatement,
        params: &[Option<Vec<u8>>],
    ) -> PgResult<Vec<Vec<Option<Vec<u8>>>>> {
        // Pre-calculate buffer size for single allocation
        let params_size: usize = params
            .iter()
            .map(|p| 4 + p.as_ref().map_or(0, |v| v.len()))
            .sum();
        
        // Bind: ~15 + stmt.name.len() + params_size, Execute: 10, Sync: 5
        let mut buf = BytesMut::with_capacity(30 + stmt.name.len() + params_size);

        // ZERO HASH, ZERO LOOKUP - just encode and send!
        PgEncoder::encode_bind_to(&mut buf, &stmt.name, params);
        PgEncoder::encode_execute_to(&mut buf);
        PgEncoder::encode_sync_to(&mut buf);

        self.stream.write_all(&buf).await?;

        let mut rows = Vec::new();

        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::BindComplete => {}
                BackendMessage::RowDescription(_) => {}
                BackendMessage::DataRow(data) => {
                    rows.push(data);
                }
                BackendMessage::CommandComplete(_) => {}
                BackendMessage::NoData => {}
                BackendMessage::ReadyForQuery(_) => {
                    return Ok(rows);
                }
                BackendMessage::ErrorResponse(err) => {
                    return Err(PgError::Query(err.message));
                }
                _ => {}
            }
        }
    }
}
