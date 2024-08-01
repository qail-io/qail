//! Query execution methods for PostgreSQL connection.
//!
//! This module provides query, query_cached, and execute_simple.

use bytes::BytesMut;
use tokio::io::AsyncWriteExt;
use crate::protocol::{BackendMessage, PgEncoder};
use super::{PgConnection, PgError, PgResult};

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
        params: &[Option<Vec<u8>>]
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
    pub async fn query_cached(
        &mut self, 
        sql: &str, 
        params: &[Option<Vec<u8>>]
    ) -> PgResult<Vec<Vec<Option<Vec<u8>>>>> {
        // Generate statement name from SQL hash
        let stmt_name = Self::sql_to_stmt_name(sql);
        let is_new = !self.prepared_statements.contains_key(&stmt_name);
        
        // Build the message: Parse (if new) + Bind + Execute + Sync
        let mut buf = BytesMut::new();
        
        if is_new {
            // Parse statement with name
            buf.extend(PgEncoder::encode_parse(&stmt_name, sql, &[]));
        }
        
        // Bind to named statement
        buf.extend(PgEncoder::encode_bind("", &stmt_name, params));
        // Execute
        buf.extend(PgEncoder::encode_execute("", 0));
        // Sync
        buf.extend(PgEncoder::encode_sync());
        
        self.stream.write_all(&buf).await?;
        
        let mut rows = Vec::new();
        let sql_owned = sql.to_string(); // For storing in cache
        
        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::ParseComplete => {
                    // Statement parsed - store with SQL text for debugging
                    self.prepared_statements.insert(stmt_name.clone(), sql_owned.clone());
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
}
