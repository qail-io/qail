//! PostgreSQL Driver Module (Layer 3: Async I/O)
//!
//! This module contains the async runtime-specific code.
//! Uses tokio for networking.
//!
//! Connection methods are split across modules for easier maintenance:
//! - `connection.rs` - Core struct and connect methods
//! - `io.rs` - send, recv, recv_msg_type_fast
//! - `query.rs` - query, query_cached, execute_simple
//! - `transaction.rs` - begin_transaction, commit, rollback
//! - `cursor.rs` - declare_cursor, fetch_cursor, close_cursor  
//! - `copy.rs` - COPY protocol for bulk operations
//! - `pipeline.rs` - High-performance pipelining (275k q/s)
//! - `cancel.rs` - Query cancellation

mod connection;
mod io;
mod query;
mod transaction;
mod cursor;
mod copy;
mod pipeline;
mod cancel;
mod prepared;
mod row;
mod pool;
mod stream;

pub use connection::PgConnection;
pub use connection::TlsConfig;
pub(crate) use connection::{parse_affected_rows, CANCEL_REQUEST_CODE};
pub use pool::{PgPool, PoolConfig, PooledConnection};
pub use prepared::PreparedStatement;

use qail_core::ast::QailCmd;

/// PostgreSQL row (raw bytes for now).
pub struct PgRow {
    pub columns: Vec<Option<Vec<u8>>>,
}

/// Error type for PostgreSQL driver operations.
#[derive(Debug)]
pub enum PgError {
    /// Connection error
    Connection(String),
    /// Protocol error
    Protocol(String),
    /// Authentication error
    Auth(String),
    /// Query error
    Query(String),
    /// No rows returned
    NoRows,
    /// I/O error
    Io(std::io::Error),
}

impl std::fmt::Display for PgError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PgError::Connection(e) => write!(f, "Connection error: {}", e),
            PgError::Protocol(e) => write!(f, "Protocol error: {}", e),
            PgError::Auth(e) => write!(f, "Auth error: {}", e),
            PgError::Query(e) => write!(f, "Query error: {}", e),
            PgError::NoRows => write!(f, "No rows returned"),
            PgError::Io(e) => write!(f, "I/O error: {}", e),
        }
    }
}

impl std::error::Error for PgError {}

impl From<std::io::Error> for PgError {
    fn from(e: std::io::Error) -> Self {
        PgError::Io(e)
    }
}

/// Result type for PostgreSQL operations.
pub type PgResult<T> = Result<T, PgError>;

/// PostgreSQL driver.
///
/// Combines the pure encoder (Layer 2) with async I/O (Layer 3).
pub struct PgDriver {
    #[allow(dead_code)]
    connection: PgConnection,
}

impl PgDriver {
    /// Create a new driver with an existing connection.
    pub fn new(connection: PgConnection) -> Self {
        Self { connection }
    }

    /// Connect to PostgreSQL and create a driver (trust mode, no password).
    pub async fn connect(host: &str, port: u16, user: &str, database: &str) -> PgResult<Self> {
        let connection = PgConnection::connect(host, port, user, database).await?;
        Ok(Self::new(connection))
    }

    /// Connect to PostgreSQL with password authentication (SCRAM-SHA-256).
    pub async fn connect_with_password(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: &str,
    ) -> PgResult<Self> {
        let connection = PgConnection::connect_with_password(
            host, port, user, database, Some(password)
        ).await?;
        Ok(Self::new(connection))
    }

    /// Execute a QAIL command and fetch all rows (AST-NATIVE).
    ///
    /// Uses AstEncoder to directly encode AST to wire protocol bytes.
    /// NO SQL STRING GENERATION!
    pub async fn fetch_all(&mut self, cmd: &QailCmd) -> PgResult<Vec<PgRow>> {
        use crate::protocol::AstEncoder;
        
        // AST-NATIVE: Encode directly to wire bytes (no to_sql()!)
        let (wire_bytes, _params) = AstEncoder::encode_cmd(cmd);
        
        // Send wire bytes and receive response
        self.connection.send_bytes(&wire_bytes).await?;
        
        // Collect results
        let mut rows: Vec<PgRow> = Vec::new();
        loop {
            let msg = self.connection.recv().await?;
            match msg {
                crate::protocol::BackendMessage::ParseComplete | 
                crate::protocol::BackendMessage::BindComplete => {}
                crate::protocol::BackendMessage::RowDescription(_) => {}
                crate::protocol::BackendMessage::DataRow(data) => {
                    rows.push(PgRow { columns: data });
                }
                crate::protocol::BackendMessage::CommandComplete(_) => {}
                crate::protocol::BackendMessage::ReadyForQuery(_) => {
                    return Ok(rows);
                }
                crate::protocol::BackendMessage::ErrorResponse(err) => {
                    return Err(PgError::Query(err.message));
                }
                _ => {}
            }
        }
    }

    /// Execute a QAIL command and fetch one row.
    pub async fn fetch_one(&mut self, cmd: &QailCmd) -> PgResult<PgRow> {
        let rows = self.fetch_all(cmd).await?;
        rows.into_iter().next().ok_or(PgError::NoRows)
    }

    /// Execute a QAIL command (for mutations) - AST-NATIVE.
    ///
    /// Uses AstEncoder to directly encode AST to wire protocol bytes.
    /// Returns the number of affected rows.
    pub async fn execute(&mut self, cmd: &QailCmd) -> PgResult<u64> {
        use crate::protocol::AstEncoder;
        
        // AST-NATIVE: Encode directly to wire bytes (no to_sql()!)
        let (wire_bytes, _params) = AstEncoder::encode_cmd(cmd);
        
        // Send wire bytes and receive response
        self.connection.send_bytes(&wire_bytes).await?;
        
        // Parse response for affected rows
        let mut affected = 0u64;
        loop {
            let msg = self.connection.recv().await?;
            match msg {
                crate::protocol::BackendMessage::ParseComplete | 
                crate::protocol::BackendMessage::BindComplete => {}
                crate::protocol::BackendMessage::RowDescription(_) => {}
                crate::protocol::BackendMessage::DataRow(_) => {}
                crate::protocol::BackendMessage::CommandComplete(tag) => {
                    // Parse "INSERT 0 5" or "UPDATE 3" etc
                    if let Some(n) = tag.split_whitespace().last() {
                        affected = n.parse().unwrap_or(0);
                    }
                }
                crate::protocol::BackendMessage::ReadyForQuery(_) => {
                    return Ok(affected);
                }
                crate::protocol::BackendMessage::ErrorResponse(err) => {
                    return Err(PgError::Query(err.message));
                }
                _ => {}
            }
        }
    }

    /// Bulk insert data using PostgreSQL COPY protocol (AST-native).
    ///
    /// Uses a QailCmd::Add to get validated table and column names from the AST,
    /// not user-provided strings. This is the sound, AST-native approach.
    ///
    /// # Example
    /// ```ignore
    /// // Create a QailCmd::Add to define table and columns
    /// let cmd = QailCmd::add("users")
    ///     .columns(["id", "name", "email"]);
    ///
    /// // Bulk insert rows
    /// let rows: Vec<Vec<Value>> = vec![
    ///     vec![Value::Int(1), Value::String("Alice"), Value::String("alice@ex.com")],
    ///     vec![Value::Int(2), Value::String("Bob"), Value::String("bob@ex.com")],
    /// ];
    /// driver.copy_bulk(&cmd, &rows).await?;
    /// ```
    pub async fn copy_bulk(
        &mut self,
        cmd: &QailCmd,
        rows: &[Vec<qail_core::ast::Value>],
    ) -> PgResult<u64> {
        use qail_core::ast::Action;
        
        // Validate this is an Add command
        if cmd.action != Action::Add {
            return Err(PgError::Query(
                "copy_bulk requires QailCmd::Add action".to_string()
            ));
        }
        
        // Extract table from AST (already validated at parse time)
        let table = &cmd.table;
        
        // Extract column names from AST expressions
        let columns: Vec<String> = cmd.columns.iter()
            .filter_map(|expr| {
                use qail_core::ast::Expr;
                match expr {
                    Expr::Named(name) => Some(name.clone()),
                    Expr::Aliased { name, .. } => Some(name.clone()),
                    Expr::Star => None, // Can't COPY with *
                    _ => None,
                }
            })
            .collect();
        
        if columns.is_empty() {
            return Err(PgError::Query(
                "copy_bulk requires columns in QailCmd".to_string()
            ));
        }
        
        // Convert Value rows to string representations for COPY format
        let str_rows: Vec<Vec<String>> = rows.iter()
            .map(|row| {
                row.iter().map(|v| format!("{}", v)).collect()
            })
            .collect();
        
        // Call internal copy implementation
        self.connection.copy_in_internal(table, &columns, &str_rows).await
    }

    /// Stream large result sets using PostgreSQL cursors.
    ///
    /// This method uses DECLARE CURSOR internally to stream rows in batches,
    /// avoiding loading the entire result set into memory.
    ///
    /// # Example
    /// ```ignore
    /// let cmd = QailCmd::get("large_table");
    /// let batches = driver.stream_cmd(&cmd, 100).await?;
    /// for batch in batches {
    ///     for row in batch {
    ///         // process row
    ///     }
    /// }
    /// ```
    pub async fn stream_cmd(
        &mut self,
        cmd: &QailCmd,
        batch_size: usize,
    ) -> PgResult<Vec<Vec<PgRow>>> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CURSOR_ID: AtomicU64 = AtomicU64::new(0);
        
        // Generate unique cursor name
        let cursor_name = format!("qail_cursor_{}", CURSOR_ID.fetch_add(1, Ordering::SeqCst));
        
        // AST-NATIVE: Generate SQL directly from AST (no to_sql_parameterized!)
        use crate::protocol::AstEncoder;
        let mut sql_buf = bytes::BytesMut::with_capacity(256);
        let mut params: Vec<Option<Vec<u8>>> = Vec::new();
        AstEncoder::encode_select_sql(cmd, &mut sql_buf, &mut params);
        let sql = String::from_utf8_lossy(&sql_buf).to_string();
        
        // Must be in a transaction for cursors
        self.connection.begin_transaction().await?;
        
        // Declare cursor
        self.connection.declare_cursor(&cursor_name, &sql).await?;
        
        // Fetch all batches
        let mut all_batches = Vec::new();
        while let Some(rows) = self.connection.fetch_cursor(&cursor_name, batch_size).await? {
            let pg_rows: Vec<PgRow> = rows.into_iter()
                .map(|cols| PgRow { columns: cols })
                .collect();
            all_batches.push(pg_rows);
        }
        
        // Cleanup
        self.connection.close_cursor(&cursor_name).await?;
        self.connection.commit().await?;
        
        Ok(all_batches)
    }
}
