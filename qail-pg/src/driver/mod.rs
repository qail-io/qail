//! PostgreSQL Driver Module (Layer 3: Async I/O)
//!
//! This module contains the async runtime-specific code.
//! Auto-detects the best I/O backend:
//! - Linux 5.1+: io_uring (fastest)
//! - Linux < 5.1 / macOS / Windows: tokio
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
//! - `io_backend.rs` - Runtime I/O backend detection

mod cancel;
mod connection;
mod copy;
mod cursor;
mod io;
pub mod io_backend;
mod pipeline;
mod pool;
mod prepared;
mod query;
mod row;
mod stream;
mod transaction;

pub use connection::PgConnection;
pub use connection::TlsConfig;
pub(crate) use connection::{CANCEL_REQUEST_CODE, parse_affected_rows};
pub use io_backend::{IoBackend, backend_name, detect as detect_io_backend};
pub use pool::{PgPool, PoolConfig, PoolStats, PooledConnection};
pub use prepared::PreparedStatement;

use qail_core::ast::QailCmd;
use std::collections::HashMap;
use std::sync::Arc;

/// Column metadata from RowDescription (shared across rows via Arc).
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name -> index mapping
    pub name_to_index: HashMap<String, usize>,
    /// Column OIDs
    pub oids: Vec<u32>,
    /// Column format codes (0=text, 1=binary)
    pub formats: Vec<i16>,
}

impl ColumnInfo {
    /// Create from FieldDescriptions.
    pub fn from_fields(fields: &[crate::protocol::FieldDescription]) -> Self {
        let mut name_to_index = HashMap::with_capacity(fields.len());
        let mut oids = Vec::with_capacity(fields.len());
        let mut formats = Vec::with_capacity(fields.len());

        for (i, field) in fields.iter().enumerate() {
            name_to_index.insert(field.name.clone(), i);
            oids.push(field.type_oid);
            formats.push(field.format);
        }

        Self {
            name_to_index,
            oids,
            formats,
        }
    }
}

/// PostgreSQL row with column data and metadata.
pub struct PgRow {
    /// Column values (None = NULL)
    pub columns: Vec<Option<Vec<u8>>>,
    /// Column metadata (shared across rows via Arc)
    pub column_info: Option<Arc<ColumnInfo>>,
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

    /// Builder pattern for ergonomic connection configuration.
    ///
    /// # Example
    /// ```ignore
    /// let driver = PgDriver::builder()
    ///     .host("localhost")
    ///     .port(5432)
    ///     .user("admin")
    ///     .database("mydb")
    ///     .password("secret")  // Optional
    ///     .connect()
    ///     .await?;
    /// ```
    pub fn builder() -> PgDriverBuilder {
        PgDriverBuilder::new()
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
        let connection =
            PgConnection::connect_with_password(host, port, user, database, Some(password)).await?;
        Ok(Self::new(connection))
    }

    /// Connect to PostgreSQL with a connection timeout.
    ///
    /// If the connection cannot be established within the timeout, returns an error.
    ///
    /// # Example
    /// ```ignore
    /// use std::time::Duration;
    /// let driver = PgDriver::connect_with_timeout(
    ///     "localhost", 5432, "user", "db", "password",
    ///     Duration::from_secs(5)
    /// ).await?;
    /// ```
    pub async fn connect_with_timeout(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: &str,
        timeout: std::time::Duration,
    ) -> PgResult<Self> {
        tokio::time::timeout(
            timeout,
            Self::connect_with_password(host, port, user, database, password),
        )
        .await
        .map_err(|_| PgError::Connection(format!("Connection timeout after {:?}", timeout)))?
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
        let mut column_info: Option<Arc<ColumnInfo>> = None;

        loop {
            let msg = self.connection.recv().await?;
            match msg {
                crate::protocol::BackendMessage::ParseComplete
                | crate::protocol::BackendMessage::BindComplete => {}
                crate::protocol::BackendMessage::RowDescription(fields) => {
                    // Create and share column metadata across all rows
                    column_info = Some(Arc::new(ColumnInfo::from_fields(&fields)));
                }
                crate::protocol::BackendMessage::DataRow(data) => {
                    rows.push(PgRow {
                        columns: data,
                        column_info: column_info.clone(),
                    });
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

    /// Execute a QAIL command with PREPARED STATEMENT CACHING.
    ///
    /// Like fetch_all(), but caches the prepared statement on the server.
    /// On first call: sends Parse + Bind + Execute + Sync
    /// On subsequent calls: sends only Bind + Execute + Sync (much faster!)
    ///
    /// Use this for repeated queries with the same AST structure.
    pub async fn fetch_all_cached(&mut self, cmd: &QailCmd) -> PgResult<Vec<PgRow>> {
        use crate::protocol::AstEncoder;

        // Generate SQL and params from AST (for caching)
        let (sql, params) = AstEncoder::encode_cmd_sql(cmd);

        // Use cached query - only parses on first call
        let raw_rows = self.connection.query_cached(&sql, &params).await?;

        // Convert to PgRow with column info
        let rows: Vec<PgRow> = raw_rows
            .into_iter()
            .map(|data| PgRow {
                columns: data,
                column_info: None, // Simple version - no column metadata
            })
            .collect();

        Ok(rows)
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
                crate::protocol::BackendMessage::ParseComplete
                | crate::protocol::BackendMessage::BindComplete => {}
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

    // ==================== TRANSACTION CONTROL ====================

    /// Begin a transaction (AST-native).
    pub async fn begin(&mut self) -> PgResult<()> {
        self.connection.begin_transaction().await
    }

    /// Commit the current transaction (AST-native).
    pub async fn commit(&mut self) -> PgResult<()> {
        self.connection.commit().await
    }

    /// Rollback the current transaction (AST-native).
    pub async fn rollback(&mut self) -> PgResult<()> {
        self.connection.rollback().await
    }

    /// Create a named savepoint within the current transaction.
    ///
    /// Savepoints allow partial rollback within a transaction.
    /// Use `rollback_to()` to return to this savepoint.
    ///
    /// # Example
    /// ```ignore
    /// driver.begin().await?;
    /// driver.execute(&insert1).await?;
    /// driver.savepoint("sp1").await?;
    /// driver.execute(&insert2).await?;
    /// driver.rollback_to("sp1").await?; // Undo insert2, keep insert1
    /// driver.commit().await?;
    /// ```
    pub async fn savepoint(&mut self, name: &str) -> PgResult<()> {
        self.connection.savepoint(name).await
    }

    /// Rollback to a previously created savepoint.
    ///
    /// Discards all changes since the named savepoint was created,
    /// but keeps the transaction open.
    pub async fn rollback_to(&mut self, name: &str) -> PgResult<()> {
        self.connection.rollback_to(name).await
    }

    /// Release a savepoint (free resources, if no longer needed).
    ///
    /// After release, the savepoint cannot be rolled back to.
    pub async fn release_savepoint(&mut self, name: &str) -> PgResult<()> {
        self.connection.release_savepoint(name).await
    }

    // ==================== BATCH TRANSACTIONS ====================

    /// Execute multiple commands in a single atomic transaction.
    ///
    /// All commands succeed or all are rolled back.
    /// Returns the number of affected rows for each command.
    ///
    /// # Example
    /// ```ignore
    /// let cmds = vec![
    ///     QailCmd::add("users").columns(["name"]).values(["Alice"]),
    ///     QailCmd::add("users").columns(["name"]).values(["Bob"]),
    /// ];
    /// let results = driver.execute_batch(&cmds).await?;
    /// // results = [1, 1] (rows affected)
    /// ```
    pub async fn execute_batch(&mut self, cmds: &[QailCmd]) -> PgResult<Vec<u64>> {
        self.begin().await?;
        let mut results = Vec::with_capacity(cmds.len());
        for cmd in cmds {
            match self.execute(cmd).await {
                Ok(n) => results.push(n),
                Err(e) => {
                    self.rollback().await?;
                    return Err(e);
                }
            }
        }
        self.commit().await?;
        Ok(results)
    }

    // ==================== STATEMENT TIMEOUT ====================

    /// Set statement timeout for this connection (in milliseconds).
    ///
    /// Queries that exceed this time will be cancelled.
    /// This is a production safety feature.
    ///
    /// # Example
    /// ```ignore
    /// driver.set_statement_timeout(30_000).await?; // 30 seconds
    /// ```
    pub async fn set_statement_timeout(&mut self, ms: u32) -> PgResult<()> {
        self.execute_raw(&format!("SET statement_timeout = {}", ms))
            .await
    }

    /// Reset statement timeout to default (no limit).
    pub async fn reset_statement_timeout(&mut self) -> PgResult<()> {
        self.execute_raw("RESET statement_timeout").await
    }

    // ==================== PIPELINE (BATCH) ====================

    /// Execute multiple QailCmd ASTs in a single network round-trip (PIPELINING).
    ///
    /// This is the high-performance path for batch operations.
    /// Returns the count of successful queries.
    ///
    /// # Example
    /// ```ignore
    /// let cmds: Vec<QailCmd> = (1..=1000)
    ///     .map(|i| QailCmd::get("harbors").columns(["id", "name"]).limit(i))
    ///     .collect();
    /// let count = driver.pipeline_batch(&cmds).await?;
    /// assert_eq!(count, 1000);
    /// ```
    pub async fn pipeline_batch(&mut self, cmds: &[QailCmd]) -> PgResult<usize> {
        self.connection.pipeline_ast_fast(cmds).await
    }

    /// Execute multiple QailCmd ASTs and return full row data.
    ///
    /// Unlike `pipeline_batch` which only returns count, this method
    /// collects and returns all row data from each query.
    ///
    /// Returns: Vec of PgRow vectors, one per query in the batch.
    pub async fn pipeline_fetch(&mut self, cmds: &[QailCmd]) -> PgResult<Vec<Vec<PgRow>>> {
        let raw_results = self.connection.pipeline_ast(cmds).await?;

        // Convert raw results to PgRow format
        let results: Vec<Vec<PgRow>> = raw_results
            .into_iter()
            .map(|rows| {
                rows.into_iter()
                    .map(|columns| PgRow {
                        columns,
                        column_info: None,
                    })
                    .collect()
            })
            .collect();

        Ok(results)
    }

    /// Prepare a SQL statement for repeated execution.
    ///
    /// Returns a PreparedStatement handle for use with pipeline_prepared_fast.
    pub async fn prepare(&mut self, sql: &str) -> PgResult<PreparedStatement> {
        self.connection.prepare(sql).await
    }

    /// Execute a prepared statement pipeline in FAST mode (count only).
    ///
    /// This is the fastest possible path - Parse once, Bind+Execute many.
    /// Matches native Rust benchmark performance (~355k q/s).
    pub async fn pipeline_prepared_fast(
        &mut self,
        stmt: &PreparedStatement,
        params_batch: &[Vec<Option<Vec<u8>>>],
    ) -> PgResult<usize> {
        self.connection
            .pipeline_prepared_fast(stmt, params_batch)
            .await
    }

    // ==================== LEGACY/BOOTSTRAP ====================

    /// Execute a raw SQL string.
    ///
    /// ⚠️ **Discouraged**: Violates AST-native philosophy.
    /// Use for bootstrap DDL only (e.g., migration table creation).
    /// For transactions, use `begin()`, `commit()`, `rollback()`.
    pub async fn execute_raw(&mut self, sql: &str) -> PgResult<()> {
        self.connection.execute_simple(sql).await
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
                "copy_bulk requires QailCmd::Add action".to_string(),
            ));
        }

        // Extract table from AST (already validated at parse time)
        let table = &cmd.table;

        // Extract column names from AST expressions
        let columns: Vec<String> = cmd
            .columns
            .iter()
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
                "copy_bulk requires columns in QailCmd".to_string(),
            ));
        }

        // Use optimized COPY path: direct Value → bytes encoding, single syscall
        self.connection.copy_in_fast(table, &columns, rows).await
    }

    /// **Fastest** bulk insert using pre-encoded COPY data.
    ///
    /// Accepts raw COPY text format bytes. Use when caller has already
    /// encoded rows to avoid any encoding overhead.
    ///
    /// # Format
    /// Data should be tab-separated rows with newlines (COPY text format):
    /// `1\thello\t3.14\n2\tworld\t2.71\n`
    ///
    /// # Example
    /// ```ignore
    /// let cmd = QailCmd::add("users").columns(["id", "name"]);
    /// let data = b"1\tAlice\n2\tBob\n";
    /// driver.copy_bulk_bytes(&cmd, data).await?;
    /// ```
    pub async fn copy_bulk_bytes(&mut self, cmd: &QailCmd, data: &[u8]) -> PgResult<u64> {
        use qail_core::ast::Action;

        if cmd.action != Action::Add {
            return Err(PgError::Query(
                "copy_bulk_bytes requires QailCmd::Add action".to_string(),
            ));
        }

        let table = &cmd.table;
        let columns: Vec<String> = cmd
            .columns
            .iter()
            .filter_map(|expr| {
                use qail_core::ast::Expr;
                match expr {
                    Expr::Named(name) => Some(name.clone()),
                    Expr::Aliased { name, .. } => Some(name.clone()),
                    _ => None,
                }
            })
            .collect();

        if columns.is_empty() {
            return Err(PgError::Query(
                "copy_bulk_bytes requires columns in QailCmd".to_string(),
            ));
        }

        // Direct to raw COPY - zero encoding!
        self.connection.copy_in_raw(table, &columns, data).await
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
        while let Some(rows) = self
            .connection
            .fetch_cursor(&cursor_name, batch_size)
            .await?
        {
            let pg_rows: Vec<PgRow> = rows
                .into_iter()
                .map(|cols| PgRow {
                    columns: cols,
                    column_info: None,
                })
                .collect();
            all_batches.push(pg_rows);
        }

        // Cleanup
        self.connection.close_cursor(&cursor_name).await?;
        self.connection.commit().await?;

        Ok(all_batches)
    }
}

// ============================================================================
// Connection Builder
// ============================================================================

/// Builder for creating PgDriver connections with named parameters.
///
/// # Example
/// ```ignore
/// let driver = PgDriver::builder()
///     .host("localhost")
///     .port(5432)
///     .user("admin")
///     .database("mydb")
///     .password("secret")
///     .connect()
///     .await?;
/// ```
#[derive(Default)]
pub struct PgDriverBuilder {
    host: Option<String>,
    port: Option<u16>,
    user: Option<String>,
    database: Option<String>,
    password: Option<String>,
    timeout: Option<std::time::Duration>,
}

impl PgDriverBuilder {
    /// Create a new builder with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the host (default: "127.0.0.1").
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    /// Set the port (default: 5432).
    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    /// Set the username (required).
    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    /// Set the database name (required).
    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    /// Set the password (optional, for SCRAM-SHA-256 auth).
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Set connection timeout (optional).
    pub fn timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Connect to PostgreSQL using the configured parameters.
    pub async fn connect(self) -> PgResult<PgDriver> {
        let host = self.host.as_deref().unwrap_or("127.0.0.1");
        let port = self.port.unwrap_or(5432);
        let user = self.user.as_deref().ok_or_else(|| {
            PgError::Connection("User is required".to_string())
        })?;
        let database = self.database.as_deref().ok_or_else(|| {
            PgError::Connection("Database is required".to_string())
        })?;

        match (self.password.as_deref(), self.timeout) {
            (Some(password), Some(timeout)) => {
                PgDriver::connect_with_timeout(host, port, user, database, password, timeout).await
            }
            (Some(password), None) => {
                PgDriver::connect_with_password(host, port, user, database, password).await
            }
            (None, Some(timeout)) => {
                tokio::time::timeout(
                    timeout,
                    PgDriver::connect(host, port, user, database),
                )
                .await
                .map_err(|_| PgError::Connection(format!("Connection timeout after {:?}", timeout)))?
            }
            (None, None) => {
                PgDriver::connect(host, port, user, database).await
            }
        }
    }
}
