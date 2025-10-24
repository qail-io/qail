//! PostgreSQL Driver Module (Layer 3: Async I/O)
//!
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
pub mod rls;
pub mod branch_sql;
mod row;
mod stream;
mod transaction;

pub use connection::PgConnection;
pub use connection::TlsConfig;
pub(crate) use connection::{CANCEL_REQUEST_CODE, parse_affected_rows};
pub use cancel::CancelToken;
pub use io_backend::{IoBackend, backend_name, detect as detect_io_backend};
pub use pool::{PgPool, PoolConfig, PoolStats, PooledConnection};
pub use prepared::PreparedStatement;
pub use rls::RlsContext;
pub use row::QailRow;

use qail_core::ast::Qail;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name_to_index: HashMap<String, usize>,
    pub oids: Vec<u32>,
    pub formats: Vec<i16>,
}

impl ColumnInfo {
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
    pub columns: Vec<Option<Vec<u8>>>,
    pub column_info: Option<Arc<ColumnInfo>>,
}

/// Error type for PostgreSQL driver operations.
#[derive(Debug)]
pub enum PgError {
    Connection(String),
    Protocol(String),
    Auth(String),
    Query(String),
    NoRows,
    /// I/O error
    Io(std::io::Error),
    /// Encoding error (parameter limit, etc.)
    Encode(String),
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
            PgError::Encode(e) => write!(f, "Encode error: {}", e),
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

/// Result of a query that returns rows (SELECT/GET).
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Column names from RowDescription.
    pub columns: Vec<String>,
    /// Rows of text-decoded values (None = NULL).
    pub rows: Vec<Vec<Option<String>>>,
}

/// Combines the pure encoder (Layer 2) with async I/O (Layer 3).
pub struct PgDriver {
    #[allow(dead_code)]
    connection: PgConnection,
    /// Current RLS context, if set. Used for multi-tenant data isolation.
    rls_context: Option<RlsContext>,
}

impl PgDriver {
    /// Create a new driver with an existing connection.
    pub fn new(connection: PgConnection) -> Self {
        Self { connection, rls_context: None }
    }

    /// Builder pattern for ergonomic connection configuration.
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

    /// Connect using DATABASE_URL environment variable.
    /// 
    /// Parses the URL format: `postgresql://user:password@host:port/database`
    /// or `postgres://user:password@host:port/database`
    /// 
    /// # Example
    /// ```ignore
    /// // Set DATABASE_URL=postgresql://user:pass@localhost:5432/mydb
    /// let driver = PgDriver::connect_env().await?;
    /// ```
    pub async fn connect_env() -> PgResult<Self> {
        let url = std::env::var("DATABASE_URL")
            .map_err(|_| PgError::Connection("DATABASE_URL environment variable not set".to_string()))?;
        Self::connect_url(&url).await
    }

    /// Connect using a PostgreSQL connection URL.
    /// 
    /// Parses the URL format: `postgresql://user:password@host:port/database`
    /// or `postgres://user:password@host:port/database`
    /// 
    /// # Example
    /// ```ignore
    /// let driver = PgDriver::connect_url("postgresql://user:pass@localhost:5432/mydb").await?;
    /// ```
    pub async fn connect_url(url: &str) -> PgResult<Self> {
        let (host, port, user, database, password) = Self::parse_database_url(url)?;
        
        if let Some(pwd) = password {
            Self::connect_with_password(&host, port, &user, &database, &pwd).await
        } else {
            Self::connect(&host, port, &user, &database).await
        }
    }

    /// Parse a PostgreSQL connection URL into components.
    /// 
    /// Format: `postgresql://user:password@host:port/database`
    /// or `postgres://user:password@host:port/database`
    /// 
    /// URL percent-encoding is automatically decoded for user and password.
    fn parse_database_url(url: &str) -> PgResult<(String, u16, String, String, Option<String>)> {
        // Remove scheme (postgresql:// or postgres://)
        let after_scheme = url.split("://").nth(1)
            .ok_or_else(|| PgError::Connection("Invalid DATABASE_URL: missing scheme".to_string()))?;
        
        // Split into auth@host parts
        let (auth_part, host_db_part) = if let Some(at_pos) = after_scheme.rfind('@') {
            (Some(&after_scheme[..at_pos]), &after_scheme[at_pos + 1..])
        } else {
            (None, after_scheme)
        };
        
        // Parse auth (user:password)
        let (user, password) = if let Some(auth) = auth_part {
            let parts: Vec<&str> = auth.splitn(2, ':').collect();
            if parts.len() == 2 {
                // URL-decode both user and password
                (
                    Self::percent_decode(parts[0]),
                    Some(Self::percent_decode(parts[1])),
                )
            } else {
                (Self::percent_decode(parts[0]), None)
            }
        } else {
            return Err(PgError::Connection("Invalid DATABASE_URL: missing user".to_string()));
        };
        
        // Parse host:port/database
        let (host_port, database) = if let Some(slash_pos) = host_db_part.find('/') {
            (&host_db_part[..slash_pos], host_db_part[slash_pos + 1..].to_string())
        } else {
            return Err(PgError::Connection("Invalid DATABASE_URL: missing database name".to_string()));
        };
        
        // Parse host:port
        let (host, port) = if let Some(colon_pos) = host_port.rfind(':') {
            let port_str = &host_port[colon_pos + 1..];
            let port = port_str.parse::<u16>()
                .map_err(|_| PgError::Connection(format!("Invalid port: {}", port_str)))?;
            (host_port[..colon_pos].to_string(), port)
        } else {
            (host_port.to_string(), 5432) // Default PostgreSQL port
        };
        
        Ok((host, port, user, database, password))
    }
    
    /// Decode URL percent-encoded string.
    /// Handles common encodings: %20 (space), %2B (+), %3D (=), %40 (@), %2F (/), etc.
    fn percent_decode(s: &str) -> String {
        let mut result = String::with_capacity(s.len());
        let mut chars = s.chars().peekable();
        
        while let Some(c) = chars.next() {
            if c == '%' {
                // Try to parse next two chars as hex
                let hex: String = chars.by_ref().take(2).collect();
                if hex.len() == 2
                    && let Ok(byte) = u8::from_str_radix(&hex, 16)
                {
                    result.push(byte as char);
                    continue;
                }
                // If parsing failed, keep original
                result.push('%');
                result.push_str(&hex);
            } else if c == '+' {
                // '+' often represents space in query strings (form encoding)
                // But in path components, keep as-is. PostgreSQL URLs use path encoding.
                result.push('+');
            } else {
                result.push(c);
            }
        }
        
        result
    }

    /// Connect to PostgreSQL with a connection timeout.
    /// If the connection cannot be established within the timeout, returns an error.
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
    /// Clear the prepared statement cache.
    /// Frees memory by removing all cached statements.
    /// Note: Statements remain on the PostgreSQL server until connection closes.
    pub fn clear_cache(&mut self) {
        self.connection.stmt_cache.clear();
        self.connection.prepared_statements.clear();
    }

    /// Get cache statistics.
    /// Returns (current_size, max_capacity).
    pub fn cache_stats(&self) -> (usize, usize) {
        (self.connection.stmt_cache.len(), self.connection.stmt_cache.cap().get())
    }

    /// Execute a QAIL command and fetch all rows (CACHED + ZERO-ALLOC).
    /// **Default method** - uses prepared statement caching for best performance.
    /// On first call: sends Parse + Bind + Execute + Sync
    /// On subsequent calls with same SQL: sends only Bind + Execute (SKIPS Parse!)
    /// Uses LRU cache with max 1000 statements (auto-evicts oldest).
    pub async fn fetch_all(&mut self, cmd: &Qail) -> PgResult<Vec<PgRow>> {
        // Delegate to fetch_all_cached for cached-by-default behavior
        self.fetch_all_cached(cmd).await
    }

    /// Execute a QAIL command and fetch all rows as a typed struct.
    /// Requires the target type to implement `QailRow` trait.
    /// 
    /// # Example
    /// ```ignore
    /// let users: Vec<User> = driver.fetch_typed::<User>(&query).await?;
    /// ```
    pub async fn fetch_typed<T: row::QailRow>(&mut self, cmd: &Qail) -> PgResult<Vec<T>> {
        let rows = self.fetch_all(cmd).await?;
        Ok(rows.iter().map(T::from_row).collect())
    }

    /// Execute a QAIL command and fetch a single row as a typed struct.
    /// Returns None if no rows are returned.
    pub async fn fetch_one_typed<T: row::QailRow>(&mut self, cmd: &Qail) -> PgResult<Option<T>> {
        let rows = self.fetch_all(cmd).await?;
        Ok(rows.first().map(T::from_row))
    }

    /// Execute a QAIL command and fetch all rows (UNCACHED).
    /// Sends Parse + Bind + Execute on every call.
    /// Use for one-off queries or when caching is not desired.
    ///
    /// Optimized: encodes wire bytes into reusable write_buf (zero-alloc).
    pub async fn fetch_all_uncached(&mut self, cmd: &Qail) -> PgResult<Vec<PgRow>> {
        use crate::protocol::AstEncoder;

        AstEncoder::encode_cmd_reuse_into(
            cmd,
            &mut self.connection.sql_buf,
            &mut self.connection.params_buf,
            &mut self.connection.write_buf,
        );

        self.connection.flush_write_buf().await?;

        let mut rows: Vec<PgRow> = Vec::with_capacity(32);
        let mut column_info: Option<Arc<ColumnInfo>> = None;

        let mut error: Option<PgError> = None;

        loop {
            let msg = self.connection.recv().await?;
            match msg {
                crate::protocol::BackendMessage::ParseComplete
                | crate::protocol::BackendMessage::BindComplete => {}
                crate::protocol::BackendMessage::RowDescription(fields) => {
                    column_info = Some(Arc::new(ColumnInfo::from_fields(&fields)));
                }
                crate::protocol::BackendMessage::DataRow(data) => {
                    if error.is_none() {
                        rows.push(PgRow {
                            columns: data,
                            column_info: column_info.clone(),
                        });
                    }
                }
                crate::protocol::BackendMessage::CommandComplete(_) => {}
                crate::protocol::BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        return Err(err);
                    }
                    return Ok(rows);
                }
                crate::protocol::BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        error = Some(PgError::Query(err.message));
                    }
                }
                _ => {}
            }
        }
    }

    /// Execute a QAIL command and fetch all rows (FAST VERSION).
    /// Uses optimized recv_with_data_fast for faster response parsing.
    /// Skips column metadata collection for maximum speed.
    pub async fn fetch_all_fast(&mut self, cmd: &Qail) -> PgResult<Vec<PgRow>> {
        use crate::protocol::AstEncoder;

        AstEncoder::encode_cmd_reuse_into(
            cmd,
            &mut self.connection.sql_buf,
            &mut self.connection.params_buf,
            &mut self.connection.write_buf,
        );

        self.connection.flush_write_buf().await?;

        // Collect results using FAST receiver
        let mut rows: Vec<PgRow> = Vec::with_capacity(32);
        let mut error: Option<PgError> = None;

        loop {
            let res = self.connection.recv_with_data_fast().await;
            match res {
                Ok((msg_type, data)) => {
                    match msg_type {
                        b'D' => {
                             // DataRow
                            if error.is_none() && let Some(columns) = data {
                                rows.push(PgRow {
                                    columns,
                                    column_info: None, // Skip metadata for speed
                                });
                            }
                        }
                        b'Z' => {
                            // ReadyForQuery
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok(rows);
                        }
                        _ => {} // 1, 2, C, T - skip Parse/Bind/CommandComplete/RowDescription
                    }
                }
                Err(e) => {
                   // recv_with_data_fast returns Err on ErrorResponse automatically.
                   // We need to capture it and continue draining.
                   // BUT recv_with_data_fast doesn't return the error *message type* if it fails.
                   // It returns PgError::Query(msg).
                   // So we capture the error, but we must continue RECVing until ReadyForQuery.
                   // However, recv_with_data_fast will KEEP returning Err(Query) if the buffer has E?
                   // No, recv_with_data_fast consumes the E message before returning Err.
                   
                   if error.is_none() {
                       error = Some(e);
                   }
                   // Continue loop to drain until ReadyForQuery... 
                   // BUT wait, does recv_with_data_fast handle the *rest* of the stream?
                   // If we call it again, it will read the NEXT message.
                   // So we just continue.
                }
            }
        }
    }

    /// Execute a QAIL command and fetch one row.
    pub async fn fetch_one(&mut self, cmd: &Qail) -> PgResult<PgRow> {
        let rows = self.fetch_all(cmd).await?;
        rows.into_iter().next().ok_or(PgError::NoRows)
    }

    /// Execute a QAIL command with PREPARED STATEMENT CACHING.
    /// Like fetch_all(), but caches the prepared statement on the server.
    /// On first call: sends Parse + Describe + Bind + Execute + Sync
    /// On subsequent calls: sends only Bind + Execute + Sync (SKIPS Parse!)
    /// Column metadata (RowDescription) is cached alongside the statement
    /// so that by-name column access works on every call.
    ///
    /// Optimized: all wire messages are batched into a single write_all syscall.
    pub async fn fetch_all_cached(&mut self, cmd: &Qail) -> PgResult<Vec<PgRow>> {
        use crate::protocol::AstEncoder;
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        self.connection.sql_buf.clear();
        self.connection.params_buf.clear();
        
        // Encode SQL to reusable buffer
        match cmd.action {
            qail_core::ast::Action::Get | qail_core::ast::Action::With => {
                crate::protocol::ast_encoder::dml::encode_select(cmd, &mut self.connection.sql_buf, &mut self.connection.params_buf).ok();
            }
            qail_core::ast::Action::Add => {
                crate::protocol::ast_encoder::dml::encode_insert(cmd, &mut self.connection.sql_buf, &mut self.connection.params_buf).ok();
            }
            qail_core::ast::Action::Set => {
                crate::protocol::ast_encoder::dml::encode_update(cmd, &mut self.connection.sql_buf, &mut self.connection.params_buf).ok();
            }
            qail_core::ast::Action::Del => {
                crate::protocol::ast_encoder::dml::encode_delete(cmd, &mut self.connection.sql_buf, &mut self.connection.params_buf).ok();
            }
            _ => {
                // Fallback for unsupported actions
                let (sql, params) = AstEncoder::encode_cmd_sql(cmd);
                let raw_rows = self.connection.query_cached(&sql, &params).await?;
                return Ok(raw_rows.into_iter().map(|data| PgRow { columns: data, column_info: None }).collect());
            }
        }

        let mut hasher = DefaultHasher::new();
        self.connection.sql_buf.hash(&mut hasher);
        let sql_hash = hasher.finish();

        let is_cache_miss = !self.connection.stmt_cache.contains(&sql_hash);

        // Build ALL wire messages into write_buf (single syscall)
        self.connection.write_buf.clear();

        let stmt_name = if let Some(name) = self.connection.stmt_cache.get(&sql_hash) {
            name.clone()
        } else {
            let name = format!("qail_{:x}", sql_hash);
            
            let sql_str = std::str::from_utf8(&self.connection.sql_buf).unwrap_or("");
            
            // Buffer Parse + Describe(Statement) for first call
            use crate::protocol::PgEncoder;
            let parse_msg = PgEncoder::encode_parse(&name, sql_str, &[]);
            let describe_msg = PgEncoder::encode_describe(false, &name);
            self.connection.write_buf.extend_from_slice(&parse_msg);
            self.connection.write_buf.extend_from_slice(&describe_msg);
            
            self.connection.stmt_cache.put(sql_hash, name.clone());
            self.connection.prepared_statements.insert(name.clone(), sql_str.to_string());
            
            name
        };

        // Append Bind + Execute + Sync to same buffer
        use crate::protocol::PgEncoder;
        PgEncoder::encode_bind_to(&mut self.connection.write_buf, &stmt_name, &self.connection.params_buf)
            .map_err(|e| PgError::Encode(e.to_string()))?;
        PgEncoder::encode_execute_to(&mut self.connection.write_buf);
        PgEncoder::encode_sync_to(&mut self.connection.write_buf);

        // Single write_all syscall for all messages
        self.connection.flush_write_buf().await?;

        // On cache hit, use the previously cached ColumnInfo
        let cached_column_info = self.connection.column_info_cache.get(&sql_hash).cloned();

        let mut rows: Vec<PgRow> = Vec::with_capacity(32);
        let mut column_info: Option<Arc<ColumnInfo>> = cached_column_info;
        let mut error: Option<PgError> = None;

        loop {
            let msg = self.connection.recv().await?;
            match msg {
                crate::protocol::BackendMessage::ParseComplete
                | crate::protocol::BackendMessage::BindComplete => {}
                crate::protocol::BackendMessage::ParameterDescription(_) => {
                    // Sent after Describe(Statement) — ignore
                }
                crate::protocol::BackendMessage::RowDescription(fields) => {
                    // Received after Describe(Statement) on cache miss
                    let info = Arc::new(ColumnInfo::from_fields(&fields));
                    if is_cache_miss {
                        self.connection.column_info_cache.insert(sql_hash, info.clone());
                    }
                    column_info = Some(info);
                }
                crate::protocol::BackendMessage::DataRow(data) => {
                    if error.is_none() {
                        rows.push(PgRow {
                            columns: data,
                            column_info: column_info.clone(),
                        });
                    }
                }
                crate::protocol::BackendMessage::CommandComplete(_) => {}
                crate::protocol::BackendMessage::NoData => {
                    // Sent by Describe for statements that return no data (e.g. pure UPDATE without RETURNING)
                }
                crate::protocol::BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        return Err(err);
                    }
                    return Ok(rows);
                }
                crate::protocol::BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        error = Some(PgError::Query(err.message));
                        // Invalidate cache to prevent "prepared statement does not exist"
                        // on next retry if the error happened during Parse/Bind.
                        self.connection.stmt_cache.clear();
                        self.connection.prepared_statements.clear();
                        self.connection.column_info_cache.clear();
                    }
                }
                _ => {}
            }
        }
    }

    /// Execute a QAIL command (for mutations) - ZERO-ALLOC.
    pub async fn execute(&mut self, cmd: &Qail) -> PgResult<u64> {
        use crate::protocol::AstEncoder;

        let wire_bytes = AstEncoder::encode_cmd_reuse(
            cmd,
            &mut self.connection.sql_buf,
            &mut self.connection.params_buf,
        );

        self.connection.send_bytes(&wire_bytes).await?;

        let mut affected = 0u64;
        let mut error: Option<PgError> = None;

        loop {
            let msg = self.connection.recv().await?;
            match msg {
                crate::protocol::BackendMessage::ParseComplete
                | crate::protocol::BackendMessage::BindComplete => {}
                crate::protocol::BackendMessage::RowDescription(_) => {}
                crate::protocol::BackendMessage::DataRow(_) => {}
                crate::protocol::BackendMessage::CommandComplete(tag) => {
                    if error.is_none() && let Some(n) = tag.split_whitespace().last() {
                        affected = n.parse().unwrap_or(0);
                    }
                }
                crate::protocol::BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        return Err(err);
                    }
                    return Ok(affected);
                }
                crate::protocol::BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        error = Some(PgError::Query(err.message));
                    }
                }
                _ => {}
            }
        }
    }

    /// Query a QAIL command and return rows (for SELECT/GET queries).
    /// Like `execute()` but collects RowDescription + DataRow messages
    /// instead of discarding them.
    pub async fn query_ast(&mut self, cmd: &Qail) -> PgResult<QueryResult> {
        use crate::protocol::AstEncoder;

        let wire_bytes = AstEncoder::encode_cmd_reuse(
            cmd,
            &mut self.connection.sql_buf,
            &mut self.connection.params_buf,
        );

        self.connection.send_bytes(&wire_bytes).await?;

        let mut columns: Vec<String> = Vec::new();
        let mut rows: Vec<Vec<Option<String>>> = Vec::new();
        let mut error: Option<PgError> = None;

        loop {
            let msg = self.connection.recv().await?;
            match msg {
                crate::protocol::BackendMessage::ParseComplete
                | crate::protocol::BackendMessage::BindComplete => {}
                crate::protocol::BackendMessage::RowDescription(fields) => {
                    columns = fields.into_iter().map(|f| f.name).collect();
                }
                crate::protocol::BackendMessage::DataRow(data) => {
                    if error.is_none() {
                        let row: Vec<Option<String>> = data
                            .into_iter()
                            .map(|col| col.map(|bytes| String::from_utf8_lossy(&bytes).to_string()))
                            .collect();
                        rows.push(row);
                    }
                }
                crate::protocol::BackendMessage::CommandComplete(_) => {}
                crate::protocol::BackendMessage::NoData => {}
                crate::protocol::BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        return Err(err);
                    }
                    return Ok(QueryResult { columns, rows });
                }
                crate::protocol::BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        error = Some(PgError::Query(err.message));
                    }
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
    /// Savepoints allow partial rollback within a transaction.
    /// Use `rollback_to()` to return to this savepoint.
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
    /// Discards all changes since the named savepoint was created,
    /// but keeps the transaction open.
    pub async fn rollback_to(&mut self, name: &str) -> PgResult<()> {
        self.connection.rollback_to(name).await
    }

    /// Release a savepoint (free resources, if no longer needed).
    /// After release, the savepoint cannot be rolled back to.
    pub async fn release_savepoint(&mut self, name: &str) -> PgResult<()> {
        self.connection.release_savepoint(name).await
    }

    // ==================== BATCH TRANSACTIONS ====================

    /// Execute multiple commands in a single atomic transaction.
    /// All commands succeed or all are rolled back.
    /// # Example
    /// ```ignore
    /// let cmds = vec![
    ///     Qail::add("users").columns(["name"]).values(["Alice"]),
    ///     Qail::add("users").columns(["name"]).values(["Bob"]),
    /// ];
    /// let results = driver.execute_batch(&cmds).await?;
    /// // results = [1, 1] (rows affected)
    /// ```
    pub async fn execute_batch(&mut self, cmds: &[Qail]) -> PgResult<Vec<u64>> {
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

    // ==================== RLS (MULTI-TENANT) ====================

    /// Set the RLS context for multi-tenant data isolation.
    ///
    /// Configures PostgreSQL session variables (`app.current_operator_id`, etc.)
    /// so that RLS policies automatically filter data by tenant.
    ///
    /// Since `PgDriver` takes `&mut self`, the borrow checker guarantees
    /// that `set_config` and all subsequent queries execute on the **same
    /// connection** — no pool race conditions possible.
    ///
    /// # Example
    /// ```ignore
    /// driver.set_rls_context(RlsContext::operator("op-123")).await?;
    /// let orders = driver.fetch_all(&Qail::get("orders")).await?;
    /// // orders only contains rows where operator_id = 'op-123'
    /// ```
    pub async fn set_rls_context(&mut self, ctx: rls::RlsContext) -> PgResult<()> {
        let sql = rls::context_to_sql(&ctx);
        self.execute_raw(&sql).await?;
        self.rls_context = Some(ctx);
        Ok(())
    }

    /// Clear the RLS context, resetting session variables to safe defaults.
    ///
    /// After clearing, all RLS-protected queries will return zero rows
    /// (empty operator_id matches nothing).
    pub async fn clear_rls_context(&mut self) -> PgResult<()> {
        self.execute_raw(rls::reset_sql()).await?;
        self.rls_context = None;
        Ok(())
    }

    /// Get the current RLS context, if any.
    pub fn rls_context(&self) -> Option<&rls::RlsContext> {
        self.rls_context.as_ref()
    }

    // ==================== PIPELINE (BATCH) ====================

    /// Execute multiple Qail ASTs in a single network round-trip (PIPELINING).
    /// # Example
    /// ```ignore
    /// let cmds: Vec<Qail> = (1..=1000)
    ///     .map(|i| Qail::get("harbors").columns(["id", "name"]).limit(i))
    ///     .collect();
    /// let count = driver.pipeline_batch(&cmds).await?;
    /// assert_eq!(count, 1000);
    /// ```
    pub async fn pipeline_batch(&mut self, cmds: &[Qail]) -> PgResult<usize> {
        self.connection.pipeline_ast_fast(cmds).await
    }

    /// Execute multiple Qail ASTs and return full row data.
    pub async fn pipeline_fetch(&mut self, cmds: &[Qail]) -> PgResult<Vec<Vec<PgRow>>> {
        let raw_results = self.connection.pipeline_ast(cmds).await?;

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
    pub async fn prepare(&mut self, sql: &str) -> PgResult<PreparedStatement> {
        self.connection.prepare(sql).await
    }

    /// Execute a prepared statement pipeline in FAST mode (count only).
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
    /// ⚠️ **Discouraged**: Violates AST-native philosophy.
    /// Use for bootstrap DDL only (e.g., migration table creation).
    /// For transactions, use `begin()`, `commit()`, `rollback()`.
    pub async fn execute_raw(&mut self, sql: &str) -> PgResult<()> {
        // Reject literal NULL bytes - they corrupt PostgreSQL connection state
        if sql.as_bytes().contains(&0) {
            return Err(crate::PgError::Protocol(
                "SQL contains NULL byte (0x00) which is invalid in PostgreSQL".to_string(),
            ));
        }
        self.connection.execute_simple(sql).await
    }

    /// Execute a raw SQL query and return rows.
    /// ⚠️ **Discouraged**: Violates AST-native philosophy.
    /// Use for bootstrap/admin queries only.
    pub async fn fetch_raw(&mut self, sql: &str) -> PgResult<Vec<PgRow>> {
        if sql.as_bytes().contains(&0) {
            return Err(crate::PgError::Protocol(
                "SQL contains NULL byte (0x00) which is invalid in PostgreSQL".to_string(),
            ));
        }
        
        use tokio::io::AsyncWriteExt;
        use crate::protocol::PgEncoder;
        
        // Use simple query protocol (no prepared statements)
        let msg = PgEncoder::encode_query_string(sql);
        self.connection.stream.write_all(&msg).await?;
        
        let mut rows: Vec<PgRow> = Vec::new();
        let mut column_info: Option<std::sync::Arc<ColumnInfo>> = None;
        

        let mut error: Option<PgError> = None;

        loop {
            let msg = self.connection.recv().await?;
            match msg {
                crate::protocol::BackendMessage::RowDescription(fields) => {
                    column_info = Some(std::sync::Arc::new(ColumnInfo::from_fields(&fields)));
                }
                crate::protocol::BackendMessage::DataRow(data) => {
                    if error.is_none() {
                        rows.push(PgRow {
                            columns: data,
                            column_info: column_info.clone(),
                        });
                    }
                }
                crate::protocol::BackendMessage::CommandComplete(_) => {}
                crate::protocol::BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        return Err(err);
                    }
                    return Ok(rows);
                }
                crate::protocol::BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        error = Some(PgError::Query(err.message));
                    }
                }
                _ => {}
            }
        }
    }

    /// Bulk insert data using PostgreSQL COPY protocol (AST-native).
    /// Uses a Qail::Add to get validated table and column names from the AST,
    /// not user-provided strings. This is the sound, AST-native approach.
    /// # Example
    /// ```ignore
    /// // Create a Qail::Add to define table and columns
    /// let cmd = Qail::add("users")
    ///     .columns(["id", "name", "email"]);
    /// // Bulk insert rows
    /// let rows: Vec<Vec<Value>> = vec![
    ///     vec![Value::Int(1), Value::String("Alice"), Value::String("alice@ex.com")],
    ///     vec![Value::Int(2), Value::String("Bob"), Value::String("bob@ex.com")],
    /// ];
    /// driver.copy_bulk(&cmd, &rows).await?;
    /// ```
    pub async fn copy_bulk(
        &mut self,
        cmd: &Qail,
        rows: &[Vec<qail_core::ast::Value>],
    ) -> PgResult<u64> {
        use qail_core::ast::Action;


        if cmd.action != Action::Add {
            return Err(PgError::Query(
                "copy_bulk requires Qail::Add action".to_string(),
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
                    Expr::Star => None, // Can't COPY with *
                    _ => None,
                }
            })
            .collect();

        if columns.is_empty() {
            return Err(PgError::Query(
                "copy_bulk requires columns in Qail".to_string(),
            ));
        }

        // Use optimized COPY path: direct Value → bytes encoding, single syscall
        self.connection.copy_in_fast(table, &columns, rows).await
    }

    /// **Fastest** bulk insert using pre-encoded COPY data.
    /// Accepts raw COPY text format bytes. Use when caller has already
    /// encoded rows to avoid any encoding overhead.
    /// # Format
    /// Data should be tab-separated rows with newlines (COPY text format):
    /// `1\thello\t3.14\n2\tworld\t2.71\n`
    /// # Example
    /// ```ignore
    /// let cmd = Qail::add("users").columns(["id", "name"]);
    /// let data = b"1\tAlice\n2\tBob\n";
    /// driver.copy_bulk_bytes(&cmd, data).await?;
    /// ```
    pub async fn copy_bulk_bytes(&mut self, cmd: &Qail, data: &[u8]) -> PgResult<u64> {
        use qail_core::ast::Action;

        if cmd.action != Action::Add {
            return Err(PgError::Query(
                "copy_bulk_bytes requires Qail::Add action".to_string(),
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
                "copy_bulk_bytes requires columns in Qail".to_string(),
            ));
        }

        // Direct to raw COPY - zero encoding!
        self.connection.copy_in_raw(table, &columns, data).await
    }

    /// Export table data using PostgreSQL COPY TO STDOUT (zero-copy streaming).
    /// Returns rows as tab-separated bytes for direct re-import via copy_bulk_bytes.
    /// # Example
    /// ```ignore
    /// let data = driver.copy_export_table("users", &["id", "name"]).await?;
    /// shadow_driver.copy_bulk_bytes(&cmd, &data).await?;
    /// ```
    pub async fn copy_export_table(
        &mut self,
        table: &str,
        columns: &[String],
    ) -> PgResult<Vec<u8>> {
        let cols = columns.join(", ");
        let sql = format!("COPY {} ({}) TO STDOUT", table, cols);
        
        self.connection.copy_out_raw(&sql).await
    }

    /// Stream large result sets using PostgreSQL cursors.
    /// This method uses DECLARE CURSOR internally to stream rows in batches,
    /// avoiding loading the entire result set into memory.
    /// # Example
    /// ```ignore
    /// let cmd = Qail::get("large_table");
    /// let batches = driver.stream_cmd(&cmd, 100).await?;
    /// for batch in batches {
    ///     for row in batch {
    ///         // process row
    ///     }
    /// }
    /// ```
    pub async fn stream_cmd(
        &mut self,
        cmd: &Qail,
        batch_size: usize,
    ) -> PgResult<Vec<Vec<PgRow>>> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CURSOR_ID: AtomicU64 = AtomicU64::new(0);

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

        self.connection.close_cursor(&cursor_name).await?;
        self.connection.commit().await?;

        Ok(all_batches)
    }
}

// ============================================================================
// Connection Builder
// ============================================================================

/// Builder for creating PgDriver connections with named parameters.
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
