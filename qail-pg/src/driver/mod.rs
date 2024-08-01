//! PostgreSQL Driver Module (Layer 3: Async I/O)
//!
//! This module contains the async runtime-specific code.
//! Uses tokio for networking.

mod connection;
mod row;
mod pool;
mod stream;

pub use connection::PgConnection;
pub use pool::{PgPool, PoolConfig, PooledConnection};

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

    /// Execute a QAIL command and fetch all rows.
    ///
    /// Uses Extended Query Protocol - parameters are sent as binary bytes,
    /// skipping the string layer entirely.
    pub async fn fetch_all(&mut self, cmd: &QailCmd) -> PgResult<Vec<PgRow>> {
        // Layer 2: Convert AST to parameterized SQL (pure, sync)
        use qail_core::transpiler::ToSqlParameterized;
        let result = cmd.to_sql_parameterized();

        // Convert Value params to binary bytes
        let params: Vec<Option<Vec<u8>>> = result.params.iter()
            .map(value_to_bytes)
            .collect();

        // Layer 3: Execute via Extended Query Protocol (async I/O)
        // Parameters are binary bytes - no string interpolation
        let raw_rows = self.connection.query(&result.sql, &params).await?;
        
        Ok(raw_rows.into_iter().map(|columns| PgRow { columns }).collect())
    }

    /// Execute a QAIL command and fetch one row.
    pub async fn fetch_one(&mut self, cmd: &QailCmd) -> PgResult<PgRow> {
        let rows = self.fetch_all(cmd).await?;
        rows.into_iter().next().ok_or(PgError::NoRows)
    }

    /// Execute a QAIL command (for mutations).
    ///
    /// Uses Extended Query Protocol - parameters are sent as binary bytes.
    /// Returns the number of affected rows.
    pub async fn execute(&mut self, cmd: &QailCmd) -> PgResult<u64> {
        // Layer 2: Convert AST to parameterized SQL (pure, sync)
        use qail_core::transpiler::ToSqlParameterized;
        let result = cmd.to_sql_parameterized();

        // Convert Value params to binary bytes
        let params: Vec<Option<Vec<u8>>> = result.params.iter()
            .map(value_to_bytes)
            .collect();

        // Layer 3: Execute via Extended Query Protocol (async I/O)
        let affected = self.connection.execute_raw(&result.sql, &params).await?;
        Ok(affected)
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
}

/// Convert a QAIL Value to PostgreSQL wire protocol bytes (text format).
fn value_to_bytes(value: &qail_core::ast::Value) -> Option<Vec<u8>> {
    use qail_core::ast::Value;
    
    match value {
        Value::Null => None,
        Value::Bool(b) => Some(if *b { b"t".to_vec() } else { b"f".to_vec() }),
        Value::Int(i) => Some(i.to_string().into_bytes()),
        Value::Float(f) => Some(f.to_string().into_bytes()),
        Value::String(s) => Some(s.as_bytes().to_vec()),
        Value::Uuid(u) => Some(u.to_string().into_bytes()),
        Value::NullUuid => None,
        Value::Timestamp(ts) => Some(ts.as_bytes().to_vec()),
        // For functions, columns, etc. - handled in SQL template, not as params
        Value::Function(_) | Value::Column(_) | Value::Param(_) | Value::NamedParam(_) => None,
        Value::Array(arr) => {
            // PostgreSQL array format: {elem1,elem2,elem3}
            let elements: Vec<String> = arr.iter()
                .map(|v| format!("{}", v))
                .collect();
            Some(format!("{{{}}}", elements.join(",")).into_bytes())
        }
        Value::Subquery(_) => None, // Subqueries are inlined in SQL
        Value::Interval { amount, unit } => {
            Some(format!("{} {}", amount, unit).into_bytes())
        }
    }
}
