//! PostgreSQL Driver Module (Layer 3: Async I/O)
//!
//! This module contains the async runtime-specific code.
//! Uses tokio for networking.

mod connection;

pub use connection::PgConnection;

use qail_core::ast::QailCmd;
use crate::protocol::PgEncoder;

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

    /// Connect to PostgreSQL and create a driver.
    pub async fn connect(host: &str, port: u16, user: &str, database: &str) -> PgResult<Self> {
        let connection = PgConnection::connect(host, port, user, database).await?;
        Ok(Self::new(connection))
    }

    /// Execute a QAIL command and fetch all rows.
    pub async fn fetch_all(&mut self, cmd: &QailCmd) -> PgResult<Vec<PgRow>> {
        // Layer 2: Encode the command to bytes (pure, sync)
        let _bytes = PgEncoder::encode_simple_query(cmd);

        // Layer 3: Send bytes over the wire (async I/O)
        // TODO: Implement using self.connection
        
        Err(PgError::Query("fetch_all not fully implemented".to_string()))
    }

    /// Execute a QAIL command and fetch one row.
    pub async fn fetch_one(&mut self, cmd: &QailCmd) -> PgResult<PgRow> {
        let rows = self.fetch_all(cmd).await?;
        rows.into_iter().next().ok_or(PgError::NoRows)
    }

    /// Execute a QAIL command (for mutations).
    pub async fn execute(&mut self, cmd: &QailCmd) -> PgResult<u64> {
        let _bytes = PgEncoder::encode_simple_query(cmd);
        
        Err(PgError::Query("execute not fully implemented".to_string()))
    }
}
