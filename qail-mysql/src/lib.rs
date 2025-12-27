//! QAIL MySQL Read-Only Driver
//!
//! Minimal MySQL wire protocol implementation for high-speed data migration.
//! Supports only: Connect, Authenticate, SELECT queries, row streaming.
//!
//! NOT A FULL DRIVER: No transactions, no prepared statements, no writes.

pub mod protocol;
pub mod connection;
pub mod auth;

pub use connection::MySqlConnection;

use std::sync::Once;
static INIT: Once = Once::new();

/// Initialize the crypto provider for TLS. Must be called before connecting.
pub fn init() {
    INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// MySQL error type.
#[derive(Debug)]
pub enum MySqlError {
    Io(std::io::Error),
    Protocol(String),
    Auth(String),
}

impl std::fmt::Display for MySqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MySqlError::Io(e) => write!(f, "I/O error: {}", e),
            MySqlError::Protocol(e) => write!(f, "Protocol error: {}", e),
            MySqlError::Auth(e) => write!(f, "Auth error: {}", e),
        }
    }
}

impl std::error::Error for MySqlError {}

impl From<std::io::Error> for MySqlError {
    fn from(e: std::io::Error) -> Self {
        MySqlError::Io(e)
    }
}

pub type MySqlResult<T> = Result<T, MySqlError>;
