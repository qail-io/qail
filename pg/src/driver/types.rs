//! Core types: ColumnInfo, PgRow, PgError, PgResult, QueryResult, ResultFormat,
//! and wire-protocol message utilities.

use std::collections::HashMap;
use std::sync::Arc;

/// Metadata about the columns returned by a query.
///
/// Maps column names to positional indices and stores OID / format
/// information so that [`PgRow`] values can be decoded correctly.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Lookup table from column name to zero-based index.
    pub name_to_index: HashMap<String, usize>,
    /// PostgreSQL type OIDs, one per column.
    pub oids: Vec<u32>,
    /// Wire format codes (0 = text, 1 = binary), one per column.
    pub formats: Vec<i16>,
}

impl ColumnInfo {
    /// Build column metadata from the `RowDescription` field list
    /// returned by the backend after a query.
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
    /// Raw column values — `None` represents SQL `NULL`.
    pub columns: Vec<Option<Vec<u8>>>,
    /// Shared column metadata for decoding values by name or type.
    pub column_info: Option<Arc<ColumnInfo>>,
}

/// Error type for PostgreSQL driver operations.
#[derive(Debug)]
pub enum PgError {
    /// TCP / TLS connection failure with the PostgreSQL server.
    Connection(String),
    /// Wire-protocol framing or decoding error.
    Protocol(String),
    /// Authentication failure (bad password, unsupported mechanism, etc.).
    Auth(String),
    /// Query execution error returned by the backend (e.g. constraint violation).
    Query(String),
    /// Structured server error with SQLSTATE and optional detail/hint fields.
    QueryServer(PgServerError),
    /// The query returned zero rows when at least one was expected.
    NoRows,
    /// I/O error (preserves inner error for chaining)
    Io(std::io::Error),
    /// Encoding error (parameter limit, etc.)
    Encode(String),
    /// Operation timed out (connection, acquire, query)
    Timeout(String),
    /// Pool exhausted — all connections are in use
    PoolExhausted {
        /// Maximum pool size that was reached.
        max: usize,
    },
    /// Pool is closed and no longer accepting requests
    PoolClosed,
}

/// Structured PostgreSQL server error fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgServerError {
    /// Severity level (e.g. `ERROR`, `FATAL`, `WARNING`).
    pub severity: String,
    /// SQLSTATE error code (e.g. `23505`).
    pub code: String,
    /// Human-readable message.
    pub message: String,
    /// Optional detailed description.
    pub detail: Option<String>,
    /// Optional hint from server.
    pub hint: Option<String>,
}

impl From<crate::protocol::ErrorFields> for PgServerError {
    fn from(value: crate::protocol::ErrorFields) -> Self {
        Self {
            severity: value.severity,
            code: value.code,
            message: value.message,
            detail: value.detail,
            hint: value.hint,
        }
    }
}

impl std::fmt::Display for PgError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PgError::Connection(e) => write!(f, "Connection error: {}", e),
            PgError::Protocol(e) => write!(f, "Protocol error: {}", e),
            PgError::Auth(e) => write!(f, "Auth error: {}", e),
            PgError::Query(e) => write!(f, "Query error: {}", e),
            PgError::QueryServer(e) => write!(f, "Query error [{}]: {}", e.code, e.message),
            PgError::NoRows => write!(f, "No rows returned"),
            PgError::Io(e) => write!(f, "I/O error: {}", e),
            PgError::Encode(e) => write!(f, "Encode error: {}", e),
            PgError::Timeout(ctx) => write!(f, "Timeout: {}", ctx),
            PgError::PoolExhausted { max } => write!(f, "Pool exhausted ({} max connections)", max),
            PgError::PoolClosed => write!(f, "Connection pool is closed"),
        }
    }
}

impl std::error::Error for PgError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PgError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for PgError {
    fn from(e: std::io::Error) -> Self {
        PgError::Io(e)
    }
}

impl From<crate::protocol::EncodeError> for PgError {
    fn from(e: crate::protocol::EncodeError) -> Self {
        PgError::Encode(e.to_string())
    }
}

impl PgError {
    /// Return structured server error fields when available.
    pub fn server_error(&self) -> Option<&PgServerError> {
        match self {
            PgError::QueryServer(err) => Some(err),
            _ => None,
        }
    }

    /// Return SQLSTATE code when available.
    pub fn sqlstate(&self) -> Option<&str> {
        self.server_error().map(|e| e.code.as_str())
    }

    /// True when a cached prepared statement can be self-healed by clearing
    /// local statement state and retrying once.
    pub fn is_prepared_statement_retryable(&self) -> bool {
        let Some(err) = self.server_error() else {
            return false;
        };

        let code = err.code.as_str();
        let message = err.message.to_ascii_lowercase();

        // invalid_sql_statement_name
        if code.eq_ignore_ascii_case("26000")
            && message.contains("prepared statement")
            && message.contains("does not exist")
        {
            return true;
        }

        // feature_not_supported + message heuristic used by PostgreSQL replans.
        if code.eq_ignore_ascii_case("0A000") && message.contains("cached plan must be replanned") {
            return true;
        }

        // Defensive message-only fallback for proxy/failover rewrites.
        message.contains("cached plan must be replanned")
    }

    /// True when server reports the prepared statement name already exists.
    ///
    /// This typically means local cache eviction drifted from server state
    /// (e.g. local entry dropped while backend statement still exists).
    /// Callers can retry once without Parse after preserving local mapping.
    pub fn is_prepared_statement_already_exists(&self) -> bool {
        let Some(err) = self.server_error() else {
            return false;
        };
        if !err.code.eq_ignore_ascii_case("42P05") {
            return false;
        }
        let message = err.message.to_ascii_lowercase();
        message.contains("prepared statement") && message.contains("already exists")
    }

    /// True when the error is a transient server condition that may succeed
    /// on retry. Covers serialization failures, deadlocks, standby
    /// unavailability, connection exceptions, and prepared-statement staleness.
    ///
    /// Callers should pair this with a bounded retry loop and backoff.
    pub fn is_transient_server_error(&self) -> bool {
        // Non-server errors that are inherently transient.
        match self {
            PgError::Timeout(_) => return true,
            PgError::Io(io) => {
                return matches!(
                    io.kind(),
                    std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::ConnectionRefused
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::Interrupted
                );
            }
            PgError::Connection(_) => return true,
            _ => {}
        }

        // Prepared-statement staleness is a subset of transient errors.
        if self.is_prepared_statement_retryable() {
            return true;
        }

        let Some(code) = self.sqlstate() else {
            return false;
        };

        matches!(
            code,
            // serialization_failure — MVCC conflict, safe to retry
            "40001"
            // deadlock_detected — PG auto-aborts one participant
            | "40P01"
            // cannot_connect_now — hot-standby recovery in progress
            | "57P03"
            // admin_shutdown / crash_shutdown — server restarting
            | "57P01"
            | "57P02"
        ) || code.starts_with("08") // connection_exception class
    }
}

/// Result type for PostgreSQL operations.
pub type PgResult<T> = Result<T, PgError>;

#[inline]
pub(crate) fn is_ignorable_session_message(msg: &crate::protocol::BackendMessage) -> bool {
    matches!(
        msg,
        crate::protocol::BackendMessage::NoticeResponse(_)
            | crate::protocol::BackendMessage::ParameterStatus { .. }
    )
}

#[inline]
pub(crate) fn unexpected_backend_message(
    phase: &str,
    msg: &crate::protocol::BackendMessage,
) -> PgError {
    PgError::Protocol(format!(
        "Unexpected backend message during {} phase: {:?}",
        phase, msg
    ))
}

#[inline]
pub(crate) fn is_ignorable_session_msg_type(msg_type: u8) -> bool {
    matches!(msg_type, b'N' | b'S')
}

#[inline]
pub(crate) fn unexpected_backend_msg_type(phase: &str, msg_type: u8) -> PgError {
    let printable = if msg_type.is_ascii_graphic() {
        msg_type as char
    } else {
        '?'
    };
    PgError::Protocol(format!(
        "Unexpected backend message type during {} phase: byte={} char={}",
        phase, msg_type, printable
    ))
}

/// Result of a query that returns rows (SELECT/GET).
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Column names from RowDescription.
    pub columns: Vec<String>,
    /// Rows of text-decoded values (None = NULL).
    pub rows: Vec<Vec<Option<String>>>,
}

/// PostgreSQL result-column wire format.
///
/// - `Text` (0): server sends textual column values.
/// - `Binary` (1): server sends binary column values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ResultFormat {
    /// Text format (`0`)
    #[default]
    Text,
    /// Binary format (`1`)
    Binary,
}

impl ResultFormat {
    #[inline]
    pub(crate) fn as_wire_code(self) -> i16 {
        match self {
            ResultFormat::Text => crate::protocol::PgEncoder::FORMAT_TEXT,
            ResultFormat::Binary => crate::protocol::PgEncoder::FORMAT_BINARY,
        }
    }
}
