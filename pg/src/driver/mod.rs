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
//! - `notification.rs` - LISTEN/NOTIFY support
//! - `io_backend.rs` - Runtime I/O backend detection

pub mod branch_sql;
mod cancel;
mod connection;
mod copy;
mod cursor;
pub mod explain;
#[cfg(all(feature = "enterprise-gssapi", target_os = "linux"))]
pub mod gss;
mod io;
pub mod io_backend;
pub mod notification;
mod pipeline;
mod pool;
mod prepared;
mod query;
pub mod rls;
mod row;
mod stream;
mod transaction;

pub use cancel::CancelToken;
pub use connection::PgConnection;
pub use connection::TlsConfig;
pub(crate) use connection::{CANCEL_REQUEST_CODE, parse_affected_rows};
pub use io_backend::{IoBackend, backend_name, detect as detect_io_backend};
pub use notification::Notification;
pub use pool::{PgPool, PoolConfig, PoolStats, PooledConnection};
pub use prepared::PreparedStatement;
pub use rls::RlsContext;
pub use row::QailRow;

use qail_core::ast::Qail;
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

/// SCRAM channel-binding policy during SASL negotiation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ScramChannelBindingMode {
    /// Do not use `SCRAM-SHA-256-PLUS` even when available.
    Disable,
    /// Prefer `SCRAM-SHA-256-PLUS`, fallback to plain SCRAM if needed.
    #[default]
    Prefer,
    /// Require `SCRAM-SHA-256-PLUS` and fail otherwise.
    Require,
}

impl ScramChannelBindingMode {
    /// Parse common config string values.
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "disable" | "off" | "false" | "no" => Some(Self::Disable),
            "prefer" | "on" | "true" | "yes" => Some(Self::Prefer),
            "require" | "required" => Some(Self::Require),
            _ => None,
        }
    }
}

/// Enterprise authentication mechanisms initiated by PostgreSQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnterpriseAuthMechanism {
    /// Kerberos V5 (`AuthenticationKerberosV5`, auth code `2`).
    KerberosV5,
    /// GSSAPI (`AuthenticationGSS`, auth code `7`).
    GssApi,
    /// SSPI (`AuthenticationSSPI`, auth code `9`, primarily Windows servers).
    Sspi,
}

/// Callback used to generate GSS/SSPI response tokens.
///
/// The callback receives:
/// - negotiated enterprise auth mechanism
/// - optional server challenge bytes (`None` for initial token)
///
/// It must return the client response token bytes to send in `GSSResponse`.
pub type GssTokenProvider = fn(EnterpriseAuthMechanism, Option<&[u8]>) -> Result<Vec<u8>, String>;

/// Structured token request for stateful Kerberos/GSS/SSPI providers.
#[derive(Debug, Clone, Copy)]
pub struct GssTokenRequest<'a> {
    /// Stable per-handshake identifier so providers can keep per-connection state.
    pub session_id: u64,
    /// Negotiated enterprise auth mechanism.
    pub mechanism: EnterpriseAuthMechanism,
    /// Server challenge token (`None` for initial token).
    pub server_token: Option<&'a [u8]>,
}

/// Stateful callback for Kerberos/GSS/SSPI response generation.
///
/// Use this when the underlying auth stack needs per-handshake context between
/// `AuthenticationGSS` and `AuthenticationGSSContinue` messages.
pub type GssTokenProviderEx =
    Arc<dyn for<'a> Fn(GssTokenRequest<'a>) -> Result<Vec<u8>, String> + Send + Sync>;

/// Password-auth mechanism policy.
///
/// Defaults allow all PostgreSQL password mechanisms for compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthSettings {
    /// Allow server-requested cleartext password auth.
    pub allow_cleartext_password: bool,
    /// Allow server-requested MD5 password auth.
    pub allow_md5_password: bool,
    /// Allow server-requested SCRAM auth.
    pub allow_scram_sha_256: bool,
    /// Allow server-requested Kerberos V5 auth flow.
    pub allow_kerberos_v5: bool,
    /// Allow server-requested GSSAPI auth flow.
    pub allow_gssapi: bool,
    /// Allow server-requested SSPI auth flow.
    pub allow_sspi: bool,
    /// SCRAM channel-binding requirement.
    pub channel_binding: ScramChannelBindingMode,
}

impl Default for AuthSettings {
    fn default() -> Self {
        Self {
            allow_cleartext_password: true,
            allow_md5_password: true,
            allow_scram_sha_256: true,
            allow_kerberos_v5: false,
            allow_gssapi: false,
            allow_sspi: false,
            channel_binding: ScramChannelBindingMode::Prefer,
        }
    }
}

impl AuthSettings {
    /// Restrictive mode: SCRAM-only password auth.
    pub fn scram_only() -> Self {
        Self {
            allow_cleartext_password: false,
            allow_md5_password: false,
            allow_scram_sha_256: true,
            allow_kerberos_v5: false,
            allow_gssapi: false,
            allow_sspi: false,
            channel_binding: ScramChannelBindingMode::Prefer,
        }
    }

    /// Restrictive mode: enterprise Kerberos/GSS only (no password auth).
    pub fn gssapi_only() -> Self {
        Self {
            allow_cleartext_password: false,
            allow_md5_password: false,
            allow_scram_sha_256: false,
            allow_kerberos_v5: true,
            allow_gssapi: true,
            allow_sspi: true,
            channel_binding: ScramChannelBindingMode::Prefer,
        }
    }

    pub(crate) fn has_any_password_method(self) -> bool {
        self.allow_cleartext_password || self.allow_md5_password || self.allow_scram_sha_256
    }
}

/// TLS policy for connection establishment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TlsMode {
    /// Do not attempt TLS.
    #[default]
    Disable,
    /// Try TLS first; fallback to plaintext only when server has no TLS support.
    Prefer,
    /// Require TLS and fail if unavailable.
    Require,
}

impl TlsMode {
    /// Parse libpq-style `sslmode` values.
    pub fn parse_sslmode(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "disable" => Some(Self::Disable),
            "allow" | "prefer" => Some(Self::Prefer),
            "require" | "verify-ca" | "verify-full" => Some(Self::Require),
            _ => None,
        }
    }
}

/// GSSAPI encryption mode for transport-level encryption via Kerberos.
///
/// Controls whether the driver attempts GSSAPI session encryption
/// (GSSENCRequest) before falling back to TLS or plaintext.
///
/// See: PostgreSQL protocol §54.2.11 — GSSAPI Session Encryption.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GssEncMode {
    /// Never attempt GSSAPI encryption.
    #[default]
    Disable,
    /// Try GSSAPI encryption first; fall back to TLS or plaintext.
    Prefer,
    /// Require GSSAPI encryption — fail if the server rejects GSSENCRequest.
    Require,
}

impl GssEncMode {
    /// Parse libpq-style `gssencmode` values.
    pub fn parse_gssencmode(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "disable" => Some(Self::Disable),
            "prefer" => Some(Self::Prefer),
            "require" => Some(Self::Require),
            _ => None,
        }
    }
}

/// Advanced connection options for enterprise deployments.
#[derive(Clone, Default)]
pub struct ConnectOptions {
    /// TLS mode for the primary connection.
    pub tls_mode: TlsMode,
    /// GSSAPI session encryption mode.
    pub gss_enc_mode: GssEncMode,
    /// Optional custom CA bundle (PEM) for TLS server validation.
    pub tls_ca_cert_pem: Option<Vec<u8>>,
    /// Optional mTLS client certificate/key config.
    pub mtls: Option<TlsConfig>,
    /// Optional callback for Kerberos/GSS/SSPI token generation.
    pub gss_token_provider: Option<GssTokenProvider>,
    /// Optional stateful Kerberos/GSS/SSPI token provider.
    pub gss_token_provider_ex: Option<GssTokenProviderEx>,
    /// Password-auth policy.
    pub auth: AuthSettings,
}

impl std::fmt::Debug for ConnectOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectOptions")
            .field("tls_mode", &self.tls_mode)
            .field("gss_enc_mode", &self.gss_enc_mode)
            .field(
                "tls_ca_cert_pem",
                &self.tls_ca_cert_pem.as_ref().map(std::vec::Vec::len),
            )
            .field("mtls", &self.mtls.as_ref().map(|_| "<configured>"))
            .field(
                "gss_token_provider",
                &self.gss_token_provider.as_ref().map(|_| "<configured>"),
            )
            .field(
                "gss_token_provider_ex",
                &self.gss_token_provider_ex.as_ref().map(|_| "<configured>"),
            )
            .field("auth", &self.auth)
            .finish()
    }
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
        Self {
            connection,
            rls_context: None,
        }
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
    ///
    /// # Arguments
    ///
    /// * `host` — PostgreSQL server hostname or IP.
    /// * `port` — TCP port (typically 5432).
    /// * `user` — PostgreSQL role name.
    /// * `database` — Target database name.
    pub async fn connect(host: &str, port: u16, user: &str, database: &str) -> PgResult<Self> {
        let connection = PgConnection::connect(host, port, user, database).await?;
        Ok(Self::new(connection))
    }

    /// Connect to PostgreSQL with password authentication.
    /// Supports server-requested auth flow: cleartext, MD5, or SCRAM-SHA-256.
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

    /// Connect with explicit security options.
    pub async fn connect_with_options(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
        options: ConnectOptions,
    ) -> PgResult<Self> {
        let connection =
            PgConnection::connect_with_options(host, port, user, database, password, options)
                .await?;
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
        let url = std::env::var("DATABASE_URL").map_err(|_| {
            PgError::Connection("DATABASE_URL environment variable not set".to_string())
        })?;
        Self::connect_url(&url).await
    }

    /// Connect using a PostgreSQL connection URL.
    ///
    /// Parses the URL format: `postgresql://user:password@host:port/database?params`
    /// or `postgres://user:password@host:port/database?params`
    ///
    /// Supports all enterprise query params (sslmode, auth_mode, gss_provider,
    /// channel_binding, etc.) — same set as `PoolConfig::from_qail_config`.
    ///
    /// # Example
    /// ```ignore
    /// let driver = PgDriver::connect_url("postgresql://user:pass@localhost:5432/mydb?sslmode=require").await?;
    /// ```
    pub async fn connect_url(url: &str) -> PgResult<Self> {
        let (host, port, user, database, password) = Self::parse_database_url(url)?;

        // Parse enterprise query params using the shared helper from pool.rs.
        let mut pool_cfg = pool::PoolConfig::new(&host, port, &user, &database);
        if let Some(pw) = &password {
            pool_cfg = pool_cfg.password(pw);
        }
        if let Some(query) = url.split('?').nth(1) {
            pool::apply_url_query_params(&mut pool_cfg, query, &host)?;
        }

        let opts = ConnectOptions {
            tls_mode: pool_cfg.tls_mode,
            gss_enc_mode: pool_cfg.gss_enc_mode,
            tls_ca_cert_pem: pool_cfg.tls_ca_cert_pem,
            mtls: pool_cfg.mtls,
            gss_token_provider: pool_cfg.gss_token_provider,
            gss_token_provider_ex: pool_cfg.gss_token_provider_ex,
            auth: pool_cfg.auth_settings,
        };

        Self::connect_with_options(&host, port, &user, &database, password.as_deref(), opts).await
    }

    /// Parse a PostgreSQL connection URL into components.
    ///
    /// Format: `postgresql://user:password@host:port/database`
    /// or `postgres://user:password@host:port/database`
    ///
    /// URL percent-encoding is automatically decoded for user and password.
    fn parse_database_url(url: &str) -> PgResult<(String, u16, String, String, Option<String>)> {
        // Remove scheme (postgresql:// or postgres://)
        let after_scheme = url.split("://").nth(1).ok_or_else(|| {
            PgError::Connection("Invalid DATABASE_URL: missing scheme".to_string())
        })?;

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
            return Err(PgError::Connection(
                "Invalid DATABASE_URL: missing user".to_string(),
            ));
        };

        // Parse host:port/database (strip query string if present)
        let (host_port, database) = if let Some(slash_pos) = host_db_part.find('/') {
            let raw_db = &host_db_part[slash_pos + 1..];
            // Strip ?query params — they're handled separately by connect_url
            let db = raw_db.split('?').next().unwrap_or(raw_db).to_string();
            (&host_db_part[..slash_pos], db)
        } else {
            return Err(PgError::Connection(
                "Invalid DATABASE_URL: missing database name".to_string(),
            ));
        };

        // Parse host:port
        let (host, port) = if let Some(colon_pos) = host_port.rfind(':') {
            let port_str = &host_port[colon_pos + 1..];
            let port = port_str
                .parse::<u16>()
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
        .map_err(|_| PgError::Timeout(format!("connection after {:?}", timeout)))?
    }
    /// Clear the prepared statement cache.
    /// Frees memory by removing all cached statements.
    /// Note: Statements remain on the PostgreSQL server until connection closes.
    pub fn clear_cache(&mut self) {
        self.connection.clear_prepared_statement_state();
    }

    /// Get cache statistics.
    /// Returns (current_size, max_capacity).
    pub fn cache_stats(&self) -> (usize, usize) {
        (
            self.connection.stmt_cache.len(),
            self.connection.stmt_cache.cap().get(),
        )
    }

    /// Execute a QAIL command and fetch all rows (CACHED + ZERO-ALLOC).
    /// **Default method** - uses prepared statement caching for best performance.
    /// On first call: sends Parse + Bind + Execute + Sync
    /// On subsequent calls with same SQL: sends only Bind + Execute (SKIPS Parse!)
    /// Uses LRU cache with max 1000 statements (auto-evicts oldest).
    pub async fn fetch_all(&mut self, cmd: &Qail) -> PgResult<Vec<PgRow>> {
        self.fetch_all_with_format(cmd, ResultFormat::Text).await
    }

    /// Execute a QAIL command and fetch all rows using a specific result format.
    ///
    /// `result_format` controls server result-column encoding:
    /// - [`ResultFormat::Text`] for standard text decoding.
    /// - [`ResultFormat::Binary`] for binary wire values.
    pub async fn fetch_all_with_format(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
    ) -> PgResult<Vec<PgRow>> {
        // Delegate to cached-by-default behavior.
        self.fetch_all_cached_with_format(cmd, result_format).await
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
        self.fetch_all_uncached_with_format(cmd, ResultFormat::Text)
            .await
    }

    /// Execute a QAIL command and fetch all rows (UNCACHED) with explicit result format.
    pub async fn fetch_all_uncached_with_format(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
    ) -> PgResult<Vec<PgRow>> {
        use crate::protocol::AstEncoder;

        AstEncoder::encode_cmd_reuse_into_with_result_format(
            cmd,
            &mut self.connection.sql_buf,
            &mut self.connection.params_buf,
            &mut self.connection.write_buf,
            result_format.as_wire_code(),
        )
        .map_err(|e| PgError::Encode(e.to_string()))?;

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
                        error = Some(PgError::QueryServer(err.into()));
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
        self.fetch_all_fast_with_format(cmd, ResultFormat::Text)
            .await
    }

    /// Execute a QAIL command and fetch all rows (FAST VERSION) with explicit result format.
    pub async fn fetch_all_fast_with_format(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
    ) -> PgResult<Vec<PgRow>> {
        use crate::protocol::AstEncoder;

        AstEncoder::encode_cmd_reuse_into_with_result_format(
            cmd,
            &mut self.connection.sql_buf,
            &mut self.connection.params_buf,
            &mut self.connection.write_buf,
            result_format.as_wire_code(),
        )
        .map_err(|e| PgError::Encode(e.to_string()))?;

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
                            if error.is_none()
                                && let Some(columns) = data
                            {
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
        self.fetch_all_cached_with_format(cmd, ResultFormat::Text)
            .await
    }

    /// Execute a QAIL command with prepared statement caching and explicit result format.
    pub async fn fetch_all_cached_with_format(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
    ) -> PgResult<Vec<PgRow>> {
        let mut retried = false;
        loop {
            match self
                .fetch_all_cached_with_format_once(cmd, result_format)
                .await
            {
                Ok(rows) => return Ok(rows),
                Err(err) if !retried && err.is_prepared_statement_retryable() => {
                    retried = true;
                    self.connection.clear_prepared_statement_state();
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn fetch_all_cached_with_format_once(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
    ) -> PgResult<Vec<PgRow>> {
        use crate::protocol::AstEncoder;
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        self.connection.sql_buf.clear();
        self.connection.params_buf.clear();

        // Encode SQL to reusable buffer
        match cmd.action {
            qail_core::ast::Action::Get | qail_core::ast::Action::With => {
                crate::protocol::ast_encoder::dml::encode_select(
                    cmd,
                    &mut self.connection.sql_buf,
                    &mut self.connection.params_buf,
                )?;
            }
            qail_core::ast::Action::Add => {
                crate::protocol::ast_encoder::dml::encode_insert(
                    cmd,
                    &mut self.connection.sql_buf,
                    &mut self.connection.params_buf,
                )?;
            }
            qail_core::ast::Action::Set => {
                crate::protocol::ast_encoder::dml::encode_update(
                    cmd,
                    &mut self.connection.sql_buf,
                    &mut self.connection.params_buf,
                )?;
            }
            qail_core::ast::Action::Del => {
                crate::protocol::ast_encoder::dml::encode_delete(
                    cmd,
                    &mut self.connection.sql_buf,
                    &mut self.connection.params_buf,
                )?;
            }
            _ => {
                // Fallback for unsupported actions
                let (sql, params) =
                    AstEncoder::encode_cmd_sql(cmd).map_err(|e| PgError::Encode(e.to_string()))?;
                let raw_rows = self
                    .connection
                    .query_cached_with_result_format(&sql, &params, result_format.as_wire_code())
                    .await?;
                return Ok(raw_rows
                    .into_iter()
                    .map(|data| PgRow {
                        columns: data,
                        column_info: None,
                    })
                    .collect());
            }
        }

        let mut hasher = DefaultHasher::new();
        self.connection.sql_buf.hash(&mut hasher);
        let sql_hash = hasher.finish();

        let is_cache_miss = !self.connection.stmt_cache.contains(&sql_hash);

        // Build ALL wire messages into write_buf (single syscall)
        self.connection.write_buf.clear();

        let stmt_name = if let Some(name) = self.connection.stmt_cache.get(&sql_hash) {
            name
        } else {
            let name = format!("qail_{:x}", sql_hash);

            // Evict LRU before borrowing sql_buf to avoid borrow conflict
            self.connection.evict_prepared_if_full();

            let sql_str = std::str::from_utf8(&self.connection.sql_buf).unwrap_or("");

            // Buffer Parse + Describe(Statement) for first call
            use crate::protocol::PgEncoder;
            let parse_msg = PgEncoder::encode_parse(&name, sql_str, &[]);
            let describe_msg = PgEncoder::encode_describe(false, &name);
            self.connection.write_buf.extend_from_slice(&parse_msg);
            self.connection.write_buf.extend_from_slice(&describe_msg);

            self.connection.stmt_cache.put(sql_hash, name.clone());
            self.connection
                .prepared_statements
                .insert(name.clone(), sql_str.to_string());

            name
        };

        // Append Bind + Execute + Sync to same buffer
        use crate::protocol::PgEncoder;
        PgEncoder::encode_bind_to_with_result_format(
            &mut self.connection.write_buf,
            &stmt_name,
            &self.connection.params_buf,
            result_format.as_wire_code(),
        )
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
                        self.connection
                            .column_info_cache
                            .insert(sql_hash, info.clone());
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
                        let query_err = PgError::QueryServer(err.into());
                        if query_err.is_prepared_statement_retryable() {
                            self.connection.clear_prepared_statement_state();
                        }
                        error = Some(query_err);
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
        )
        .map_err(|e| PgError::Encode(e.to_string()))?;

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
                    if error.is_none()
                        && let Some(n) = tag.split_whitespace().last()
                    {
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
                        error = Some(PgError::QueryServer(err.into()));
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
        self.query_ast_with_format(cmd, ResultFormat::Text).await
    }

    /// Query a QAIL command and return rows using an explicit result format.
    pub async fn query_ast_with_format(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
    ) -> PgResult<QueryResult> {
        use crate::protocol::AstEncoder;

        let wire_bytes = AstEncoder::encode_cmd_reuse_with_result_format(
            cmd,
            &mut self.connection.sql_buf,
            &mut self.connection.params_buf,
            result_format.as_wire_code(),
        )
        .map_err(|e| PgError::Encode(e.to_string()))?;

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
                        error = Some(PgError::QueryServer(err.into()));
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

        use crate::protocol::PgEncoder;
        use tokio::io::AsyncWriteExt;

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
                        error = Some(PgError::QueryServer(err.into()));
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
    pub async fn stream_cmd(&mut self, cmd: &Qail, batch_size: usize) -> PgResult<Vec<Vec<PgRow>>> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CURSOR_ID: AtomicU64 = AtomicU64::new(0);

        let cursor_name = format!("qail_cursor_{}", CURSOR_ID.fetch_add(1, Ordering::SeqCst));

        // AST-NATIVE: Generate SQL directly from AST (no to_sql_parameterized!)
        use crate::protocol::AstEncoder;
        let mut sql_buf = bytes::BytesMut::with_capacity(256);
        let mut params: Vec<Option<Vec<u8>>> = Vec::new();
        AstEncoder::encode_select_sql(cmd, &mut sql_buf, &mut params)
            .map_err(|e| PgError::Encode(e.to_string()))?;
        let sql = String::from_utf8_lossy(&sql_buf).to_string();

        // Must be in a transaction for cursors
        self.connection.begin_transaction().await?;

        // Declare cursor
        // Declare cursor with bind params — Extended Query Protocol handles $1, $2 etc.
        self.connection.declare_cursor(&cursor_name, &sql, &params).await?;

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
    connect_options: ConnectOptions,
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

    /// Set the password (optional, for cleartext/MD5/SCRAM-SHA-256 auth).
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Set connection timeout (optional).
    pub fn timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set TLS policy (`disable`, `prefer`, `require`).
    pub fn tls_mode(mut self, mode: TlsMode) -> Self {
        self.connect_options.tls_mode = mode;
        self
    }

    /// Set GSSAPI session encryption mode (`disable`, `prefer`, `require`).
    pub fn gss_enc_mode(mut self, mode: GssEncMode) -> Self {
        self.connect_options.gss_enc_mode = mode;
        self
    }

    /// Set custom CA bundle PEM for TLS validation.
    pub fn tls_ca_cert_pem(mut self, ca_pem: Vec<u8>) -> Self {
        self.connect_options.tls_ca_cert_pem = Some(ca_pem);
        self
    }

    /// Enable mTLS using client certificate/key config.
    pub fn mtls(mut self, config: TlsConfig) -> Self {
        self.connect_options.mtls = Some(config);
        self.connect_options.tls_mode = TlsMode::Require;
        self
    }

    /// Override password-auth policy.
    pub fn auth_settings(mut self, settings: AuthSettings) -> Self {
        self.connect_options.auth = settings;
        self
    }

    /// Set SCRAM channel-binding mode.
    pub fn channel_binding_mode(mut self, mode: ScramChannelBindingMode) -> Self {
        self.connect_options.auth.channel_binding = mode;
        self
    }

    /// Set Kerberos/GSS/SSPI token provider callback.
    pub fn gss_token_provider(mut self, provider: GssTokenProvider) -> Self {
        self.connect_options.gss_token_provider = Some(provider);
        self
    }

    /// Set a stateful Kerberos/GSS/SSPI token provider.
    pub fn gss_token_provider_ex(mut self, provider: GssTokenProviderEx) -> Self {
        self.connect_options.gss_token_provider_ex = Some(provider);
        self
    }

    /// Connect to PostgreSQL using the configured parameters.
    pub async fn connect(self) -> PgResult<PgDriver> {
        let host = self.host.unwrap_or_else(|| "127.0.0.1".to_string());
        let port = self.port.unwrap_or(5432);
        let user = self
            .user
            .ok_or_else(|| PgError::Connection("User is required".to_string()))?;
        let database = self
            .database
            .ok_or_else(|| PgError::Connection("Database is required".to_string()))?;

        let password = self.password;
        let options = self.connect_options;

        if let Some(timeout) = self.timeout {
            let options = options.clone();
            tokio::time::timeout(
                timeout,
                PgDriver::connect_with_options(
                    &host,
                    port,
                    &user,
                    &database,
                    password.as_deref(),
                    options,
                ),
            )
            .await
            .map_err(|_| PgError::Timeout(format!("connection after {:?}", timeout)))?
        } else {
            PgDriver::connect_with_options(
                &host,
                port,
                &user,
                &database,
                password.as_deref(),
                options,
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PgError, PgServerError};

    fn server_error(code: &str, message: &str) -> PgError {
        PgError::QueryServer(PgServerError {
            severity: "ERROR".to_string(),
            code: code.to_string(),
            message: message.to_string(),
            detail: None,
            hint: None,
        })
    }

    #[test]
    fn prepared_statement_missing_is_retryable() {
        let err = server_error("26000", "prepared statement \"s1\" does not exist");
        assert!(err.is_prepared_statement_retryable());
    }

    #[test]
    fn cached_plan_replanned_is_retryable() {
        let err = server_error("0A000", "cached plan must be replanned");
        assert!(err.is_prepared_statement_retryable());
    }

    #[test]
    fn unrelated_server_error_is_not_retryable() {
        let err = server_error("23505", "duplicate key value violates unique constraint");
        assert!(!err.is_prepared_statement_retryable());
    }

    // ══════════════════════════════════════════════════════════════════
    // is_transient_server_error
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn serialization_failure_is_transient() {
        let err = server_error("40001", "could not serialize access");
        assert!(err.is_transient_server_error());
    }

    #[test]
    fn deadlock_detected_is_transient() {
        let err = server_error("40P01", "deadlock detected");
        assert!(err.is_transient_server_error());
    }

    #[test]
    fn cannot_connect_now_is_transient() {
        let err = server_error("57P03", "the database system is starting up");
        assert!(err.is_transient_server_error());
    }

    #[test]
    fn admin_shutdown_is_transient() {
        let err = server_error(
            "57P01",
            "terminating connection due to administrator command",
        );
        assert!(err.is_transient_server_error());
    }

    #[test]
    fn connection_exception_class_is_transient() {
        let err = server_error("08006", "connection failure");
        assert!(err.is_transient_server_error());
    }

    #[test]
    fn connection_does_not_exist_is_transient() {
        let err = server_error("08003", "connection does not exist");
        assert!(err.is_transient_server_error());
    }

    #[test]
    fn unique_violation_is_not_transient() {
        let err = server_error("23505", "duplicate key value violates unique constraint");
        assert!(!err.is_transient_server_error());
    }

    #[test]
    fn syntax_error_is_not_transient() {
        let err = server_error("42601", "syntax error at or near \"SELECT\"");
        assert!(!err.is_transient_server_error());
    }

    #[test]
    fn timeout_error_is_transient() {
        let err = PgError::Timeout("query after 30s".to_string());
        assert!(err.is_transient_server_error());
    }

    #[test]
    fn io_connection_reset_is_transient() {
        let err = PgError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "connection reset by peer",
        ));
        assert!(err.is_transient_server_error());
    }

    #[test]
    fn io_permission_denied_is_not_transient() {
        let err = PgError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "permission denied",
        ));
        assert!(!err.is_transient_server_error());
    }

    #[test]
    fn connection_error_is_transient() {
        let err = PgError::Connection("host not found".to_string());
        assert!(err.is_transient_server_error());
    }

    #[test]
    fn prepared_stmt_retryable_counts_as_transient() {
        let err = server_error("26000", "prepared statement \"s1\" does not exist");
        assert!(err.is_transient_server_error());
    }

    // ══════════════════════════════════════════════════════════════════
    // TlsMode parse_sslmode (Phase 1b)
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn tls_mode_parse_disable() {
        assert_eq!(
            super::TlsMode::parse_sslmode("disable"),
            Some(super::TlsMode::Disable)
        );
    }

    #[test]
    fn tls_mode_parse_prefer_variants() {
        assert_eq!(
            super::TlsMode::parse_sslmode("prefer"),
            Some(super::TlsMode::Prefer)
        );
        assert_eq!(
            super::TlsMode::parse_sslmode("allow"),
            Some(super::TlsMode::Prefer),
            "libpq 'allow' maps to Prefer"
        );
    }

    #[test]
    fn tls_mode_parse_require_variants() {
        // All three map to Require — verify-ca and verify-full require
        // TLS but certificate validation is handled at the rustls layer.
        assert_eq!(
            super::TlsMode::parse_sslmode("require"),
            Some(super::TlsMode::Require)
        );
        assert_eq!(
            super::TlsMode::parse_sslmode("verify-ca"),
            Some(super::TlsMode::Require),
            "verify-ca → Require (CA validation at TLS layer)"
        );
        assert_eq!(
            super::TlsMode::parse_sslmode("verify-full"),
            Some(super::TlsMode::Require),
            "verify-full → Require (hostname validation at TLS layer)"
        );
    }

    #[test]
    fn tls_mode_parse_case_insensitive() {
        assert_eq!(
            super::TlsMode::parse_sslmode("REQUIRE"),
            Some(super::TlsMode::Require)
        );
        assert_eq!(
            super::TlsMode::parse_sslmode("Verify-Full"),
            Some(super::TlsMode::Require)
        );
    }

    #[test]
    fn tls_mode_parse_unknown_returns_none() {
        assert_eq!(super::TlsMode::parse_sslmode("invalid"), None);
        assert_eq!(super::TlsMode::parse_sslmode(""), None);
    }

    #[test]
    fn tls_mode_parse_trims_whitespace() {
        assert_eq!(
            super::TlsMode::parse_sslmode("  require  "),
            Some(super::TlsMode::Require)
        );
    }

    #[test]
    fn tls_mode_default_is_disable() {
        assert_eq!(super::TlsMode::default(), super::TlsMode::Disable);
    }

    // ══════════════════════════════════════════════════════════════════
    // AuthSettings behavior matrix (Phase 1c)
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn auth_default_allows_all_password_methods() {
        let auth = super::AuthSettings::default();
        assert!(auth.allow_cleartext_password);
        assert!(auth.allow_md5_password);
        assert!(auth.allow_scram_sha_256);
        assert!(auth.has_any_password_method());
    }

    #[test]
    fn auth_default_disables_enterprise_methods() {
        let auth = super::AuthSettings::default();
        assert!(
            !auth.allow_kerberos_v5,
            "Kerberos V5 should be disabled by default"
        );
        assert!(!auth.allow_gssapi, "GSSAPI should be disabled by default");
        assert!(!auth.allow_sspi, "SSPI should be disabled by default");
    }

    #[test]
    fn auth_scram_only_restricts_to_scram() {
        let auth = super::AuthSettings::scram_only();
        // Only SCRAM allowed
        assert!(auth.allow_scram_sha_256);
        assert!(!auth.allow_cleartext_password);
        assert!(!auth.allow_md5_password);
        // Enterprise auth still disabled
        assert!(!auth.allow_kerberos_v5);
        assert!(!auth.allow_gssapi);
        assert!(!auth.allow_sspi);
        // Still has a password method
        assert!(auth.has_any_password_method());
    }

    #[test]
    fn auth_gssapi_only_disables_all_passwords() {
        let auth = super::AuthSettings::gssapi_only();
        // No password methods
        assert!(!auth.allow_cleartext_password);
        assert!(!auth.allow_md5_password);
        assert!(!auth.allow_scram_sha_256);
        assert!(!auth.has_any_password_method());
        // All enterprise methods enabled
        assert!(auth.allow_kerberos_v5);
        assert!(auth.allow_gssapi);
        assert!(auth.allow_sspi);
    }

    #[test]
    fn auth_has_any_password_when_only_cleartext() {
        let auth = super::AuthSettings {
            allow_cleartext_password: true,
            allow_md5_password: false,
            allow_scram_sha_256: false,
            ..super::AuthSettings::default()
        };
        assert!(auth.has_any_password_method());
    }

    #[test]
    fn auth_no_password_method_when_all_disabled() {
        let auth = super::AuthSettings {
            allow_cleartext_password: false,
            allow_md5_password: false,
            allow_scram_sha_256: false,
            ..super::AuthSettings::default()
        };
        assert!(!auth.has_any_password_method());
    }

    #[test]
    fn auth_enterprise_mechanisms_are_distinct() {
        // Verify the three enterprise mechanisms are distinct values
        assert_ne!(
            super::EnterpriseAuthMechanism::KerberosV5,
            super::EnterpriseAuthMechanism::GssApi
        );
        assert_ne!(
            super::EnterpriseAuthMechanism::GssApi,
            super::EnterpriseAuthMechanism::Sspi
        );
        assert_ne!(
            super::EnterpriseAuthMechanism::KerberosV5,
            super::EnterpriseAuthMechanism::Sspi
        );
    }

    #[test]
    fn auth_channel_binding_default_is_prefer() {
        let auth = super::AuthSettings::default();
        assert_eq!(auth.channel_binding, super::ScramChannelBindingMode::Prefer);
    }

    // ══════════════════════════════════════════════════════════════════
    // parse_database_url — query-string stripping
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn parse_database_url_basic() {
        let (host, port, user, db, pw) =
            super::PgDriver::parse_database_url("postgresql://admin:secret@localhost:5432/mydb")
                .unwrap();
        assert_eq!(host, "localhost");
        assert_eq!(port, 5432);
        assert_eq!(user, "admin");
        assert_eq!(db, "mydb");
        assert_eq!(pw, Some("secret".to_string()));
    }

    #[test]
    fn parse_database_url_strips_query_params() {
        let (_, _, _, db, _) = super::PgDriver::parse_database_url(
            "postgresql://user:pass@host:5432/mydb?sslmode=require&auth_mode=scram_only",
        )
        .unwrap();
        assert_eq!(db, "mydb", "query params must not leak into database name");
    }

    #[test]
    fn parse_database_url_strips_single_query_param() {
        let (_, _, _, db, _) =
            super::PgDriver::parse_database_url("postgres://u:p@h/testdb?gss_provider=linux_krb5")
                .unwrap();
        assert_eq!(db, "testdb");
    }

    #[test]
    fn parse_database_url_no_query_still_works() {
        let (_, _, _, db, _) =
            super::PgDriver::parse_database_url("postgresql://user@host:5432/cleandb").unwrap();
        assert_eq!(db, "cleandb");
    }
}
