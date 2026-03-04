//! PostgreSQL Connection
//!
//! Low-level TCP connection with wire protocol handling.
//! This is Layer 3 (async I/O).
//!
//! Methods are split across modules for easier maintenance:
//! - `io.rs` - Core I/O (send, recv)
//! - `query.rs` - Query execution
//! - `transaction.rs` - Transaction control
//! - `cursor.rs` - Streaming cursors
//! - `copy.rs` - COPY protocol
//! - `pipeline.rs` - High-performance pipelining
//! - `cancel.rs` - Query cancellation

use super::notification::Notification;
use super::stream::PgStream;
use super::{
    AuthSettings, ConnectOptions, EnterpriseAuthMechanism, GssEncMode, GssTokenProvider,
    GssTokenProviderEx, GssTokenRequest, PgError, PgResult, ScramChannelBindingMode, TlsMode,
};
use crate::protocol::{BackendMessage, FrontendMessage, ScramClient, TransactionStatus};
use bytes::BytesMut;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// Statement cache capacity per connection.
const STMT_CACHE_CAPACITY: NonZeroUsize = NonZeroUsize::new(100).unwrap();

/// Small, allocation-bounded prepared statement cache.
///
/// This mirrors the subset of `lru::LruCache` APIs used by the driver while
/// avoiding external unsoundness advisories on `IterMut` (which we don't use).
#[derive(Debug)]
pub(crate) struct StatementCache {
    capacity: NonZeroUsize,
    entries: HashMap<u64, String>,
    order: VecDeque<u64>, // Front = LRU, back = MRU
}

impl StatementCache {
    pub(crate) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            capacity,
            entries: HashMap::with_capacity(capacity.get()),
            order: VecDeque::with_capacity(capacity.get()),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn cap(&self) -> NonZeroUsize {
        self.capacity
    }

    pub(crate) fn contains(&self, key: &u64) -> bool {
        self.entries.contains_key(key)
    }

    pub(crate) fn get(&mut self, key: &u64) -> Option<String> {
        let value = self.entries.get(key).cloned()?;
        self.touch(*key);
        Some(value)
    }

    pub(crate) fn put(&mut self, key: u64, value: String) {
        if let std::collections::hash_map::Entry::Occupied(mut e) = self.entries.entry(key) {
            e.insert(value);
            self.touch(key);
            return;
        }

        if self.entries.len() >= self.capacity.get() {
            let _ = self.pop_lru();
        }

        self.entries.insert(key, value);
        self.order.push_back(key);
    }

    pub(crate) fn pop_lru(&mut self) -> Option<(u64, String)> {
        while let Some(key) = self.order.pop_front() {
            if let Some(value) = self.entries.remove(&key) {
                return Some((key, value));
            }
        }
        None
    }

    pub(crate) fn remove(&mut self, key: &u64) -> Option<String> {
        let removed = self.entries.remove(key);
        if removed.is_some() {
            self.order.retain(|k| k != key);
        }
        removed
    }

    pub(crate) fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
    }

    fn touch(&mut self, key: u64) {
        self.order.retain(|k| *k != key);
        self.order.push_back(key);
    }
}

/// Initial buffer capacity (64KB for pipeline performance)
pub(crate) const BUFFER_CAPACITY: usize = 65536;

/// SSLRequest message bytes (request code: 80877103)
const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 4, 210, 22, 47];

/// GSSENCRequest message bytes (request code: 80877104)
/// Byte breakdown: length=8 (00 00 00 08), code=80877104 (04 D2 16 30)
const GSSENC_REQUEST: [u8; 8] = [0, 0, 0, 8, 4, 210, 22, 48];

/// Result of sending a GSSENCRequest to the server.
#[derive(Debug)]
enum GssEncNegotiationResult {
    /// Server responded 'G' — willing to perform GSSAPI encryption.
    /// The TCP stream is returned for the caller to establish the
    /// GSSAPI security context and wrap all subsequent traffic.
    Accepted(TcpStream),
    /// Server responded 'N' — unwilling to perform GSSAPI encryption.
    Rejected,
    /// Server sent an ErrorMessage — must not be displayed to user
    /// (CVE-2024-10977: server not yet authenticated).
    ServerError,
}

/// CancelRequest protocol code: 80877102
pub(crate) const CANCEL_REQUEST_CODE: i32 = 80877102;

/// Monotonic session id source for stateful GSS provider callbacks.
static GSS_SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Default timeout for TCP connect + PostgreSQL handshake.
/// Prevents Slowloris DoS where a malicious server accepts TCP but never responds.
pub(crate) const DEFAULT_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const CONNECT_TRANSPORT_PLAIN: &str = "plain";
const CONNECT_TRANSPORT_TLS: &str = "tls";
const CONNECT_TRANSPORT_MTLS: &str = "mtls";
const CONNECT_TRANSPORT_GSSENC: &str = "gssenc";
const CONNECT_BACKEND_TOKIO: &str = "tokio";
#[cfg(all(target_os = "linux", feature = "io_uring"))]
const CONNECT_BACKEND_IO_URING: &str = "io_uring";

/// TLS configuration for mutual TLS (client certificate authentication).
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Client certificate in PEM format
    pub client_cert_pem: Vec<u8>,
    /// Client private key in PEM format
    pub client_key_pem: Vec<u8>,
    /// Optional CA certificate for server verification (uses system certs if None)
    pub ca_cert_pem: Option<Vec<u8>>,
}

impl TlsConfig {
    /// Create a new TLS config from file paths.
    pub fn from_files(
        cert_path: impl AsRef<std::path::Path>,
        key_path: impl AsRef<std::path::Path>,
        ca_path: Option<impl AsRef<std::path::Path>>,
    ) -> std::io::Result<Self> {
        Ok(Self {
            client_cert_pem: std::fs::read(cert_path)?,
            client_key_pem: std::fs::read(key_path)?,
            ca_cert_pem: ca_path.map(|p| std::fs::read(p)).transpose()?,
        })
    }
}

/// Bundled connection parameters for internal functions.
///
/// Groups the 8 common arguments to avoid exceeding clippy's
/// `too_many_arguments` threshold.
struct ConnectParams<'a> {
    host: &'a str,
    port: u16,
    user: &'a str,
    database: &'a str,
    password: Option<&'a str>,
    auth_settings: AuthSettings,
    gss_token_provider: Option<GssTokenProvider>,
    gss_token_provider_ex: Option<GssTokenProviderEx>,
    startup_params: Vec<(String, String)>,
}

#[inline]
fn has_logical_replication_startup_mode(startup_params: &[(String, String)]) -> bool {
    startup_params.iter().any(|(k, v)| {
        k.eq_ignore_ascii_case("replication") && v.eq_ignore_ascii_case("database")
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartupAuthFlow {
    CleartextPassword,
    Md5Password,
    Scram { server_final_seen: bool },
    EnterpriseGss { mechanism: EnterpriseAuthMechanism },
}

impl StartupAuthFlow {
    fn label(self) -> &'static str {
        match self {
            Self::CleartextPassword => "cleartext-password",
            Self::Md5Password => "md5-password",
            Self::Scram { .. } => "scram",
            Self::EnterpriseGss { mechanism } => match mechanism {
                EnterpriseAuthMechanism::KerberosV5 => "kerberos-v5",
                EnterpriseAuthMechanism::GssApi => "gssapi",
                EnterpriseAuthMechanism::Sspi => "sspi",
            },
        }
    }
}

/// A raw PostgreSQL connection.
pub struct PgConnection {
    pub(crate) stream: PgStream,
    pub(crate) buffer: BytesMut,
    pub(crate) write_buf: BytesMut,
    pub(crate) sql_buf: BytesMut,
    pub(crate) params_buf: Vec<Option<Vec<u8>>>,
    pub(crate) prepared_statements: HashMap<String, String>,
    pub(crate) stmt_cache: StatementCache,
    /// Cache of column metadata (RowDescription) per statement hash.
    /// PostgreSQL only sends RowDescription after Parse, not on subsequent Bind+Execute.
    /// This cache ensures by-name column access works even for cached prepared statements.
    pub(crate) column_info_cache: HashMap<u64, Arc<super::ColumnInfo>>,
    pub(crate) process_id: i32,
    pub(crate) secret_key: i32,
    /// Buffer for asynchronous LISTEN/NOTIFY notifications.
    /// Populated by `recv()` when it encounters NotificationResponse messages.
    pub(crate) notifications: VecDeque<Notification>,
    /// True while a logical replication CopyBoth stream is active.
    pub(crate) replication_stream_active: bool,
    /// True when StartupMessage was sent with `replication=database`.
    pub(crate) replication_mode_enabled: bool,
    /// Last seen wal_end from a replication XLogData frame.
    pub(crate) last_replication_wal_end: Option<u64>,
    /// Sticky fail-closed flag for uncertain protocol/I-O state.
    /// Once set, the connection must not return to pool reuse.
    pub(crate) io_desynced: bool,
    /// Statement names scheduled for server-side `Close` on next write.
    /// This keeps backend prepared state aligned with local LRU eviction.
    pub(crate) pending_statement_closes: Vec<String>,
    /// Reentrancy guard for pending-close drain path.
    pub(crate) draining_statement_closes: bool,
}

impl PgConnection {
    /// Connect to PostgreSQL server without authentication (trust mode).
    ///
    /// # Arguments
    ///
    /// * `host` — PostgreSQL server hostname or IP.
    /// * `port` — TCP port (typically 5432).
    /// * `user` — PostgreSQL role name.
    /// * `database` — Target database name.
    pub async fn connect(host: &str, port: u16, user: &str, database: &str) -> PgResult<Self> {
        Self::connect_with_password(host, port, user, database, None).await
    }

    /// Connect to PostgreSQL server with optional password authentication.
    /// Includes a default 10-second timeout covering TCP connect + handshake.
    pub async fn connect_with_password(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
    ) -> PgResult<Self> {
        Self::connect_with_password_and_auth(
            host,
            port,
            user,
            database,
            password,
            AuthSettings::default(),
        )
        .await
    }

    /// Connect to PostgreSQL with explicit enterprise options.
    ///
    /// Negotiation preface order follows libpq:
    ///   1. If gss_enc_mode != Disable → try GSSENCRequest on fresh TCP
    ///   2. If GSSENC rejected/unavailable and tls_mode != Disable → try SSLRequest
    ///   3. If both rejected/unavailable → plain StartupMessage
    pub async fn connect_with_options(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
        options: ConnectOptions,
    ) -> PgResult<Self> {
        let ConnectOptions {
            tls_mode,
            gss_enc_mode,
            tls_ca_cert_pem,
            mtls,
            gss_token_provider,
            gss_token_provider_ex,
            auth,
            startup_params,
        } = options;

        if mtls.is_some() && matches!(tls_mode, TlsMode::Disable) {
            return Err(PgError::Connection(
                "Invalid connect options: mTLS requires tls_mode=Prefer or Require".to_string(),
            ));
        }

        // Enforce gss_enc_mode policy before mTLS early-return.
        // GSSENC and mTLS are both transport-level encryption; using
        // both simultaneously is not supported by the PostgreSQL protocol.
        if gss_enc_mode == GssEncMode::Require && mtls.is_some() {
            return Err(PgError::Connection(
                "gssencmode=require is incompatible with mTLS — both provide \
                 transport encryption; use one or the other"
                    .to_string(),
            ));
        }

        if let Some(mtls_config) = mtls {
            // gss_enc_mode is Disable or Prefer here (Require rejected above).
            // mTLS already provides transport encryption; skip GSSENC.
            return Self::connect_mtls_with_password_and_auth_and_gss(
                ConnectParams {
                    host,
                    port,
                    user,
                    database,
                    password,
                    auth_settings: auth,
                    gss_token_provider,
                    gss_token_provider_ex,
                    startup_params: startup_params.clone(),
                },
                mtls_config,
            )
            .await;
        }

        // ── Phase 1: Try GSSENC if requested ──────────────────────────
        if gss_enc_mode != GssEncMode::Disable {
            match Self::try_gssenc_request(host, port).await {
                Ok(GssEncNegotiationResult::Accepted(tcp_stream)) => {
                    let connect_started = Instant::now();
                    record_connect_attempt(CONNECT_TRANSPORT_GSSENC, CONNECT_BACKEND_TOKIO);
                    #[cfg(all(feature = "enterprise-gssapi", target_os = "linux"))]
                    {
                        let gssenc_fut = async {
                            let gss_stream = super::gss::gssenc_handshake(tcp_stream, host)
                                .await
                                .map_err(PgError::Auth)?;
                            let mut conn = Self {
                                stream: PgStream::GssEnc(gss_stream),
                                buffer: BytesMut::with_capacity(BUFFER_CAPACITY),
                                write_buf: BytesMut::with_capacity(BUFFER_CAPACITY),
                                sql_buf: BytesMut::with_capacity(512),
                                params_buf: Vec::with_capacity(16),
                                prepared_statements: HashMap::new(),
                                stmt_cache: StatementCache::new(STMT_CACHE_CAPACITY),
                                column_info_cache: HashMap::new(),
                                process_id: 0,
                                secret_key: 0,
                                notifications: VecDeque::new(),
                                replication_stream_active: false,
                                replication_mode_enabled: has_logical_replication_startup_mode(
                                    &startup_params,
                                ),
                                last_replication_wal_end: None,
                                io_desynced: false,
                                pending_statement_closes: Vec::new(),
                                draining_statement_closes: false,
                            };
                            conn.send(FrontendMessage::Startup {
                                user: user.to_string(),
                                database: database.to_string(),
                                startup_params: startup_params.clone(),
                            })
                            .await?;
                            conn.handle_startup(
                                user,
                                password,
                                auth,
                                gss_token_provider,
                                gss_token_provider_ex,
                            )
                            .await?;
                            Ok(conn)
                        };
                        let result: PgResult<Self> =
                            tokio::time::timeout(DEFAULT_CONNECT_TIMEOUT, gssenc_fut)
                                .await
                                .map_err(|_| {
                                    PgError::Connection(format!(
                                        "GSSENC connection timeout after {:?} \
                                 (handshake + auth)",
                                        DEFAULT_CONNECT_TIMEOUT
                                    ))
                                })?;
                        record_connect_result(
                            CONNECT_TRANSPORT_GSSENC,
                            CONNECT_BACKEND_TOKIO,
                            &result,
                            connect_started.elapsed(),
                        );
                        return result;
                    }
                    #[cfg(not(all(feature = "enterprise-gssapi", target_os = "linux")))]
                    {
                        let _ = tcp_stream;
                        let err = PgError::Connection(
                            "Server accepted GSSENCRequest but GSSAPI encryption requires \
                             feature enterprise-gssapi on Linux"
                                .to_string(),
                        );
                        metrics::histogram!(
                            "qail_pg_connect_duration_seconds",
                            "transport" => CONNECT_TRANSPORT_GSSENC,
                            "backend" => CONNECT_BACKEND_TOKIO,
                            "outcome" => "error"
                        )
                        .record(connect_started.elapsed().as_secs_f64());
                        metrics::counter!(
                            "qail_pg_connect_failure_total",
                            "transport" => CONNECT_TRANSPORT_GSSENC,
                            "backend" => CONNECT_BACKEND_TOKIO,
                            "error_kind" => connect_error_kind(&err)
                        )
                        .increment(1);
                        return Err(err);
                    }
                }
                Ok(GssEncNegotiationResult::Rejected)
                | Ok(GssEncNegotiationResult::ServerError) => {
                    if gss_enc_mode == GssEncMode::Require {
                        return Err(PgError::Connection(
                            "gssencmode=require but server rejected GSSENCRequest".to_string(),
                        ));
                    }
                    // gss_enc_mode == Prefer — fall through to TLS / plain
                }
                Err(e) => {
                    if gss_enc_mode == GssEncMode::Require {
                        return Err(e);
                    }
                    // gss_enc_mode == Prefer — connection error, fall through
                    tracing::debug!(
                        host = %host,
                        port = %port,
                        error = %e,
                        "gssenc_prefer_fallthrough"
                    );
                }
            }
        }

        // ── Phase 2: TLS / plain per sslmode ──────────────────────────
        match tls_mode {
            TlsMode::Disable => {
                Self::connect_with_password_and_auth_and_gss(ConnectParams {
                    host,
                    port,
                    user,
                    database,
                    password,
                    auth_settings: auth,
                    gss_token_provider,
                    gss_token_provider_ex,
                    startup_params: startup_params.clone(),
                })
                .await
            }
            TlsMode::Require => {
                Self::connect_tls_with_auth_and_gss(
                    ConnectParams {
                        host,
                        port,
                        user,
                        database,
                        password,
                        auth_settings: auth,
                        gss_token_provider,
                        gss_token_provider_ex,
                        startup_params: startup_params.clone(),
                    },
                    tls_ca_cert_pem.as_deref(),
                )
                .await
            }
            TlsMode::Prefer => {
                match Self::connect_tls_with_auth_and_gss(
                    ConnectParams {
                        host,
                        port,
                        user,
                        database,
                        password,
                        auth_settings: auth,
                        gss_token_provider,
                        gss_token_provider_ex: gss_token_provider_ex.clone(),
                        startup_params: startup_params.clone(),
                    },
                    tls_ca_cert_pem.as_deref(),
                )
                .await
                {
                    Ok(conn) => Ok(conn),
                    Err(PgError::Connection(msg))
                        if msg.contains("Server does not support TLS") =>
                    {
                        Self::connect_with_password_and_auth_and_gss(ConnectParams {
                            host,
                            port,
                            user,
                            database,
                            password,
                            auth_settings: auth,
                            gss_token_provider,
                            gss_token_provider_ex,
                            startup_params: startup_params.clone(),
                        })
                        .await
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }

    /// Attempt GSSAPI session encryption negotiation.
    ///
    /// Opens a fresh TCP connection, sends GSSENCRequest (80877104),
    /// reads exactly one byte (CVE-2021-23222 safe), and returns
    /// the result.  The entire operation is bounded by
    /// `DEFAULT_CONNECT_TIMEOUT`.
    async fn try_gssenc_request(host: &str, port: u16) -> PgResult<GssEncNegotiationResult> {
        tokio::time::timeout(
            DEFAULT_CONNECT_TIMEOUT,
            Self::try_gssenc_request_inner(host, port),
        )
        .await
        .map_err(|_| {
            PgError::Connection(format!(
                "GSSENCRequest timeout after {:?}",
                DEFAULT_CONNECT_TIMEOUT
            ))
        })?
    }

    /// Inner GSSENCRequest logic without timeout wrapper.
    async fn try_gssenc_request_inner(host: &str, port: u16) -> PgResult<GssEncNegotiationResult> {
        use tokio::io::AsyncReadExt;

        let addr = format!("{}:{}", host, port);
        let mut tcp_stream = TcpStream::connect(&addr).await?;
        tcp_stream.set_nodelay(true)?;

        // Send the 8-byte GSSENCRequest.
        tcp_stream.write_all(&GSSENC_REQUEST).await?;
        tcp_stream.flush().await?;

        // CVE-2021-23222: Read exactly one byte.  The server must
        // respond with a single 'G' or 'N'.  Any additional bytes
        // in the buffer indicate a buffer-stuffing attack.
        let mut response = [0u8; 1];
        tcp_stream.read_exact(&mut response).await?;

        match response[0] {
            b'G' => {
                // CVE-2021-23222 check: verify no extra bytes are buffered.
                // Use a non-blocking peek to detect leftover data.
                let mut peek_buf = [0u8; 1];
                match tcp_stream.try_read(&mut peek_buf) {
                    Ok(0) => {} // EOF — fine (shouldn't happen yet but harmless)
                    Ok(_n) => {
                        // Extra bytes after 'G' — possible buffer-stuffing.
                        return Err(PgError::Connection(
                            "Protocol violation: extra bytes after GSSENCRequest 'G' response \
                             (possible CVE-2021-23222 buffer-stuffing attack)"
                                .to_string(),
                        ));
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // No extra data — this is the expected path.
                    }
                    Err(e) => {
                        return Err(PgError::Io(e));
                    }
                }
                Ok(GssEncNegotiationResult::Accepted(tcp_stream))
            }
            b'N' => Ok(GssEncNegotiationResult::Rejected),
            b'E' => {
                // Server sent an ErrorMessage.  Per CVE-2024-10977 we
                // must NOT display this to users since the server has
                // not been authenticated.  Log at trace only.
                tracing::trace!(
                    host = %host,
                    port = %port,
                    "gssenc_request_server_error (suppressed per CVE-2024-10977)"
                );
                Ok(GssEncNegotiationResult::ServerError)
            }
            other => Err(PgError::Connection(format!(
                "Unexpected response to GSSENCRequest: 0x{:02X} \
                     (expected 'G'=0x47 or 'N'=0x4E)",
                other
            ))),
        }
    }

    /// Connect to PostgreSQL server with optional password authentication and auth policy.
    pub async fn connect_with_password_and_auth(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
        auth_settings: AuthSettings,
    ) -> PgResult<Self> {
        Self::connect_with_password_and_auth_and_gss(ConnectParams {
            host,
            port,
            user,
            database,
            password,
            auth_settings,
            gss_token_provider: None,
            gss_token_provider_ex: None,
            startup_params: Vec::new(),
        })
        .await
    }

    async fn connect_with_password_and_auth_and_gss(params: ConnectParams<'_>) -> PgResult<Self> {
        let connect_started = Instant::now();
        let attempt_backend = plain_connect_attempt_backend();
        record_connect_attempt(CONNECT_TRANSPORT_PLAIN, attempt_backend);
        let result = tokio::time::timeout(
            DEFAULT_CONNECT_TIMEOUT,
            Self::connect_with_password_inner(params),
        )
        .await
        .map_err(|_| {
            PgError::Connection(format!(
                "Connection timeout after {:?} (TCP connect + handshake)",
                DEFAULT_CONNECT_TIMEOUT
            ))
        })?;
        let backend = result
            .as_ref()
            .map(|conn| connect_backend_for_stream(&conn.stream))
            .unwrap_or(attempt_backend);
        record_connect_result(
            CONNECT_TRANSPORT_PLAIN,
            backend,
            &result,
            connect_started.elapsed(),
        );
        result
    }

    /// Inner connection logic without timeout wrapper.
    async fn connect_with_password_inner(params: ConnectParams<'_>) -> PgResult<Self> {
        let ConnectParams {
            host,
            port,
            user,
            database,
            password,
            auth_settings,
            gss_token_provider,
            gss_token_provider_ex,
            startup_params,
        } = params;
        let replication_mode_enabled = has_logical_replication_startup_mode(&startup_params);
        let addr = format!("{}:{}", host, port);
        let stream = Self::connect_plain_stream(&addr).await?;

        let mut conn = Self {
            stream,
            buffer: BytesMut::with_capacity(BUFFER_CAPACITY),
            write_buf: BytesMut::with_capacity(BUFFER_CAPACITY), // 64KB write buffer
            sql_buf: BytesMut::with_capacity(512),
            params_buf: Vec::with_capacity(16), // SQL encoding buffer
            prepared_statements: HashMap::new(),
            stmt_cache: StatementCache::new(STMT_CACHE_CAPACITY),
            column_info_cache: HashMap::new(),
            process_id: 0,
            secret_key: 0,
            notifications: VecDeque::new(),
            replication_stream_active: false,
            replication_mode_enabled,
            last_replication_wal_end: None,
            io_desynced: false,
            pending_statement_closes: Vec::new(),
            draining_statement_closes: false,
        };

        conn.send(FrontendMessage::Startup {
            user: user.to_string(),
            database: database.to_string(),
            startup_params,
        })
        .await?;

        conn.handle_startup(
            user,
            password,
            auth_settings,
            gss_token_provider,
            gss_token_provider_ex,
        )
        .await?;

        Ok(conn)
    }

    async fn connect_plain_stream(addr: &str) -> PgResult<PgStream> {
        let tcp_stream = TcpStream::connect(addr).await?;
        tcp_stream.set_nodelay(true)?;

        #[cfg(all(target_os = "linux", feature = "io_uring"))]
        {
            if should_try_uring_plain() {
                match super::uring::UringTcpStream::from_tokio(tcp_stream) {
                    Ok(uring_stream) => {
                        tracing::info!(
                            addr = %addr,
                            "qail-pg: using io_uring plain TCP transport"
                        );
                        return Ok(PgStream::Uring(uring_stream));
                    }
                    Err(e) => {
                        tracing::warn!(
                            addr = %addr,
                            error = %e,
                            "qail-pg: io_uring stream conversion failed; falling back to tokio TCP"
                        );
                        let fallback = TcpStream::connect(addr).await?;
                        fallback.set_nodelay(true)?;
                        return Ok(PgStream::Tcp(fallback));
                    }
                }
            }
        }

        Ok(PgStream::Tcp(tcp_stream))
    }

    /// Connect to PostgreSQL server with TLS encryption.
    /// Includes a default 10-second timeout covering TCP connect + TLS + handshake.
    pub async fn connect_tls(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
    ) -> PgResult<Self> {
        Self::connect_tls_with_auth(
            host,
            port,
            user,
            database,
            password,
            AuthSettings::default(),
            None,
        )
        .await
    }

    /// Connect to PostgreSQL over TLS with explicit auth policy and optional custom CA bundle.
    pub async fn connect_tls_with_auth(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
        auth_settings: AuthSettings,
        ca_cert_pem: Option<&[u8]>,
    ) -> PgResult<Self> {
        Self::connect_tls_with_auth_and_gss(
            ConnectParams {
                host,
                port,
                user,
                database,
                password,
                auth_settings,
                gss_token_provider: None,
                gss_token_provider_ex: None,
                startup_params: Vec::new(),
            },
            ca_cert_pem,
        )
        .await
    }

    async fn connect_tls_with_auth_and_gss(
        params: ConnectParams<'_>,
        ca_cert_pem: Option<&[u8]>,
    ) -> PgResult<Self> {
        let connect_started = Instant::now();
        record_connect_attempt(CONNECT_TRANSPORT_TLS, CONNECT_BACKEND_TOKIO);
        let result = tokio::time::timeout(
            DEFAULT_CONNECT_TIMEOUT,
            Self::connect_tls_inner(params, ca_cert_pem),
        )
        .await
        .map_err(|_| {
            PgError::Connection(format!(
                "TLS connection timeout after {:?}",
                DEFAULT_CONNECT_TIMEOUT
            ))
        })?;
        record_connect_result(
            CONNECT_TRANSPORT_TLS,
            CONNECT_BACKEND_TOKIO,
            &result,
            connect_started.elapsed(),
        );
        result
    }

    /// Inner TLS connection logic without timeout wrapper.
    async fn connect_tls_inner(
        params: ConnectParams<'_>,
        ca_cert_pem: Option<&[u8]>,
    ) -> PgResult<Self> {
        let ConnectParams {
            host,
            port,
            user,
            database,
            password,
            auth_settings,
            gss_token_provider,
            gss_token_provider_ex,
            startup_params,
        } = params;
        let replication_mode_enabled = has_logical_replication_startup_mode(&startup_params);
        use tokio::io::AsyncReadExt;
        use tokio_rustls::TlsConnector;
        use tokio_rustls::rustls::ClientConfig;
        use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, pem::PemObject};

        let addr = format!("{}:{}", host, port);
        let mut tcp_stream = TcpStream::connect(&addr).await?;

        // Send SSLRequest
        tcp_stream.write_all(&SSL_REQUEST).await?;

        // Read response
        let mut response = [0u8; 1];
        tcp_stream.read_exact(&mut response).await?;

        if response[0] != b'S' {
            return Err(PgError::Connection(
                "Server does not support TLS".to_string(),
            ));
        }

        let mut root_cert_store = tokio_rustls::rustls::RootCertStore::empty();

        if let Some(ca_pem) = ca_cert_pem {
            let certs = CertificateDer::pem_slice_iter(ca_pem)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| PgError::Connection(format!("Invalid CA certificate PEM: {}", e)))?;
            if certs.is_empty() {
                return Err(PgError::Connection(
                    "No CA certificates found in provided PEM".to_string(),
                ));
            }
            for cert in certs {
                let _ = root_cert_store.add(cert);
            }
        } else {
            let certs = rustls_native_certs::load_native_certs();
            for cert in certs.certs {
                let _ = root_cert_store.add(cert);
            }
        }

        let config = ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();

        let connector = TlsConnector::from(Arc::new(config));
        let server_name = ServerName::try_from(host.to_string())
            .map_err(|_| PgError::Connection("Invalid hostname for TLS".to_string()))?;

        let tls_stream = connector
            .connect(server_name, tcp_stream)
            .await
            .map_err(|e| PgError::Connection(format!("TLS handshake failed: {}", e)))?;

        let mut conn = Self {
            stream: PgStream::Tls(Box::new(tls_stream)),
            buffer: BytesMut::with_capacity(BUFFER_CAPACITY),
            write_buf: BytesMut::with_capacity(BUFFER_CAPACITY),
            sql_buf: BytesMut::with_capacity(512),
            params_buf: Vec::with_capacity(16),
            prepared_statements: HashMap::new(),
            stmt_cache: StatementCache::new(STMT_CACHE_CAPACITY),
            column_info_cache: HashMap::new(),
            process_id: 0,
            secret_key: 0,
            notifications: VecDeque::new(),
            replication_stream_active: false,
            replication_mode_enabled,
            last_replication_wal_end: None,
            io_desynced: false,
            pending_statement_closes: Vec::new(),
            draining_statement_closes: false,
        };

        conn.send(FrontendMessage::Startup {
            user: user.to_string(),
            database: database.to_string(),
            startup_params,
        })
        .await?;

        conn.handle_startup(
            user,
            password,
            auth_settings,
            gss_token_provider,
            gss_token_provider_ex,
        )
        .await?;

        Ok(conn)
    }

    /// Connect with mutual TLS (client certificate authentication).
    /// # Arguments
    /// * `host` - PostgreSQL server hostname
    /// * `port` - PostgreSQL server port
    /// * `user` - Database user
    /// * `database` - Database name
    /// * `config` - TLS configuration with client cert/key
    /// # Example
    /// ```ignore
    /// let config = TlsConfig {
    ///     client_cert_pem: include_bytes!("client.crt").to_vec(),
    ///     client_key_pem: include_bytes!("client.key").to_vec(),
    ///     ca_cert_pem: Some(include_bytes!("ca.crt").to_vec()),
    /// };
    /// let conn = PgConnection::connect_mtls("localhost", 5432, "user", "db", config).await?;
    /// ```
    pub async fn connect_mtls(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        config: TlsConfig,
    ) -> PgResult<Self> {
        Self::connect_mtls_with_password_and_auth(
            host,
            port,
            user,
            database,
            None,
            config,
            AuthSettings::default(),
        )
        .await
    }

    /// Connect with mutual TLS and optional password fallback.
    pub async fn connect_mtls_with_password_and_auth(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
        config: TlsConfig,
        auth_settings: AuthSettings,
    ) -> PgResult<Self> {
        Self::connect_mtls_with_password_and_auth_and_gss(
            ConnectParams {
                host,
                port,
                user,
                database,
                password,
                auth_settings,
                gss_token_provider: None,
                gss_token_provider_ex: None,
                startup_params: Vec::new(),
            },
            config,
        )
        .await
    }

    async fn connect_mtls_with_password_and_auth_and_gss(
        params: ConnectParams<'_>,
        config: TlsConfig,
    ) -> PgResult<Self> {
        let connect_started = Instant::now();
        record_connect_attempt(CONNECT_TRANSPORT_MTLS, CONNECT_BACKEND_TOKIO);
        let result = tokio::time::timeout(
            DEFAULT_CONNECT_TIMEOUT,
            Self::connect_mtls_inner(params, config),
        )
        .await
        .map_err(|_| {
            PgError::Connection(format!(
                "mTLS connection timeout after {:?}",
                DEFAULT_CONNECT_TIMEOUT
            ))
        })?;
        record_connect_result(
            CONNECT_TRANSPORT_MTLS,
            CONNECT_BACKEND_TOKIO,
            &result,
            connect_started.elapsed(),
        );
        result
    }

    /// Inner mTLS connection logic without timeout wrapper.
    async fn connect_mtls_inner(params: ConnectParams<'_>, config: TlsConfig) -> PgResult<Self> {
        let ConnectParams {
            host,
            port,
            user,
            database,
            password,
            auth_settings,
            gss_token_provider,
            gss_token_provider_ex,
            startup_params,
        } = params;
        let replication_mode_enabled = has_logical_replication_startup_mode(&startup_params);
        use tokio::io::AsyncReadExt;
        use tokio_rustls::TlsConnector;
        use tokio_rustls::rustls::{
            ClientConfig,
            pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject},
        };

        let addr = format!("{}:{}", host, port);
        let mut tcp_stream = TcpStream::connect(&addr).await?;

        // Send SSLRequest
        tcp_stream.write_all(&SSL_REQUEST).await?;

        // Read response
        let mut response = [0u8; 1];
        tcp_stream.read_exact(&mut response).await?;

        if response[0] != b'S' {
            return Err(PgError::Connection(
                "Server does not support TLS".to_string(),
            ));
        }

        let mut root_cert_store = tokio_rustls::rustls::RootCertStore::empty();

        if let Some(ca_pem) = &config.ca_cert_pem {
            let certs = CertificateDer::pem_slice_iter(ca_pem)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| PgError::Connection(format!("Invalid CA certificate PEM: {}", e)))?;
            if certs.is_empty() {
                return Err(PgError::Connection(
                    "No CA certificates found in provided PEM".to_string(),
                ));
            }
            for cert in certs {
                let _ = root_cert_store.add(cert);
            }
        } else {
            // Use system certs
            let certs = rustls_native_certs::load_native_certs();
            for cert in certs.certs {
                let _ = root_cert_store.add(cert);
            }
        }

        let client_certs: Vec<CertificateDer<'static>> =
            CertificateDer::pem_slice_iter(&config.client_cert_pem)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| PgError::Connection(format!("Invalid client cert PEM: {}", e)))?;
        if client_certs.is_empty() {
            return Err(PgError::Connection(
                "No client certificates found in PEM".to_string(),
            ));
        }

        let client_key = PrivateKeyDer::from_pem_slice(&config.client_key_pem)
            .map_err(|e| PgError::Connection(format!("Invalid client key PEM: {}", e)))?;

        let tls_config = ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_client_auth_cert(client_certs, client_key)
            .map_err(|e| PgError::Connection(format!("Invalid client cert/key: {}", e)))?;

        let connector = TlsConnector::from(Arc::new(tls_config));
        let server_name = ServerName::try_from(host.to_string())
            .map_err(|_| PgError::Connection("Invalid hostname for TLS".to_string()))?;

        let tls_stream = connector
            .connect(server_name, tcp_stream)
            .await
            .map_err(|e| PgError::Connection(format!("mTLS handshake failed: {}", e)))?;

        let mut conn = Self {
            stream: PgStream::Tls(Box::new(tls_stream)),
            buffer: BytesMut::with_capacity(BUFFER_CAPACITY),
            write_buf: BytesMut::with_capacity(BUFFER_CAPACITY),
            sql_buf: BytesMut::with_capacity(512),
            params_buf: Vec::with_capacity(16),
            prepared_statements: HashMap::new(),
            stmt_cache: StatementCache::new(STMT_CACHE_CAPACITY),
            column_info_cache: HashMap::new(),
            process_id: 0,
            secret_key: 0,
            notifications: VecDeque::new(),
            replication_stream_active: false,
            replication_mode_enabled,
            last_replication_wal_end: None,
            io_desynced: false,
            pending_statement_closes: Vec::new(),
            draining_statement_closes: false,
        };

        conn.send(FrontendMessage::Startup {
            user: user.to_string(),
            database: database.to_string(),
            startup_params,
        })
        .await?;

        conn.handle_startup(
            user,
            password,
            auth_settings,
            gss_token_provider,
            gss_token_provider_ex,
        )
        .await?;

        Ok(conn)
    }

    /// Connect to PostgreSQL server via Unix domain socket.
    #[cfg(unix)]
    pub async fn connect_unix(
        socket_path: &str,
        user: &str,
        database: &str,
        password: Option<&str>,
    ) -> PgResult<Self> {
        use tokio::net::UnixStream;

        let unix_stream = UnixStream::connect(socket_path).await?;

        let mut conn = Self {
            stream: PgStream::Unix(unix_stream),
            buffer: BytesMut::with_capacity(BUFFER_CAPACITY),
            write_buf: BytesMut::with_capacity(BUFFER_CAPACITY),
            sql_buf: BytesMut::with_capacity(512),
            params_buf: Vec::with_capacity(16),
            prepared_statements: HashMap::new(),
            stmt_cache: StatementCache::new(STMT_CACHE_CAPACITY),
            column_info_cache: HashMap::new(),
            process_id: 0,
            secret_key: 0,
            notifications: VecDeque::new(),
            replication_stream_active: false,
            replication_mode_enabled: false,
            last_replication_wal_end: None,
            io_desynced: false,
            pending_statement_closes: Vec::new(),
            draining_statement_closes: false,
        };

        conn.send(FrontendMessage::Startup {
            user: user.to_string(),
            database: database.to_string(),
            startup_params: Vec::new(),
        })
        .await?;

        conn.handle_startup(user, password, AuthSettings::default(), None, None)
            .await?;

        Ok(conn)
    }

    /// Handle startup sequence (auth + params).
    async fn handle_startup(
        &mut self,
        user: &str,
        password: Option<&str>,
        auth_settings: AuthSettings,
        gss_token_provider: Option<GssTokenProvider>,
        gss_token_provider_ex: Option<GssTokenProviderEx>,
    ) -> PgResult<()> {
        let mut scram_client: Option<ScramClient> = None;
        let mut startup_auth_flow: Option<StartupAuthFlow> = None;
        let mut saw_auth_ok = false;
        let gss_session_id = GSS_SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut gss_roundtrips: u32 = 0;
        const MAX_GSS_ROUNDTRIPS: u32 = 32;

        loop {
            let msg = self.recv().await?;
            if saw_auth_ok
                && matches!(
                    &msg,
                    BackendMessage::AuthenticationOk
                        | BackendMessage::AuthenticationKerberosV5
                        | BackendMessage::AuthenticationGSS
                        | BackendMessage::AuthenticationGSSContinue(_)
                        | BackendMessage::AuthenticationSSPI
                        | BackendMessage::AuthenticationCleartextPassword
                        | BackendMessage::AuthenticationMD5Password(_)
                        | BackendMessage::AuthenticationSASL(_)
                        | BackendMessage::AuthenticationSASLContinue(_)
                        | BackendMessage::AuthenticationSASLFinal(_)
                )
            {
                return Err(PgError::Protocol(
                    "Received authentication challenge after AuthenticationOk".to_string(),
                ));
            }
            match msg {
                BackendMessage::AuthenticationOk => {
                    if let Some(StartupAuthFlow::Scram {
                        server_final_seen: false,
                    }) = startup_auth_flow
                    {
                        return Err(PgError::Protocol(
                            "Received AuthenticationOk before AuthenticationSASLFinal".to_string(),
                        ));
                    }
                    saw_auth_ok = true;
                }
                BackendMessage::AuthenticationKerberosV5 => {
                    if let Some(flow) = startup_auth_flow {
                        return Err(PgError::Protocol(format!(
                            "Received AuthenticationKerberosV5 while {} authentication is in progress",
                            flow.label()
                        )));
                    }
                    startup_auth_flow = Some(StartupAuthFlow::EnterpriseGss {
                        mechanism: EnterpriseAuthMechanism::KerberosV5,
                    });

                    if !auth_settings.allow_kerberos_v5 {
                        return Err(PgError::Auth(
                            "Server requested Kerberos V5 authentication, but Kerberos V5 is disabled by AuthSettings".to_string(),
                        ));
                    }

                    if gss_token_provider.is_none() && gss_token_provider_ex.is_none() {
                        return Err(PgError::Auth(
                            "Kerberos V5 authentication requested but no GSS token provider is configured. Set ConnectOptions.gss_token_provider or ConnectOptions.gss_token_provider_ex.".to_string(),
                        ));
                    }

                    let token = generate_gss_token(
                        gss_session_id,
                        EnterpriseAuthMechanism::KerberosV5,
                        None,
                        gss_token_provider,
                        gss_token_provider_ex.as_ref(),
                    )
                    .map_err(|e| {
                        PgError::Auth(format!("Kerberos V5 token generation failed: {}", e))
                    })?;

                    self.send(FrontendMessage::GSSResponse(token)).await?;
                }
                BackendMessage::AuthenticationGSS => {
                    if let Some(flow) = startup_auth_flow {
                        return Err(PgError::Protocol(format!(
                            "Received AuthenticationGSS while {} authentication is in progress",
                            flow.label()
                        )));
                    }
                    startup_auth_flow = Some(StartupAuthFlow::EnterpriseGss {
                        mechanism: EnterpriseAuthMechanism::GssApi,
                    });

                    if !auth_settings.allow_gssapi {
                        return Err(PgError::Auth(
                            "Server requested GSSAPI authentication, but GSSAPI is disabled by AuthSettings".to_string(),
                        ));
                    }

                    if gss_token_provider.is_none() && gss_token_provider_ex.is_none() {
                        return Err(PgError::Auth(
                            "GSSAPI authentication requested but no GSS token provider is configured. Set ConnectOptions.gss_token_provider or ConnectOptions.gss_token_provider_ex.".to_string(),
                        ));
                    }

                    let token = generate_gss_token(
                        gss_session_id,
                        EnterpriseAuthMechanism::GssApi,
                        None,
                        gss_token_provider,
                        gss_token_provider_ex.as_ref(),
                    )
                    .map_err(|e| {
                        PgError::Auth(format!("GSSAPI initial token generation failed: {}", e))
                    })?;

                    self.send(FrontendMessage::GSSResponse(token)).await?;
                }
                BackendMessage::AuthenticationSSPI => {
                    if let Some(flow) = startup_auth_flow {
                        return Err(PgError::Protocol(format!(
                            "Received AuthenticationSSPI while {} authentication is in progress",
                            flow.label()
                        )));
                    }
                    startup_auth_flow = Some(StartupAuthFlow::EnterpriseGss {
                        mechanism: EnterpriseAuthMechanism::Sspi,
                    });

                    if !auth_settings.allow_sspi {
                        return Err(PgError::Auth(
                            "Server requested SSPI authentication, but SSPI is disabled by AuthSettings".to_string(),
                        ));
                    }

                    if gss_token_provider.is_none() && gss_token_provider_ex.is_none() {
                        return Err(PgError::Auth(
                            "SSPI authentication requested but no GSS token provider is configured. Set ConnectOptions.gss_token_provider or ConnectOptions.gss_token_provider_ex.".to_string(),
                        ));
                    }

                    let token = generate_gss_token(
                        gss_session_id,
                        EnterpriseAuthMechanism::Sspi,
                        None,
                        gss_token_provider,
                        gss_token_provider_ex.as_ref(),
                    )
                    .map_err(|e| {
                        PgError::Auth(format!("SSPI initial token generation failed: {}", e))
                    })?;

                    self.send(FrontendMessage::GSSResponse(token)).await?;
                }
                BackendMessage::AuthenticationGSSContinue(server_token) => {
                    gss_roundtrips += 1;
                    if gss_roundtrips > MAX_GSS_ROUNDTRIPS {
                        return Err(PgError::Auth(format!(
                            "GSS handshake exceeded {} roundtrips — aborting",
                            MAX_GSS_ROUNDTRIPS
                        )));
                    }

                    let mechanism = match startup_auth_flow {
                        Some(StartupAuthFlow::EnterpriseGss { mechanism }) => mechanism,
                        Some(flow) => {
                            return Err(PgError::Protocol(format!(
                                "Received AuthenticationGSSContinue while {} authentication is in progress",
                                flow.label()
                            )));
                        }
                        None => {
                            return Err(PgError::Auth(
                                "Received GSSContinue without AuthenticationGSS/SSPI/KerberosV5 init"
                                    .to_string(),
                            ));
                        }
                    };

                    if gss_token_provider.is_none() && gss_token_provider_ex.is_none() {
                        return Err(PgError::Auth(
                            "Received GSSContinue but no GSS token provider is configured. Set ConnectOptions.gss_token_provider or ConnectOptions.gss_token_provider_ex.".to_string(),
                        ));
                    }

                    let token = generate_gss_token(
                        gss_session_id,
                        mechanism,
                        Some(&server_token),
                        gss_token_provider,
                        gss_token_provider_ex.as_ref(),
                    )
                    .map_err(|e| {
                        PgError::Auth(format!("GSS continue token generation failed: {}", e))
                    })?;

                    // Only send the response if there is actually a token to
                    // send.  When gss_init_sec_context returns GSS_S_COMPLETE
                    // on the final round, the token may be empty.  Sending an
                    // empty GSSResponse ('p') after the server already
                    // considers auth complete trips the "invalid frontend
                    // message type 112" FATAL in PostgreSQL.
                    if !token.is_empty() {
                        self.send(FrontendMessage::GSSResponse(token)).await?;
                    }
                }
                BackendMessage::AuthenticationCleartextPassword => {
                    if let Some(flow) = startup_auth_flow {
                        return Err(PgError::Protocol(format!(
                            "Received AuthenticationCleartextPassword while {} authentication is in progress",
                            flow.label()
                        )));
                    }
                    startup_auth_flow = Some(StartupAuthFlow::CleartextPassword);

                    if !auth_settings.allow_cleartext_password {
                        return Err(PgError::Auth(
                            "Server requested cleartext authentication, but cleartext is disabled by AuthSettings"
                                .to_string(),
                        ));
                    }
                    let password = password.ok_or_else(|| {
                        PgError::Auth("Password required for cleartext authentication".to_string())
                    })?;
                    self.send(FrontendMessage::PasswordMessage(password.to_string()))
                        .await?;
                }
                BackendMessage::AuthenticationMD5Password(salt) => {
                    if let Some(flow) = startup_auth_flow {
                        return Err(PgError::Protocol(format!(
                            "Received AuthenticationMD5Password while {} authentication is in progress",
                            flow.label()
                        )));
                    }
                    startup_auth_flow = Some(StartupAuthFlow::Md5Password);

                    if !auth_settings.allow_md5_password {
                        return Err(PgError::Auth(
                            "Server requested MD5 authentication, but MD5 is disabled by AuthSettings"
                                .to_string(),
                        ));
                    }
                    let password = password.ok_or_else(|| {
                        PgError::Auth("Password required for MD5 authentication".to_string())
                    })?;
                    let md5_password = md5_password_message(user, password, salt);
                    self.send(FrontendMessage::PasswordMessage(md5_password))
                        .await?;
                }
                BackendMessage::AuthenticationSASL(mechanisms) => {
                    if let Some(flow) = startup_auth_flow {
                        return Err(PgError::Protocol(format!(
                            "Received AuthenticationSASL while {} authentication is in progress",
                            flow.label()
                        )));
                    }
                    startup_auth_flow = Some(StartupAuthFlow::Scram {
                        server_final_seen: false,
                    });

                    if !auth_settings.allow_scram_sha_256 {
                        return Err(PgError::Auth(
                            "Server requested SCRAM authentication, but SCRAM is disabled by AuthSettings"
                                .to_string(),
                        ));
                    }
                    let password = password.ok_or_else(|| {
                        PgError::Auth("Password required for SCRAM authentication".to_string())
                    })?;

                    let tls_binding = self.tls_server_end_point_channel_binding();
                    let (mechanism, channel_binding_data) = select_scram_mechanism(
                        &mechanisms,
                        tls_binding,
                        auth_settings.channel_binding,
                    )
                    .map_err(PgError::Auth)?;

                    let client = if let Some(binding_data) = channel_binding_data {
                        ScramClient::new_with_tls_server_end_point(user, password, binding_data)
                    } else {
                        ScramClient::new(user, password)
                    };
                    let first_message = client.client_first_message();

                    self.send(FrontendMessage::SASLInitialResponse {
                        mechanism,
                        data: first_message,
                    })
                    .await?;

                    scram_client = Some(client);
                }
                BackendMessage::AuthenticationSASLContinue(server_data) => {
                    match startup_auth_flow {
                        Some(StartupAuthFlow::Scram {
                            server_final_seen: false,
                        }) => {}
                        Some(StartupAuthFlow::Scram {
                            server_final_seen: true,
                        }) => {
                            return Err(PgError::Protocol(
                                "Received AuthenticationSASLContinue after AuthenticationSASLFinal"
                                    .to_string(),
                            ));
                        }
                        Some(flow) => {
                            return Err(PgError::Protocol(format!(
                                "Received AuthenticationSASLContinue while {} authentication is in progress",
                                flow.label()
                            )));
                        }
                        None => {
                            return Err(PgError::Auth(
                                "Received SASL Continue without SASL init".to_string(),
                            ));
                        }
                    }

                    let client = scram_client.as_mut().ok_or_else(|| {
                        PgError::Auth("Received SASL Continue without SASL init".to_string())
                    })?;

                    let final_message = client
                        .process_server_first(&server_data)
                        .map_err(|e| PgError::Auth(format!("SCRAM error: {}", e)))?;

                    self.send(FrontendMessage::SASLResponse(final_message))
                        .await?;
                }
                BackendMessage::AuthenticationSASLFinal(server_signature) => {
                    match startup_auth_flow {
                        Some(StartupAuthFlow::Scram {
                            server_final_seen: false,
                        }) => {
                            startup_auth_flow = Some(StartupAuthFlow::Scram {
                                server_final_seen: true,
                            });
                        }
                        Some(StartupAuthFlow::Scram {
                            server_final_seen: true,
                        }) => {
                            return Err(PgError::Protocol(
                                "Received duplicate AuthenticationSASLFinal".to_string(),
                            ));
                        }
                        Some(flow) => {
                            return Err(PgError::Protocol(format!(
                                "Received AuthenticationSASLFinal while {} authentication is in progress",
                                flow.label()
                            )));
                        }
                        None => {
                            return Err(PgError::Auth(
                                "Received SASL Final without SASL init".to_string(),
                            ));
                        }
                    }

                    let client = scram_client.as_ref().ok_or_else(|| {
                        PgError::Auth("Received SASL Final without SASL init".to_string())
                    })?;
                    client
                        .verify_server_final(&server_signature)
                        .map_err(|e| PgError::Auth(format!("Server verification failed: {}", e)))?;
                }
                BackendMessage::ParameterStatus { .. } => {
                    if !saw_auth_ok {
                        return Err(PgError::Protocol(
                            "Received ParameterStatus before AuthenticationOk".to_string(),
                        ));
                    }
                }
                BackendMessage::BackendKeyData {
                    process_id,
                    secret_key,
                } => {
                    if !saw_auth_ok {
                        return Err(PgError::Protocol(
                            "Received BackendKeyData before AuthenticationOk".to_string(),
                        ));
                    }
                    self.process_id = process_id;
                    self.secret_key = secret_key;
                }
                BackendMessage::ReadyForQuery(TransactionStatus::Idle)
                | BackendMessage::ReadyForQuery(TransactionStatus::InBlock)
                | BackendMessage::ReadyForQuery(TransactionStatus::Failed) => {
                    if !saw_auth_ok {
                        return Err(PgError::Protocol(
                            "Startup completed without AuthenticationOk".to_string(),
                        ));
                    }
                    return Ok(());
                }
                BackendMessage::ErrorResponse(err) => {
                    return Err(PgError::Connection(err.message));
                }
                BackendMessage::NoticeResponse(_) => {}
                _ => {
                    return Err(PgError::Protocol(
                        "Unexpected backend message during startup".to_string(),
                    ));
                }
            }
        }
    }

    /// Build SCRAM `tls-server-end-point` channel-binding bytes from the server leaf cert.
    ///
    /// PostgreSQL expects the hash of the peer certificate DER for
    /// `SCRAM-SHA-256-PLUS` channel binding. We currently use SHA-256 here.
    fn tls_server_end_point_channel_binding(&self) -> Option<Vec<u8>> {
        let PgStream::Tls(tls) = &self.stream else {
            return None;
        };

        let (_, conn) = tls.get_ref();
        let certs = conn.peer_certificates()?;
        let leaf_cert = certs.first()?;

        let mut hasher = Sha256::new();
        hasher.update(leaf_cert.as_ref());
        Some(hasher.finalize().to_vec())
    }

    /// Gracefully close the connection by sending a Terminate message.
    /// This tells the server we're done and allows proper cleanup.
    pub async fn close(mut self) -> PgResult<()> {
        use crate::protocol::PgEncoder;

        // Send Terminate packet ('X')
        let terminate = PgEncoder::encode_terminate();
        self.write_all_with_timeout(&terminate, "stream write")
            .await?;
        self.flush_with_timeout("stream flush").await?;

        Ok(())
    }

    /// Maximum prepared statements per connection before LRU eviction kicks in.
    ///
    /// This prevents memory spikes from dynamic batch filters generating
    /// thousands of unique SQL shapes within a single request. Using LRU
    /// eviction instead of nuclear `.clear()` preserves hot statements.
    pub(crate) const MAX_PREPARED_PER_CONN: usize = 128;

    /// Evict the least-recently-used prepared statement if at capacity.
    ///
    /// Called before every new statement registration to enforce
    /// `MAX_PREPARED_PER_CONN`. Both `stmt_cache` (LRU ordering) and
    /// `prepared_statements` (name→SQL map) are kept in sync.
    pub(crate) fn evict_prepared_if_full(&mut self) {
        if self.prepared_statements.len() >= Self::MAX_PREPARED_PER_CONN {
            // Pop the LRU entry from the cache
            if let Some((evicted_hash, evicted_name)) = self.stmt_cache.pop_lru() {
                self.prepared_statements.remove(&evicted_name);
                self.column_info_cache.remove(&evicted_hash);
                self.pending_statement_closes.push(evicted_name);
            } else {
                // stmt_cache is empty but prepared_statements is full —
                // shouldn't happen in normal flow, but handle defensively
                // by clearing the oldest entry from the HashMap.
                if let Some(key) = self.prepared_statements.keys().next().cloned() {
                    self.prepared_statements.remove(&key);
                    self.pending_statement_closes.push(key);
                }
            }
        }
    }

    /// Clear all local prepared-statement state for this connection.
    ///
    /// Used by one-shot self-heal paths when server-side statement state
    /// becomes invalid after DDL or failover.
    pub(crate) fn clear_prepared_statement_state(&mut self) {
        self.stmt_cache.clear();
        self.prepared_statements.clear();
        self.column_info_cache.clear();
        self.pending_statement_closes.clear();
    }
}

fn generate_gss_token(
    session_id: u64,
    mechanism: EnterpriseAuthMechanism,
    server_token: Option<&[u8]>,
    legacy_provider: Option<GssTokenProvider>,
    stateful_provider: Option<&GssTokenProviderEx>,
) -> Result<Vec<u8>, String> {
    if let Some(provider) = stateful_provider {
        return provider(GssTokenRequest {
            session_id,
            mechanism,
            server_token,
        });
    }

    if let Some(provider) = legacy_provider {
        return provider(mechanism, server_token);
    }

    Err("No GSS token provider configured".to_string())
}

fn plain_connect_attempt_backend() -> &'static str {
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    {
        if should_try_uring_plain() {
            return CONNECT_BACKEND_IO_URING;
        }
    }
    CONNECT_BACKEND_TOKIO
}

fn connect_backend_for_stream(stream: &PgStream) -> &'static str {
    match stream {
        PgStream::Tcp(_) => CONNECT_BACKEND_TOKIO,
        #[cfg(all(target_os = "linux", feature = "io_uring"))]
        PgStream::Uring(_) => CONNECT_BACKEND_IO_URING,
        PgStream::Tls(_) => CONNECT_BACKEND_TOKIO,
        #[cfg(unix)]
        PgStream::Unix(_) => CONNECT_BACKEND_TOKIO,
        #[cfg(all(feature = "enterprise-gssapi", target_os = "linux"))]
        PgStream::GssEnc(_) => CONNECT_BACKEND_TOKIO,
    }
}

fn connect_error_kind(error: &PgError) -> &'static str {
    match error {
        PgError::Connection(_) => "connection",
        PgError::Protocol(_) => "protocol",
        PgError::Auth(_) => "auth",
        PgError::Query(_) | PgError::QueryServer(_) => "query",
        PgError::NoRows => "no_rows",
        PgError::Io(_) => "io",
        PgError::Encode(_) => "encode",
        PgError::Timeout(_) => "timeout",
        PgError::PoolExhausted { .. } => "pool_exhausted",
        PgError::PoolClosed => "pool_closed",
    }
}

fn record_connect_attempt(transport: &'static str, backend: &'static str) {
    metrics::counter!(
        "qail_pg_connect_attempt_total",
        "transport" => transport,
        "backend" => backend
    )
    .increment(1);
}

fn record_connect_result(
    transport: &'static str,
    backend: &'static str,
    result: &PgResult<PgConnection>,
    elapsed: std::time::Duration,
) {
    let outcome = if result.is_ok() { "success" } else { "error" };
    metrics::histogram!(
        "qail_pg_connect_duration_seconds",
        "transport" => transport,
        "backend" => backend,
        "outcome" => outcome
    )
    .record(elapsed.as_secs_f64());

    if let Err(error) = result {
        metrics::counter!(
            "qail_pg_connect_failure_total",
            "transport" => transport,
            "backend" => backend,
            "error_kind" => connect_error_kind(error)
        )
        .increment(1);
    } else {
        metrics::counter!(
            "qail_pg_connect_success_total",
            "transport" => transport,
            "backend" => backend
        )
        .increment(1);
    }
}

fn select_scram_mechanism(
    mechanisms: &[String],
    tls_server_end_point_binding: Option<Vec<u8>>,
    channel_binding_mode: ScramChannelBindingMode,
) -> Result<(String, Option<Vec<u8>>), String> {
    let has_scram = mechanisms.iter().any(|m| m == "SCRAM-SHA-256");
    let has_scram_plus = mechanisms.iter().any(|m| m == "SCRAM-SHA-256-PLUS");

    match channel_binding_mode {
        ScramChannelBindingMode::Disable => {
            if has_scram {
                return Ok(("SCRAM-SHA-256".to_string(), None));
            }
            Err(format!(
                "channel_binding=disable, but server does not advertise SCRAM-SHA-256. Available: {:?}",
                mechanisms
            ))
        }
        ScramChannelBindingMode::Prefer => {
            if has_scram_plus {
                if let Some(binding) = tls_server_end_point_binding {
                    return Ok(("SCRAM-SHA-256-PLUS".to_string(), Some(binding)));
                }

                if has_scram {
                    return Ok(("SCRAM-SHA-256".to_string(), None));
                }

                return Err(
                    "Server requires SCRAM-SHA-256-PLUS but TLS channel binding is unavailable"
                        .to_string(),
                );
            }

            if has_scram {
                return Ok(("SCRAM-SHA-256".to_string(), None));
            }

            Err(format!(
                "Server doesn't support SCRAM-SHA-256. Available: {:?}",
                mechanisms
            ))
        }
        ScramChannelBindingMode::Require => {
            if !has_scram_plus {
                return Err(
                    "channel_binding=require, but server does not advertise SCRAM-SHA-256-PLUS"
                        .to_string(),
                );
            }
            let binding = tls_server_end_point_binding.ok_or_else(|| {
                "channel_binding=require, but TLS channel binding data is unavailable".to_string()
            })?;
            Ok(("SCRAM-SHA-256-PLUS".to_string(), Some(binding)))
        }
    }
}

/// PostgreSQL MD5 password response: `md5` + md5(hex(md5(password + user)) + 4-byte salt).
fn md5_password_message(user: &str, password: &str, salt: [u8; 4]) -> String {
    use md5::{Digest, Md5};

    let mut inner = Md5::new();
    inner.update(password.as_bytes());
    inner.update(user.as_bytes());
    let inner_hex = format!("{:x}", inner.finalize());

    let mut outer = Md5::new();
    outer.update(inner_hex.as_bytes());
    outer.update(salt);
    format!("md5{:x}", outer.finalize())
}

/// Drop implementation sends Terminate packet if possible.
/// This ensures proper cleanup even without explicit close() call.
impl Drop for PgConnection {
    fn drop(&mut self) {
        // Try to send Terminate packet synchronously using try_write
        // This is best-effort - if it fails, TCP RST will handle cleanup
        let terminate: [u8; 5] = [b'X', 0, 0, 0, 4];

        match &mut self.stream {
            PgStream::Tcp(tcp) => {
                // try_write is non-blocking
                let _ = tcp.try_write(&terminate);
            }
            #[cfg(all(target_os = "linux", feature = "io_uring"))]
            PgStream::Uring(stream) => {
                // io_uring transport uses blocking worker operations;
                // terminate packet in Drop is not viable, but force socket
                // shutdown so timed-out worker ops unblock promptly.
                let _ = stream.abort_inflight();
            }
            PgStream::Tls(_) => {
                // TLS requires async write which we can't do in Drop.
                // The TCP connection close will still notify the server.
                // For graceful TLS shutdown, use connection.close() explicitly.
            }
            #[cfg(unix)]
            PgStream::Unix(unix) => {
                let _ = unix.try_write(&terminate);
            }
            #[cfg(all(feature = "enterprise-gssapi", target_os = "linux"))]
            PgStream::GssEnc(_) => {
                // GSSENC requires async wrap+write; skip in Drop.
            }
        }
    }
}

pub(crate) fn parse_affected_rows(tag: &str) -> u64 {
    tag.split_whitespace()
        .last()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

#[cfg(all(target_os = "linux", feature = "io_uring"))]
fn should_try_uring_plain() -> bool {
    super::io_backend::should_use_uring_plain_transport()
}

#[cfg(test)]
mod tests {
    use super::{md5_password_message, select_scram_mechanism};
    use crate::driver::ScramChannelBindingMode;
    #[cfg(unix)]
    use {
        super::{PgConnection, StatementCache},
        crate::driver::ColumnInfo,
        crate::driver::stream::PgStream,
        bytes::BytesMut,
        std::collections::{HashMap, VecDeque},
        std::num::NonZeroUsize,
        std::sync::Arc,
        tokio::net::UnixStream,
    };

    #[cfg(unix)]
    fn test_conn() -> PgConnection {
        let (unix_stream, _peer) = UnixStream::pair().expect("unix stream pair");
        PgConnection {
            stream: PgStream::Unix(unix_stream),
            buffer: BytesMut::with_capacity(1024),
            write_buf: BytesMut::with_capacity(1024),
            sql_buf: BytesMut::with_capacity(256),
            params_buf: Vec::new(),
            prepared_statements: HashMap::new(),
            stmt_cache: StatementCache::new(NonZeroUsize::new(2).expect("non-zero")),
            column_info_cache: HashMap::new(),
            process_id: 0,
            secret_key: 0,
            notifications: VecDeque::new(),
            replication_stream_active: false,
            replication_mode_enabled: false,
            last_replication_wal_end: None,
            io_desynced: false,
            pending_statement_closes: Vec::new(),
            draining_statement_closes: false,
        }
    }

    #[test]
    fn test_md5_password_message_known_vector() {
        let hash = md5_password_message("postgres", "secret", [0x12, 0x34, 0x56, 0x78]);
        assert_eq!(hash, "md521561af64619ca746c2a6c4d6cbedb30");
    }

    #[test]
    fn test_md5_password_message_is_stable() {
        let a = md5_password_message("user_a", "pw", [1, 2, 3, 4]);
        let b = md5_password_message("user_a", "pw", [1, 2, 3, 4]);
        assert_eq!(a, b);
        assert!(a.starts_with("md5"));
        assert_eq!(a.len(), 35);
    }

    #[test]
    fn test_select_scram_plus_when_binding_available() {
        let mechanisms = vec![
            "SCRAM-SHA-256".to_string(),
            "SCRAM-SHA-256-PLUS".to_string(),
        ];
        let binding = vec![1, 2, 3];
        let (mechanism, selected_binding) = select_scram_mechanism(
            &mechanisms,
            Some(binding.clone()),
            ScramChannelBindingMode::Prefer,
        )
        .unwrap();
        assert_eq!(mechanism, "SCRAM-SHA-256-PLUS");
        assert_eq!(selected_binding, Some(binding));
    }

    #[test]
    fn test_select_scram_fallback_without_binding() {
        let mechanisms = vec![
            "SCRAM-SHA-256".to_string(),
            "SCRAM-SHA-256-PLUS".to_string(),
        ];
        let (mechanism, selected_binding) =
            select_scram_mechanism(&mechanisms, None, ScramChannelBindingMode::Prefer).unwrap();
        assert_eq!(mechanism, "SCRAM-SHA-256");
        assert_eq!(selected_binding, None);
    }

    #[test]
    fn test_select_scram_plus_only_requires_binding() {
        let mechanisms = vec!["SCRAM-SHA-256-PLUS".to_string()];
        let err =
            select_scram_mechanism(&mechanisms, None, ScramChannelBindingMode::Prefer).unwrap_err();
        assert!(err.contains("SCRAM-SHA-256-PLUS"));
    }

    #[test]
    fn test_select_scram_require_fails_without_plus() {
        let mechanisms = vec!["SCRAM-SHA-256".to_string()];
        let err = select_scram_mechanism(
            &mechanisms,
            Some(vec![1, 2, 3]),
            ScramChannelBindingMode::Require,
        )
        .unwrap_err();
        assert!(err.contains("channel_binding=require"));
        assert!(err.contains("SCRAM-SHA-256-PLUS"));
    }

    #[test]
    fn test_select_scram_disable_rejects_plus_only() {
        let mechanisms = vec!["SCRAM-SHA-256-PLUS".to_string()];
        let err = select_scram_mechanism(&mechanisms, None, ScramChannelBindingMode::Disable)
            .unwrap_err();
        assert!(err.contains("channel_binding=disable"));
    }

    #[test]
    fn test_select_scram_require_fails_without_tls_binding() {
        let mechanisms = vec![
            "SCRAM-SHA-256".to_string(),
            "SCRAM-SHA-256-PLUS".to_string(),
        ];
        let err = select_scram_mechanism(&mechanisms, None, ScramChannelBindingMode::Require)
            .unwrap_err();
        assert!(err.contains("channel_binding=require"));
        assert!(err.contains("unavailable"));
    }

    #[test]
    fn test_select_scram_require_succeeds_with_plus_and_binding() {
        let mechanisms = vec![
            "SCRAM-SHA-256".to_string(),
            "SCRAM-SHA-256-PLUS".to_string(),
        ];
        let binding = vec![10, 20, 30];
        let (mechanism, selected_binding) = select_scram_mechanism(
            &mechanisms,
            Some(binding.clone()),
            ScramChannelBindingMode::Require,
        )
        .unwrap();
        assert_eq!(mechanism, "SCRAM-SHA-256-PLUS");
        assert_eq!(selected_binding, Some(binding));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_evict_prepared_if_full_queues_server_close_and_clears_column_info() {
        let mut conn = test_conn();
        conn.stmt_cache = StatementCache::new(
            NonZeroUsize::new(PgConnection::MAX_PREPARED_PER_CONN).expect("non-zero"),
        );
        for i in 0..PgConnection::MAX_PREPARED_PER_CONN {
            let name = format!("s{}", i);
            conn.prepared_statements
                .insert(name.clone(), format!("SELECT {}", i));
            conn.stmt_cache.put(i as u64, name);
        }
        conn.column_info_cache.insert(
            0,
            Arc::new(ColumnInfo {
                name_to_index: HashMap::new(),
                oids: Vec::new(),
                formats: Vec::new(),
            }),
        );

        conn.evict_prepared_if_full();

        assert_eq!(
            conn.prepared_statements.len(),
            PgConnection::MAX_PREPARED_PER_CONN - 1
        );
        assert_eq!(conn.pending_statement_closes, vec!["s0".to_string()]);
        assert!(!conn.column_info_cache.contains_key(&0));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_clear_prepared_statement_state_clears_pending_closes() {
        let mut conn = test_conn();
        conn.pending_statement_closes.push("s_dead".to_string());
        conn.prepared_statements
            .insert("s1".to_string(), "SELECT 1".to_string());
        conn.stmt_cache.put(1, "s1".to_string());
        conn.column_info_cache.insert(
            1,
            Arc::new(ColumnInfo {
                name_to_index: HashMap::new(),
                oids: Vec::new(),
                formats: Vec::new(),
            }),
        );

        conn.clear_prepared_statement_state();

        assert!(conn.pending_statement_closes.is_empty());
        assert!(conn.prepared_statements.is_empty());
        assert_eq!(conn.stmt_cache.len(), 0);
        assert!(conn.column_info_cache.is_empty());
    }
}
