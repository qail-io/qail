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

use super::super::notification::Notification;
use super::super::stream::PgStream;
use super::super::{AuthSettings, EnterpriseAuthMechanism};
use crate::protocol::PROTOCOL_VERSION_3_2;
use bytes::BytesMut;
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::net::TcpStream;

/// Statement cache capacity per connection.
pub(super) const STMT_CACHE_CAPACITY: NonZeroUsize = NonZeroUsize::new(100).unwrap();

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
pub(super) const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 4, 210, 22, 47];

/// GSSENCRequest message bytes (request code: 80877104)
/// Byte breakdown: length=8 (00 00 00 08), code=80877104 (04 D2 16 30)
pub(super) const GSSENC_REQUEST: [u8; 8] = [0, 0, 0, 8, 4, 210, 22, 48];

/// Result of sending a GSSENCRequest to the server.
#[derive(Debug)]
pub(super) enum GssEncNegotiationResult {
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
pub(super) static GSS_SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Default timeout for TCP connect + PostgreSQL handshake.
/// Prevents Slowloris DoS where a malicious server accepts TCP but never responds.
pub(crate) const DEFAULT_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
pub(super) const CONNECT_TRANSPORT_PLAIN: &str = "plain";
pub(super) const CONNECT_TRANSPORT_TLS: &str = "tls";
pub(super) const CONNECT_TRANSPORT_MTLS: &str = "mtls";
pub(super) const CONNECT_TRANSPORT_GSSENC: &str = "gssenc";
pub(super) const CONNECT_BACKEND_TOKIO: &str = "tokio";
#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub(super) const CONNECT_BACKEND_IO_URING: &str = "io_uring";

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
#[derive(Clone)]
pub(super) struct ConnectParams<'a> {
    pub(super) host: &'a str,
    pub(super) port: u16,
    pub(super) user: &'a str,
    pub(super) database: &'a str,
    pub(super) password: Option<&'a str>,
    pub(super) auth_settings: AuthSettings,
    pub(super) gss_token_provider: Option<super::super::GssTokenProvider>,
    pub(super) gss_token_provider_ex: Option<super::super::GssTokenProviderEx>,
    pub(super) protocol_minor: u16,
    pub(super) startup_params: Vec<(String, String)>,
}

#[inline]
pub(super) fn has_logical_replication_startup_mode(startup_params: &[(String, String)]) -> bool {
    startup_params
        .iter()
        .any(|(k, v)| k.eq_ignore_ascii_case("replication") && v.eq_ignore_ascii_case("database"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StartupAuthFlow {
    CleartextPassword,
    Md5Password,
    Scram { server_final_seen: bool },
    EnterpriseGss { mechanism: EnterpriseAuthMechanism },
}

impl StartupAuthFlow {
    pub(super) fn label(self) -> &'static str {
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
    pub(crate) column_info_cache: HashMap<u64, Arc<super::super::ColumnInfo>>,
    pub(crate) process_id: i32,
    /// Legacy 4-byte cancel secret key (protocol 3.0-compatible wrappers).
    ///
    /// For protocol 3.2 extended key lengths, this remains `0` and callers
    /// must use `cancel_key_bytes`.
    pub(crate) secret_key: i32,
    /// Full cancel key bytes (`4..=256`) from BackendKeyData.
    pub(crate) cancel_key_bytes: Vec<u8>,
    /// Startup protocol minor requested by this connection (for example `2` for 3.2).
    pub(crate) requested_protocol_minor: u16,
    /// Startup protocol minor negotiated with the server.
    pub(crate) negotiated_protocol_minor: u16,
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
    #[inline]
    pub(crate) fn default_protocol_minor() -> u16 {
        (PROTOCOL_VERSION_3_2 & 0xFFFF) as u16
    }

    /// Startup protocol minor requested by this connection.
    #[inline]
    pub fn requested_protocol_minor(&self) -> u16 {
        self.requested_protocol_minor
    }

    /// Startup protocol minor negotiated with the server.
    #[inline]
    pub fn negotiated_protocol_minor(&self) -> u16 {
        self.negotiated_protocol_minor
    }
}
