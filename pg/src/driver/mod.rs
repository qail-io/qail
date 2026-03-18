//! PostgreSQL Driver Module (Layer 3: Async I/O)
//!
//! Auto-detects the best I/O backend:
//! - Linux 5.1+: io_uring (fastest)
//! - Linux < 5.1 / macOS / Windows: tokio
//!
//! Submodules:
//! - `types` — Core types (PgError, PgRow, ColumnInfo, ResultFormat)
//! - `auth_types` — Auth/security types (AuthSettings, TlsMode, GssEncMode)
//! - `pg_driver` — High-level async driver (connect, fetch, execute, txn)
//! - `pg_driver_builder` — Builder pattern for PgDriver
//! - `connection` — Low-level PgConnection
//! - `pool` — Connection pool (PgPool, PoolConfig, PooledConnection)

// ── Internal submodules ─────────────────────────────────────────────
mod auth_types;
pub mod branch_sql;
mod builder;
mod cancel;
mod connection;
mod copy;
mod core;
mod cursor;
#[cfg(test)]
mod driver_tests;
pub mod explain;
pub(crate) mod extended_flow;
mod fetch;
#[cfg(all(feature = "enterprise-gssapi", target_os = "linux"))]
pub mod gss;
mod io;
pub mod io_backend;
pub mod notification;
mod ops;
mod pipeline;
mod pool;
mod prepared;
mod query;
mod replication;
pub mod rls;
mod row;
mod stream;
mod transaction;
mod types;
#[cfg(all(target_os = "linux", feature = "io_uring"))]
mod uring;

// ── Public API ──────────────────────────────────────────────────────
pub use auth_types::{
    AuthSettings, ConnectOptions, EnterpriseAuthMechanism, GssEncMode, GssTokenProvider,
    GssTokenProviderEx, GssTokenRequest, ScramChannelBindingMode, TlsMode,
};
pub use builder::PgDriverBuilder;
pub use cancel::CancelToken;
pub use connection::{PgConnection, TlsConfig};
pub use core::PgDriver;
pub use notification::Notification;
pub use pool::{
    PgPool, PoolConfig, PoolStats, PooledConnection, ScopedPoolFuture, scope,
    spawn_pool_maintenance,
};
pub use prepared::PreparedStatement;
pub use replication::{
    IdentifySystem, ReplicationKeepalive, ReplicationOption, ReplicationSlotInfo,
    ReplicationStreamMessage, ReplicationStreamStart, ReplicationXLogData,
};
pub use rls::RlsContext;
pub use row::QailRow;
pub use types::{ColumnInfo, PgError, PgResult, PgRow, PgServerError, QueryResult, ResultFormat};

// ── Crate-internal re-exports ───────────────────────────────────────
pub(crate) use connection::{CANCEL_REQUEST_CODE, parse_affected_rows};
pub(crate) use types::{
    is_ignorable_session_message, is_ignorable_session_msg_type, unexpected_backend_message,
    unexpected_backend_msg_type,
};
