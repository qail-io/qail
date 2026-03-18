//! PostgreSQL driver with AST-native wire encoding.
//!
//! **Features:** Zero-alloc encoding, LRU cache (100 max), connection pooling, COPY protocol.
//!
//! ```ignore
//! let mut driver = PgDriver::connect("localhost", 5432, "user", "db").await?;
//! let rows = driver.fetch_all(&Qail::get("users").limit(10)).await?;
//! ```

#![deny(deprecated)]

pub mod driver;
pub mod protocol;
pub mod types;

pub use driver::explain;
#[cfg(all(feature = "enterprise-gssapi", target_os = "linux"))]
pub use driver::gss::{
    LinuxKrb5PreflightReport, LinuxKrb5ProviderConfig, linux_krb5_preflight,
    linux_krb5_token_provider,
};
pub use driver::{
    AuthSettings, ConnectOptions, EnterpriseAuthMechanism, GssEncMode, GssTokenProvider,
    GssTokenProviderEx, GssTokenRequest, IdentifySystem, Notification, PgConnection, PgDriver,
    PgDriverBuilder, PgError, PgPool, PgResult, PgRow, PgServerError, PoolConfig, PoolStats,
    PooledConnection, QailRow, QueryResult, ReplicationKeepalive, ReplicationOption,
    ReplicationSlotInfo, ReplicationStreamMessage, ReplicationStreamStart, ReplicationXLogData,
    ResultFormat, ScopedPoolFuture, ScramChannelBindingMode, TlsConfig, TlsMode, scope,
    spawn_pool_maintenance,
};
pub use protocol::PgEncoder;
pub use types::{
    Cidr, Date, FromPg, Inet, Json, MacAddr, Numeric, Time, Timestamp, ToPg, TypeError, Uuid,
};

/// Generate the RLS SQL string for pipelined execution.
///
/// Returns the `BEGIN; SET LOCAL statement_timeout = ...; SELECT set_config(...)`
/// string that can be passed to `PooledConnection::fetch_all_with_rls()`.
pub fn rls_sql_with_timeout(ctx: &qail_core::rls::RlsContext, timeout_ms: u32) -> String {
    driver::rls::context_to_sql_with_timeout(ctx, timeout_ms)
}

/// Generate the RLS SQL string with both statement and lock timeouts.
///
/// When `lock_timeout_ms` is 0, the lock_timeout clause is omitted.
pub fn rls_sql_with_timeouts(
    ctx: &qail_core::rls::RlsContext,
    statement_timeout_ms: u32,
    lock_timeout_ms: u32,
) -> String {
    driver::rls::context_to_sql_with_timeouts(ctx, statement_timeout_ms, lock_timeout_ms)
}
