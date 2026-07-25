//! QAIL Postgres Driver.
//!
//! `qail-pg` executes `qail-core` AST commands through the native PostgreSQL
//! wire protocol. It owns connection I/O, TLS, authentication, pooling,
//! prepared AST execution, pipeline execution, COPY, LISTEN/NOTIFY, and
//! PostgreSQL type conversion.
//!
//! The normal application path is:
//!
//! ```text
//! qail_core::Qail AST -> qail_pg::PgDriver/PgPool -> PostgreSQL wire protocol
//! ```
//!
//! SQL text may still appear in debugging, EXPLAIN output, or server-side
//! PostgreSQL parse/plan behavior, but application code should build `Qail`
//! commands instead of concatenating SQL strings.
//!
//! ```ignore
//! use qail_core::prelude::*;
//! use qail_pg::PgDriver;
//!
//! let mut driver = PgDriver::connect("localhost", 5432, "user", "db").await?;
//! let cmd = Qail::get("users").columns(["id", "email"]).limit(10);
//! let rows = driver.fetch_all(&cmd).await?;
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
    AstPipelineMode, AuthSettings, AutoCountPath, AutoCountPlan, ConnectOptions,
    EnterpriseAuthMechanism, GssEncMode, GssTokenProvider, GssTokenProviderEx, GssTokenRequest,
    IdentifySystem, Notification, PgBytesRow, PgConnection, PgDriver, PgDriverBuilder, PgError,
    PgPool, PgResult, PgRow, PgServerError, PoolConfig, PoolStats, PooledConnection,
    PreparedAstQuery, QailRow, QueryResult, ReplicationKeepalive, ReplicationOption,
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
