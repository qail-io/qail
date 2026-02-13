//! PostgreSQL driver with AST-native wire encoding.
//!
//! **Features:** Zero-alloc encoding, LRU cache (100 max), connection pooling, COPY protocol.
//!
//! ```ignore
//! let mut driver = PgDriver::connect("localhost", 5432, "user", "db").await?;
//! let rows = driver.fetch_all(&Qail::get("users").limit(10)).await?;
//! ```

pub mod driver;
pub mod protocol;
pub mod types;

pub use driver::{
    PgConnection, PgDriver, PgDriverBuilder, PgError, PgPool, PgResult, PgRow, PoolConfig, PoolStats,
    PooledConnection, QailRow, QueryResult,
};
pub use protocol::PgEncoder;
pub use driver::explain;
pub use types::{Date, FromPg, Json, Numeric, Time, Timestamp, ToPg, TypeError, Uuid};

/// Generate the RLS SQL string for pipelined execution.
///
/// Returns the `BEGIN; SET LOCAL statement_timeout = ...; SELECT set_config(...)`
/// string that can be passed to `PooledConnection::fetch_all_with_rls()`.
pub fn rls_sql_with_timeout(ctx: &qail_core::rls::RlsContext, timeout_ms: u32) -> String {
    driver::rls::context_to_sql_with_timeout(ctx, timeout_ms)
}
