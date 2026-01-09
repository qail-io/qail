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
    PooledConnection, QailRow,
};
pub use protocol::PgEncoder;
pub use types::{Date, FromPg, Json, Numeric, Time, Timestamp, ToPg, TypeError, Uuid};
