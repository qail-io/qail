//! QAIL driver for Redis - ephemeral state and caching.
//!
//! ⚠️ **PROTOTYPE** - Early development.
//!
//! "Redis stores time — QAIL decides."
//!
//! Uses the unified Qail AST from qail-core with Redis-specific actions.
//!
//! # Example
//! ```ignore
//! use qail_redis::{RedisDriver, RedisExt};
//! use qail_core::prelude::*;
//!
//! let mut driver = RedisDriver::connect("localhost", 6379).await?;
//!
//! // Unified QAIL AST
//! driver.execute(&Qail::redis_set("session:123", b"data".to_vec()).redis_ex(3600)).await?;
//! let value = driver.execute(&Qail::redis_get("session:123")).await?;
//! ```

pub mod cmd;
pub mod decoder;
pub mod driver;
pub mod encoder;
pub mod error;
pub mod pool;
pub mod transport;
pub mod value;

// Re-export the RedisExt trait for fluent methods
pub use cmd::RedisExt;
pub use driver::RedisDriver;
pub use error::{RedisError, RedisResult};
pub use pool::{RedisPool, PoolConfig};
pub use value::Value;

/// Re-export qail-core prelude for convenience.
pub mod prelude {
    pub use qail_core::prelude::*;
    pub use crate::{RedisDriver, RedisError, RedisResult, RedisExt, Value};
    pub use crate::{RedisPool, PoolConfig};
}
