//! PostgreSQL Connection Pool
//!
//! Provides connection pooling with prepared-statement caching, RLS pipelining,
//! churn protection, and hot-statement cross-connection sharing.

mod churn;
mod config;
mod connection;
mod fetch;
mod gss;
mod lifecycle;
#[cfg(test)]
mod tests;

// ── Public API ──────────────────────────────────────────────────────
pub use churn::PoolStats;
pub use config::PoolConfig;
pub use connection::PooledConnection;
pub use lifecycle::{PgPool, spawn_pool_maintenance};

// ── Crate-internal ──────────────────────────────────────────────────
pub(crate) use config::apply_url_query_params;
