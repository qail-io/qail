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

use crate::driver::PgResult;
use std::future::Future;
use std::pin::Pin;

/// Boxed async return type for scoped pool helpers (`with_rls`, `with_tenant`, etc.).
pub type ScopedPoolFuture<'a, T> = Pin<Box<dyn Future<Output = PgResult<T>> + Send + 'a>>;

/// Helper to box async closures for scoped pool helpers.
///
/// This avoids writing `Box::pin(...)` directly at every callsite.
///
/// # Example
/// ```ignore
/// use qail_pg::scope;
///
/// let users = pool
///     .with_tenant(tenant_id, |conn| scope(async move {
///         conn.fetch_all_uncached(&cmd).await
///     }))
///     .await?;
/// ```
#[inline]
pub fn scope<'a, T, Fut>(fut: Fut) -> ScopedPoolFuture<'a, T>
where
    Fut: Future<Output = PgResult<T>> + Send + 'a,
{
    Box::pin(fut)
}

// ── Public API ──────────────────────────────────────────────────────
pub use churn::PoolStats;
pub use config::PoolConfig;
pub use connection::PooledConnection;
pub use lifecycle::{PgPool, spawn_pool_maintenance};

// ── Crate-internal ──────────────────────────────────────────────────
pub(crate) use config::apply_url_query_params;
