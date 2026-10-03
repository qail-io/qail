//! High-performance prepared statement handling.
//!
//! This module provides zero-allocation prepared statement caching
//! to match Go pgx performance.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// A prepared statement handle with pre-computed statement name.
/// This avoids per-query SQL hashing. Typed execution checks the connection's
/// recorded SQL before binding; an evicted handle must be prepared again.
/// Create once, execute many times.
/// # Example
/// ```ignore
/// // Prepare once (compute hash + register with PostgreSQL)
/// let stmt = conn.prepare("SELECT id, name FROM users WHERE id = $1").await?;
/// // Execute many times without hashing the SQL again.
/// for id in 1..1000 {
///     conn.execute_prepared(&stmt, &[Some(id.to_string().into_bytes())]).await?;
/// }
/// ```
#[derive(Clone)]
pub struct PreparedStatement {
    /// Pre-computed statement name (e.g., "s1234567890abcdef")
    pub(crate) name: String,
    /// SQL the handle stands for. The name is a 64-bit hash, so typed
    /// execution compares this with the connection's record before binding.
    pub(crate) sql: Arc<[u8]>,
}

impl std::fmt::Debug for PreparedStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedStatement")
            .field("name", &self.name)
            .field("sql", &String::from_utf8_lossy(&self.sql))
            .finish()
    }
}

/// A fully prepared AST query handle.
///
/// This stores:
/// - precomputed prepared statement identity (`stmt`)
/// - pre-encoded bind parameters (`params`)
/// - source SQL text (`sql`) for retry re-prepare paths
///
/// Use with `PgDriver::fetch_all_prepared_ast()` for the lowest-overhead
/// repeated execution of an identical AST command.
#[derive(Clone, Debug)]
pub struct PreparedAstQuery {
    pub(crate) stmt: PreparedStatement,
    pub(crate) params: Vec<Option<Vec<u8>>>,
    pub(crate) sql: String,
    pub(crate) sql_hash: u64,
}

impl PreparedAstQuery {
    /// Statement name assigned when this handle was prepared.
    /// Execution may recover under a different name after eviction or collision.
    #[inline]
    pub fn statement_name(&self) -> &str {
        self.stmt.name()
    }

    /// Number of bind parameters encoded in this query.
    #[inline]
    pub fn param_count(&self) -> usize {
        self.params.len()
    }
}

impl PreparedStatement {
    /// Create a new prepared statement handle from SQL bytes.
    /// This hashes the SQL bytes directly without String allocation.
    /// It does not register SQL with a connection; call `PgConnection::prepare()`
    /// before typed execution.
    #[inline]
    pub fn from_sql_bytes(sql_bytes: &[u8]) -> Self {
        let name = sql_bytes_to_stmt_name(sql_bytes);
        Self {
            name,
            sql: Arc::from(sql_bytes),
        }
    }

    /// Create from SQL string (convenience method).
    /// This does not register SQL with a connection; use `PgConnection::prepare()`
    /// to obtain a handle ready for typed execution.
    #[inline]
    pub fn from_sql(sql: &str) -> Self {
        Self::from_sql_bytes(sql.as_bytes())
    }

    /// Get the statement name.
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Hash SQL bytes for prepared-statement cache keys.
#[inline]
pub fn sql_bytes_hash(sql: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    sql.hash(&mut hasher);
    hasher.finish()
}

/// Convert a hashed SQL key into a deterministic statement name.
#[inline]
pub fn stmt_name_from_hash(hash: u64) -> String {
    format!("s{hash:016x}")
}

/// Statement name the AST fetch paths (`fetch_all_cached`,
/// `fetch_all_with_rls`, pool hot statements) derive from a cache key.
#[inline]
pub(crate) fn ast_stmt_name_from_hash(hash: u64) -> String {
    format!("qail_{hash:x}")
}

/// Hash SQL bytes directly to statement name (no String allocation).
/// This is faster than hashing a String because:
/// 1. No UTF-8 validation
/// 2. No heap allocation for String
/// 3. Direct byte hashing
#[inline]
pub fn sql_bytes_to_stmt_name(sql: &[u8]) -> String {
    stmt_name_from_hash(sql_bytes_hash(sql))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stmt_name_from_bytes() {
        let sql = b"SELECT id, name FROM users WHERE id = $1";
        let name1 = sql_bytes_to_stmt_name(sql);
        let name2 = sql_bytes_to_stmt_name(sql);
        let hash = sql_bytes_hash(sql);
        let name3 = stmt_name_from_hash(hash);
        assert_eq!(name1, name2); // Deterministic
        assert_eq!(name1, name3);
        assert!(name1.starts_with("s"));
        assert_eq!(name1.len(), 17); // "s" + 16 hex chars
    }

    #[test]
    fn test_prepared_statement() {
        let stmt = PreparedStatement::from_sql("SELECT * FROM users WHERE id = $1 AND name = $2");
        assert!(stmt.name.starts_with("s"));
    }
}
