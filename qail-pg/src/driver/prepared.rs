//! High-performance prepared statement handling.
//!
//! This module provides zero-allocation prepared statement caching
//! to match Go pgx performance.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// A prepared statement handle with pre-computed statement name.
///
/// This eliminates per-query hash computation and HashMap lookup.
/// Create once, execute many times.
///
/// # Example
/// ```ignore
/// // Prepare once (compute hash + register with PostgreSQL)
/// let stmt = conn.prepare("SELECT id, name FROM users WHERE id = $1").await?;
///
/// // Execute many times (no hash, no lookup!)
/// for id in 1..1000 {
///     conn.execute_prepared(&stmt, &[Some(id.to_string().into_bytes())]).await?;
/// }
/// ```
#[derive(Clone, Debug)]
pub struct PreparedStatement {
    /// Pre-computed statement name (e.g., "s1234567890abcdef")
    pub(crate) name: String,
    /// Number of parameters (reserved for future validation)
    #[allow(dead_code)]
    pub(crate) param_count: usize,
}

impl PreparedStatement {
    /// Create a new prepared statement handle from SQL bytes.
    /// 
    /// This hashes the SQL bytes directly without String allocation.
    #[inline]
    pub fn from_sql_bytes(sql_bytes: &[u8]) -> Self {
        let name = sql_bytes_to_stmt_name(sql_bytes);
        // Count $N placeholders (simple heuristic)
        let param_count = sql_bytes.windows(2)
            .filter(|w| w[0] == b'$' && w[1].is_ascii_digit())
            .count();
        Self { name, param_count }
    }
    
    /// Create from SQL string (convenience method).
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

/// Hash SQL bytes directly to statement name (no String allocation).
///
/// This is faster than hashing a String because:
/// 1. No UTF-8 validation
/// 2. No heap allocation for String
/// 3. Direct byte hashing
#[inline]
pub fn sql_bytes_to_stmt_name(sql: &[u8]) -> String {
    let mut hasher = DefaultHasher::new();
    sql.hash(&mut hasher);
    format!("s{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_stmt_name_from_bytes() {
        let sql = b"SELECT id, name FROM users WHERE id = $1";
        let name1 = sql_bytes_to_stmt_name(sql);
        let name2 = sql_bytes_to_stmt_name(sql);
        assert_eq!(name1, name2);  // Deterministic
        assert!(name1.starts_with("s"));
        assert_eq!(name1.len(), 17);  // "s" + 16 hex chars
    }
    
    #[test]
    fn test_prepared_statement() {
        let stmt = PreparedStatement::from_sql("SELECT * FROM users WHERE id = $1 AND name = $2");
        assert_eq!(stmt.param_count, 2);
        assert!(stmt.name.starts_with("s"));
    }
}
