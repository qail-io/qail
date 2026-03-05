use super::*;

// ============================================================================
// Query Complexity Guard
// ============================================================================

/// Guards against excessively complex queries.
///
/// Limits:
/// - `max_depth`: Maximum nesting depth (joins, subqueries)
/// - `max_filters`: Maximum number of filter conditions
/// - `max_joins`: Maximum number of JOIN operations
#[derive(Debug, Clone)]
pub struct QueryComplexityGuard {
    /// Maximum nesting depth (subqueries, CTEs, set operations).
    pub max_depth: usize,
    /// Maximum number of filter conditions.
    pub max_filters: usize,
    /// Maximum number of JOIN operations.
    pub max_joins: usize,
}

impl QueryComplexityGuard {
    /// Create a complexity guard with custom limits.
    pub fn new(max_depth: usize, max_filters: usize, max_joins: usize) -> Self {
        Self {
            max_depth,
            max_filters,
            max_joins,
        }
    }

    /// Default production limits
    pub fn production() -> Self {
        Self {
            max_depth: 5,
            max_filters: 20,
            max_joins: 10,
        }
    }

    /// Check query complexity against limits
    pub fn check(
        &self,
        depth: usize,
        filter_count: usize,
        join_count: usize,
    ) -> Result<(), ApiError> {
        if depth > self.max_depth {
            return Err(ApiError::new(ApiErrorData {
                code: "QUERY_TOO_COMPLEX".to_string(),
                message: format!("Query depth {} exceeds maximum {}", depth, self.max_depth),
                details: None,
                request_id: None,
                hint: None,
                table: None,
                column: None,
            }));
        }
        if filter_count > self.max_filters {
            return Err(ApiError::new(ApiErrorData {
                code: "QUERY_TOO_COMPLEX".to_string(),
                message: format!(
                    "Filter count {} exceeds maximum {}",
                    filter_count, self.max_filters
                ),
                details: None,
                request_id: None,
                hint: None,
                table: None,
                column: None,
            }));
        }
        if join_count > self.max_joins {
            return Err(ApiError::new(ApiErrorData {
                code: "QUERY_TOO_COMPLEX".to_string(),
                message: format!(
                    "Join count {} exceeds maximum {}",
                    join_count, self.max_joins
                ),
                details: None,
                request_id: None,
                hint: None,
                table: None,
                column: None,
            }));
        }
        Ok(())
    }
}
