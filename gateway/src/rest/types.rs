//! Request/response types for REST endpoints.

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ============================================================================
// Query parameters
// ============================================================================

/// Query parameters for list endpoints
#[derive(Debug, Deserialize, Default)]
pub struct ListParams {
    /// Max rows to return (default: 50, max: 1000)
    pub limit: Option<i64>,
    /// Offset for pagination
    pub offset: Option<i64>,
    /// Sort: `column:asc` or multi-column `col1:asc,col2:desc`
    pub sort: Option<String>,
    /// Columns to select: `id,name,email`
    pub select: Option<String>,
    /// Expand FK relations via LEFT JOIN: `?expand=orders` or `?expand=orders,products`
    pub expand: Option<String>,
    /// Cursor-based pagination: value of the sort column to paginate after
    pub cursor: Option<String>,
    /// Distinct on columns: `?distinct=col1,col2`
    pub distinct: Option<String>,
    /// Stream NDJSON: `?stream=true` for line-delimited JSON response
    #[serde(default)]
    pub stream: Option<bool>,
    /// Full-text search: `?search=term` searches across text columns
    pub search: Option<String>,
    /// Columns to search: `?search_columns=name,description` (default: all text columns)
    pub search_columns: Option<String>,
}

impl ListParams {
    /// Validate and normalize REST pagination.
    pub(crate) fn bounded_limit_offset(
        &self,
        max_result_rows: usize,
    ) -> Result<(i64, i64), String> {
        let max_rows = max_result_rows.min(1000) as i64;
        let limit = self.limit.unwrap_or(50);
        if limit <= 0 {
            return Err("limit must be greater than zero".to_string());
        }

        let offset = self.offset.unwrap_or(0);
        if offset < 0 {
            return Err("offset must not be negative".to_string());
        }

        Ok((limit.min(max_rows), offset.min(100_000)))
    }
}

/// Query parameters for mutation (create/update/delete) returning support
#[derive(Debug, Deserialize, Default)]
pub struct MutationParams {
    /// Columns to return: `*` for all, or `id,name` for specific columns
    pub returning: Option<String>,
    /// Upsert conflict column: `?on_conflict=id`
    pub on_conflict: Option<String>,
    /// Upsert conflict action: `update` (default) or `nothing`
    pub on_conflict_action: Option<String>,
}

/// Query parameters for aggregation endpoint
#[derive(Debug, Deserialize, Default)]
pub struct AggregateParams {
    /// Aggregate function: count, sum, avg, min, max
    pub func: Option<String>,
    /// Column to aggregate (default: * for count)
    pub column: Option<String>,
    /// Group by columns: `group_by=status,type`
    pub group_by: Option<String>,
    /// Distinct aggregation: `?distinct=true`
    pub distinct: Option<String>,
}

// ============================================================================
// Response types
// ============================================================================

/// Response for paginated list endpoints.
#[derive(Debug, Serialize)]
pub struct ListResponse {
    /// Result rows.
    pub data: Vec<Value>,
    /// Number of rows in this page.
    pub count: usize,
    /// Total rows matching the query (if counted).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<i64>,
    /// Page size used.
    pub limit: i64,
    /// Row offset used.
    pub offset: i64,
}

/// Response for single-row endpoints (get by ID).
#[derive(Debug, Serialize)]
pub struct SingleResponse {
    /// The row as a JSON value.
    pub data: Value,
}

/// Response for delete endpoints.
#[derive(Debug, Serialize)]
pub struct DeleteResponse {
    /// Whether the delete operation succeeded.
    pub deleted: bool,
}

/// Response for aggregation endpoints.
#[derive(Debug, Serialize)]
pub struct AggregateResponse {
    /// Aggregation result rows.
    pub data: Vec<Value>,
    /// Number of result groups.
    pub count: usize,
}

/// Response for batch create (bulk insert) endpoints.
#[derive(Debug, Serialize)]
pub struct BatchCreateResponse {
    /// Inserted rows.
    pub data: Vec<Value>,
    /// Number of rows inserted.
    pub count: usize,
}

#[cfg(test)]
mod tests {
    use super::ListParams;

    #[test]
    fn list_params_reject_invalid_pagination_bounds() {
        let params = ListParams {
            limit: Some(0),
            ..Default::default()
        };
        assert!(
            params
                .bounded_limit_offset(1_000)
                .unwrap_err()
                .contains("limit")
        );

        let params = ListParams {
            offset: Some(-1),
            ..Default::default()
        };
        assert!(
            params
                .bounded_limit_offset(1_000)
                .unwrap_err()
                .contains("offset")
        );
    }

    #[test]
    fn list_params_clamps_high_pagination_bounds() {
        let params = ListParams {
            limit: Some(5_000),
            offset: Some(500_000),
            ..Default::default()
        };

        assert_eq!(
            params.bounded_limit_offset(2_000).unwrap(),
            (1_000, 100_000)
        );
    }
}
