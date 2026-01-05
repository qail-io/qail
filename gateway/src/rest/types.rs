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
