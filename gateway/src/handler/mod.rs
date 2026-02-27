//! HTTP Request Handlers for QAIL Gateway.
//!
//! Organized into submodules:
//! - `admin`: Health checks and Swagger UI
//! - `query`: Qail AST query execution (text, binary, fast, batch)
//! - `convert`: PgRow → JSON conversion utilities
//! - `qdrant`: Qdrant vector operations

mod admin;
mod convert;
#[cfg(feature = "qdrant")]
mod qdrant;
mod query;

// ── Public re-exports (preserves existing `crate::handler::*` paths) ──

pub use admin::{health_check, health_check_internal, swagger_ui};
pub use convert::row_to_json;
pub use query::{execute_batch, execute_query, execute_query_binary, execute_query_fast};
pub(crate) use query::{is_query_allowed, clamp_query_limit};

// ── Shared types ──

use serde::{Deserialize, Serialize};

/// Public health check response (minimal, safe for public exposure)
#[derive(Debug, Serialize)]
pub struct HealthCheckPublic {
    /// Health status string (e.g. `"ok"`).
    pub status: String,
    /// Crate / deployment version.
    pub version: String,
}

/// Internal health check response (includes operational metrics)
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    /// Health status string (e.g. `"ok"`).
    pub status: String,
    /// Crate / deployment version.
    pub version: String,
    /// Number of actively used pool connections.
    pub pool_active: usize,
    /// Number of idle pool connections.
    pub pool_idle: usize,
    /// Tenant boundary invariant metrics for internal monitoring.
    /// `violation_rows > 0` means RLS may be compromised.
    pub tenant_guard: crate::tenant_guard::TenantGuardSnapshot,
}

/// Query response
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResponse {
    /// Result rows as JSON objects.
    pub rows: Vec<serde_json::Value>,
    /// Number of rows returned.
    pub count: usize,
}

/// Fast query response (array-of-arrays, no column names)
/// Used by /qail/fast for data pipelines and internal services.
#[derive(Debug, Serialize)]
pub struct FastQueryResponse {
    /// Result rows as arrays of values (no column names).
    pub rows: Vec<Vec<serde_json::Value>>,
    /// Number of rows returned.
    pub count: usize,
}

/// Batch query request
#[derive(Debug, Deserialize)]
pub struct BatchRequest {
    /// List of Qail query strings to execute.
    pub queries: Vec<String>,
    /// Whether to wrap the batch in a single transaction (default: `true`).
    #[serde(default = "default_true")]
    pub transaction: bool,
}

/// Serde default that returns `true` — used for `BatchRequest::transaction`.
fn default_true() -> bool {
    true
}

/// Batch query response
#[derive(Debug, Serialize)]
pub struct BatchResponse {
    /// Per-query results.
    pub results: Vec<BatchQueryResult>,
    /// Total number of queries submitted.
    pub total: usize,
    /// Number of queries that succeeded.
    pub success: usize,
}

/// Result for a single query in a batch
#[derive(Debug, Serialize)]
pub struct BatchQueryResult {
    /// Zero-based index of this query within the batch.
    pub index: usize,
    /// Whether this individual query succeeded.
    pub success: bool,
    /// Rows returned (present only on success for queries that return data).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<serde_json::Value>>,
    /// Row count (present only on success).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<usize>,
    /// Error message (present only on failure).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
