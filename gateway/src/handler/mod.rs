//! HTTP Request Handlers for QAIL Gateway.
//!
//! Organized into submodules:
//! - `admin`: Health checks and Swagger UI
//! - `query`: Qail AST query execution (text, binary, fast, batch)
//! - `convert`: PgRow → JSON conversion utilities
//! - `qdrant`: Qdrant vector operations

mod admin;
mod convert;
mod query;
mod qdrant;

// ── Public re-exports (preserves existing `crate::handler::*` paths) ──

pub use admin::{health_check, health_check_internal, swagger_ui};
pub use convert::row_to_json;
pub use query::{execute_batch, execute_query, execute_query_binary, execute_query_fast};

// ── Shared types ──

use serde::{Deserialize, Serialize};

/// Public health check response (minimal, safe for public exposure)
#[derive(Debug, Serialize)]
pub struct HealthCheckPublic {
    pub status: String,
    pub version: String,
}

/// Internal health check response (includes operational metrics)
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub pool_active: usize,
    pub pool_idle: usize,
    /// Tenant boundary invariant metrics for internal monitoring.
    /// `violation_rows > 0` means RLS may be compromised.
    pub tenant_guard: crate::tenant_guard::TenantGuardSnapshot,
}

/// Query response
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResponse {
    pub rows: Vec<serde_json::Value>,
    pub count: usize,
}

/// Fast query response (array-of-arrays, no column names)
/// Used by /qail/fast for data pipelines and internal services.
#[derive(Debug, Serialize)]
pub struct FastQueryResponse {
    pub rows: Vec<Vec<serde_json::Value>>,
    pub count: usize,
}

/// Batch query request
#[derive(Debug, Deserialize)]
pub struct BatchRequest {
    pub queries: Vec<String>,
    #[serde(default = "default_true")]
    pub transaction: bool,
}

fn default_true() -> bool { true }

/// Batch query response
#[derive(Debug, Serialize)]
pub struct BatchResponse {
    pub results: Vec<BatchQueryResult>,
    pub total: usize,
    pub success: usize,
}

/// Result for a single query in a batch
#[derive(Debug, Serialize)]
pub struct BatchQueryResult {
    pub index: usize,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
