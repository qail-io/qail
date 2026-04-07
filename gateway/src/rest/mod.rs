//! Auto-REST route generation from schema
//!
//! Given a `SchemaRegistry`, generates RESTful CRUD endpoints for every table:
//!
//! ```text
//! GET    /api/{table}                    → List with pagination, filtering, sorting
//! GET    /api/{table}?expand=rel         → List with LEFT JOIN on FK relation
//! GET    /api/{table}?expand=nested:rel  → List with nested FK objects/arrays
//! GET    /api/{table}?stream=true        → NDJSON streaming response
//! GET    /api/{table}?col.gte=10         → Filter key-style
//! GET    /api/{table}?col=gte.10         → Filter value-style
//! GET    /api/{table}?sort=col:desc      → Sort explicit direction
//! GET    /api/{table}?sort=-col          → Sort prefix direction
//! GET    /api/{table}?distinct=col       → Distinct on columns
//! GET    /api/{table}/aggregate          → Aggregation (count, sum, avg, min, max)
//! GET    /api/{table}/_aggregate         → Aggregation alias (compat)
//! GET    /api/{table}/_explain           → EXPLAIN ANALYZE for query
//! GET    /api/{table}/{id}               → Get single row by PK
//! POST   /api/{table}                    → Create row(s) from JSON body (single or batch)
//! POST   /api/{table}?on_conflict=col    → Upsert (insert or update on conflict)
//! POST   /api/{table}?returning=*        → Return created row(s)
//! PATCH  /api/{table}/{id}               → Partial update from JSON body
//! PATCH  /api/{table}/{id}?returning=*   → Return updated row
//! DELETE /api/{table}/{id}               → Delete by PK
//! GET    /api/{parent}/{id}/{child}      → Nested list: children filtered by parent FK
//! POST   /api/rpc/{function}             → Call function with JSON args
//! ```

mod branch;
mod devex;
mod explain;
mod filters;
mod handlers;
pub mod nested;
mod routes;
pub mod types;

use axum::{Router, http::HeaderMap};
use std::sync::Arc;

use qail_core::branch::BranchContext;
use qail_core::transpiler::ToSql;

use crate::GatewayState;

// Re-export public request/response types
pub use types::{
    AggregateParams, AggregateResponse, BatchCreateResponse, DeleteResponse, ListParams,
    ListResponse, MutationParams, SingleResponse,
};

/// Extract branch context from X-Branch-ID header.
#[allow(dead_code)]
fn extract_branch_from_headers(headers: &HeaderMap) -> BranchContext {
    let branch_id = headers.get("x-branch-id").and_then(|v| v.to_str().ok());
    BranchContext::from_header(branch_id)
}

/// Extract table name from the request path (e.g., `/api/users` → `users`)
fn extract_table_name(uri: &axum::http::Uri) -> Option<String> {
    let path = uri.path();
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    if parts.len() >= 2 && parts[0] == "api" {
        Some(parts[1].to_string())
    } else {
        None
    }
}

/// Check if the request has `X-Qail-Debug: true` header.
fn is_debug_request(headers: &HeaderMap) -> bool {
    headers
        .get("x-qail-debug")
        .and_then(|v| v.to_str().ok())
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false)
}

/// Generate the SQL string from a Qail AST command (for debug output).
/// Uses the transpiler's `to_sql()` method which shows the final SQL
/// after RLS policy injection.
fn debug_sql(cmd: &qail_core::ast::Qail) -> String {
    cmd.to_sql()
}

/// Resolve the tenant scope column for a table from loaded schema metadata.
///
/// Prefers the configured canonical tenant column (`tenant_id` by default).
/// Falls back to legacy `operator_id` to keep tenant isolation fail-closed
/// during ongoing schema migrations.
pub(crate) fn tenant_scope_column_for_table(
    state: &GatewayState,
    table_name: &str,
) -> Option<String> {
    let table = state.schema.table(table_name)?;
    if table
        .columns
        .iter()
        .any(|col| col.name == state.config.tenant_column)
    {
        return Some(state.config.tenant_column.clone());
    }

    if table.columns.iter().any(|col| col.name == "operator_id") {
        tracing::debug!(
            table = %table_name,
            "Using legacy operator_id as tenant scope column"
        );
        return Some("operator_id".to_string());
    }

    None
}

/// Resolve tenant scope for a table in the current request context.
///
/// Returns `(scope_column, tenant_id)` when:
/// - auth has a non-empty tenant scope
/// - table is not configured as tenant-guard exempt
/// - a tenant scope column can be derived from schema
pub(crate) fn tenant_scope_filter_for_table(
    state: &GatewayState,
    auth: &crate::auth::AuthContext,
    table_name: &str,
) -> Option<(String, String)> {
    let tenant_id = auth.tenant_id.as_deref()?.trim();
    if tenant_id.is_empty() {
        return None;
    }

    if state
        .config
        .tenant_guard_exempt_tables
        .iter()
        .any(|t| t == table_name)
    {
        return None;
    }

    let scope_column = tenant_scope_column_for_table(state, table_name)?;
    Some((scope_column, tenant_id.to_string()))
}

/// Generate REST routes for all tables in the schema registry.
///
/// Returns an Axum Router with routes like `/api/users`, `/api/orders`, etc.
///
/// **Manifest**: Also writes a `rest_manifest.json` file to the config root
/// directory, listing every endpoint (allowed + blocked) for security auditing.
pub fn auto_rest_routes(state: Arc<GatewayState>) -> Router<Arc<GatewayState>> {
    routes::auto_rest_routes(state)
}
