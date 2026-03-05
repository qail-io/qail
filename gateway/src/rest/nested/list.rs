use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    response::Json,
};
use qail_core::ast::{Operator, Value as QailValue};
use serde_json::Value;
use uuid::Uuid;

use crate::GatewayState;
use crate::auth::authenticate_request;
use crate::handler::row_to_json;
use crate::middleware::ApiError;
use crate::rest::filters::{apply_filters, apply_sorting, parse_filters};
use crate::rest::types::{ListParams, ListResponse};

/// GET /api/{parent}/:id/{child} — list child rows filtered by parent FK
///
/// Example: `GET /api/users/123/orders` → `get orders[user_id = 123]`
///
/// Supports the same query parameters as the main list handler.
pub(crate) async fn nested_list_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(parent_id): Path<String>,
    Query(params): Query<ListParams>,
    request: axum::extract::Request,
) -> Result<Json<ListResponse>, ApiError> {
    let path = request.uri().path().to_string();
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();

    // /api/{parent}/{id}/{child}
    if parts.len() < 4 || parts[0] != "api" {
        return Err(ApiError::not_found("nested route"));
    }
    let parent_table = parts[1].to_string();
    let child_table = parts[3].to_string();

    // SECURITY: Block nested access to/from inaccessible tables
    let parent_blocked = if !state.allowed_tables.is_empty() {
        !state.allowed_tables.contains(&parent_table)
    } else {
        state.blocked_tables.contains(&parent_table)
    };
    if parent_blocked {
        return Err(ApiError::forbidden(format!(
            "Table '{}' is not accessible via REST",
            parent_table
        )));
    }
    let child_blocked = if !state.allowed_tables.is_empty() {
        !state.allowed_tables.contains(&child_table)
    } else {
        state.blocked_tables.contains(&child_table)
    };
    if child_blocked {
        return Err(ApiError::forbidden(format!(
            "Table '{}' is not accessible via REST",
            child_table
        )));
    }

    // Validate parent UUID format
    Uuid::parse_str(&parent_id)
        .map_err(|_| ApiError::parse_error(format!("Invalid UUID: {}", parent_id)))?;

    // Look up FK relation: child → parent
    let (fk_col, _pk_col) = state
        .schema
        .relation_for(&child_table, &parent_table)
        .ok_or_else(|| {
            ApiError::not_found(format!("No relation: {} → {}", child_table, parent_table))
        })?;

    let auth = authenticate_request(state.as_ref(), &headers).await?;

    let max_rows = state.config.max_result_rows.min(1000) as i64;
    let limit = params.limit.unwrap_or(50).clamp(1, max_rows);
    let offset = params.offset.unwrap_or(0).clamp(0, 100_000);

    // Build: get child[fk_col = parent_id]
    let mut cmd = qail_core::ast::Qail::get(&child_table).filter(
        fk_col,
        Operator::Eq,
        QailValue::String(parent_id),
    );

    // Column selection
    if let Some(ref select) = params.select {
        let cols: Vec<&str> = select
            .split(',')
            .map(|s| s.trim())
            .filter(|s| *s == "*" || crate::rest::filters::is_safe_identifier(s))
            .collect();
        if !cols.is_empty() {
            cmd = cmd.columns(cols);
        }
    }

    // Sorting (multi-column)
    if let Some(ref sort) = params.sort {
        cmd = apply_sorting(cmd, sort);
    }

    // Distinct
    if let Some(ref distinct) = params.distinct {
        let cols: Vec<&str> = distinct
            .split(',')
            .map(|s| s.trim())
            .filter(|s| crate::rest::filters::is_safe_identifier(s))
            .collect();
        if !cols.is_empty() {
            cmd = cmd.distinct_on(cols);
        }
    }

    // Parse and apply filters from query string
    let query_string = request.uri().query().unwrap_or("");
    let filters = parse_filters(query_string);
    cmd = apply_filters(cmd, &filters);

    // Full-text search
    if let Some(ref term) = params.search {
        let cols = params.search_columns.as_deref().unwrap_or("name");
        // SECURITY: Validate search column identifier.
        if crate::rest::filters::is_safe_identifier(cols) {
            cmd = cmd.filter(cols, Operator::TextSearch, QailValue::String(term.clone()));
        } else {
            tracing::warn!(cols = %cols, "nested: search_columns rejected by identifier guard");
        }
    }

    cmd = cmd.limit(limit);
    cmd = cmd.offset(offset);

    // Apply RLS
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;

    // Execute — single query, no N+1
    let mut conn = state
        .acquire_with_auth_rls_guarded(&auth, Some(&child_table))
        .await?;

    let rows = conn
        .fetch_all_uncached(&cmd)
        .await
        .map_err(|e| ApiError::from_pg_driver_error(&e, Some(&child_table)));

    // Release connection back to pool before processing results
    conn.release().await;

    let rows = rows?;
    let data: Vec<Value> = rows.iter().map(row_to_json).collect();

    // ── Tenant Boundary Invariant ────────────────────────────────────
    if let Some(ref tenant_id) = auth.tenant_id {
        let _proof = crate::tenant_guard::verify_tenant_boundary(
            &data,
            tenant_id,
            &state.config.tenant_column,
            &child_table,
            "rest_nested_list",
        )
        .map_err(|v| {
            tracing::error!("{}", v);
            crate::middleware::ApiError::internal("Data integrity error")
        })?;
    }

    let count = data.len();

    Ok(Json(ListResponse {
        data,
        count,
        total: None,
        limit,
        offset,
    }))
}
