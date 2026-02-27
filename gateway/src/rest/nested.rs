//! Nested route handler and FK expansion.
//!
//! - `nested_list_handler` — GET /api/{parent}/:id/{child}
//! - `expand_nested` — Resolve `?expand=nested:rel` into nested JSON

use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    response::Json,
};
use qail_core::ast::{Operator, Value as QailValue};
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;

use crate::GatewayState;
use crate::auth::authenticate_request;
use crate::handler::row_to_json;
use crate::middleware::ApiError;

use super::filters::{apply_filters, apply_sorting, parse_filters};
use super::types::*;

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
        .pool
        .acquire_with_rls_timeouts(
            auth.to_rls_context(),
            state.config.statement_timeout_ms,
            state.config.lock_timeout_ms,
        )
        .await
        .map_err(|e| ApiError::from_pg_driver_error(&e, Some(&child_table)))?;

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

/// Expand FK relations into nested JSON objects/arrays.
///
/// - **Forward FK** (e.g., `orders?expand=nested:users`):
///   `order.user_id` → `order.user = {id, name, ...}` (nested object)
/// - **Reverse FK** (e.g., `users?expand=nested:orders`):
///   `user` → `user.orders = [{...}, {...}]` (nested array)
///
/// Uses batched WHERE IN queries to avoid N+1.
pub(crate) async fn expand_nested(
    state: &Arc<GatewayState>,
    table_name: &str,
    data: &mut [Value],
    relations: &[&str],
    auth: &crate::auth::AuthContext,
) -> Result<(), ApiError> {
    let mut conn = state
        .pool
        .acquire_with_rls_timeouts(
            auth.to_rls_context(),
            state.config.statement_timeout_ms,
            state.config.lock_timeout_ms,
        )
        .await
        .map_err(|e| ApiError::from_pg_driver_error(&e, Some(table_name)))?;

    for rel in relations {
        // Try forward FK: this table → rel table
        if let Some((fk_col, ref_col)) = state.schema.relation_for(table_name, rel) {
            // Collect all FK values from data
            let fk_values: Vec<QailValue> = data
                .iter()
                .filter_map(|row| row.get(fk_col).cloned())
                .filter(|v| !v.is_null())
                .map(|v| match v {
                    Value::String(s) => QailValue::String(s),
                    Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            QailValue::Int(i)
                        } else {
                            QailValue::String(n.to_string())
                        }
                    }
                    other => QailValue::String(other.to_string()),
                })
                .collect();

            if fk_values.is_empty() {
                continue;
            }

            // Fetch related rows in one query: get rel[ref_col IN (...)]
            let mut cmd = qail_core::ast::Qail::get(*rel).filter(
                ref_col,
                Operator::In,
                QailValue::Array(fk_values),
            );
            state
                .policy_engine
                .apply_policies(auth, &mut cmd)
                .map_err(|e| ApiError::forbidden(e.to_string()))?;

            let rows = match conn.fetch_all_uncached(&cmd).await {
                Ok(r) => r,
                Err(e) => {
                    conn.release().await;
                    return Err(ApiError::from_pg_driver_error(&e, Some(rel)));
                }
            };

            // Index by PK
            let related: std::collections::HashMap<String, Value> = rows
                .iter()
                .map(|row| {
                    let json = row_to_json(row);
                    let key = json
                        .get(ref_col)
                        .map(|v| v.as_str().unwrap_or(&v.to_string()).to_string())
                        .unwrap_or_default();
                    (key, json)
                })
                .collect();

            // Inject nested object
            for row in data.iter_mut() {
                if let Some(fk_val) = row.get(fk_col) {
                    let key = fk_val.as_str().unwrap_or(&fk_val.to_string()).to_string();
                    if let Some(related_row) = related.get(&key)
                        && let Some(obj) = row.as_object_mut()
                    {
                        obj.insert(rel.to_string(), related_row.clone());
                    }
                }
            }
            continue;
        }

        // Try reverse FK: rel table → this table
        if let Some((fk_col, ref_col)) = state.schema.relation_for(rel, table_name) {
            // Collect all PK values from data
            let pk_values: Vec<QailValue> = data
                .iter()
                .filter_map(|row| row.get(ref_col).cloned())
                .filter(|v| !v.is_null())
                .map(|v| match v {
                    Value::String(s) => QailValue::String(s),
                    Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            QailValue::Int(i)
                        } else {
                            QailValue::String(n.to_string())
                        }
                    }
                    other => QailValue::String(other.to_string()),
                })
                .collect();

            if pk_values.is_empty() {
                continue;
            }

            // Fetch all child rows: get rel[fk_col IN (...)]
            let mut cmd = qail_core::ast::Qail::get(*rel).filter(
                fk_col,
                Operator::In,
                QailValue::Array(pk_values),
            );
            state
                .policy_engine
                .apply_policies(auth, &mut cmd)
                .map_err(|e| ApiError::forbidden(e.to_string()))?;

            let rows = match conn.fetch_all_uncached(&cmd).await {
                Ok(r) => r,
                Err(e) => {
                    conn.release().await;
                    return Err(ApiError::from_pg_driver_error(&e, Some(rel)));
                }
            };

            // Group by FK value
            let mut grouped: std::collections::HashMap<String, Vec<Value>> =
                std::collections::HashMap::new();
            for row in &rows {
                let json = row_to_json(row);
                let key = json
                    .get(fk_col)
                    .map(|v| v.as_str().unwrap_or(&v.to_string()).to_string())
                    .unwrap_or_default();
                grouped.entry(key).or_default().push(json);
            }

            // Inject nested array
            for row in data.iter_mut() {
                if let Some(pk_val) = row.get(ref_col) {
                    let key = pk_val.as_str().unwrap_or(&pk_val.to_string()).to_string();
                    let children = grouped.get(&key).cloned().unwrap_or_default();
                    if let Some(obj) = row.as_object_mut() {
                        obj.insert(rel.to_string(), serde_json::json!(children));
                    }
                }
            }
            continue;
        }

        // Release connection before returning error
        conn.release().await;
        return Err(ApiError::parse_error(format!(
            "No relation between '{}' and '{}' for nested expansion",
            table_name, rel
        )));
    }

    // Release connection back to pool
    conn.release().await;

    Ok(())
}
