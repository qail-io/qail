//! Branch management handlers and Copy-on-Write helpers for data virtualization.
//!
//! - `apply_branch_overlay` — CoW Read: merge overlay into query results
//! - `redirect_to_overlay` — CoW Write: redirect mutations to overlay table
//! - `branch_create_handler` — POST /api/_branch
//! - `branch_list_handler` — GET /api/_branch
//! - `branch_delete_handler` — DELETE /api/_branch/:name
//! - `branch_merge_handler` — POST /api/_branch/:name/merge

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json},
};
use serde_json::{Value, json};
use std::sync::Arc;

use crate::GatewayState;
use crate::auth::extract_auth_from_headers;
use crate::handler::row_to_json;
use crate::middleware::ApiError;

use super::filters::json_to_qail_value;

// ============================================================================
// Branch CoW helpers — Data Virtualization
// ============================================================================

/// Apply branch overlay to main table data (CoW Read).
///
/// When a branch is active, reads from `_qail_branch_rows` and merges:
/// - `insert` overlays → appended to results
/// - `update` overlays → replace matching PK rows
/// - `delete` overlays → remove matching PK rows
pub(crate) async fn apply_branch_overlay(
    conn: &mut qail_pg::driver::PooledConnection,
    branch_name: &str,
    table_name: &str,
    data: &mut Vec<Value>,
    pk_column: &str,
) {
    let sql = qail_pg::driver::branch_sql::read_overlay_sql(branch_name, table_name);
    let overlay_rows = match conn.get_mut().simple_query(&sql).await {
        Ok(rows) => rows,
        Err(_) => return, // Overlay tables might not exist yet
    };

    for row in &overlay_rows {
        let row_pk = row.get_string(0).unwrap_or_default();
        let operation = row.get_string(1).unwrap_or_default();
        let row_data_str = row.get_string(2).unwrap_or_default();

        match operation.as_str() {
            "insert" => {
                // Append new row
                if let Ok(val) = serde_json::from_str::<Value>(&row_data_str) {
                    data.push(val);
                }
            }
            "update" => {
                // Replace matching PK row
                if let Ok(new_val) = serde_json::from_str::<Value>(&row_data_str) {
                    let mut found = false;
                    for existing in data.iter_mut() {
                        if let Some(existing_pk) = existing.get(pk_column).and_then(|v| {
                            v.as_str()
                                .map(|s| s.to_string())
                                .or_else(|| Some(v.to_string()))
                        }) && existing_pk == row_pk
                        {
                            *existing = new_val.clone();
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        // PK not in main — treat as insert
                        data.push(new_val);
                    }
                }
            }
            "delete" => {
                // Remove matching PK row
                data.retain(|existing| {
                    existing
                        .get(pk_column)
                        .and_then(|v| {
                            v.as_str()
                                .map(|s| s.to_string())
                                .or_else(|| Some(v.to_string()))
                        })
                        .map(|pk| pk != row_pk)
                        .unwrap_or(true)
                });
            }
            _ => {}
        }
    }
}

/// Redirect a write to the branch overlay (CoW Write).
///
/// Instead of inserting into the main table, stores the row in `_qail_branch_rows`.
pub(crate) async fn redirect_to_overlay(
    conn: &mut qail_pg::driver::PooledConnection,
    branch_name: &str,
    table_name: &str,
    row_pk: &str,
    operation: &str,
    row_data: &Value,
) -> Result<(), ApiError> {
    let sql =
        qail_pg::driver::branch_sql::write_overlay_sql(branch_name, table_name, row_pk, operation);
    let data_str = serde_json::to_string(row_data).unwrap_or_default();
    // Use escape_literal for proper SQL escaping (handles backslashes, NUL, quotes)
    let safe_data = qail_pg::driver::branch_sql::escape_literal(&data_str);
    let full_sql = sql.replace("$1", &format!("{}::jsonb", safe_data));
    conn.get_mut()
        .execute_simple(&full_sql)
        .await
        .map_err(|e| ApiError::internal(format!("Branch overlay write failed: {}", e)))?;
    Ok(())
}

// ============================================================================
// Branch management handlers — Data Virtualization
// ============================================================================

/// POST /api/_branch — Create a new branch
pub(crate) async fn branch_create_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let auth = extract_auth_from_headers(&headers);
    if !auth.is_authenticated() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "Authentication required"})),
        )
            .into_response();
    }

    // SECURITY (E6): Branch operations require admin role.
    if auth.role != "admin" && auth.role != "super_admin" {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "Admin role required for branch operations"})),
        )
            .into_response();
    }

    let name = match body.get("name").and_then(|v| v.as_str()) {
        Some(n) => n,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "Missing 'name' field"})),
            )
                .into_response();
        }
    };

    let parent = body.get("parent").and_then(|v| v.as_str());

    let mut conn = match state
        .pool
        .acquire_with_rls_timeouts(
            auth.to_rls_context(),
            state.config.statement_timeout_ms,
            state.config.lock_timeout_ms,
        )
        .await
    {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Pool error: {}", e)})),
            )
                .into_response();
        }
    };

    // Auto-bootstrap: create internal tables if they don't exist
    let ddl = qail_pg::driver::branch_sql::create_branch_tables_sql();
    if let Err(e) = conn.get_mut().execute_simple(ddl).await {
        tracing::warn!("Branch DDL bootstrap (may already exist): {}", e);
    }

    let sql = qail_pg::driver::branch_sql::create_branch_sql(name, parent);
    match conn.get_mut().execute_simple(&sql).await {
        Ok(_) => (
            StatusCode::CREATED,
            Json(json!({"branch": name, "status": "created"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::CONFLICT,
            Json(json!({"error": format!("Failed to create branch: {}", e)})),
        )
            .into_response(),
    }
}

/// GET /api/_branch — List all branches
pub(crate) async fn branch_list_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let auth = extract_auth_from_headers(&headers);
    if !auth.is_authenticated() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "Authentication required"})),
        )
            .into_response();
    }

    let mut conn = match state
        .pool
        .acquire_with_rls_timeouts(
            auth.to_rls_context(),
            state.config.statement_timeout_ms,
            state.config.lock_timeout_ms,
        )
        .await
    {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Pool error: {}", e)})),
            )
                .into_response();
        }
    };

    let sql = qail_pg::driver::branch_sql::list_branches_sql();
    match conn.get_mut().simple_query(sql).await {
        Ok(rows) => {
            let branches: Vec<Value> = rows.iter().map(row_to_json).collect();
            Json(json!({"branches": branches})).into_response()
        }
        Err(_) => {
            // Tables may not exist yet
            Json(json!({"branches": []})).into_response()
        }
    }
}

/// DELETE /api/_branch/:name — Soft-delete a branch
pub(crate) async fn branch_delete_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> impl IntoResponse {
    let auth = extract_auth_from_headers(&headers);
    if !auth.is_authenticated() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "Authentication required"})),
        )
            .into_response();
    }

    // SECURITY (E6): Branch operations require admin role.
    if auth.role != "admin" && auth.role != "super_admin" {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "Admin role required for branch operations"})),
        )
            .into_response();
    }

    let mut conn = match state
        .pool
        .acquire_with_rls_timeouts(
            auth.to_rls_context(),
            state.config.statement_timeout_ms,
            state.config.lock_timeout_ms,
        )
        .await
    {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Pool error: {}", e)})),
            )
                .into_response();
        }
    };

    let sql = qail_pg::driver::branch_sql::delete_branch_sql(&name);
    match conn.get_mut().execute_simple(&sql).await {
        Ok(_) => Json(json!({"branch": name, "status": "deleted"})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("Failed to delete branch: {}", e)})),
        )
            .into_response(),
    }
}

/// POST /api/_branch/:name/merge — Merge branch overlay into main tables
pub(crate) async fn branch_merge_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> impl IntoResponse {
    // Auth check: require authenticated user
    let auth = extract_auth_from_headers(&headers);
    if !auth.is_authenticated() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "Authentication required for branch operations"})),
        )
            .into_response();
    }

    let mut conn = match state
        .pool
        .acquire_with_rls_timeouts(
            auth.to_rls_context(),
            state.config.statement_timeout_ms,
            state.config.lock_timeout_ms,
        )
        .await
    {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Pool error: {}", e)})),
            )
                .into_response();
        }
    };

    // Get overlay stats before merge
    let stats_sql = qail_pg::driver::branch_sql::branch_stats_sql(&name);
    let stats = match conn.get_mut().simple_query(&stats_sql).await {
        Ok(rows) => rows.iter().map(row_to_json).collect::<Vec<_>>(),
        Err(_) => vec![],
    };

    // Apply overlay rows to main tables — inside a transaction
    if let Err(e) = conn.get_mut().execute_simple("BEGIN;").await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("Failed to start transaction: {}", e)})),
        )
            .into_response();
    }

    let overlay_sql = qail_pg::driver::branch_sql::merge_overlay_rows_sql(&name);
    let mut applied = 0u32;
    let mut errors: Vec<String> = Vec::new();

    match conn.get_mut().simple_query(&overlay_sql).await {
        Ok(overlay_rows) => {
            for row in &overlay_rows {
                let table = row.get_string(0).unwrap_or_default();
                let row_pk = row.get_string(1).unwrap_or_default();
                let operation = row.get_string(2).unwrap_or_default();
                let row_data_str = row.get_string(3).unwrap_or_default();

                // Build a Qail AST command instead of raw SQL strings.
                // This routes through AstEncoder → Extended Query Protocol,
                // where all values are parameterized (never string-interpolated).
                let cmd = match operation.as_str() {
                    "insert" => {
                        if let Ok(val) = serde_json::from_str::<Value>(&row_data_str) {
                            if let Some(obj) = val.as_object() {
                                let mut q = qail_core::ast::Qail::add(&table);
                                for (k, v) in obj {
                                    q = q.set_value(k, json_to_qail_value(v));
                                }
                                // Use empty conflict columns for DO NOTHING on any constraint
                                q = q.on_conflict_nothing::<String>(&[]);
                                Some(q)
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    "update" => {
                        if let Ok(val) = serde_json::from_str::<Value>(&row_data_str) {
                            if let Some(obj) = val.as_object() {
                                let mut q = qail_core::ast::Qail::set(&table);
                                for (k, v) in obj {
                                    q = q.set_value(k, json_to_qail_value(v));
                                }
                                q = q.eq("id", row_pk.clone());
                                Some(q)
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    "delete" => {
                        let q = qail_core::ast::Qail::del(&table).eq("id", row_pk.clone());
                        Some(q)
                    }
                    _ => None,
                };

                if let Some(qail_cmd) = cmd {
                    match conn.fetch_all_uncached(&qail_cmd).await {
                        Ok(_) => applied += 1,
                        Err(e) => errors.push(format!("{}.{}: {}", table, row_pk, e)),
                    }
                }
            }
        }
        Err(e) => {
            errors.push(format!("Failed to read overlay: {}", e));
        }
    }

    // Rollback on errors, commit on success
    if !errors.is_empty() {
        let _ = conn.get_mut().execute_simple("ROLLBACK;").await;
        return (
            StatusCode::CONFLICT,
            Json(json!({"error": "Merge failed — rolled back", "merge_errors": errors})),
        )
            .into_response();
    }

    // Mark as merged (inside the same transaction)
    let merge_sql = qail_pg::driver::branch_sql::mark_merged_sql(&name);
    match conn.get_mut().execute_simple(&merge_sql).await {
        Ok(_) => {
            // COMMIT the transaction
            let _ = conn.get_mut().execute_simple("COMMIT;").await;
            let mut response = json!({
                "branch": name,
                "status": "merged",
                "applied": applied,
                "overlay_stats": stats,
            });
            if !errors.is_empty() {
                response["merge_errors"] = json!(errors);
            }
            Json(response).into_response()
        }
        Err(e) => {
            let _ = conn.get_mut().execute_simple("ROLLBACK;").await;
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Failed to merge branch: {}", e)})),
            )
                .into_response()
        }
    }
}
