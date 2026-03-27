use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json},
};
use serde_json::{Value, json};
use std::sync::Arc;

use crate::GatewayState;
use crate::auth::authenticate_request;
use crate::handler::row_to_json;
use crate::rest::branch::validate_branch_name;
use crate::rest::filters::json_to_qail_value;

/// POST /api/_branch/:name/merge — Merge branch overlay into main tables.
pub(crate) async fn branch_merge_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> impl IntoResponse {
    let auth = match authenticate_request(state.as_ref(), &headers).await {
        Ok(auth) => auth,
        Err(e) => return e.into_response(),
    };
    if !auth.is_authenticated() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "Authentication required for branch operations"})),
        )
            .into_response();
    }
    if !auth.can_use_branching() {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "Platform administrator role required for branch merge"})),
        )
            .into_response();
    }
    if let Err(e) = validate_branch_name(&name) {
        return e.into_response();
    }

    let mut conn = match state.acquire_with_auth_rls_guarded(&auth, None).await {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };

    let stats_sql = qail_pg::driver::branch_sql::branch_stats_sql(&name);
    let stats = match conn.get_mut() {
        Ok(pg_conn) => match pg_conn.simple_query(&stats_sql).await {
            Ok(rows) => rows.iter().map(row_to_json).collect::<Vec<_>>(),
            Err(_) => vec![],
        },
        Err(_) => vec![],
    };

    match conn.get_mut() {
        Ok(pg_conn) => {
            if let Err(e) = pg_conn.execute_simple("BEGIN;").await {
                tracing::error!("Branch merge transaction start failed: {}", e);
                conn.release().await;
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "Failed to start merge transaction"})),
                )
                    .into_response();
            }
        }
        Err(e) => {
            tracing::error!("Branch connection released unexpectedly: {}", e);
            conn.release().await;
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "Database connection unavailable"})),
            )
                .into_response();
        }
    }

    let overlay_sql = qail_pg::driver::branch_sql::merge_overlay_rows_sql(&name);
    let mut applied = 0u32;
    let mut errors: Vec<String> = Vec::new();

    match conn.get_mut() {
        Ok(pg_conn) => match pg_conn.simple_query(&overlay_sql).await {
            Ok(overlay_rows) => {
                for row in &overlay_rows {
                    let table = row
                        .try_get_by_name::<String>("table_name")
                        .ok()
                        .or_else(|| row.get_string(0))
                        .unwrap_or_default();
                    let row_pk = row
                        .try_get_by_name::<String>("row_pk")
                        .ok()
                        .or_else(|| row.get_string(1))
                        .unwrap_or_default();
                    let operation = row
                        .try_get_by_name::<String>("operation")
                        .ok()
                        .or_else(|| row.get_string(2))
                        .unwrap_or_default();
                    let row_data_str = row
                        .try_get_by_name::<String>("row_data")
                        .ok()
                        .or_else(|| row.get_string(3))
                        .unwrap_or_default();

                    let cmd = match operation.as_str() {
                        "insert" => {
                            if let Ok(val) = serde_json::from_str::<Value>(&row_data_str) {
                                if let Some(obj) = val.as_object() {
                                    let mut q = qail_core::ast::Qail::add(&table);
                                    for (k, v) in obj {
                                        q = q.set_value(k, json_to_qail_value(v));
                                    }
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
                                    let pk_col = state
                                        .schema
                                        .table(&table)
                                        .and_then(|t| t.primary_key.as_deref())
                                        .unwrap_or("id");
                                    q = q.eq(pk_col, row_pk.clone());
                                    Some(q)
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        }
                        "delete" => {
                            let pk_col = state
                                .schema
                                .table(&table)
                                .and_then(|t| t.primary_key.as_deref())
                                .unwrap_or("id");
                            let q = qail_core::ast::Qail::del(&table).eq(pk_col, row_pk.clone());
                            Some(q)
                        }
                        _ => None,
                    };

                    if let Some(mut qail_cmd) = cmd {
                        state.optimize_qail_for_execution(&mut qail_cmd);
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
        },
        Err(e) => errors.push(format!("Failed to access DB connection: {}", e)),
    }

    if !errors.is_empty() {
        if let Ok(pg_conn) = conn.get_mut() {
            let _ = pg_conn.execute_simple("ROLLBACK;").await;
        }
        conn.release().await;
        return (
            StatusCode::CONFLICT,
            Json(json!({"error": "Merge failed — rolled back", "merge_errors": errors})),
        )
            .into_response();
    }

    let merge_sql = qail_pg::driver::branch_sql::mark_merged_sql(&name);
    let result = match conn.get_mut() {
        Ok(pg_conn) => match pg_conn.simple_query(&merge_sql).await {
            Ok(rows) => {
                if rows.is_empty() {
                    let _ = pg_conn.execute_simple("ROLLBACK;").await;
                    (
                        StatusCode::CONFLICT,
                        Json(json!({"error": "Branch not found or not active"})),
                    )
                        .into_response()
                } else {
                    match pg_conn.execute_simple("COMMIT;").await {
                        Ok(_) => {
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
                            tracing::error!("Branch merge COMMIT failed for '{}': {}", name, e);
                            let _ = pg_conn.execute_simple("ROLLBACK;").await;
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(json!({"error": "Merge transaction failed to commit"})),
                            )
                                .into_response()
                        }
                    }
                }
            }
            Err(e) => {
                let _ = pg_conn.execute_simple("ROLLBACK;").await;
                tracing::error!("Failed to merge branch '{}': {}", name, e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "Failed to merge branch"})),
                )
                    .into_response()
            }
        },
        Err(e) => {
            tracing::error!("Branch connection released unexpectedly: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "Database connection unavailable"})),
            )
                .into_response()
        }
    };
    conn.release().await;
    result
}
