use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json},
};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::Arc;

use crate::GatewayState;
use crate::auth::authenticate_request;
use crate::handler::row_to_json;
use crate::middleware::ApiError;
use crate::rest::branch::validate_branch_name;
use crate::rest::filters::json_to_qail_value;

const BRANCH_MERGE_SAVEPOINT: &str = "qail_branch_merge";

fn apply_insert_conflict_target(
    cmd: qail_core::ast::Qail,
    pk_col: Option<&str>,
) -> qail_core::ast::Qail {
    match pk_col {
        Some(pk_col) => cmd.on_conflict_nothing(&[pk_col]),
        None => cmd,
    }
}

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

    if let Err(e) = conn.savepoint(BRANCH_MERGE_SAVEPOINT).await {
        tracing::error!("Branch merge savepoint start failed: {}", e);
        conn.release().await;
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "Failed to start merge transaction"})),
        )
            .into_response();
    }

    let overlay_sql = qail_pg::driver::branch_sql::merge_overlay_rows_sql(&name);
    let mut applied = 0u32;
    let mut errors: Vec<String> = Vec::new();
    let mut mutated_tables: HashSet<String> = HashSet::new();

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
                                    q = apply_insert_conflict_target(
                                        q,
                                        state
                                            .schema
                                            .table(&table)
                                            .and_then(|t| t.primary_key.as_deref()),
                                    );
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
                            Ok(_) => {
                                applied += 1;
                                mutated_tables.insert(table.clone());
                            }
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
        if let Err(e) = conn.rollback_to(BRANCH_MERGE_SAVEPOINT).await {
            tracing::error!("Branch merge rollback failed for '{}': {}", name, e);
            let _ = conn.rollback_and_release().await;
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "Merge rollback failed"})),
            )
                .into_response();
        }
        if let Err(e) = conn.release_savepoint(BRANCH_MERGE_SAVEPOINT).await {
            tracing::warn!(
                "Branch merge savepoint release after rollback failed: {}",
                e
            );
        }
        conn.release().await;
        return (
            StatusCode::CONFLICT,
            Json(json!({"error": "Merge failed — rolled back", "merge_errors": errors})),
        )
            .into_response();
    }

    let merge_sql = qail_pg::driver::branch_sql::mark_merged_sql(&name);
    let mut rollback_merge = false;
    let mut commit_merge = false;
    let result = match conn.get_mut() {
        Ok(pg_conn) => match pg_conn.simple_query(&merge_sql).await {
            Ok(rows) => {
                if rows.is_empty() {
                    rollback_merge = true;
                    (
                        StatusCode::CONFLICT,
                        Json(json!({"error": "Branch not found or not active"})),
                    )
                        .into_response()
                } else {
                    commit_merge = true;
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
            }
            Err(e) => {
                rollback_merge = true;
                tracing::error!("Failed to merge branch '{}': {}", name, e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "Failed to merge branch"})),
                )
                    .into_response()
            }
        },
        Err(e) => {
            rollback_merge = true;
            tracing::error!("Branch connection released unexpectedly: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "Database connection unavailable"})),
            )
                .into_response()
        }
    };

    if rollback_merge {
        if let Err(e) = conn.rollback_to(BRANCH_MERGE_SAVEPOINT).await {
            tracing::error!("Branch merge rollback failed for '{}': {}", name, e);
            let _ = conn.rollback_and_release().await;
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "Merge rollback failed"})),
            )
                .into_response();
        }
        if let Err(e) = conn.release_savepoint(BRANCH_MERGE_SAVEPOINT).await {
            tracing::warn!(
                "Branch merge savepoint release after rollback failed: {}",
                e
            );
        }
    } else if commit_merge && let Err(e) = conn.release_savepoint(BRANCH_MERGE_SAVEPOINT).await {
        tracing::error!("Branch merge savepoint commit failed for '{}': {}", name, e);
        let _ = conn.rollback_and_release().await;
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "Merge transaction failed to commit"})),
        )
            .into_response();
    }

    if commit_merge {
        if let Err(e) = conn.release_checked().await {
            for table in &mutated_tables {
                state.cache.invalidate_table(table);
            }
            return ApiError::from_pg_driver_error(&e, None).into_response();
        }
        for table in mutated_tables {
            state.cache.invalidate_table(&table);
        }
    } else {
        conn.release().await;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::apply_insert_conflict_target;
    use qail_core::ast::ConflictAction;

    #[test]
    fn insert_merge_uses_pk_conflict_target_when_known() {
        let cmd = apply_insert_conflict_target(qail_core::ast::Qail::add("orders"), Some("id"));

        let on_conflict = cmd.on_conflict.expect("on conflict");
        assert_eq!(on_conflict.columns, vec!["id".to_string()]);
        assert!(matches!(on_conflict.action, ConflictAction::DoNothing));
    }

    #[test]
    fn insert_merge_omits_on_conflict_when_pk_unknown() {
        let cmd = apply_insert_conflict_target(qail_core::ast::Qail::add("orders"), None);

        assert!(cmd.on_conflict.is_none());
    }
}
