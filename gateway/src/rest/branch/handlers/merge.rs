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
    obj: &serde_json::Map<String, Value>,
    pk_col: Option<&str>,
) -> qail_core::ast::Qail {
    match pk_col {
        Some(pk_col) => {
            let updates: Vec<(&str, qail_core::ast::Expr)> = obj
                .keys()
                .filter(|k| k.as_str() != pk_col)
                .filter(|k| crate::rest::filters::is_safe_identifier(k))
                .map(|k| {
                    (
                        k.as_str(),
                        qail_core::ast::Expr::Named(format!("EXCLUDED.{}", k)),
                    )
                })
                .collect();
            if updates.is_empty() {
                cmd.on_conflict_nothing(&[pk_col])
            } else {
                cmd.on_conflict_update(&[pk_col], &updates)
            }
        }
        None => cmd,
    }
}

fn parse_overlay_object(
    operation: &str,
    row_data_str: &str,
) -> Result<serde_json::Map<String, Value>, String> {
    let val = serde_json::from_str::<Value>(row_data_str)
        .map_err(|e| format!("Invalid {} overlay JSON: {}", operation, e))?;
    val.as_object()
        .cloned()
        .ok_or_else(|| format!("Invalid {} overlay row_data: expected object", operation))
}

fn build_branch_overlay_merge_cmd(
    table: &str,
    row_pk: &str,
    operation: &str,
    row_data_str: &str,
    pk_col: Option<&str>,
) -> Result<qail_core::ast::Qail, String> {
    match operation {
        "insert" => {
            let obj = parse_overlay_object(operation, row_data_str)?;
            let mut q = qail_core::ast::Qail::add(table);
            for (k, v) in &obj {
                q = q.set_value(k, json_to_qail_value(v));
            }
            Ok(apply_insert_conflict_target(q, &obj, pk_col))
        }
        "update" => {
            let obj = parse_overlay_object(operation, row_data_str)?;
            let mut q = qail_core::ast::Qail::set(table);
            for (k, v) in &obj {
                q = q.set_value(k, json_to_qail_value(v));
            }
            let pk_col = pk_col.unwrap_or("id");
            Ok(q.eq(pk_col, row_pk.to_string()).returning([pk_col]))
        }
        "delete" => {
            let pk_col = pk_col.unwrap_or("id");
            Ok(qail_core::ast::Qail::del(table)
                .eq(pk_col, row_pk.to_string())
                .returning([pk_col]))
        }
        _ => Err(format!(
            "Unsupported branch overlay operation '{}'",
            operation
        )),
    }
}

fn branch_merge_requires_affected_row(operation: &str) -> bool {
    matches!(operation, "update" | "delete")
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

    let lock_sql = qail_pg::driver::branch_sql::lock_active_branch_for_merge_sql(&name);
    match conn.get_mut() {
        Ok(pg_conn) => match pg_conn.simple_query(&lock_sql).await {
            Ok(rows) if !rows.is_empty() => {}
            Ok(_) => {
                let _ = conn.rollback_to(BRANCH_MERGE_SAVEPOINT).await;
                let _ = conn.release_savepoint(BRANCH_MERGE_SAVEPOINT).await;
                conn.release().await;
                return (
                    StatusCode::CONFLICT,
                    Json(json!({"error": "Branch not found or not active"})),
                )
                    .into_response();
            }
            Err(e) => {
                tracing::error!("Branch merge lock failed for '{}': {}", name, e);
                let _ = conn.rollback_to(BRANCH_MERGE_SAVEPOINT).await;
                let _ = conn.release_savepoint(BRANCH_MERGE_SAVEPOINT).await;
                conn.release().await;
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "Failed to lock branch for merge"})),
                )
                    .into_response();
            }
        },
        Err(e) => {
            tracing::error!("Branch connection unavailable before merge lock: {}", e);
            let _ = conn.rollback_to(BRANCH_MERGE_SAVEPOINT).await;
            let _ = conn.release_savepoint(BRANCH_MERGE_SAVEPOINT).await;
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

                    let pk_col = state
                        .schema
                        .table(&table)
                        .and_then(|t| t.primary_key.as_deref());
                    match build_branch_overlay_merge_cmd(
                        &table,
                        &row_pk,
                        &operation,
                        &row_data_str,
                        pk_col,
                    ) {
                        Ok(mut qail_cmd) => {
                            state.optimize_qail_for_execution(&mut qail_cmd);
                            match conn.fetch_all_uncached(&qail_cmd).await {
                                Ok(rows)
                                    if branch_merge_requires_affected_row(&operation)
                                        && rows.is_empty() =>
                                {
                                    errors.push(format!(
                                        "{}.{}: merge {} affected no rows",
                                        table, row_pk, operation
                                    ));
                                }
                                Ok(_) => {
                                    applied += 1;
                                    mutated_tables.insert(table.clone());
                                }
                                Err(e) => errors.push(format!("{}.{}: {}", table, row_pk, e)),
                            }
                        }
                        Err(e) => {
                            errors.push(format!("{}.{}: {}", table, row_pk, e));
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
    use super::{
        apply_insert_conflict_target, branch_merge_requires_affected_row,
        build_branch_overlay_merge_cmd,
    };
    use qail_core::ast::{Action, ConflictAction, Expr};
    use serde_json::{Map, json};

    #[test]
    fn insert_merge_uses_pk_conflict_update_when_known() {
        let mut obj = Map::new();
        obj.insert("id".to_string(), json!("order-1"));
        obj.insert("status".to_string(), json!("paid"));

        let cmd =
            apply_insert_conflict_target(qail_core::ast::Qail::add("orders"), &obj, Some("id"));

        let on_conflict = cmd.on_conflict.expect("on conflict");
        assert_eq!(on_conflict.columns, vec!["id".to_string()]);
        let ConflictAction::DoUpdate { assignments } = on_conflict.action else {
            panic!("expected conflict update");
        };
        assert_eq!(
            assignments,
            vec![(
                "status".to_string(),
                Expr::Named("EXCLUDED.status".to_string())
            )]
        );
    }

    #[test]
    fn insert_merge_with_only_pk_uses_do_nothing() {
        let mut obj = Map::new();
        obj.insert("id".to_string(), json!("order-1"));

        let cmd =
            apply_insert_conflict_target(qail_core::ast::Qail::add("orders"), &obj, Some("id"));

        let on_conflict = cmd.on_conflict.expect("on conflict");
        assert_eq!(on_conflict.columns, vec!["id".to_string()]);
        assert!(matches!(on_conflict.action, ConflictAction::DoNothing));
    }

    #[test]
    fn insert_merge_omits_on_conflict_when_pk_unknown() {
        let obj = Map::new();
        let cmd = apply_insert_conflict_target(qail_core::ast::Qail::add("orders"), &obj, None);

        assert!(cmd.on_conflict.is_none());
    }

    #[test]
    fn overlay_merge_cmd_rejects_unknown_operation() {
        let err = build_branch_overlay_merge_cmd("orders", "order-1", "patch", "{}", Some("id"))
            .expect_err("unknown overlay operation must fail closed");
        assert!(err.contains("Unsupported branch overlay operation"));
    }

    #[test]
    fn overlay_merge_cmd_rejects_malformed_json() {
        let err = build_branch_overlay_merge_cmd("orders", "order-1", "insert", "{bad", Some("id"))
            .expect_err("malformed insert overlay JSON must fail closed");
        assert!(err.contains("Invalid insert overlay JSON"));
    }

    #[test]
    fn overlay_merge_cmd_rejects_non_object_update_payload() {
        let err = build_branch_overlay_merge_cmd("orders", "order-1", "update", "[]", Some("id"))
            .expect_err("non-object update overlay must fail closed");
        assert!(err.contains("expected object"));
    }

    #[test]
    fn overlay_merge_update_returns_pk_to_detect_missing_target() {
        let cmd = build_branch_overlay_merge_cmd(
            "orders",
            "order-1",
            "update",
            r#"{"status":"paid"}"#,
            Some("id"),
        )
        .expect("update overlay should build");

        assert_eq!(cmd.action, Action::Set);
        assert_eq!(cmd.returning, Some(vec![Expr::Named("id".into())]));
        assert!(branch_merge_requires_affected_row("update"));
    }

    #[test]
    fn overlay_merge_cmd_builds_delete_with_schema_pk() {
        let cmd =
            build_branch_overlay_merge_cmd("orders", "order-1", "delete", "null", Some("uuid"))
                .expect("delete overlay should build without row_data object");

        assert_eq!(cmd.action, Action::Del);
        assert_eq!(cmd.table, "orders");
        assert_eq!(cmd.cages[0].conditions[0].left, Expr::Named("uuid".into()));
        assert_eq!(cmd.returning, Some(vec![Expr::Named("uuid".into())]));
        assert!(branch_merge_requires_affected_row("delete"));
        assert!(!branch_merge_requires_affected_row("insert"));
    }
}
