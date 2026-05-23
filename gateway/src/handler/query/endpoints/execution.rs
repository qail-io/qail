use super::*;

/// Fast query execution — uses fetch_all_fast (AST-native wire).
/// Returns array-of-arrays without column names.
pub(super) async fn execute_qail_cmd_fast(
    state: &Arc<GatewayState>,
    auth: &crate::auth::AuthContext,
    cmd: &qail_core::ast::Qail,
    tenant_guard_plan: Option<&crate::tenant_guard::TenantGuardPlan>,
    extensions: &axum::http::Extensions,
) -> Result<Json<FastQueryResponse>, ApiError> {
    let mut cmd = cmd.clone();
    state.optimize_qail_for_execution(&mut cmd);
    let is_read_only = command_is_read_only_for_release(&cmd);

    let (depth, filters, joins) = query_complexity(&cmd);
    if let Err(api_err) = state.complexity_guard.check(depth, filters, joins) {
        crate::metrics::record_complexity_rejected();
        return Err(api_err);
    }

    if matches!(
        cmd.action,
        Action::Search
            | Action::Upsert
            | Action::Scroll
            | Action::CreateCollection
            | Action::DeleteCollection
    ) {
        return Err(ApiError::bad_request(
            "UNSUPPORTED_ACTION",
            "Vector operations not supported on /qail/fast",
        ));
    }

    let mut conn = state
        .acquire_with_auth_rls_guarded(auth, Some(&cmd.table))
        .await?;

    let timer = crate::metrics::QueryTimer::new(&cmd.table, &cmd.action.to_string());
    let rows = if tenant_guard_plan.is_some_and(|plan| plan.verify_rows) {
        // Tenant boundary verification needs column metadata so it can locate
        // the tenant column before returning positional arrays.
        conn.fetch_all_uncached(&cmd).await
    } else {
        conn.fetch_all_fast(&cmd).await
    }
    .map_err(|e| ApiError::from_pg_driver_error(&e, Some(&cmd.table)));
    let duration_ms = timer.elapsed_ms();
    timer.finish(rows.is_ok());
    let rows = match rows {
        Ok(rows) => {
            if is_read_only {
                conn.release().await;
            } else {
                conn.release_checked()
                    .await
                    .map_err(|e| ApiError::from_pg_driver_error(&e, Some(&cmd.table)))?;
            }
            rows
        }
        Err(err) => {
            conn.release().await;
            return Err(err);
        }
    };

    if let (Some(tenant_id), Some(plan)) = (auth.tenant_id.as_deref(), tenant_guard_plan)
        && plan.verify_rows
    {
        let guard_rows: Vec<serde_json::Value> = rows.iter().map(row_to_json).collect();
        let _proof = crate::tenant_guard::verify_tenant_boundary(
            &guard_rows,
            tenant_id,
            &plan.column,
            &cmd.table,
            "qail_cmd_fast",
        )
        .map_err(|v| {
            tracing::error!("{}", v);
            ApiError::with_code("TENANT_BOUNDARY_VIOLATION", "Data integrity error")
        })?;
    }

    let strip_index = tenant_guard_plan
        .filter(|plan| plan.strip_output_column)
        .and_then(|plan| {
            rows.first()
                .and_then(|row| crate::tenant_guard::tenant_column_index(row, &plan.column))
        });
    let json_rows: Vec<Vec<serde_json::Value>> = rows
        .iter()
        .map(|row| {
            let mut values = row_to_array(row);
            if let Some(idx) = strip_index
                && idx < values.len()
            {
                values.remove(idx);
            }
            values
        })
        .collect();

    let count = json_rows.len();
    let request_id = match extensions.get::<crate::middleware::RequestId>() {
        Some(id) => id.0.clone(),
        None => String::new(),
    };

    Ok(Json(FastQueryResponse {
        rows: json_rows,
        count,
        metadata: Some(crate::handler::ResponseMetadata {
            request_id,
            duration_ms: Some(duration_ms),
        }),
    }))
}

/// Extract query complexity metrics from a QAIL AST.
///
/// Returns (depth, filter_count, join_count) where:
/// - depth = CTEs + set ops + source subquery nesting
/// - filter_count = total conditions across all Filter cages
/// - join_count = number of JOIN clauses
pub(super) async fn execute_qail_cmd(
    state: &Arc<GatewayState>,
    auth: &crate::auth::AuthContext,
    cmd: &qail_core::ast::Qail,
    tenant_guard_plan: Option<&crate::tenant_guard::TenantGuardPlan>,
    extensions: &axum::http::Extensions,
) -> Result<Json<QueryResponse>, ApiError> {
    let mut cmd = cmd.clone();
    state.optimize_qail_for_execution(&mut cmd);

    let (depth, filters, joins) = query_complexity(&cmd);
    if let Err(api_err) = state.complexity_guard.check(depth, filters, joins) {
        tracing::warn!(
            table = %cmd.table,
            depth, filters, joins,
            "Query rejected by complexity guard"
        );
        crate::metrics::record_complexity_rejected();
        return Err(api_err);
    }

    if matches!(
        cmd.action,
        Action::Search
            | Action::Upsert
            | Action::Scroll
            | Action::CreateCollection
            | Action::DeleteCollection
    ) {
        #[cfg(feature = "qdrant")]
        {
            return execute_qdrant_cmd(state, auth, &cmd).await;
        }
        #[cfg(not(feature = "qdrant"))]
        {
            return Err(ApiError::bad_request(
                "QDRANT_DISABLED",
                "Vector operations require the 'qdrant' feature",
            ));
        }
    }

    let table = qail_table_name(&cmd.table);
    let is_read_only = command_is_read_only_for_release(&cmd);
    let should_cache_query = command_is_cacheable_query(&cmd);

    let cache_key = auth_scoped_cache_key(auth, &cmd);

    if should_cache_query && let Some(cached) = state.cache.get(&cache_key) {
        tracing::debug!("Cache HIT for table '{}'", table);
        if let Ok(mut response) = serde_json::from_str::<QueryResponse>(&cached) {
            let request_id = match extensions.get::<crate::middleware::RequestId>() {
                Some(id) => id.0.clone(),
                None => String::new(),
            };
            response.metadata = Some(crate::handler::ResponseMetadata {
                request_id,
                duration_ms: None, // Cached
            });
            return Ok(Json(response));
        }
    }

    let mut conn = state
        .acquire_raw_with_auth_guarded(auth, Some(&cmd.table))
        .await?;

    let rls_sql = qail_pg::rls_sql_with_timeouts(
        &auth.to_rls_context(),
        state.config.statement_timeout_ms,
        state.config.lock_timeout_ms,
    );

    let timer = crate::metrics::QueryTimer::new(&cmd.table, &cmd.action.to_string());
    let rows = conn
        .fetch_all_with_rls(&cmd, &rls_sql)
        .await
        .map_err(|e| ApiError::from_pg_driver_error(&e, Some(&cmd.table)));
    let duration_ms = timer.elapsed_ms();
    timer.finish(rows.is_ok());
    let rows = match rows {
        Ok(rows) => {
            if is_read_only {
                conn.release().await;
            } else {
                conn.release_checked()
                    .await
                    .map_err(|e| ApiError::from_pg_driver_error(&e, Some(&cmd.table)))?;
            }
            rows
        }
        Err(err) => {
            conn.release().await;
            return Err(err);
        }
    };

    let mut json_rows: Vec<serde_json::Value> = rows.iter().map(row_to_json).collect();

    let _proof = if let (Some(tenant_id), Some(plan)) =
        (auth.tenant_id.as_deref(), tenant_guard_plan)
        && plan.verify_rows
    {
        crate::tenant_guard::verify_tenant_boundary(
            &json_rows,
            tenant_id,
            &plan.column,
            &table,
            "qail_cmd",
        )
        .map_err(|v| {
            tracing::error!("{}", v);
            ApiError::with_code("TENANT_BOUNDARY_VIOLATION", "Data integrity error")
        })?
    } else {
        crate::tenant_guard::TenantVerified::unscoped()
    };

    if let Some(plan) = tenant_guard_plan
        && plan.strip_output_column
    {
        crate::tenant_guard::strip_tenant_column_from_json_rows(&mut json_rows, &plan.column);
    }

    let count = json_rows.len();
    let request_id = match extensions.get::<crate::middleware::RequestId>() {
        Some(id) => id.0.clone(),
        None => String::new(),
    };

    let response = QueryResponse {
        rows: json_rows,
        count,
        metadata: Some(crate::handler::ResponseMetadata {
            request_id,
            duration_ms: Some(duration_ms),
        }),
    };

    if should_cache_query {
        if let Ok(json) = serde_json::to_string(&response) {
            let cache_tables = cache_tables_for_qail(&cmd);
            let cache_table_refs: Vec<&str> = cache_tables.iter().map(String::as_str).collect();
            state
                .cache
                .set_for_tables(&cache_key, &cache_table_refs, json);
            tracing::debug!("Cache STORE for table '{}' ({} rows)", table, count);
        }
    } else if !is_read_only {
        for cache_table in cache_tables_for_qail(&cmd) {
            state.cache.invalidate_table(&cache_table);
            tracing::debug!("Cache INVALIDATED for table '{}' (mutation)", cache_table);
        }
    }

    Ok(Json(response))
}

fn command_is_read_only_for_release(cmd: &qail_core::ast::Qail) -> bool {
    let action_is_read_only = matches!(
        cmd.action,
        Action::Get | Action::Cnt | Action::JsonTable | Action::With | Action::Export
    );
    action_is_read_only
        && cmd.ctes.iter().all(|cte| {
            command_is_read_only_for_release(&cte.base_query)
                && cte
                    .recursive_query
                    .as_deref()
                    .is_none_or(command_is_read_only_for_release)
        })
        && cmd
            .source_query
            .as_deref()
            .is_none_or(command_is_read_only_for_release)
        && cmd
            .set_ops
            .iter()
            .all(|(_, set_query)| command_is_read_only_for_release(set_query))
}

fn command_is_cacheable_query(cmd: &qail_core::ast::Qail) -> bool {
    matches!(cmd.action, Action::Get) && command_is_read_only_for_release(cmd)
}

#[cfg(test)]
mod tests {
    use super::{command_is_cacheable_query, command_is_read_only_for_release};
    use crate::handler::query::cache_tables_for_qail;
    use qail_core::ast::{Qail, SetOp};

    #[test]
    fn cache_tables_include_cte_body_not_alias() {
        let cmd = Qail::get("recent").with("recent", Qail::get("orders"));

        assert_eq!(cache_tables_for_qail(&cmd), vec!["orders"]);
    }

    #[test]
    fn cache_tables_include_set_op_dependencies() {
        let mut cmd = Qail::get("orders");
        cmd.set_ops
            .push((SetOp::UnionAll, Box::new(Qail::get("archived_orders"))));

        assert_eq!(
            cache_tables_for_qail(&cmd),
            vec!["orders", "archived_orders"]
        );
    }

    #[test]
    fn nested_mutating_cte_is_not_read_only_or_cacheable() {
        let cmd = Qail::get("audit_view").with("audit_view", Qail::add("audit_log"));

        assert!(!command_is_read_only_for_release(&cmd));
        assert!(!command_is_cacheable_query(&cmd));
    }
}
