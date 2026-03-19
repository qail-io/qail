use super::*;

/// Fast query execution — uses fetch_all_fast (AST-native wire).
/// Returns array-of-arrays without column names.
pub(super) async fn execute_qail_cmd_fast(
    state: &Arc<GatewayState>,
    auth: &crate::auth::AuthContext,
    cmd: &qail_core::ast::Qail,
) -> Result<Json<FastQueryResponse>, ApiError> {
    use qail_core::ast::Action;
    let mut cmd = cmd.clone();
    state.optimize_qail_for_execution(&mut cmd);

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
    let rows = conn
        .fetch_all_fast(&cmd)
        .await
        .map_err(|e| ApiError::from_pg_driver_error(&e, Some(&cmd.table)));
    timer.finish(rows.is_ok());
    conn.release().await;
    let rows = rows?;

    let json_rows: Vec<Vec<serde_json::Value>> = rows.iter().map(row_to_array).collect();

    let count = json_rows.len();
    Ok(Json(FastQueryResponse {
        rows: json_rows,
        count,
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
) -> Result<Json<QueryResponse>, ApiError> {
    use qail_core::ast::Action;
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
            return execute_qdrant_cmd(state, &cmd).await;
        }
        #[cfg(not(feature = "qdrant"))]
        {
            return Err(ApiError::bad_request(
                "QDRANT_DISABLED",
                "Vector operations require the 'qdrant' feature",
            ));
        }
    }

    let table = &cmd.table;
    let is_read_query = matches!(cmd.action, Action::Get);

    let tenant = auth.tenant_id.as_deref().unwrap_or("_anon");
    let cache_key = format!("{}:{}:{}", tenant, auth.user_id, exact_cache_key(&cmd));

    if is_read_query && let Some(cached) = state.cache.get(&cache_key) {
        tracing::debug!("Cache HIT for table '{}'", table);
        if let Ok(response) = serde_json::from_str::<QueryResponse>(&cached) {
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
    timer.finish(rows.is_ok());
    conn.release().await;

    let rows = rows?;

    let json_rows: Vec<serde_json::Value> = rows.iter().map(row_to_json).collect();

    let _proof = if let Some(ref tenant_id) = auth.tenant_id {
        crate::tenant_guard::verify_tenant_boundary(
            &json_rows,
            tenant_id,
            &state.config.tenant_column,
            table,
            "qail_cmd",
        )
        .map_err(|v| {
            tracing::error!("{}", v);
            ApiError::with_code("TENANT_BOUNDARY_VIOLATION", "Data integrity error")
        })?
    } else {
        crate::tenant_guard::TenantVerified::unscoped()
    };

    let count = json_rows.len();

    let response = QueryResponse {
        rows: json_rows,
        count,
    };

    if is_read_query {
        if let Ok(json) = serde_json::to_string(&response) {
            state.cache.set(&cache_key, table, json);
            tracing::debug!("Cache STORE for table '{}' ({} rows)", table, count);
        }
    } else {
        state.cache.invalidate_table(table);
        tracing::debug!("Cache INVALIDATED for table '{}' (mutation)", table);
    }

    Ok(Json(response))
}
