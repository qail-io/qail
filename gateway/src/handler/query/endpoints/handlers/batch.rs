use super::*;

/// Execute a batch of Qail queries (POST /qail/batch).
pub async fn execute_batch(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Json(request): Json<BatchRequest>,
) -> Result<Json<BatchResponse>, ApiError> {
    if request.queries.is_empty() {
        return Err(ApiError::bad_request("EMPTY_BATCH", "Empty query batch"));
    }

    if request.queries.len() > state.config.max_batch_queries {
        return Err(ApiError::bad_request(
            "BATCH_TOO_LARGE",
            format!(
                "Batch size {} exceeds maximum of {}",
                request.queries.len(),
                state.config.max_batch_queries,
            ),
        ));
    }

    let auth = authenticate_request(state.as_ref(), &headers).await?;

    tracing::info!(
        "Executing batch of {} queries (txn={}, user: {})",
        request.queries.len(),
        request.transaction,
        auth.user_id
    );

    let mut results = Vec::with_capacity(request.queries.len());
    let mut success_count = 0;

    let mut conn = state.acquire_with_auth_rls_guarded(&auth, None).await?;

    if request.transaction {
        match conn.get_mut() {
            Ok(pg_conn) => {
                if let Err(e) = pg_conn.execute_simple("BEGIN;").await {
                    tracing::error!("Transaction start failed: {}", e);
                    conn.release().await;
                    return Err(ApiError::with_code("TXN_ERROR", "Transaction start failed"));
                }
            }
            Err(e) => {
                conn.release().await;
                return Err(ApiError::from_pg_driver_error(&e, None));
            }
        }
    }

    let mut had_error = false;

    for (index, query_text) in request.queries.iter().enumerate() {
        let query_text = query_text.trim();

        let mut cmd = match qail_core::parser::parse(query_text) {
            Ok(cmd) => cmd,
            Err(e) => {
                results.push(BatchQueryResult {
                    index,
                    success: false,
                    rows: None,
                    count: None,
                    error: Some(format!("Parse error: {}", e)),
                });
                if request.transaction {
                    had_error = true;
                    break;
                }
                continue;
            }
        };

        if let Err(e) = reject_dangerous_action(&cmd) {
            results.push(BatchQueryResult {
                index,
                success: false,
                rows: None,
                count: None,
                error: Some(e.message.clone()),
            });
            if request.transaction {
                had_error = true;
                break;
            }
            continue;
        }

        if !is_query_allowed(&state.allow_list, Some(query_text), &cmd) {
            results.push(BatchQueryResult {
                index,
                success: false,
                rows: None,
                count: None,
                error: Some("Query not in allow-list".to_string()),
            });
            if request.transaction {
                had_error = true;
                break;
            }
            continue;
        }

        if let Err(e) = state.policy_engine.apply_policies(&auth, &mut cmd) {
            results.push(BatchQueryResult {
                index,
                success: false,
                rows: None,
                count: None,
                error: Some(format!("Policy error: {}", e)),
            });
            if request.transaction {
                had_error = true;
                break;
            }
            continue;
        }

        state.optimize_qail_for_execution(&mut cmd);

        let (depth, filters, joins) = query_complexity(&cmd);
        if let Err(api_err) = state.complexity_guard.check(depth, filters, joins) {
            results.push(BatchQueryResult {
                index,
                success: false,
                rows: None,
                count: None,
                error: Some(api_err.message.clone()),
            });
            if request.transaction {
                had_error = true;
                break;
            }
            continue;
        }

        clamp_query_limit(&mut cmd, state.config.max_result_rows);

        let timer = crate::metrics::QueryTimer::new(&cmd.table, &cmd.action.to_string());
        match conn.fetch_all_uncached(&cmd).await {
            Ok(rows) => {
                timer.finish(true);
                let json_rows: Vec<serde_json::Value> = rows.iter().map(row_to_json).collect();
                let count = json_rows.len();

                if matches!(cmd.action, qail_core::ast::Action::Get)
                    && let Some(ref tenant_id) = auth.tenant_id
                    && let Err(v) = crate::tenant_guard::verify_tenant_boundary(
                        &json_rows,
                        tenant_id,
                        &state.config.tenant_column,
                        &cmd.table,
                        "batch_query",
                    )
                {
                    tracing::error!("{}", v);
                    results.push(BatchQueryResult {
                        index,
                        success: false,
                        rows: None,
                        count: None,
                        error: Some("Data integrity error".to_string()),
                    });
                    if request.transaction {
                        had_error = true;
                        break;
                    }
                    continue;
                }

                if !matches!(cmd.action, qail_core::ast::Action::Get) {
                    state.cache.invalidate_table(&cmd.table);
                }

                results.push(BatchQueryResult {
                    index,
                    success: true,
                    rows: Some(json_rows),
                    count: Some(count),
                    error: None,
                });
                success_count += 1;
            }
            Err(e) => {
                timer.finish(false);
                tracing::error!("Batch query [{}] error: {}", index, e);
                results.push(BatchQueryResult {
                    index,
                    success: false,
                    rows: None,
                    count: None,
                    error: Some("Query execution failed".to_string()),
                });
                if request.transaction {
                    had_error = true;
                    break;
                }
                if let Ok(pg_conn) = conn.get_mut() {
                    let rls_sql = qail_pg::rls_sql_with_timeouts(
                        &auth.to_rls_context(),
                        state.config.statement_timeout_ms,
                        state.config.lock_timeout_ms,
                    );
                    let reset_sql = format!("ROLLBACK; {}", rls_sql);
                    if let Err(re) = pg_conn.execute_simple(&reset_sql).await {
                        tracing::warn!(
                            "Batch non-txn reset failed after query error: {}; \
                             remaining queries will fail",
                            re
                        );
                    }
                }
            }
        }
    }

    if request.transaction {
        if had_error {
            if let Ok(pg_conn) = conn.get_mut() {
                let _ = pg_conn.execute_simple("ROLLBACK;").await;
            }
            tracing::warn!("Batch transaction rolled back due to error");
        } else {
            match conn.get_mut() {
                Ok(pg_conn) => {
                    if let Err(e) = pg_conn.execute_simple("COMMIT;").await {
                        tracing::error!("Transaction commit failed: {}", e);
                        conn.release().await;
                        return Err(ApiError::with_code(
                            "TXN_ERROR",
                            "Transaction commit failed",
                        ));
                    }
                }
                Err(e) => {
                    conn.release().await;
                    return Err(ApiError::from_pg_driver_error(&e, None));
                }
            }
        }
    }

    conn.release().await;

    let total = results.len();

    Ok(Json(BatchResponse {
        results,
        total,
        success: success_count,
    }))
}
