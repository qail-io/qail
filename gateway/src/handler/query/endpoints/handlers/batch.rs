use super::*;

const BATCH_SAVEPOINT: &str = "qail_batch";

async fn rollback_query_savepoint(conn: &mut qail_pg::PooledConnection, savepoint: &str) {
    if let Err(e) = conn.rollback_to(savepoint).await {
        tracing::warn!(
            savepoint = %savepoint,
            error = %e,
            "Batch query savepoint rollback failed"
        );
        return;
    }
    if let Err(e) = conn.release_savepoint(savepoint).await {
        tracing::warn!(
            savepoint = %savepoint,
            error = %e,
            "Batch query savepoint release after rollback failed"
        );
    }
}

fn mark_transaction_results_rolled_back(results: &mut [BatchQueryResult]) {
    for result in results.iter_mut().filter(|result| result.success) {
        result.success = false;
        result.rows = None;
        result.count = None;
        result.error = Some("Rolled back due to batch transaction error".to_string());
    }
}

fn batch_command_is_read_only(cmd: &qail_core::ast::Qail) -> bool {
    let action_is_read_only = matches!(
        cmd.action,
        qail_core::ast::Action::Get
            | qail_core::ast::Action::Cnt
            | qail_core::ast::Action::JsonTable
            | qail_core::ast::Action::With
            | qail_core::ast::Action::Export
    );
    action_is_read_only
        && cmd.ctes.iter().all(|cte| {
            batch_command_is_read_only(&cte.base_query)
                && cte
                    .recursive_query
                    .as_deref()
                    .is_none_or(batch_command_is_read_only)
        })
        && cmd
            .source_query
            .as_deref()
            .is_none_or(batch_command_is_read_only)
        && cmd
            .set_ops
            .iter()
            .all(|(_, set_query)| batch_command_is_read_only(set_query))
}

/// Execute a batch of Qail queries (POST /qail/batch).
pub async fn execute_batch(
    State(state): State<Arc<GatewayState>>,
    extensions: axum::http::Extensions,
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
    let mut conn: Option<qail_pg::PooledConnection> = None;
    let mut batch_has_write = false;

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

        let tenant_guard_plan = match crate::tenant_guard::prepare_tenant_guarded_query(
            state.as_ref(),
            &auth,
            &mut cmd,
        ) {
            Ok(column) => column,
            Err(e) => {
                results.push(BatchQueryResult {
                    index,
                    success: false,
                    rows: None,
                    count: None,
                    error: Some(e.to_string()),
                });
                if request.transaction {
                    had_error = true;
                    break;
                }
                continue;
            }
        };

        let allow_list_raw_query = if tenant_guard_plan.is_some() {
            None
        } else {
            Some(query_text)
        };
        if !is_query_allowed(&state.allow_list, allow_list_raw_query, &cmd) {
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
        if let Some(ref plan) = tenant_guard_plan
            && plan.verify_rows
            && let Err(e) =
                crate::tenant_guard::ensure_verifiable_tenant_projection(&cmd, &plan.column)
        {
            results.push(BatchQueryResult {
                index,
                success: false,
                rows: None,
                count: None,
                error: Some(e.to_string()),
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
        let command_is_read_only = batch_command_is_read_only(&cmd);

        if conn.is_none() {
            let mut acquired = state.acquire_with_auth_rls_guarded(&auth, None).await?;
            if request.transaction
                && let Err(e) = acquired.savepoint(BATCH_SAVEPOINT).await
            {
                tracing::error!("Batch transaction savepoint start failed: {}", e);
                acquired.release().await;
                return Err(ApiError::with_code("TXN_ERROR", "Transaction start failed"));
            }
            conn = Some(acquired);
        }
        let Some(conn_ref) = conn.as_mut() else {
            return Err(ApiError::internal(
                "Batch connection initialization failed unexpectedly",
            ));
        };

        let query_savepoint = if request.transaction {
            None
        } else {
            let savepoint = format!("qail_batch_{}", index);
            if let Err(e) = conn_ref.savepoint(&savepoint).await {
                tracing::error!("Batch query savepoint start failed: {}", e);
                if let Some(conn) = conn.take() {
                    let _ = conn.rollback_and_release().await;
                }
                return Err(ApiError::with_code(
                    "TXN_ERROR",
                    "Batch query savepoint start failed",
                ));
            }
            Some(savepoint)
        };

        let timer = crate::metrics::QueryTimer::new(&cmd.table, &cmd.action.to_string());
        match conn_ref.fetch_all_uncached(&cmd).await {
            Ok(rows) => {
                timer.finish(true);
                let mut json_rows: Vec<serde_json::Value> = rows.iter().map(row_to_json).collect();
                let count = json_rows.len();

                if let (Some(tenant_id), Some(plan)) =
                    (auth.tenant_id.as_deref(), tenant_guard_plan.as_ref())
                    && plan.verify_rows
                    && let Err(v) = crate::tenant_guard::verify_tenant_boundary(
                        &json_rows,
                        tenant_id,
                        &plan.column,
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
                    if let Some(ref savepoint) = query_savepoint {
                        rollback_query_savepoint(conn_ref, savepoint).await;
                    }
                    continue;
                }

                if let Some(plan) = tenant_guard_plan.as_ref()
                    && plan.strip_output_column
                {
                    crate::tenant_guard::strip_tenant_column_from_json_rows(
                        &mut json_rows,
                        &plan.column,
                    );
                }

                if let Some(ref savepoint) = query_savepoint
                    && let Err(e) = conn_ref.release_savepoint(savepoint).await
                {
                    tracing::error!("Batch query savepoint release failed: {}", e);
                    rollback_query_savepoint(conn_ref, savepoint).await;
                    results.push(BatchQueryResult {
                        index,
                        success: false,
                        rows: None,
                        count: None,
                        error: Some("Batch query savepoint release failed".to_string()),
                    });
                    if request.transaction {
                        had_error = true;
                        break;
                    }
                    continue;
                }

                if !command_is_read_only {
                    batch_has_write = true;
                    for cache_table in cache_tables_for_qail(&cmd) {
                        state.cache.invalidate_table(&cache_table);
                    }
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
                if let Some(ref savepoint) = query_savepoint {
                    rollback_query_savepoint(conn_ref, savepoint).await;
                }
            }
        }
    }

    if let Some(mut conn) = conn {
        if request.transaction {
            if had_error {
                if let Err(e) = conn.rollback_to(BATCH_SAVEPOINT).await {
                    tracing::error!("Batch transaction rollback failed: {}", e);
                    let _ = conn.rollback_and_release().await;
                    return Err(ApiError::with_code(
                        "TXN_ERROR",
                        "Transaction rollback failed",
                    ));
                }
                if let Err(e) = conn.release_savepoint(BATCH_SAVEPOINT).await {
                    tracing::warn!(
                        "Batch transaction savepoint release after rollback failed: {}",
                        e
                    );
                }
                tracing::warn!("Batch transaction rolled back due to error");
                mark_transaction_results_rolled_back(&mut results);
                success_count = 0;
            } else if let Err(e) = conn.release_savepoint(BATCH_SAVEPOINT).await {
                tracing::error!("Batch transaction savepoint commit failed: {}", e);
                let _ = conn.rollback_and_release().await;
                return Err(ApiError::with_code(
                    "TXN_ERROR",
                    "Transaction commit failed",
                ));
            }
        }
        if batch_has_write {
            conn.release_checked()
                .await
                .map_err(|e| ApiError::from_pg_driver_error(&e, None))?;
        } else {
            conn.release().await;
        }
    }

    let total = results.len();
    let request_id = match extensions.get::<crate::middleware::RequestId>() {
        Some(id) => id.0.clone(),
        None => String::new(),
    };

    Ok(Json(BatchResponse {
        results,
        total,
        success: success_count,
        metadata: Some(crate::handler::ResponseMetadata {
            request_id,
            duration_ms: None, // Complex to calculate per-batch accurately here
        }),
    }))
}
