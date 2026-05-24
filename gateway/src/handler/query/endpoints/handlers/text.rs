use super::common::parse_cached_query;
use super::*;

fn build_export_tenant_violation_check(
    cmd: &qail_core::ast::Qail,
    tenant_column: &str,
    tenant_id: &str,
) -> qail_core::ast::Qail {
    use qail_core::ast::{AggregateFunc, CageKind, Expr, Operator, Value as QailValue};

    let mut guard_cmd = cmd.clone();
    guard_cmd.action = Action::Get;
    guard_cmd.columns = vec![Expr::Aggregate {
        col: "*".to_string(),
        func: AggregateFunc::Count,
        distinct: false,
        filter: None,
        alias: Some("violation_count".to_string()),
    }];
    guard_cmd.distinct = false;
    guard_cmd.distinct_on.clear();
    guard_cmd.having.clear();
    guard_cmd.set_ops.clear();
    guard_cmd.fetch = None;
    guard_cmd
        .cages
        .retain(|cage| matches!(cage.kind, CageKind::Filter));

    let filter_column = if guard_cmd.joins.is_empty() {
        tenant_column.to_string()
    } else {
        format!("{}.{}", guard_cmd.table, tenant_column)
    };
    guard_cmd.filter(
        filter_column,
        Operator::Ne,
        QailValue::String(tenant_id.to_string()),
    )
}

fn export_violation_count(row: &serde_json::Value) -> Result<u64, &'static str> {
    row.get("violation_count")
        .and_then(|value| match value {
            serde_json::Value::Number(n) => n.as_u64(),
            serde_json::Value::String(s) => s.parse::<u64>().ok(),
            _ => None,
        })
        .ok_or("export tenant guard returned a missing or invalid violation_count")
}

pub async fn execute_query(
    State(state): State<Arc<GatewayState>>,
    extensions: axum::http::Extensions,
    headers: HeaderMap,
    body: String,
) -> Result<Json<QueryResponse>, ApiError> {
    let query_text = body.trim();

    if query_text.is_empty() {
        return Err(ApiError::bad_request("EMPTY_QUERY", "Empty query"));
    }

    let auth = authenticate_request(state.as_ref(), &headers).await?;

    tracing::debug!(
        "Executing text query: {} (user: {})",
        query_text,
        auth.user_id
    );

    let mut cmd = parse_cached_query(&state, query_text)?;

    reject_dangerous_action(&cmd)?;
    let tenant_guard_plan =
        crate::tenant_guard::prepare_tenant_guarded_query(state.as_ref(), &auth, &mut cmd)
            .map_err(|e| ApiError::bad_request("TENANT_GUARD_PROJECTION", e.to_string()))?;

    let allow_list_raw_query = if tenant_guard_plan.is_some() {
        None
    } else {
        Some(query_text)
    };
    if !is_query_allowed(&state.allow_list, allow_list_raw_query, &cmd) {
        tracing::warn!("Query rejected by allow-list: {}", query_text);
        return Err(ApiError::with_code(
            "QUERY_NOT_ALLOWED",
            "Query not in allow-list",
        ));
    }

    if let Err(e) = state.policy_engine.apply_policies(&auth, &mut cmd) {
        tracing::warn!("Policy error: {}", e);
        return Err(ApiError::with_code("POLICY_DENIED", e.to_string()));
    }
    if let Some(ref plan) = tenant_guard_plan
        && plan.verify_rows
    {
        crate::tenant_guard::ensure_verifiable_tenant_projection(&cmd, &plan.column)
            .map_err(|e| ApiError::forbidden(e.to_string()))?;
    }

    clamp_query_limit(&mut cmd, state.config.max_result_rows);
    execute_qail_cmd(&state, &auth, &cmd, tenant_guard_plan.as_ref(), &extensions).await
}

/// Execute a streaming export query (POST /qail/export).
///
/// Accepts QAIL text that must compile to `Action::Export` and streams raw
/// COPY TO STDOUT chunks to the HTTP response body.
pub async fn execute_query_export(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    body: String,
) -> Result<Response, ApiError> {
    let query_text = body.trim();
    if query_text.is_empty() {
        return Err(ApiError::bad_request("EMPTY_QUERY", "Empty query"));
    }

    let auth = authenticate_request(state.as_ref(), &headers).await?;
    let mut cmd = parse_cached_query(&state, query_text)?;

    if cmd.action != Action::Export {
        return Err(ApiError::bad_request(
            "EXPORT_ONLY",
            "Endpoint /qail/export only accepts QAIL export commands",
        ));
    }

    reject_dangerous_action(&cmd)?;

    let tenant_guard_plan =
        crate::tenant_guard::prepare_tenant_guarded_query(state.as_ref(), &auth, &mut cmd)
            .map_err(|e| ApiError::bad_request("TENANT_GUARD_PROJECTION", e.to_string()))?;

    let allow_list_raw_query = if tenant_guard_plan.is_some() {
        None
    } else {
        Some(query_text)
    };
    if !is_query_allowed(&state.allow_list, allow_list_raw_query, &cmd) {
        tracing::warn!("Export query rejected by allow-list: {}", query_text);
        return Err(ApiError::with_code(
            "QUERY_NOT_ALLOWED",
            "Query not in allow-list",
        ));
    }

    if let Err(e) = state.policy_engine.apply_policies(&auth, &mut cmd) {
        tracing::warn!("Policy error: {}", e);
        return Err(ApiError::with_code("POLICY_DENIED", e.to_string()));
    }

    state.optimize_qail_for_execution(&mut cmd);

    let (depth, filters, joins) = query_complexity(&cmd);
    if let Err(api_err) = state.complexity_guard.check(depth, filters, joins) {
        crate::metrics::record_complexity_rejected();
        return Err(api_err);
    }

    let mut conn = state
        .acquire_with_auth_rls_guarded(&auth, Some(&cmd.table))
        .await?;

    if let Some(tenant_id) = auth.tenant_id.as_deref()
        && let Some(tenant_column) =
            crate::tenant_guard::tenant_guard_column_for_table(state.as_ref(), &cmd.table)
    {
        let mut guard_cmd = build_export_tenant_violation_check(&cmd, &tenant_column, tenant_id);
        state.optimize_qail_for_execution(&mut guard_cmd);
        let guard_rows = match conn.fetch_all_uncached(&guard_cmd).await {
            Ok(rows) => rows,
            Err(e) => {
                conn.release().await;
                return Err(ApiError::from_pg_driver_error(&e, Some(&cmd.table)));
            }
        };
        let violation_count = match guard_rows.first().map(row_to_json) {
            Some(row) => match export_violation_count(&row) {
                Ok(count) => count,
                Err(reason) => {
                    conn.release().await;
                    tracing::error!(
                        table = %cmd.table,
                        endpoint = "qail_export",
                        reason,
                        "TENANT_BOUNDARY_VIOLATION - export preflight returned malformed guard count"
                    );
                    return Err(ApiError::with_code(
                        "TENANT_BOUNDARY_VIOLATION",
                        "Data integrity error",
                    ));
                }
            },
            None => {
                conn.release().await;
                tracing::error!(
                    table = %cmd.table,
                    endpoint = "qail_export",
                    "TENANT_BOUNDARY_VIOLATION - export preflight returned no guard count row"
                );
                return Err(ApiError::with_code(
                    "TENANT_BOUNDARY_VIOLATION",
                    "Data integrity error",
                ));
            }
        };
        if violation_count > 0 {
            conn.release().await;
            tracing::error!(
                table = %cmd.table,
                endpoint = "qail_export",
                violation_count,
                "TENANT_BOUNDARY_VIOLATION - export preflight found cross-tenant rows"
            );
            return Err(ApiError::with_code(
                "TENANT_BOUNDARY_VIOLATION",
                "Data integrity error",
            ));
        }
    }

    let cmd_for_stream = cmd.clone();
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(8);

    tokio::spawn(async move {
        let tx_for_chunks = tx.clone();
        let result = conn
            .copy_export_stream_raw(&cmd_for_stream, move |chunk| {
                let tx = tx_for_chunks.clone();
                async move {
                    tx.send(Ok(Bytes::from(chunk))).await.map_err(|_| {
                        qail_pg::PgError::Query(
                            "export stream receiver dropped before completion".to_string(),
                        )
                    })
                }
            })
            .await;

        if let Err(e) = result {
            let _ = tx
                .send(Err(std::io::Error::other(format!(
                    "export stream failed: {}",
                    e
                ))))
                .await;
        }
        conn.release().await;
    });

    let stream = futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });

    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::export_violation_count;
    use serde_json::json;

    #[test]
    fn export_violation_count_accepts_numeric_and_string_counts() {
        assert_eq!(
            export_violation_count(&json!({"violation_count": 0})).unwrap(),
            0
        );
        assert_eq!(
            export_violation_count(&json!({"violation_count": "12"})).unwrap(),
            12
        );
    }

    #[test]
    fn export_violation_count_rejects_missing_or_malformed_counts() {
        assert!(export_violation_count(&json!({})).is_err());
        assert!(export_violation_count(&json!({"violation_count": null})).is_err());
        assert!(export_violation_count(&json!({"violation_count": -1})).is_err());
        assert!(export_violation_count(&json!({"violation_count": "oops"})).is_err());
    }
}
