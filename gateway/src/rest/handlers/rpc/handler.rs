use super::*;

fn is_rpc_void_context_error(err: &qail_pg::PgError) -> bool {
    let Some(server) = err.server_error() else {
        return false;
    };

    let message = server.message.to_ascii_lowercase();
    server.code.eq_ignore_ascii_case("42809")
        || (message.contains("void") && message.contains("cannot accept type"))
        || message.contains("function returning void")
}

fn query_has_typed_params(query: &super::RpcBoundQuery) -> bool {
    query.param_type_oids.len() == query.params.len() && !query.param_type_oids.contains(&0)
}

async fn execute_rpc_rows(
    conn: &mut qail_pg::PooledConnection,
    query: &super::RpcBoundQuery,
    result_format: qail_pg::ResultFormat,
) -> Result<Vec<qail_pg::PgRow>, qail_pg::PgError> {
    if query_has_typed_params(query) {
        conn.query_rows_with_param_types_with_format(
            &query.sql,
            &query.param_type_oids,
            &query.params,
            result_format,
        )
        .await
    } else {
        conn.query_rows_with_params_with_format(&query.sql, &query.params, result_format)
            .await
    }
}

/// POST /api/rpc/{function} — invoke PostgreSQL functions with JSON args.
///
/// Body forms:
/// - object: named args (`{ "tenant_id": "...", "limit": 10 }`)
/// - array: positional args (`["...", 10]`)
/// - scalar/null: single positional argument
/// - empty body: no arguments
pub(crate) async fn rpc_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(function_name): Path<String>,
    request: axum::extract::Request,
) -> Result<Json<Value>, ApiError> {
    let started_at = Instant::now();
    let auth = authenticate_request(state.as_ref(), &headers).await?;
    if !auth.is_authenticated() {
        crate::metrics::record_rpc_call(started_at.elapsed().as_secs_f64() * 1000.0, false, "text");
        return Err(ApiError::auth_error(
            "Authentication required for RPC invocation",
        ));
    }

    let Some(rpc_allow_list) = state.rpc_allow_list.as_ref() else {
        crate::metrics::record_rpc_allowlist_rejection();
        crate::metrics::record_rpc_call(started_at.elapsed().as_secs_f64() * 1000.0, false, "text");
        return Err(ApiError::forbidden(
            "RPC endpoint is disabled until rpc_allowlist_path is configured",
        ));
    };

    let function = RpcFunctionName::parse(&function_name)?;
    enforce_rpc_name_contract(
        state.config.rpc_require_schema_qualified,
        Some(rpc_allow_list),
        &function,
    )?;

    let mut policy_probe = qail_core::ast::Qail::get(function.canonical());
    state
        .policy_engine
        .apply_policies(&auth, &mut policy_probe)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;

    let body = axum::body::to_bytes(request.into_body(), state.config.max_request_body_bytes)
        .await
        .map_err(|e| ApiError::parse_error(e.to_string()))?;
    let args: Option<Value> = if body.is_empty() {
        None
    } else {
        Some(
            serde_json::from_slice(&body)
                .map_err(|e| ApiError::parse_error(format!("Invalid RPC JSON body: {}", e)))?,
        )
    };

    let result_format = match headers
        .get("x-qail-result-format")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
    {
        None | Some("") => qail_pg::ResultFormat::Text,
        Some(v) if v.eq_ignore_ascii_case("text") => qail_pg::ResultFormat::Text,
        Some(v) if v.eq_ignore_ascii_case("binary") => qail_pg::ResultFormat::Binary,
        Some(other) => {
            return Err(ApiError::parse_error(format!(
                "Invalid x-qail-result-format '{}'. Use 'text' or 'binary'.",
                other
            )));
        }
    };
    let result_format_label = if matches!(result_format, qail_pg::ResultFormat::Binary) {
        "binary"
    } else {
        "text"
    };

    let mut conn = state.acquire_with_auth_rls_guarded(&auth, None).await?;

    let contract = match super::signature::enforce_rpc_signature_contract(
        &state,
        &mut conn,
        &function,
        args.as_ref(),
    )
    .await
    {
        Ok(contract) => contract,
        Err(err) => {
            conn.release().await;
            crate::metrics::record_rpc_call(
                started_at.elapsed().as_secs_f64() * 1000.0,
                false,
                result_format_label,
            );
            return Err(err);
        }
    };
    let execution_mode = contract.execution_mode;
    let signature = contract.signature;

    if matches!(execution_mode, super::signature::RpcExecutionMode::Void) {
        let scalar_query =
            match build_rpc_bound_sql(&function, args.as_ref(), signature.as_ref(), true) {
                Ok(query) => query,
                Err(err) => {
                    conn.release().await;
                    crate::metrics::record_rpc_call(
                        started_at.elapsed().as_secs_f64() * 1000.0,
                        false,
                        result_format_label,
                    );
                    return Err(err);
                }
            };

        if let Err(e) = execute_rpc_rows(&mut conn, &scalar_query, result_format).await {
            conn.release().await;
            crate::metrics::record_rpc_call(
                started_at.elapsed().as_secs_f64() * 1000.0,
                false,
                result_format_label,
            );
            return Err(ApiError::from_pg_driver_error(&e, None));
        }

        conn.release().await;
        crate::metrics::record_rpc_call(
            started_at.elapsed().as_secs_f64() * 1000.0,
            true,
            result_format_label,
        );
        return Ok(Json(json!({
            "data": [],
            "count": 0,
            "function": function.canonical(),
            "result_format": result_format_label,
        })));
    }

    let row_query = match build_rpc_bound_sql(&function, args.as_ref(), signature.as_ref(), false) {
        Ok(query) => query,
        Err(err) => {
            conn.release().await;
            crate::metrics::record_rpc_call(
                started_at.elapsed().as_secs_f64() * 1000.0,
                false,
                result_format_label,
            );
            return Err(err);
        }
    };

    let rows = match execute_rpc_rows(&mut conn, &row_query, result_format).await {
        Ok(rows) => rows,
        Err(e)
            if matches!(execution_mode, super::signature::RpcExecutionMode::Unknown)
                && is_rpc_void_context_error(&e) =>
        {
            let scalar_query =
                match build_rpc_bound_sql(&function, args.as_ref(), signature.as_ref(), true) {
                    Ok(query) => query,
                    Err(err) => {
                        conn.release().await;
                        crate::metrics::record_rpc_call(
                            started_at.elapsed().as_secs_f64() * 1000.0,
                            false,
                            result_format_label,
                        );
                        return Err(err);
                    }
                };

            if let Err(void_err) = execute_rpc_rows(&mut conn, &scalar_query, result_format).await {
                conn.release().await;
                crate::metrics::record_rpc_call(
                    started_at.elapsed().as_secs_f64() * 1000.0,
                    false,
                    result_format_label,
                );
                return Err(ApiError::from_pg_driver_error(&void_err, None));
            }

            conn.release().await;
            crate::metrics::record_rpc_call(
                started_at.elapsed().as_secs_f64() * 1000.0,
                true,
                result_format_label,
            );
            return Ok(Json(json!({
                "data": [],
                "count": 0,
                "function": function.canonical(),
                "result_format": result_format_label,
            })));
        }
        Err(e) => {
            conn.release().await;
            crate::metrics::record_rpc_call(
                started_at.elapsed().as_secs_f64() * 1000.0,
                false,
                result_format_label,
            );
            return Err(ApiError::from_pg_driver_error(&e, None));
        }
    };

    conn.release().await;

    let data: Vec<Value> = rows.iter().map(row_to_json).collect();
    let count = data.len();
    crate::metrics::record_rpc_call(
        started_at.elapsed().as_secs_f64() * 1000.0,
        true,
        result_format_label,
    );

    Ok(Json(json!({
        "data": data,
        "count": count,
        "function": function.canonical(),
        "result_format": result_format_label,
    })))
}

#[cfg(test)]
mod tests {
    use super::is_rpc_void_context_error;

    #[test]
    fn detects_void_context_server_error() {
        let err = qail_pg::PgError::QueryServer(qail_pg::PgServerError {
            severity: "ERROR".to_string(),
            code: "42809".to_string(),
            message: "function returning void called in context that cannot accept type void"
                .to_string(),
            detail: None,
            hint: None,
        });
        assert!(is_rpc_void_context_error(&err));
    }

    #[test]
    fn ignores_non_void_server_error() {
        let err = qail_pg::PgError::QueryServer(qail_pg::PgServerError {
            severity: "ERROR".to_string(),
            code: "23505".to_string(),
            message: "duplicate key value violates unique constraint".to_string(),
            detail: None,
            hint: None,
        });
        assert!(!is_rpc_void_context_error(&err));
    }
}
