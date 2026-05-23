use super::*;
use crate::server::RpcCallableSignature;

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

fn scalar_arg_matches_tenant(value: &Value, tenant_id: &str) -> bool {
    match value {
        Value::String(s) => s == tenant_id,
        Value::Number(n) => n.to_string() == tenant_id,
        Value::Bool(b) => b.to_string() == tenant_id,
        Value::Null | Value::Array(_) | Value::Object(_) => false,
    }
}

fn enforce_rpc_tenant_arg_boundary(
    args: Option<&Value>,
    signature: Option<&RpcCallableSignature>,
    tenant_id: Option<&str>,
    tenant_column: &str,
    function_name: &str,
) -> Result<(), ApiError> {
    let (Some(args), Some(tenant_id)) = (args, tenant_id) else {
        return Ok(());
    };
    let tenant_column = tenant_column.trim().to_ascii_lowercase();
    if tenant_column.is_empty() {
        return Ok(());
    }

    let tenant_arg = match args {
        Value::Object(map) => map.iter().find_map(|(key, value)| {
            (key.trim().eq_ignore_ascii_case(&tenant_column)).then_some(value)
        }),
        Value::Array(items) => signature.and_then(|signature| {
            signature
                .arg_names
                .iter()
                .enumerate()
                .find_map(|(idx, name)| {
                    name.as_deref()
                        .is_some_and(|name| name.eq_ignore_ascii_case(&tenant_column))
                        .then(|| items.get(idx))
                        .flatten()
                })
        }),
        scalar => signature.and_then(|signature| {
            signature
                .arg_names
                .first()
                .and_then(|name| name.as_deref())
                .is_some_and(|name| name.eq_ignore_ascii_case(&tenant_column))
                .then_some(scalar)
        }),
    };

    if let Some(value) = tenant_arg
        && !scalar_arg_matches_tenant(value, tenant_id)
    {
        return Err(ApiError::forbidden(format!(
            "RPC tenant argument '{}' for '{}' must match authenticated tenant",
            tenant_column, function_name
        )));
    }

    Ok(())
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
    if let Err(err) = enforce_rpc_tenant_arg_boundary(
        args.as_ref(),
        signature.as_ref(),
        auth.tenant_id.as_deref(),
        &state.config.tenant_column,
        &function.canonical(),
    ) {
        conn.release().await;
        crate::metrics::record_rpc_call(
            started_at.elapsed().as_secs_f64() * 1000.0,
            false,
            result_format_label,
        );
        return Err(err);
    }

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

        if let Err(e) = conn.release_checked().await {
            state.cache.invalidate_all();
            crate::metrics::record_rpc_call(
                started_at.elapsed().as_secs_f64() * 1000.0,
                false,
                result_format_label,
            );
            return Err(ApiError::from_pg_driver_error(&e, None));
        }
        state.cache.invalidate_all();
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

            if let Err(e) = conn.release_checked().await {
                state.cache.invalidate_all();
                crate::metrics::record_rpc_call(
                    started_at.elapsed().as_secs_f64() * 1000.0,
                    false,
                    result_format_label,
                );
                return Err(ApiError::from_pg_driver_error(&e, None));
            }
            state.cache.invalidate_all();
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

    let data: Vec<Value> = rows.iter().map(row_to_json).collect();
    if let Some(ref tenant_id) = auth.tenant_id
        && let Err(v) = crate::tenant_guard::verify_tenant_boundary(
            &data,
            tenant_id,
            &state.config.tenant_column,
            &function.canonical(),
            "rest_rpc",
        )
    {
        tracing::error!("{}", v);
        let _ = conn.rollback_and_release().await;
        crate::metrics::record_rpc_call(
            started_at.elapsed().as_secs_f64() * 1000.0,
            false,
            result_format_label,
        );
        return Err(ApiError::with_code(
            "TENANT_BOUNDARY_VIOLATION",
            "Data integrity error",
        ));
    }

    if let Err(e) = conn.release_checked().await {
        state.cache.invalidate_all();
        crate::metrics::record_rpc_call(
            started_at.elapsed().as_secs_f64() * 1000.0,
            false,
            result_format_label,
        );
        return Err(ApiError::from_pg_driver_error(&e, None));
    }
    state.cache.invalidate_all();

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
    use super::{enforce_rpc_tenant_arg_boundary, is_rpc_void_context_error};
    use crate::server::RpcCallableSignature;
    use serde_json::json;

    fn signature_with_args(arg_names: &[Option<&str>]) -> RpcCallableSignature {
        RpcCallableSignature {
            total_args: arg_names.len(),
            default_args: 0,
            variadic: false,
            arg_names: arg_names
                .iter()
                .map(|name| name.map(str::to_string))
                .collect(),
            arg_types: vec!["text".to_string(); arg_names.len()],
            arg_type_oids: vec![0; arg_names.len()],
            variadic_element_oid: None,
            identity_args: String::new(),
            result_type: "void".to_string(),
        }
    }

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

    #[test]
    fn rpc_tenant_boundary_rejects_mismatched_named_tenant_arg() {
        let args = json!({"tenant_id": "tenant-b", "order_id": "order-1"});

        let err = enforce_rpc_tenant_arg_boundary(
            Some(&args),
            None,
            Some("tenant-a"),
            "tenant_id",
            "api.delete_order",
        )
        .unwrap_err();

        assert_eq!(err.status_code(), axum::http::StatusCode::FORBIDDEN);
    }

    #[test]
    fn rpc_tenant_boundary_checks_positional_arg_when_signature_names_it() {
        let args = json!(["tenant-b", "order-1"]);
        let signature = signature_with_args(&[Some("tenant_id"), Some("order_id")]);

        let err = enforce_rpc_tenant_arg_boundary(
            Some(&args),
            Some(&signature),
            Some("tenant-a"),
            "tenant_id",
            "api.delete_order",
        )
        .unwrap_err();

        assert_eq!(err.status_code(), axum::http::StatusCode::FORBIDDEN);
    }

    #[test]
    fn rpc_tenant_boundary_allows_matching_tenant_arg() {
        let args = json!({"tenant_id": "tenant-a", "order_id": "order-1"});

        enforce_rpc_tenant_arg_boundary(
            Some(&args),
            None,
            Some("tenant-a"),
            "tenant_id",
            "api.delete_order",
        )
        .unwrap();
    }
}
