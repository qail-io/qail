use super::*;

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

    let call_target = match build_rpc_call_target(&function, args.as_ref()) {
        Ok(target) => target,
        Err(err) => {
            crate::metrics::record_rpc_call(
                started_at.elapsed().as_secs_f64() * 1000.0,
                false,
                result_format_label,
            );
            return Err(err);
        }
    };
    let sql = match build_rpc_sql(&function, args.as_ref()) {
        Ok(sql) => sql,
        Err(err) => {
            crate::metrics::record_rpc_call(
                started_at.elapsed().as_secs_f64() * 1000.0,
                false,
                result_format_label,
            );
            return Err(err);
        }
    };

    let mut conn = state.acquire_with_auth_rls_guarded(&auth, None).await?;

    if let Err(err) = super::signature::enforce_rpc_signature_contract(
        &state,
        &mut conn,
        &function,
        args.as_ref(),
        &sql,
    )
    .await
    {
        conn.release().await;
        crate::metrics::record_rpc_call(
            started_at.elapsed().as_secs_f64() * 1000.0,
            false,
            result_format_label,
        );
        return Err(err);
    }

    let mut cmd = qail_core::ast::Qail::get(call_target);
    state.optimize_qail_for_execution(&mut cmd);

    let rows = match conn
        .fetch_all_uncached_with_format(&cmd, result_format)
        .await
    {
        Ok(rows) => rows,
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
