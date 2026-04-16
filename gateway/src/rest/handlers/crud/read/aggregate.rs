use super::*;

pub(crate) async fn aggregate_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Query(params): Query<AggregateParams>,
    request: axum::extract::Request,
) -> Result<Json<AggregateResponse>, ApiError> {
    // Extract table from path: /api/{table}/aggregate → table
    let path = request.uri().path().to_string();
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    if parts.len() < 3 || parts[0] != "api" {
        return Err(ApiError::not_found("aggregate route"));
    }
    let table_name = parts[1].to_string();
    check_table_not_blocked(&state, &table_name)?;

    let _table = state
        .schema
        .table(&table_name)
        .ok_or_else(|| ApiError::not_found(&table_name))?;

    let auth = authenticate_request(state.as_ref(), &headers).await?;
    let tenant_scope =
        crate::rest::tenant_scope_filter_for_table(state.as_ref(), &auth, &table_name);

    let func_name = params.func.as_deref().unwrap_or("count");
    let agg_func = match func_name.to_lowercase().as_str() {
        "count" => AggregateFunc::Count,
        "sum" => AggregateFunc::Sum,
        "avg" => AggregateFunc::Avg,
        "min" => AggregateFunc::Min,
        "max" => AggregateFunc::Max,
        _ => {
            return Err(ApiError::parse_error(format!(
                "Unknown aggregate function: '{}'. Use: count, sum, avg, min, max",
                func_name
            )));
        }
    };

    let col_name = params.column.as_deref().unwrap_or("*");

    // SECURITY: Validate aggregate column identifier.
    if col_name != "*" && !crate::rest::filters::is_safe_identifier(col_name) {
        return Err(ApiError::parse_error(format!(
            "Invalid aggregate column: '{}'",
            col_name
        )));
    }

    let is_distinct = params
        .distinct
        .as_deref()
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    // Build aggregate expression
    let agg_expr = Expr::Aggregate {
        col: if col_name == "*" {
            "*".to_string()
        } else {
            col_name.to_string()
        },
        func: agg_func,
        distinct: is_distinct,
        filter: None,
        alias: None,
    };

    let mut cmd = qail_core::ast::Qail::get(&table_name).column_expr(agg_expr);

    // Group by
    if let Some(ref group_by) = params.group_by {
        let group_exprs: Vec<Expr> = group_by
            .split(',')
            .map(|s| s.trim())
            .filter(|s| crate::rest::filters::is_safe_identifier(s))
            .map(|s| Expr::Named(s.to_string()))
            .collect();
        // Add group-by columns to SELECT so they appear in the result
        for expr in &group_exprs {
            cmd = cmd.column_expr(expr.clone());
        }
        cmd = cmd.group_by_expr(group_exprs);
    }

    // Parse and apply filters from query string
    let query_string = request.uri().query().unwrap_or("");
    let filters = parse_filters(query_string);
    cmd = apply_filters(cmd, &filters);
    if let Some((scope_column, tenant_id)) = tenant_scope.as_ref() {
        cmd = cmd.filter(
            scope_column,
            Operator::Eq,
            QailValue::String(tenant_id.clone()),
        );
    }

    // Apply RLS
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;
    state.optimize_qail_for_execution(&mut cmd);

    // Execute
    let mut conn = state
        .acquire_with_auth_rls_guarded(&auth, Some(&table_name))
        .await?;

    let rows = conn
        .fetch_all_uncached(&cmd)
        .await
        .map_err(|e| ApiError::from_pg_driver_error(&e, Some(&table_name)));

    conn.release().await;
    let rows = rows?;

    let data: Vec<Value> = rows.iter().map(row_to_json).collect();

    // ── Tenant Boundary Invariant ────────────────────────────────────
    if let Some((scope_column, tenant_id)) = tenant_scope.as_ref() {
        let _proof = crate::tenant_guard::verify_tenant_boundary(
            &data,
            tenant_id,
            scope_column,
            &table_name,
            "rest_aggregate",
        )
        .map_err(|v| {
            tracing::error!("{}", v);
            ApiError::internal("Data integrity error")
        })?;
    }

    let count = data.len();

    Ok(Json(AggregateResponse { data, count }))
}
