use super::*;

pub(crate) async fn list_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Query(params): Query<ListParams>,
    request: axum::extract::Request,
) -> Result<Response, ApiError> {
    let table_name =
        extract_table_name(request.uri()).ok_or_else(|| ApiError::not_found("table"))?;
    check_table_not_blocked(&state, &table_name)?;

    let _table = state
        .schema
        .table(&table_name)
        .ok_or_else(|| ApiError::not_found(&table_name))?;

    let auth = authenticate_request(state.as_ref(), &headers).await?;

    // Build Qail AST
    let max_rows = state.config.max_result_rows.min(1000) as i64;
    let limit = params.limit.unwrap_or(50).clamp(1, max_rows);
    let offset = params.offset.unwrap_or(0).clamp(0, 100_000);

    let mut cmd = qail_core::ast::Qail::get(&table_name);

    // Column selection
    if let Some(ref select) = params.select {
        let mut cols: Vec<&str> = select
            .split(',')
            .map(|s| s.trim())
            .filter(|s| *s == "*" || crate::rest::filters::is_safe_identifier(s))
            .collect();

        // SECURITY: Ensure tenant column is always projected so verify_tenant_boundary()
        // can check row ownership. Without this, a malicious client could bypass the
        // tenant guard by omitting the tenant column from `select`.
        if !cols.contains(&"*")
            && auth.tenant_id.is_some()
            && !cols
                .iter()
                .any(|c| *c == state.config.tenant_column.as_str())
        {
            cols.push(&state.config.tenant_column);
        }

        if !cols.is_empty() {
            cmd = cmd.columns(cols);
        }
    }

    // Sorting (multi-column) — default to `id ASC` for deterministic pagination
    if let Some(ref sort) = params.sort {
        cmd = apply_sorting(cmd, sort);
    } else {
        cmd = cmd.order_asc("id");
    }

    // Distinct
    if let Some(ref distinct) = params.distinct {
        let cols: Vec<&str> = distinct
            .split(',')
            .map(|s| s.trim())
            .filter(|s| crate::rest::filters::is_safe_identifier(s))
            .collect();
        if !cols.is_empty() {
            cmd = cmd.distinct_on(cols);
        }
    }

    // Expand FK relations via LEFT JOIN
    let mut has_joins = false;
    if let Some(ref expand) = params.expand {
        let relations: Vec<&str> = {
            let mut seen = std::collections::HashSet::new();
            expand
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty() && !s.starts_with("nested:") && seen.insert(*s))
                .collect()
        };
        if relations.len() > state.config.max_expand_depth {
            return Err(ApiError::parse_error(format!(
                "Too many expand relations ({}). Maximum is {}",
                relations.len(),
                state.config.max_expand_depth
            )));
        }
        for rel in relations {
            // SECURITY: Block expand into blocked tables
            check_table_not_blocked(&state, rel)?;

            // Try: this table references `rel` (forward: orders?expand=users)
            if let Some((fk_col, ref_col)) = state.schema.relation_for(&table_name, rel) {
                let left = format!("{}.{}", table_name, fk_col);
                let right = format!("{}.{}", rel, ref_col);
                cmd = cmd.join(JoinKind::Left, rel, &left, &right);
                has_joins = true;
                continue;
            }
            // Reverse relation (one-to-many) multiplies parent rows on flat JOIN.
            // Force nested expansion to preserve parent-row semantics.
            if state.schema.relation_for(rel, &table_name).is_some() {
                return Err(ApiError::parse_error(format!(
                    "Reverse relation '{}' expands one-to-many and can duplicate parent rows. Use 'nested:{}' instead.",
                    rel, rel
                )));
            }
            return Err(ApiError::parse_error(format!(
                "No relation between '{}' and '{}'",
                table_name, rel
            )));
        }
    }

    // When JOINs are present, table-qualify base table columns in SELECT
    // to avoid ambiguous column errors (e.g., both tables have `tenant_id`)
    if has_joins {
        if cmd.columns.is_empty() || cmd.columns == vec![Expr::Named("*".into())] {
            // SELECT * → qualify with table name: SELECT base_table.*
            cmd.columns = vec![Expr::Named(format!("{}.*", table_name))];
        } else {
            // Qualify each unqualified column: col → base_table.col
            cmd.columns = cmd
                .columns
                .into_iter()
                .map(|expr| match expr {
                    Expr::Named(ref name) if !name.contains('.') => {
                        Expr::Named(format!("{}.{}", table_name, name))
                    }
                    other => other,
                })
                .collect();
        }
    }

    // Parse and apply filters from query string
    let query_string = request.uri().query().unwrap_or("");
    let filters = parse_filters(query_string);
    cmd = apply_filters(cmd, &filters);

    // Cursor-based pagination: filter rows after the cursor value
    if let Some(ref cursor) = params.cursor {
        let (sort_col, sort_desc) = primary_sort_for_cursor(params.sort.as_deref());
        let cursor_val = parse_scalar_value(cursor);
        if sort_desc {
            cmd = cmd.lt(&sort_col, cursor_val);
        } else {
            cmd = cmd.gt(&sort_col, cursor_val);
        }
    }

    // Full-text search
    if let Some(ref term) = params.search {
        let cols = params.search_columns.as_deref().unwrap_or("name");
        // SECURITY: Validate search column identifier.
        if crate::rest::filters::is_safe_identifier(cols) {
            cmd = cmd.filter(cols, Operator::TextSearch, QailValue::String(term.clone()));
        } else {
            tracing::warn!(cols = %cols, "search_columns rejected by identifier guard");
        }
    }

    // Pagination
    cmd = cmd.limit(limit);
    cmd = cmd.offset(offset);

    // Apply RLS policies
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;

    // When JOINs are present, table-qualify unqualified filter columns
    // to avoid ambiguous column errors (e.g., RLS `tenant_id` → `base_table.tenant_id`)
    if has_joins {
        for cage in &mut cmd.cages {
            for cond in &mut cage.conditions {
                if let Expr::Named(ref name) = cond.left
                    && !name.contains('.')
                {
                    cond.left = Expr::Named(format!("{}.{}", table_name, name));
                }
            }
        }
    }

    state.optimize_qail_for_execution(&mut cmd);

    // Build cache key from full URI + user identity
    let is_streaming = params.stream.unwrap_or(false);
    let has_branch = headers.get("x-branch-id").is_some();
    let has_nested = params
        .expand
        .as_deref()
        .is_some_and(|e| e.contains("nested:"));
    let can_cache = !is_streaming && !has_branch && !has_nested;
    // SECURITY (E1): Include tenant_id to prevent cross-tenant cache poisoning.
    let tenant = auth.tenant_id.as_deref().unwrap_or("_anon");
    let cache_key = format!(
        "rest:{}:{}:{}:{}",
        tenant,
        table_name,
        auth.user_id,
        request.uri()
    );

    // Check cache for simple read queries
    if can_cache && let Some(cached) = state.cache.get(&cache_key) {
        let mut response = Response::new(Body::from(cached));
        *response.status_mut() = StatusCode::OK;
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        response
            .headers_mut()
            .insert("x-cache", HeaderValue::from_static("HIT"));
        return Ok(response);
    }

    // ── Per-tenant concurrency guard ────────────────────────────────────
    let tenant_id = auth
        .tenant_id
        .clone()
        .unwrap_or_else(|| "_anon".to_string());
    let _concurrency_permit = state
        .tenant_semaphore
        .try_acquire(&tenant_id)
        .await
        .ok_or_else(|| {
            tracing::warn!(
                tenant = %tenant_id,
                table = %table_name,
                "Tenant concurrency limit reached"
            );
            ApiError::rate_limited()
        })?;

    // Execute
    let mut conn = state
        .acquire_with_auth_rls_guarded(&auth, Some(&table_name))
        .await?;

    // ── EXPLAIN Pre-check ──────────────────────────────────────────────
    // Run EXPLAIN (FORMAT JSON) for queries with expand depth ≥ threshold
    // to reject outrageously expensive queries before they consume resources.
    {
        use qail_pg::explain::{ExplainMode, check_estimate};
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let expand_depth = params
            .expand
            .as_deref()
            .map(|e| e.split(',').filter(|s| !s.trim().is_empty()).count())
            .unwrap_or(0);

        let should_explain = match state.explain_config.mode {
            ExplainMode::Off => false,
            ExplainMode::Enforce => true,
            ExplainMode::Precheck => expand_depth >= state.explain_config.depth_threshold,
        };

        if should_explain {
            // Hash the SQL shape for cache lookup
            let sql_shape = cmd.to_sql();
            let mut hasher = DefaultHasher::new();
            sql_shape.hash(&mut hasher);
            let shape_hash = hasher.finish();

            let estimate = if let Some(cached) = state.explain_cache.get(shape_hash, None) {
                cached
            } else {
                // Run EXPLAIN on the live connection
                match conn.explain_estimate(&cmd).await {
                    Ok(Some(est)) => {
                        state.explain_cache.insert(shape_hash, est.clone());
                        est
                    }
                    Ok(None) => {
                        // SECURITY (E8): In Enforce mode, fail closed.
                        if matches!(state.explain_config.mode, ExplainMode::Enforce) {
                            tracing::warn!(
                                table = %table_name,
                                sql = %sql_shape,
                                "EXPLAIN pre-check: parse failure in Enforce mode — rejecting query"
                            );
                            conn.release().await;
                            return Err(ApiError::internal(
                                "EXPLAIN pre-check failed (enforce mode)",
                            ));
                        }
                        tracing::warn!(
                            table = %table_name,
                            sql = %sql_shape,
                            "EXPLAIN pre-check: failed to parse EXPLAIN output, allowing query"
                        );
                        qail_pg::explain::ExplainEstimate {
                            total_cost: 0.0,
                            plan_rows: 0,
                        }
                    }
                    Err(e) => {
                        // SECURITY (E8): In Enforce mode, fail closed.
                        if matches!(state.explain_config.mode, ExplainMode::Enforce) {
                            tracing::warn!(
                                table = %table_name,
                                error = %e,
                                "EXPLAIN pre-check: EXPLAIN failed in Enforce mode — rejecting query"
                            );
                            conn.release().await;
                            return Err(ApiError::internal(
                                "EXPLAIN pre-check failed (enforce mode)",
                            ));
                        }
                        tracing::warn!(
                            table = %table_name,
                            error = %e,
                            "EXPLAIN pre-check: EXPLAIN query failed, allowing query"
                        );
                        qail_pg::explain::ExplainEstimate {
                            total_cost: 0.0,
                            plan_rows: 0,
                        }
                    }
                }
            };
            // P1-E: Log cost estimates for observability
            tracing::info!(
                table = %table_name,
                explain_cost = estimate.total_cost,
                explain_rows = estimate.plan_rows,
                expand_depth,
                "EXPLAIN estimate"
            );

            let decision = check_estimate(&estimate, &state.explain_config);
            if decision.is_rejected() {
                let msg = decision.rejection_message().unwrap_or_default();
                let Some(detail) = decision.rejection_detail() else {
                    tracing::error!(
                        table = %table_name,
                        "EXPLAIN pre-check rejected query without rejection detail"
                    );
                    conn.release().await;
                    return Err(ApiError::internal("EXPLAIN pre-check rejected query"));
                };
                tracing::warn!(
                    table = %table_name,
                    cost = estimate.total_cost,
                    rows = estimate.plan_rows,
                    expand_depth,
                    "EXPLAIN pre-check REJECTED query"
                );
                conn.release().await;
                return Err(ApiError::too_expensive(msg, detail));
            }
        }
    }

    let timer = crate::metrics::QueryTimer::new(&table_name, "select");
    let rows = conn
        .fetch_all_uncached(&cmd)
        .await
        .map_err(|e| ApiError::from_pg_driver_error(&e, Some(&table_name)));
    timer.finish(rows.is_ok());

    // Release connection early — after this point only JSON processing remains.
    // Branch overlay still needs conn, so we do it before release.
    let mut data: Vec<Value> = match &rows {
        Ok(rows) => rows.iter().map(row_to_json).collect(),
        Err(_) => Vec::new(),
    };

    // Branch overlay merge (CoW Read) — admin-gated
    let branch_ctx = extract_branch_from_headers(&headers);
    if let Some(branch_name) = branch_ctx.branch_name() {
        if !auth.can_use_branching() {
            conn.release().await;
            return Err(ApiError::forbidden(
                "Platform administrator role required for branch overlay reads",
            ));
        }
        let pk_col = _table.primary_key.as_deref().unwrap_or("id");
        apply_branch_overlay(&mut conn, branch_name, &table_name, &mut data, pk_col).await;
    }

    // Deterministic cleanup — connection is no longer needed
    conn.release().await;

    // Now propagate the error if query failed
    let _rows = rows?;

    // ── Tenant Boundary Invariant ────────────────────────────────────
    // Skip guard for tables that are cross-tenant by design (e.g.,
    // resellers need to see other tenants' pricing via active contracts).
    let is_exempt = state
        .config
        .tenant_guard_exempt_tables
        .iter()
        .any(|t| t == &table_name);
    if !is_exempt && let Some(ref tenant_id) = auth.tenant_id {
        let _proof = crate::tenant_guard::verify_tenant_boundary(
            &data,
            tenant_id,
            &state.config.tenant_column,
            &table_name,
            "rest_list",
        )
        .map_err(|v| {
            tracing::error!("{}", v);
            ApiError::internal("Data integrity error")
        })?;
    }

    let count = data.len();

    // Nested FK expansion: `?expand=nested:users,nested:items`
    // Runs sub-queries for each relation and stitches into nested JSON
    if let Some(ref expand) = params.expand {
        let nested_rels: Vec<&str> = expand
            .split(',')
            .map(|s| s.trim())
            .filter(|s| s.starts_with("nested:"))
            .map(|s| &s[7..])
            .collect();

        if !nested_rels.is_empty() && !data.is_empty() {
            expand_nested(&state, &table_name, &mut data, &nested_rels, &auth).await?;
        }
    }

    // NDJSON streaming: one JSON object per line
    if is_streaming {
        let mut body = String::new();
        for row in &data {
            body.push_str(&serde_json::to_string(row).unwrap_or_default());
            body.push('\n');
        }
        let mut response = Response::new(Body::from(body));
        *response.status_mut() = StatusCode::OK;
        response.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-ndjson"),
        );
        return Ok(response);
    }

    let response_body = ListResponse {
        data,
        count,
        total: None,
        limit,
        offset,
    };

    let debug = is_debug_request(&headers);
    let debug_sql_str = if debug { Some(debug_sql(&cmd)) } else { None };

    // Store in cache for simple queries
    if can_cache && let Ok(json) = serde_json::to_string(&response_body) {
        state.cache.set(&cache_key, &table_name, json);
    }

    let mut response = Json(response_body).into_response();

    // Attach debug headers if X-Qail-Debug was requested
    if let Some(sql) = debug_sql_str {
        let hdrs = response.headers_mut();
        if let Ok(val) = axum::http::HeaderValue::from_str(&sql) {
            hdrs.insert("x-qail-sql", val);
        }
        if let Ok(val) = axum::http::HeaderValue::from_str(&table_name) {
            hdrs.insert("x-qail-table", val);
        }
    }

    Ok(response)
}
