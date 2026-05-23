use super::*;

fn rest_list_cache_key(
    tenant: &str,
    table_name: &str,
    auth: &crate::auth::AuthContext,
    uri: &axum::http::Uri,
    cmd: &qail_core::ast::Qail,
) -> String {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    tenant.hash(&mut hasher);
    table_name.hash(&mut hasher);
    auth.user_id.hash(&mut hasher);
    auth.role.hash(&mut hasher);
    uri.to_string().hash(&mut hasher);
    qail_core::wire::encode_cmd_text(cmd).hash(&mut hasher);

    let mut claims: Vec<_> = auth.claims.iter().collect();
    claims.sort_by_key(|(left, _)| *left);
    for (key, value) in claims {
        key.hash(&mut hasher);
        serde_json::to_string(value)
            .unwrap_or_default()
            .hash(&mut hasher);
    }

    format!("rest:{}:{}:{:016x}", tenant, table_name, hasher.finish())
}

fn projection_json_column_name(expr: &Expr) -> Result<Option<String>, ApiError> {
    match expr {
        Expr::Star => Ok(None),
        Expr::Named(name) => {
            let name = name.trim();
            if name == "*" || name.ends_with(".*") {
                return Ok(None);
            }
            if !crate::rest::filters::is_safe_identifier(name) {
                return Err(ApiError::forbidden(format!(
                    "Branch overlay projection cannot be safely enforced for projection '{}'",
                    name
                )));
            }
            Ok(name
                .rsplit('.')
                .next()
                .filter(|column| !column.is_empty())
                .map(str::to_string))
        }
        other => Err(ApiError::forbidden(format!(
            "Branch overlay projection cannot be safely enforced for expression {:?}",
            other
        ))),
    }
}

pub(super) fn branch_projection_columns_from_cmd(
    cmd: &qail_core::ast::Qail,
) -> Result<Option<Vec<String>>, ApiError> {
    if cmd.columns.is_empty() {
        return Ok(None);
    }

    let mut selected = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for expr in &cmd.columns {
        let Some(column) = projection_json_column_name(expr)? else {
            return Ok(None);
        };
        if seen.insert(column.clone()) {
            selected.push(column);
        }
    }

    if selected.is_empty() {
        return Ok(None);
    }

    Ok(Some(selected))
}

pub(super) fn branch_policy_filter_cages_from(
    cmd: &qail_core::ast::Qail,
    start: usize,
) -> Vec<qail_core::ast::Cage> {
    cmd.cages
        .iter()
        .skip(start)
        .filter(|cage| matches!(cage.kind, qail_core::ast::CageKind::Filter))
        .cloned()
        .collect()
}

fn policy_filter_json_column_name(
    condition: &qail_core::ast::Condition,
) -> Result<String, ApiError> {
    let column = policy_filter_column_name(condition)?;
    let column = column
        .rsplit('.')
        .next()
        .filter(|column| !column.is_empty())
        .ok_or_else(|| {
            ApiError::forbidden(
                "Branch overlay policy filter cannot be safely enforced for an empty column",
            )
        })?;
    if !crate::rest::filters::is_safe_identifier(column) {
        return Err(ApiError::forbidden(format!(
            "Branch overlay policy filter cannot be safely projected for column '{}'",
            column
        )));
    }
    Ok(column.to_string())
}

pub(super) fn ensure_branch_policy_filter_columns_projected(
    cmd: &mut qail_core::ast::Qail,
    cages: &[qail_core::ast::Cage],
) -> Result<(), ApiError> {
    if cmd.columns.is_empty() {
        return Ok(());
    }

    let mut existing = std::collections::HashSet::new();
    for expr in &cmd.columns {
        let Some(column) = projection_json_column_name(expr)? else {
            return Ok(());
        };
        existing.insert(column);
    }

    let mut to_append = Vec::new();
    for cage in cages {
        for condition in &cage.conditions {
            let column = policy_filter_json_column_name(condition)?;
            if existing.insert(column.clone()) {
                to_append.push(Expr::Named(column));
            }
        }
    }
    cmd.columns.extend(to_append);
    Ok(())
}

pub(crate) async fn list_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Query(params): Query<ListParams>,
    request: axum::extract::Request,
) -> Result<Response, ApiError> {
    let table_name =
        extract_table_name(request.uri()).ok_or_else(|| ApiError::not_found("table"))?;
    check_table_not_blocked(&state, &table_name)?;

    let table = state
        .schema
        .table(&table_name)
        .ok_or_else(|| ApiError::not_found(&table_name))?;

    let auth = authenticate_request(state.as_ref(), &headers).await?;
    let tenant_scope =
        crate::rest::tenant_scope_filter_for_table(state.as_ref(), &auth, &table_name);
    let tenant_scope_column = tenant_scope.as_ref().map(|(col, _)| col.as_str());
    let branch_ctx = extract_branch_from_headers(&headers)?;
    if branch_ctx.branch_name().is_some() && !auth.can_use_branching() {
        return Err(ApiError::forbidden(
            "Platform administrator role required for branch overlay reads",
        ));
    }
    let has_branch = branch_ctx.branch_name().is_some();

    // Build Qail AST
    let max_rows = state.config.max_result_rows.min(1000) as i64;
    let limit = params.limit.unwrap_or(50).clamp(1, max_rows);
    let offset = params.offset.unwrap_or(0).clamp(0, 100_000);

    let mut cmd = qail_core::ast::Qail::get(&table_name);
    let mut strip_tenant_scope_column = false;

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
        if let Some(scope_column) = tenant_scope_column
            && !cols.contains(&"*")
            && !cols.contains(&scope_column)
        {
            cols.push(scope_column);
            strip_tenant_scope_column = true;
        }

        if !cols.is_empty() {
            cmd = cmd.columns(cols);
        }
    }

    let default_sort_column = table.primary_key.as_deref().unwrap_or("id");

    // Sorting (multi-column) — default to the schema primary key for deterministic pagination
    if let Some(ref sort) = params.sort {
        cmd = apply_sorting(cmd, sort);
    } else if crate::rest::filters::is_safe_identifier(default_sort_column) {
        cmd = cmd.order_asc(default_sort_column);
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
    let mut cache_tables = vec![table_name.clone()];
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
                if let Some((scope_column, tenant_id)) =
                    crate::rest::tenant_scope_filter_for_table(state.as_ref(), &auth, rel)
                    && let Some(join) = cmd.joins.last_mut()
                {
                    join.on
                        .get_or_insert_with(Vec::new)
                        .push(qail_core::ast::Condition {
                            left: Expr::Named(format!("{}.{}", rel, scope_column)),
                            op: Operator::Eq,
                            value: QailValue::String(tenant_id),
                            is_array_unnest: false,
                        });
                }
                has_joins = true;
                cache_tables.push(rel.to_string());
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
    if let Some((scope_column, tenant_id)) = tenant_scope.as_ref() {
        cmd = cmd.filter(
            scope_column,
            Operator::Eq,
            QailValue::String(tenant_id.clone()),
        );
    }

    // Cursor-based pagination: filter rows after the cursor value
    if let Some(ref cursor) = params.cursor {
        let (sort_col, sort_desc) =
            primary_sort_for_cursor(params.sort.as_deref(), default_sort_column);
        let cursor_val = parse_scalar_value(cursor);
        if sort_desc {
            cmd = cmd.lt(&sort_col, cursor_val);
        } else {
            cmd = cmd.gt(&sort_col, cursor_val);
        }
    }

    // Full-text search
    if let Some(ref term) = params.search {
        let cols_input = params.search_columns.as_deref().unwrap_or("name");
        let cols = crate::rest::filters::parse_identifier_csv(cols_input)
            .map_err(ApiError::parse_error)?;
        let search_cols = cols.join(",");
        cmd = cmd.filter(
            &search_cols,
            Operator::TextSearch,
            QailValue::String(term.clone()),
        );
    }

    // Pagination. Branch overlay reads need a broader bounded base set so the
    // overlay can be merged before applying the requested page window.
    if has_branch {
        cmd = cmd.limit(max_rows);
    } else {
        cmd = cmd.limit(limit);
        cmd = cmd.offset(offset);
    }

    // Apply RLS policies
    let branch_policy_filter_start = cmd.cages.len();
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

    if let Some(scope_column) = tenant_scope_column {
        strip_tenant_scope_column |=
            crate::tenant_guard::ensure_tenant_column_projected(&mut cmd, scope_column)
                .map_err(|e| ApiError::bad_request("TENANT_GUARD_PROJECTION", e.to_string()))?;
    }

    let branch_policy_filter_cages = if has_branch {
        branch_policy_filter_cages_from(&cmd, branch_policy_filter_start)
    } else {
        Vec::new()
    };
    let branch_projection = if has_branch {
        branch_projection_columns_from_cmd(&cmd)?
    } else {
        None
    };
    if has_branch {
        ensure_branch_policy_filter_columns_projected(&mut cmd, &branch_policy_filter_cages)?;
    }

    state.optimize_qail_for_execution(&mut cmd);

    // Build cache key from full URI + user identity
    let is_streaming = params.stream.unwrap_or(false);
    let has_nested = params
        .expand
        .as_deref()
        .is_some_and(|e| e.contains("nested:"));
    let can_cache = !is_streaming && !has_branch && !has_nested;
    // SECURITY (E1): Include tenant_id to prevent cross-tenant cache poisoning.
    let tenant = auth.tenant_id.as_deref().unwrap_or("_anon");
    let cache_key = rest_list_cache_key(tenant, &table_name, &auth, request.uri(), &cmd);

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

    // Branch overlay merge (CoW Read)
    if let Some(branch_name) = branch_ctx.branch_name() {
        let pk_col = table.primary_key.as_deref().unwrap_or("id");
        apply_branch_overlay(&mut conn, branch_name, &table_name, &mut data, pk_col).await;
        if let Err(e) = apply_branch_read_constraints(
            &mut data,
            BranchReadConstraintInput {
                filters: &filters,
                policy_filter_cages: &branch_policy_filter_cages,
                search: params.search.as_deref(),
                search_columns: params.search_columns.as_deref(),
                cursor: params.cursor.as_deref(),
                sort: params.sort.as_deref(),
                default_sort_column,
                offset,
                limit,
            },
        ) {
            conn.release().await;
            return Err(e);
        }
        if let Some(selected_columns) = branch_projection.as_ref() {
            project_rows_to_selected_columns(&mut data, selected_columns);
        }
    }

    // Deterministic cleanup — connection is no longer needed
    conn.release().await;

    // Now propagate the error if query failed
    let _rows = rows?;

    // ── Tenant Boundary Invariant ────────────────────────────────────
    // Skip guard for tables that are cross-tenant by design (e.g.,
    // resellers need to see other tenants' pricing via active contracts).
    if let Some((scope_column, tenant_id)) = tenant_scope.as_ref() {
        let _proof = crate::tenant_guard::verify_tenant_boundary(
            &data,
            tenant_id,
            scope_column,
            &table_name,
            "rest_list",
        )
        .map_err(|v| {
            tracing::error!("{}", v);
            ApiError::internal("Data integrity error")
        })?;
    }
    if strip_tenant_scope_column && let Some(scope_column) = tenant_scope_column {
        crate::tenant_guard::strip_tenant_column_from_json_rows(&mut data, scope_column);
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
        let cache_table_refs: Vec<&str> = cache_tables.iter().map(String::as_str).collect();
        state
            .cache
            .set_for_tables(&cache_key, &cache_table_refs, json);
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

struct BranchReadConstraintInput<'a> {
    filters: &'a [(String, Operator, QailValue)],
    policy_filter_cages: &'a [qail_core::ast::Cage],
    search: Option<&'a str>,
    search_columns: Option<&'a str>,
    cursor: Option<&'a str>,
    sort: Option<&'a str>,
    default_sort_column: &'a str,
    offset: i64,
    limit: i64,
}

fn apply_branch_read_constraints(
    data: &mut Vec<Value>,
    input: BranchReadConstraintInput<'_>,
) -> Result<(), ApiError> {
    let mut filtered = Vec::with_capacity(data.len());
    for row in data.drain(..) {
        if row_matches_filters(&row, input.filters)
            && row_matches_policy_filter_cages(&row, input.policy_filter_cages)?
            && row_matches_search(&row, input.search, input.search_columns)
        {
            filtered.push(row);
        }
    }
    *data = filtered;

    let (sort_col, desc) = primary_sort_for_cursor(input.sort, input.default_sort_column);
    if let Some(cursor) = input.cursor {
        let cursor_val = parse_scalar_value(cursor);
        let cursor_op = if desc { Operator::Lt } else { Operator::Gt };
        data.retain(|row| row_matches_filter(row, &sort_col, cursor_op, &cursor_val));
    }

    data.sort_by(|left, right| compare_json_field(left, right, &sort_col, desc));

    let start = input.offset.max(0) as usize;
    let take = input.limit.max(0) as usize;
    if start >= data.len() {
        data.clear();
        return Ok(());
    }
    let end = start.saturating_add(take).min(data.len());
    if start > 0 {
        data.drain(0..start);
    }
    data.truncate(end - start);
    Ok(())
}

fn policy_filter_column_name(condition: &qail_core::ast::Condition) -> Result<&str, ApiError> {
    match &condition.left {
        Expr::Named(name) => Ok(name.as_str()),
        other => Err(ApiError::forbidden(format!(
            "Branch overlay policy filter cannot be safely enforced for expression {:?}",
            other
        ))),
    }
}

fn row_matches_policy_condition(
    row: &Value,
    condition: &qail_core::ast::Condition,
) -> Result<bool, ApiError> {
    if condition.op != Operator::Eq {
        return Err(ApiError::forbidden(
            "Branch overlay policy filters support only equality conditions",
        ));
    }

    let column = policy_filter_column_name(condition)?;
    Ok(row_matches_filter(
        row,
        column,
        condition.op,
        &condition.value,
    ))
}

pub(super) fn row_matches_policy_filter_cages(
    row: &Value,
    cages: &[qail_core::ast::Cage],
) -> Result<bool, ApiError> {
    let mut has_or_condition = false;
    let mut any_or_condition_matches = false;

    for cage in cages
        .iter()
        .filter(|cage| matches!(cage.kind, qail_core::ast::CageKind::Filter))
    {
        match cage.logical_op {
            qail_core::ast::LogicalOp::And => {
                for condition in &cage.conditions {
                    if !row_matches_policy_condition(row, condition)? {
                        return Ok(false);
                    }
                }
            }
            qail_core::ast::LogicalOp::Or => {
                for condition in &cage.conditions {
                    has_or_condition = true;
                    if row_matches_policy_condition(row, condition)? {
                        any_or_condition_matches = true;
                    }
                }
            }
        }
    }

    Ok(!has_or_condition || any_or_condition_matches)
}

fn row_field<'a>(row: &'a Value, column: &str) -> Option<&'a Value> {
    let object = row.as_object()?;
    object
        .get(column)
        .or_else(|| column.rsplit('.').next().and_then(|last| object.get(last)))
}

fn qail_value_matches_json(value: &QailValue, json: &Value) -> bool {
    match value {
        QailValue::Null | QailValue::NullUuid => json.is_null(),
        QailValue::Bool(expected) => json.as_bool() == Some(*expected),
        QailValue::Int(expected) => json.as_i64() == Some(*expected),
        QailValue::Float(expected) => json
            .as_f64()
            .is_some_and(|actual| (actual - expected).abs() < f64::EPSILON),
        QailValue::String(expected)
        | QailValue::Json(expected)
        | QailValue::Timestamp(expected) => json.as_str() == Some(expected.as_str()),
        QailValue::Uuid(expected) => json.as_str() == Some(&expected.to_string()),
        _ => false,
    }
}

fn json_as_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|s| s.parse::<f64>().ok()))
}

fn compare_json_to_qail(value: &Value, expected: &QailValue) -> Option<std::cmp::Ordering> {
    match expected {
        QailValue::Int(n) => json_as_f64(value)?.partial_cmp(&(*n as f64)),
        QailValue::Float(n) => json_as_f64(value)?.partial_cmp(n),
        QailValue::String(s) | QailValue::Json(s) | QailValue::Timestamp(s) => {
            value.as_str()?.partial_cmp(s.as_str())
        }
        QailValue::Uuid(u) => value.as_str()?.partial_cmp(u.to_string().as_str()),
        _ => None,
    }
}

fn string_like(value: &Value, pattern: &QailValue, case_insensitive: bool) -> bool {
    let Some(actual) = value.as_str() else {
        return false;
    };
    let expected = match pattern {
        QailValue::String(s) => s.as_str(),
        _ => return false,
    };
    let expected = expected.trim_matches('%');
    if case_insensitive {
        actual
            .to_ascii_lowercase()
            .contains(&expected.to_ascii_lowercase())
    } else {
        actual.contains(expected)
    }
}

fn row_matches_filter(row: &Value, column: &str, op: Operator, expected: &QailValue) -> bool {
    let value = row_field(row, column);
    match op {
        Operator::Eq => value.is_some_and(|value| qail_value_matches_json(expected, value)),
        Operator::Ne => value.is_none_or(|value| !qail_value_matches_json(expected, value)),
        Operator::IsNull => value.is_none_or(Value::is_null),
        Operator::IsNotNull => value.is_some_and(|value| !value.is_null()),
        Operator::In => match expected {
            QailValue::Array(items) => value.is_some_and(|value| {
                items
                    .iter()
                    .any(|item| qail_value_matches_json(item, value))
            }),
            _ => false,
        },
        Operator::NotIn => match expected {
            QailValue::Array(items) => value.is_none_or(|value| {
                !items
                    .iter()
                    .any(|item| qail_value_matches_json(item, value))
            }),
            _ => false,
        },
        Operator::Gt | Operator::Gte | Operator::Lt | Operator::Lte => value
            .and_then(|value| compare_json_to_qail(value, expected))
            .is_some_and(|ordering| match op {
                Operator::Gt => ordering.is_gt(),
                Operator::Gte => ordering.is_gt() || ordering.is_eq(),
                Operator::Lt => ordering.is_lt(),
                Operator::Lte => ordering.is_lt() || ordering.is_eq(),
                _ => false,
            }),
        Operator::Like | Operator::Contains => {
            value.is_some_and(|value| string_like(value, expected, false))
        }
        Operator::ILike | Operator::Fuzzy => {
            value.is_some_and(|value| string_like(value, expected, true))
        }
        Operator::NotLike => value.is_none_or(|value| !string_like(value, expected, false)),
        Operator::NotILike => value.is_none_or(|value| !string_like(value, expected, true)),
        _ => false,
    }
}

fn row_matches_filters(row: &Value, filters: &[(String, Operator, QailValue)]) -> bool {
    filters
        .iter()
        .all(|(column, op, value)| row_matches_filter(row, column, *op, value))
}

fn row_matches_search(row: &Value, search: Option<&str>, search_columns: Option<&str>) -> bool {
    let Some(term) = search else {
        return true;
    };
    let needle = term.to_ascii_lowercase();
    let columns = search_columns.unwrap_or("name");
    columns
        .split(',')
        .map(str::trim)
        .filter(|column| !column.is_empty())
        .any(|column| {
            row_field(row, column)
                .and_then(Value::as_str)
                .is_some_and(|value| value.to_ascii_lowercase().contains(&needle))
        })
}

fn compare_json_field(left: &Value, right: &Value, column: &str, desc: bool) -> std::cmp::Ordering {
    let ordering = match (row_field(left, column), row_field(right, column)) {
        (Some(Value::Number(left)), Some(Value::Number(right))) => left
            .as_f64()
            .and_then(|left| right.as_f64().and_then(|right| left.partial_cmp(&right)))
            .unwrap_or(std::cmp::Ordering::Equal),
        (Some(Value::String(left)), Some(Value::String(right))) => left.cmp(right),
        (Some(Value::Bool(left)), Some(Value::Bool(right))) => left.cmp(right),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        _ => std::cmp::Ordering::Equal,
    };
    if desc { ordering.reverse() } else { ordering }
}

#[cfg(test)]
mod tests {
    use super::{
        BranchReadConstraintInput, apply_branch_read_constraints,
        branch_projection_columns_from_cmd, project_rows_to_selected_columns,
        row_matches_policy_filter_cages,
    };
    use crate::auth::AuthContext;
    use crate::policy::{OperationType, PolicyDef, PolicyEngine};
    use qail_core::ast::{
        Cage, CageKind, Condition, Expr, LogicalOp, Operator, Value as QailValue,
    };
    use serde_json::json;

    fn policy_cage(column: &str, op: Operator, value: QailValue) -> Cage {
        Cage {
            kind: CageKind::Filter,
            conditions: vec![Condition {
                left: Expr::Named(column.to_string()),
                op,
                value,
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::Or,
        }
    }

    #[test]
    fn branch_read_constraints_apply_policy_equality_to_overlay_rows() {
        let mut rows = vec![
            json!({"id": 1, "region": "west"}),
            json!({"id": 2, "region": "east"}),
        ];
        let cages = vec![policy_cage(
            "region",
            Operator::Eq,
            QailValue::String("west".to_string()),
        )];

        apply_branch_read_constraints(
            &mut rows,
            BranchReadConstraintInput {
                filters: &[],
                policy_filter_cages: &cages,
                search: None,
                search_columns: None,
                cursor: None,
                sort: None,
                default_sort_column: "id",
                offset: 0,
                limit: 50,
            },
        )
        .unwrap();

        assert_eq!(rows, vec![json!({"id": 1, "region": "west"})]);
    }

    #[test]
    fn branch_policy_filter_fails_closed_on_unsupported_operator() {
        let row = json!({"id": 1, "region": "west"});
        let cages = vec![policy_cage("id", Operator::Gt, QailValue::Int(0))];

        let err = row_matches_policy_filter_cages(&row, &cages).unwrap_err();

        assert_eq!(err.status_code(), axum::http::StatusCode::FORBIDDEN);
    }

    #[test]
    fn branch_projection_uses_post_policy_columns() {
        let mut engine = PolicyEngine::new();
        engine.add_policy(PolicyDef {
            name: "hide_secret".to_string(),
            table: "orders".to_string(),
            filter: None,
            role: None,
            operations: vec![OperationType::Read],
            allowed_columns: vec![],
            denied_columns: vec!["secret".to_string()],
        });
        let auth = AuthContext {
            user_id: "user-1".to_string(),
            role: "user".to_string(),
            tenant_id: Some("tenant-a".to_string()),
            claims: std::collections::HashMap::new(),
        };
        let mut cmd =
            qail_core::ast::Qail::get("orders").columns(["id", "total", "secret", "tenant_id"]);

        engine.apply_policies(&auth, &mut cmd).unwrap();
        let selected = branch_projection_columns_from_cmd(&cmd).unwrap().unwrap();

        assert_eq!(
            selected,
            vec![
                "id".to_string(),
                "total".to_string(),
                "tenant_id".to_string()
            ]
        );
    }

    #[test]
    fn branch_projection_projected_rows_do_not_leak_denied_columns() {
        let mut rows = vec![json!({
            "id": 1,
            "total": 42,
            "secret": "hidden",
            "tenant_id": "tenant-a"
        })];
        let cmd = qail_core::ast::Qail::get("orders").columns(["id", "total", "tenant_id"]);
        let selected = branch_projection_columns_from_cmd(&cmd).unwrap().unwrap();

        project_rows_to_selected_columns(&mut rows, &selected);

        assert_eq!(
            rows,
            vec![json!({"id": 1, "total": 42, "tenant_id": "tenant-a"})]
        );
    }

    #[test]
    fn branch_policy_filter_columns_are_projected_after_public_projection_snapshot() {
        let cages = vec![policy_cage(
            "region",
            Operator::Eq,
            QailValue::String("west".to_string()),
        )];
        let mut cmd = qail_core::ast::Qail::get("orders").columns(["id", "total"]);
        let public_projection = branch_projection_columns_from_cmd(&cmd).unwrap().unwrap();

        super::ensure_branch_policy_filter_columns_projected(&mut cmd, &cages).unwrap();

        assert_eq!(
            public_projection,
            vec!["id".to_string(), "total".to_string()]
        );
        assert_eq!(
            cmd.columns,
            vec![
                Expr::Named("id".to_string()),
                Expr::Named("total".to_string()),
                Expr::Named("region".to_string())
            ]
        );
    }
}
