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
        crate::auth::canonical_json_value(value).hash(&mut hasher);
    }

    format!("rest:{}:{}:{:016x}", tenant, table_name, hasher.finish())
}

fn encode_ndjson_rows(data: &[Value]) -> Result<String, ApiError> {
    let mut body = String::new();
    for row in data {
        let line = serde_json::to_string(row)
            .map_err(|e| ApiError::internal(format!("NDJSON row serialization failed: {}", e)))?;
        body.push_str(&line);
        body.push('\n');
    }
    Ok(body)
}

fn attach_rest_list_debug_headers(
    response: &mut Response,
    debug: bool,
    table_name: &str,
    cmd: &qail_core::ast::Qail,
) {
    if !debug {
        return;
    }

    let hdrs = response.headers_mut();
    if let Ok(val) = HeaderValue::from_str(&debug_sql(cmd)) {
        hdrs.insert("x-qail-sql", val);
    }
    if let Ok(val) = HeaderValue::from_str(table_name) {
        hdrs.insert("x-qail-table", val);
    }
}

fn rest_explain_cache_shape_hash(sql_shape: &str, auth: &crate::auth::AuthContext) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    sql_shape.hash(&mut hasher);
    auth.transaction_scope_fingerprint().hash(&mut hasher);
    hasher.finish()
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

struct BranchReplayProjectionInput<'a> {
    filters: &'a [(String, Operator, QailValue)],
    policy_filter_cages: &'a [qail_core::ast::Cage],
    search: Option<&'a str>,
    search_columns: Option<&'a str>,
    sort: Option<&'a str>,
    default_sort_column: &'a str,
    distinct_columns: &'a [String],
    pk_column: &'a str,
    table_name: &'a str,
    has_joins: bool,
}

fn branch_replay_base_column_name(column: &str, table_name: &str) -> Result<String, ApiError> {
    if !crate::rest::filters::is_safe_identifier(column) {
        return Err(ApiError::forbidden(format!(
            "Branch overlay replay cannot safely project column '{}'",
            column
        )));
    }

    let Some((qualifier, column_name)) = column.rsplit_once('.') else {
        return Ok(column.to_string());
    };

    if qualifier != table_name {
        return Err(ApiError::forbidden(format!(
            "Branch overlay replay cannot safely enforce related-table column '{}'",
            column
        )));
    }

    if column_name.is_empty() {
        return Err(ApiError::forbidden(
            "Branch overlay replay cannot safely project an empty column",
        ));
    }
    Ok(column_name.to_string())
}

fn branch_replay_projection_expr(column: &str, input: &BranchReplayProjectionInput<'_>) -> String {
    if input.has_joins && !column.contains('.') {
        format!("{}.{}", input.table_name, column)
    } else {
        column.to_string()
    }
}

fn ensure_branch_replay_columns_projected(
    cmd: &mut qail_core::ast::Qail,
    input: BranchReplayProjectionInput<'_>,
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

    let mut required = Vec::new();
    let mut seen_required = std::collections::HashSet::new();
    let mut push_required = |column: &str| -> Result<(), ApiError> {
        let base_column = branch_replay_base_column_name(column, input.table_name)?;
        if seen_required.insert(base_column.clone()) {
            required.push((base_column, column.to_string()));
        }
        Ok(())
    };

    push_required(input.pk_column)?;

    for (column, _, _) in input.filters {
        push_required(column)?;
    }

    for cage in input.policy_filter_cages {
        for condition in &cage.conditions {
            push_required(policy_filter_column_name(condition)?)?;
        }
    }

    if input.search.is_some() {
        let cols_input = input.search_columns.unwrap_or("name");
        for column in parse_identifier_csv(cols_input).map_err(ApiError::parse_error)? {
            push_required(&column)?;
        }
    }

    for sort_key in branch_sort_keys(input.sort, input.default_sort_column)? {
        push_required(&sort_key.column)?;
    }

    for column in input.distinct_columns {
        push_required(column)?;
    }

    let mut to_append = Vec::new();
    for (base_column, source_column) in required {
        if existing.insert(base_column.clone()) {
            to_append.push(Expr::Named(branch_replay_projection_expr(
                &source_column,
                &input,
            )));
        }
    }

    cmd.columns.extend(to_append);
    Ok(())
}

fn apply_list_distinct(
    mut cmd: qail_core::ast::Qail,
    distinct: Option<&str>,
    has_branch: bool,
) -> Result<(qail_core::ast::Qail, Vec<String>), String> {
    let Some(distinct) = distinct else {
        return Ok((cmd, Vec::new()));
    };

    let cols = parse_identifier_csv(distinct)?;
    if has_branch {
        return Ok((cmd, cols));
    }

    cmd = cmd.distinct_on(cols);
    Ok((cmd, Vec::new()))
}

fn nested_parent_key_column_for_relation(
    schema: &crate::schema::SchemaRegistry,
    table_name: &str,
    rel: &str,
) -> Result<String, ApiError> {
    if let Some((fk_col, _)) = schema.relation_for(table_name, rel) {
        return Ok(fk_col.to_string());
    }
    if let Some((_, ref_col)) = schema.relation_for(rel, table_name) {
        return Ok(ref_col.to_string());
    }
    Err(ApiError::parse_error(format!(
        "No relation between '{}' and '{}' for nested expansion",
        table_name, rel
    )))
}

fn projection_includes_json_column(
    cmd: &qail_core::ast::Qail,
    column: &str,
) -> Result<bool, ApiError> {
    if cmd.columns.is_empty() {
        return Ok(true);
    }

    for expr in &cmd.columns {
        let Some(projected) = projection_json_column_name(expr)? else {
            return Ok(true);
        };
        if projected == column {
            return Ok(true);
        }
    }
    Ok(false)
}

fn ensure_nested_parent_key_columns_projected(
    cmd: &mut qail_core::ast::Qail,
    schema: &crate::schema::SchemaRegistry,
    table_name: &str,
    nested_rels: &[&str],
) -> Result<Vec<String>, ApiError> {
    let mut strip_after_expand = Vec::new();

    for rel in nested_rels {
        let column = nested_parent_key_column_for_relation(schema, table_name, rel)?;
        if projection_includes_json_column(cmd, &column)? {
            continue;
        }

        let projection = if cmd.joins.is_empty() {
            column.clone()
        } else {
            format!("{}.{}", table_name, column)
        };
        cmd.columns.push(Expr::Named(projection));
        strip_after_expand.push(column);
    }

    Ok(strip_after_expand)
}

fn split_expand_relations(
    expand: &str,
    max_expand_depth: usize,
) -> Result<(Vec<&str>, Vec<&str>), ApiError> {
    parse_expand_relations(expand, max_expand_depth).map_err(ApiError::parse_error)
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
    let mut strip_nested_parent_key_columns = Vec::new();

    // Build Qail AST
    let (limit, offset) = params
        .bounded_limit_offset(state.config.max_result_rows)
        .map_err(ApiError::parse_error)?;

    let mut cmd = qail_core::ast::Qail::get(&table_name);
    let mut strip_tenant_scope_column = false;

    // Column selection
    if let Some(ref select) = params.select {
        let mut cols =
            crate::rest::filters::parse_select_columns(select).map_err(ApiError::parse_error)?;

        // SECURITY: Ensure tenant column is always projected so verify_tenant_boundary()
        // can check row ownership. Without this, a malicious client could bypass the
        // tenant guard by omitting the tenant column from `select`.
        if let Some(scope_column) = tenant_scope_column
            && !cols.iter().any(|col| col == "*")
            && !cols.iter().any(|col| col == scope_column)
        {
            cols.push(scope_column.to_string());
            strip_tenant_scope_column = true;
        }

        if !cols.is_empty() {
            cmd = cmd.columns(cols);
        }
    }

    let default_sort_column = table.primary_key.as_deref().unwrap_or("id");

    // Sorting (multi-column) — default to the schema primary key for deterministic pagination
    if let Some(ref sort) = params.sort {
        cmd = apply_sorting(cmd, sort).map_err(ApiError::parse_error)?;
    } else if crate::rest::filters::is_safe_identifier(default_sort_column) {
        cmd = cmd.order_asc(default_sort_column);
    }

    // Distinct
    let (next_cmd, branch_distinct_columns) =
        apply_list_distinct(cmd, params.distinct.as_deref(), has_branch)
            .map_err(ApiError::parse_error)?;
    cmd = next_cmd;

    // Expand FK relations via LEFT JOIN
    let mut has_joins = false;
    let mut cache_tables = vec![table_name.clone()];
    let mut nested_rels = Vec::new();
    if let Some(ref expand) = params.expand {
        let (relations, parsed_nested_rels) =
            split_expand_relations(expand, state.config.max_expand_depth)?;
        nested_rels = parsed_nested_rels;
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

    if !nested_rels.is_empty() {
        strip_nested_parent_key_columns = ensure_nested_parent_key_columns_projected(
            &mut cmd,
            &state.schema,
            &table_name,
            &nested_rels,
        )?;
    }

    // Parse and apply filters from query string
    let query_string = request.uri().query().unwrap_or("");
    let filters = parse_filters_checked(query_string).map_err(ApiError::parse_error)?;
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
        let cursor_val = parse_cursor_value(cursor).map_err(ApiError::parse_error)?;
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
        cmd = cmd.limit(branch_base_fetch_limit(
            offset,
            limit,
            state.config.max_result_rows,
        )?);
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
        crate::rest::filters::qualify_base_filter_columns_for_join(&mut cmd, &table_name);
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
        let pk_col = table.primary_key.as_deref().unwrap_or("id");
        ensure_branch_replay_columns_projected(
            &mut cmd,
            BranchReplayProjectionInput {
                filters: &filters,
                policy_filter_cages: &branch_policy_filter_cages,
                search: params.search.as_deref(),
                search_columns: params.search_columns.as_deref(),
                sort: params.sort.as_deref(),
                default_sort_column,
                distinct_columns: &branch_distinct_columns,
                pk_column: pk_col,
                table_name: &table_name,
                has_joins,
            },
        )?;
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
    let debug = is_debug_request(&headers);

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
        attach_rest_list_debug_headers(&mut response, debug, &table_name, &cmd);
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
            let shape_hash = rest_explain_cache_shape_hash(&sql_shape, &auth);

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
    let mut branch_base_has_more = false;
    if has_branch {
        let materialization_cap = state.config.max_result_rows.max(1);
        if data.len() > materialization_cap {
            branch_base_has_more = true;
            data.truncate(materialization_cap);
        }
    }

    // Branch overlay merge (CoW Read)
    if let Some(branch_name) = branch_ctx.branch_name() {
        let pk_col = table.primary_key.as_deref().unwrap_or("id");
        if let Err(e) =
            apply_branch_overlay(&mut conn, branch_name, &table_name, &mut data, pk_col).await
        {
            conn.release().await;
            return Err(e);
        }
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
                base_has_more: branch_base_has_more,
                materialization_cap: state.config.max_result_rows,
                distinct_columns: &branch_distinct_columns,
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
    let count = data.len();

    // Nested FK expansion: `?expand=nested:users,nested:items`
    // Runs sub-queries for each relation and stitches into nested JSON
    if !nested_rels.is_empty() && !data.is_empty() {
        expand_nested(&state, &table_name, &mut data, &nested_rels, &auth).await?;
    }
    for column in &strip_nested_parent_key_columns {
        crate::tenant_guard::strip_tenant_column_from_json_rows(&mut data, column);
    }
    if strip_tenant_scope_column && let Some(scope_column) = tenant_scope_column {
        crate::tenant_guard::strip_tenant_column_from_json_rows(&mut data, scope_column);
    }

    // NDJSON streaming: one JSON object per line
    if is_streaming {
        let body = encode_ndjson_rows(&data)?;
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

    // Store in cache for simple queries
    if can_cache && let Ok(json) = serde_json::to_string(&response_body) {
        let cache_table_refs: Vec<&str> = cache_tables.iter().map(String::as_str).collect();
        state
            .cache
            .set_for_tables(&cache_key, &cache_table_refs, json);
    }

    let mut response = Json(response_body).into_response();
    attach_rest_list_debug_headers(&mut response, debug, &table_name, &cmd);

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
    base_has_more: bool,
    materialization_cap: usize,
    distinct_columns: &'a [String],
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BranchSortKey {
    column: String,
    desc: bool,
}

fn branch_sort_default_column(default_sort_column: &str) -> &str {
    if crate::rest::filters::is_safe_identifier(default_sort_column) {
        default_sort_column
    } else {
        "id"
    }
}

fn branch_sort_keys(
    sort: Option<&str>,
    default_sort_column: &str,
) -> Result<Vec<BranchSortKey>, ApiError> {
    let fallback = branch_sort_default_column(default_sort_column);
    let Some(sort) = sort else {
        return Ok(vec![BranchSortKey {
            column: fallback.to_string(),
            desc: false,
        }]);
    };

    let mut keys = Vec::new();
    for part in sort.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return Err(ApiError::parse_error("Sort contains an empty entry"));
        }

        if let Some(column) = part.strip_prefix('-') {
            let column = column.trim();
            if column.is_empty() || !crate::rest::filters::is_safe_identifier(column) {
                return Err(ApiError::parse_error(format!(
                    "Invalid sort column '{}'",
                    column
                )));
            }
            keys.push(BranchSortKey {
                column: column.to_string(),
                desc: true,
            });
            continue;
        }

        if let Some(column) = part.strip_prefix('+') {
            let column = column.trim();
            if column.is_empty() || !crate::rest::filters::is_safe_identifier(column) {
                return Err(ApiError::parse_error(format!(
                    "Invalid sort column '{}'",
                    column
                )));
            }
            keys.push(BranchSortKey {
                column: column.to_string(),
                desc: false,
            });
            continue;
        }

        if let Some((column, direction)) = part.split_once(':') {
            let column = column.trim();
            let direction = direction.trim();
            if column.is_empty() || !crate::rest::filters::is_safe_identifier(column) {
                return Err(ApiError::parse_error(format!(
                    "Invalid sort column '{}'",
                    column
                )));
            }
            let desc = if direction.eq_ignore_ascii_case("desc") {
                true
            } else if direction.eq_ignore_ascii_case("asc") {
                false
            } else {
                return Err(ApiError::parse_error(format!(
                    "Invalid sort direction '{}'",
                    direction
                )));
            };
            keys.push(BranchSortKey {
                column: column.to_string(),
                desc,
            });
            continue;
        }

        if !crate::rest::filters::is_safe_identifier(part) {
            return Err(ApiError::parse_error(format!(
                "Invalid sort column '{}'",
                part
            )));
        }
        keys.push(BranchSortKey {
            column: part.to_string(),
            desc: false,
        });
    }

    Ok(keys)
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

    let sort_keys = branch_sort_keys(input.sort, input.default_sort_column)?;
    let primary_sort = sort_keys.first().ok_or_else(|| {
        ApiError::internal("Branch sort parser returned no sort keys after validation")
    })?;
    if let Some(cursor) = input.cursor {
        let cursor_val = parse_cursor_value(cursor).map_err(ApiError::parse_error)?;
        let cursor_op = if primary_sort.desc {
            Operator::Lt
        } else {
            Operator::Gt
        };
        data.retain(|row| row_matches_filter(row, &primary_sort.column, cursor_op, &cursor_val));
    }

    data.sort_by(|left, right| compare_json_sort_keys(left, right, &sort_keys));
    apply_branch_distinct(data, input.distinct_columns);

    let required_window =
        (input.offset.max(0) as usize).saturating_add(input.limit.max(0) as usize);
    if input.base_has_more && data.len() < required_window {
        return Err(ApiError::bad_request(
            "BRANCH_REPLAY_WINDOW_TOO_LARGE",
            format!(
                "Branch replay cannot materialize the requested page within configured max_result_rows ({}) after overlay operations",
                input.materialization_cap.max(1)
            ),
        ));
    }

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
                let mut any_condition_matches = false;
                for condition in &cage.conditions {
                    if row_matches_policy_condition(row, condition)? {
                        any_condition_matches = true;
                    }
                }
                if !any_condition_matches {
                    return Ok(false);
                }
            }
        }
    }

    Ok(true)
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

fn sql_like_pattern_matches(actual: &str, pattern: &str, case_insensitive: bool) -> bool {
    let actual = if case_insensitive {
        actual.to_ascii_lowercase()
    } else {
        actual.to_string()
    };
    let pattern = if case_insensitive {
        pattern.to_ascii_lowercase()
    } else {
        pattern.to_string()
    };
    let actual: Vec<char> = actual.chars().collect();
    let pattern: Vec<char> = pattern.chars().collect();
    let mut dp = vec![vec![false; actual.len() + 1]; pattern.len() + 1];
    dp[0][0] = true;

    for p_idx in 0..pattern.len() {
        if pattern[p_idx] == '%' {
            dp[p_idx + 1][0] = dp[p_idx][0];
        }
        for a_idx in 0..actual.len() {
            dp[p_idx + 1][a_idx + 1] = match pattern[p_idx] {
                '%' => dp[p_idx][a_idx + 1] || dp[p_idx + 1][a_idx],
                '_' => dp[p_idx][a_idx],
                ch => dp[p_idx][a_idx] && ch == actual[a_idx],
            };
        }
    }

    dp[pattern.len()][actual.len()]
}

fn string_like(value: &Value, pattern: &QailValue, case_insensitive: bool) -> bool {
    let Some(actual) = value.as_str() else {
        return false;
    };
    let expected = match pattern {
        QailValue::String(s) => s.as_str(),
        _ => return false,
    };
    sql_like_pattern_matches(actual, expected, case_insensitive)
}

fn string_fuzzy(value: &Value, pattern: &QailValue) -> bool {
    let expected = match pattern {
        QailValue::String(s) => format!("%{}%", s),
        _ => return false,
    };
    string_like(value, &QailValue::String(expected), true)
}

fn string_contains(value: &Value, pattern: &QailValue) -> bool {
    let Some(actual) = value.as_str() else {
        return false;
    };
    let expected = match pattern {
        QailValue::String(s) => s.as_str(),
        _ => return false,
    };
    actual.contains(expected)
}

fn present_non_null_json(value: Option<&Value>) -> Option<&Value> {
    value.filter(|value| !value.is_null())
}

fn qail_value_is_sql_null(value: &QailValue) -> bool {
    matches!(value, QailValue::Null | QailValue::NullUuid)
}

fn row_matches_filter(row: &Value, column: &str, op: Operator, expected: &QailValue) -> bool {
    let value = row_field(row, column);
    match op {
        Operator::Eq => value.is_some_and(|value| qail_value_matches_json(expected, value)),
        Operator::Ne => {
            !qail_value_is_sql_null(expected)
                && present_non_null_json(value)
                    .is_some_and(|value| !qail_value_matches_json(expected, value))
        }
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
            QailValue::Array(items) => {
                !items.iter().any(qail_value_is_sql_null)
                    && present_non_null_json(value).is_some_and(|value| {
                        !items
                            .iter()
                            .any(|item| qail_value_matches_json(item, value))
                    })
            }
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
        Operator::Like => value.is_some_and(|value| string_like(value, expected, false)),
        Operator::Contains => value.is_some_and(|value| string_contains(value, expected)),
        Operator::ILike => value.is_some_and(|value| string_like(value, expected, true)),
        Operator::Fuzzy => value.is_some_and(|value| string_fuzzy(value, expected)),
        Operator::NotLike => {
            present_non_null_json(value).is_some_and(|value| !string_like(value, expected, false))
        }
        Operator::NotILike => {
            present_non_null_json(value).is_some_and(|value| !string_like(value, expected, true))
        }
        _ => false,
    }
}

fn branch_base_fetch_limit(
    offset: i64,
    requested_limit: i64,
    materialization_cap: usize,
) -> Result<i64, ApiError> {
    let offset = offset.max(0);
    let requested_limit = requested_limit.max(1);
    let materialization_cap = i64::try_from(materialization_cap.max(1)).unwrap_or(i64::MAX);
    let required_window = offset.saturating_add(requested_limit);
    if required_window > materialization_cap {
        return Err(ApiError::bad_request(
            "BRANCH_PAGE_WINDOW_TOO_LARGE",
            format!(
                "Branch page offset + limit exceeds configured max_result_rows ({materialization_cap})"
            ),
        ));
    }

    Ok(materialization_cap.saturating_add(1))
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
    let left = present_non_null_json(row_field(left, column));
    let right = present_non_null_json(row_field(right, column));
    let ordering = match (left, right) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (Some(_), None) => std::cmp::Ordering::Less,
        (Some(Value::Number(left)), Some(Value::Number(right))) => left
            .as_f64()
            .and_then(|left| right.as_f64().and_then(|right| left.partial_cmp(&right)))
            .unwrap_or(std::cmp::Ordering::Equal),
        (Some(Value::String(left)), Some(Value::String(right))) => left.cmp(right),
        (Some(Value::Bool(left)), Some(Value::Bool(right))) => left.cmp(right),
        _ => std::cmp::Ordering::Equal,
    };
    if desc { ordering.reverse() } else { ordering }
}

fn compare_json_sort_keys(
    left: &Value,
    right: &Value,
    sort_keys: &[BranchSortKey],
) -> std::cmp::Ordering {
    for sort_key in sort_keys {
        let ordering = compare_json_field(left, right, &sort_key.column, sort_key.desc);
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }
    std::cmp::Ordering::Equal
}

fn branch_distinct_value(row: &Value, column: &str) -> String {
    match row_field(row, column) {
        Some(value) => serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()),
        None => "null".to_string(),
    }
}

fn apply_branch_distinct(data: &mut Vec<Value>, distinct_columns: &[String]) {
    if distinct_columns.is_empty() {
        return;
    }

    let mut seen = std::collections::HashSet::new();
    data.retain(|row| {
        let key: Vec<String> = distinct_columns
            .iter()
            .map(|column| branch_distinct_value(row, column))
            .collect();
        seen.insert(key)
    });
}

#[cfg(test)]
mod tests {
    use super::{
        BranchReadConstraintInput, BranchReplayProjectionInput, apply_branch_distinct,
        apply_branch_read_constraints, apply_list_distinct, attach_rest_list_debug_headers,
        branch_base_fetch_limit, branch_projection_columns_from_cmd, encode_ndjson_rows,
        ensure_branch_replay_columns_projected, ensure_nested_parent_key_columns_projected,
        project_rows_to_selected_columns, rest_explain_cache_shape_hash, rest_list_cache_key,
        row_matches_policy_filter_cages, split_expand_relations,
    };
    use crate::auth::AuthContext;
    use crate::policy::{OperationType, PolicyDef, PolicyEngine};
    use crate::schema::SchemaRegistry;
    use axum::{body::Body, http::StatusCode};
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

    fn branch_constraint_input<'a>() -> BranchReadConstraintInput<'a> {
        BranchReadConstraintInput {
            filters: &[],
            policy_filter_cages: &[],
            search: None,
            search_columns: None,
            cursor: None,
            sort: None,
            default_sort_column: "id",
            offset: 0,
            limit: 50,
            base_has_more: false,
            materialization_cap: 10_000,
            distinct_columns: &[],
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
                policy_filter_cages: &cages,
                ..branch_constraint_input()
            },
        )
        .unwrap();

        assert_eq!(rows, vec![json!({"id": 1, "region": "west"})]);
    }

    #[test]
    fn branch_list_distinct_defers_sql_distinct_to_replay() {
        let cmd = qail_core::ast::Qail::get("orders");

        let (branch_cmd, branch_distinct_columns) =
            apply_list_distinct(cmd.clone(), Some("status,region"), true).unwrap();
        assert!(
            branch_cmd.distinct_on.is_empty(),
            "branch replay must not let SQL DISTINCT discard runner-up rows before overlay replay"
        );
        assert_eq!(branch_distinct_columns, vec!["status", "region"]);

        let (normal_cmd, normal_distinct_columns) =
            apply_list_distinct(cmd, Some("status,region"), false).unwrap();
        assert_eq!(
            normal_cmd.distinct_on,
            vec![Expr::Named("status".into()), Expr::Named("region".into())]
        );
        assert!(normal_distinct_columns.is_empty());
    }

    #[test]
    fn encode_ndjson_rows_serializes_each_row_on_its_own_line() {
        let rows = vec![
            json!({"id": 1, "status": "ready"}),
            json!({"id": 2, "status": "queued"}),
        ];

        let body = encode_ndjson_rows(&rows).expect("valid JSON values should serialize");

        assert_eq!(
            body,
            "{\"id\":1,\"status\":\"ready\"}\n{\"id\":2,\"status\":\"queued\"}\n"
        );
    }

    #[test]
    fn debug_headers_attach_to_cached_and_uncached_rest_list_responses() {
        let cmd = qail_core::ast::Qail::get("orders").columns(["id"]);
        let mut response = axum::response::Response::new(Body::empty());
        *response.status_mut() = StatusCode::OK;

        attach_rest_list_debug_headers(&mut response, true, "orders", &cmd);

        assert_eq!(
            response
                .headers()
                .get("x-qail-table")
                .and_then(|value| value.to_str().ok()),
            Some("orders")
        );
        assert!(
            response
                .headers()
                .get("x-qail-sql")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|sql| sql.contains("orders"))
        );
    }

    #[test]
    fn explain_cache_shape_hash_includes_auth_scope() {
        let sql_shape = "SELECT * FROM orders WHERE tenant_id = $1";
        let operator = AuthContext {
            user_id: "user-1".to_string(),
            role: "operator".to_string(),
            tenant_id: Some("tenant-a".to_string()),
            claims: std::collections::HashMap::new(),
        };
        let mut viewer = operator.clone();
        viewer.role = "viewer".to_string();
        let mut other_tenant = operator.clone();
        other_tenant.tenant_id = Some("tenant-b".to_string());

        assert_ne!(
            rest_explain_cache_shape_hash(sql_shape, &operator),
            rest_explain_cache_shape_hash(sql_shape, &viewer)
        );
        assert_ne!(
            rest_explain_cache_shape_hash(sql_shape, &operator),
            rest_explain_cache_shape_hash(sql_shape, &other_tenant)
        );
    }

    #[test]
    fn rest_list_cache_key_canonicalizes_nested_claim_objects() {
        let uri: axum::http::Uri = "/api/orders?select=id".parse().unwrap();
        let cmd = qail_core::ast::Qail::get("orders").columns(["id"]);
        let mut left_claims = std::collections::HashMap::new();
        left_claims.insert(
            "scope".to_string(),
            serde_json::json!({"b": 2, "a": {"z": true, "m": [1, 2]}}),
        );
        let mut right_claims = std::collections::HashMap::new();
        right_claims.insert(
            "scope".to_string(),
            serde_json::json!({"a": {"m": [1, 2], "z": true}, "b": 2}),
        );
        let left = AuthContext {
            user_id: "user-1".to_string(),
            role: "operator".to_string(),
            tenant_id: Some("tenant-a".to_string()),
            claims: left_claims,
        };
        let right = AuthContext {
            user_id: "user-1".to_string(),
            role: "operator".to_string(),
            tenant_id: Some("tenant-a".to_string()),
            claims: right_claims,
        };

        assert_eq!(
            rest_list_cache_key("tenant-a", "orders", &left, &uri, &cmd),
            rest_list_cache_key("tenant-a", "orders", &right, &uri, &cmd)
        );
    }

    #[test]
    fn branch_read_constraints_not_like_honors_sql_wildcards() {
        let mut rows = vec![json!({"id": 1, "name": "acb"})];
        let filters = vec![(
            "name".to_string(),
            Operator::NotLike,
            QailValue::String("a_b".to_string()),
        )];

        apply_branch_read_constraints(
            &mut rows,
            BranchReadConstraintInput {
                filters: &filters,
                ..branch_constraint_input()
            },
        )
        .unwrap();

        assert!(
            rows.is_empty(),
            "branch replay must treat '_' as a SQL LIKE wildcard"
        );
    }

    #[test]
    fn branch_read_constraints_negative_filters_exclude_null_and_missing_rows() {
        let filters = vec![(
            "status".to_string(),
            Operator::Ne,
            QailValue::String("archived".to_string()),
        )];
        let mut rows = vec![
            json!({"id": 1, "status": "open"}),
            json!({"id": 2, "status": "archived"}),
            json!({"id": 3, "status": null}),
            json!({"id": 4}),
        ];

        apply_branch_read_constraints(
            &mut rows,
            BranchReadConstraintInput {
                filters: &filters,
                ..branch_constraint_input()
            },
        )
        .unwrap();

        assert_eq!(rows, vec![json!({"id": 1, "status": "open"})]);
    }

    #[test]
    fn branch_read_constraints_not_in_excludes_null_missing_and_null_rhs() {
        let mut rows = vec![
            json!({"id": 1, "status": "open"}),
            json!({"id": 2, "status": "archived"}),
            json!({"id": 3, "status": null}),
            json!({"id": 4}),
        ];
        let filters = vec![(
            "status".to_string(),
            Operator::NotIn,
            QailValue::Array(vec![QailValue::String("archived".to_string())]),
        )];

        apply_branch_read_constraints(
            &mut rows,
            BranchReadConstraintInput {
                filters: &filters,
                ..branch_constraint_input()
            },
        )
        .unwrap();

        assert_eq!(rows, vec![json!({"id": 1, "status": "open"})]);

        let mut rows = vec![json!({"id": 1, "status": "open"})];
        let filters = vec![(
            "status".to_string(),
            Operator::NotIn,
            QailValue::Array(vec![QailValue::Null]),
        )];

        apply_branch_read_constraints(
            &mut rows,
            BranchReadConstraintInput {
                filters: &filters,
                ..branch_constraint_input()
            },
        )
        .unwrap();

        assert!(rows.is_empty());
    }

    #[test]
    fn branch_read_constraints_not_like_excludes_null_and_missing_rows() {
        let filters = vec![(
            "name".to_string(),
            Operator::NotLike,
            QailValue::String("A%".to_string()),
        )];
        let mut rows = vec![
            json!({"id": 1, "name": "Beta"}),
            json!({"id": 2, "name": "Alice"}),
            json!({"id": 3, "name": null}),
            json!({"id": 4}),
        ];

        apply_branch_read_constraints(
            &mut rows,
            BranchReadConstraintInput {
                filters: &filters,
                ..branch_constraint_input()
            },
        )
        .unwrap();

        assert_eq!(rows, vec![json!({"id": 1, "name": "Beta"})]);
    }

    #[test]
    fn branch_read_constraints_applies_all_sort_keys_before_limit() {
        let mut rows = vec![
            json!({"id": 1, "status": "open"}),
            json!({"id": 2, "status": "open"}),
            json!({"id": 3, "status": "open"}),
        ];

        apply_branch_read_constraints(
            &mut rows,
            BranchReadConstraintInput {
                sort: Some("status:asc,id:desc"),
                limit: 2,
                ..branch_constraint_input()
            },
        )
        .unwrap();

        assert_eq!(
            rows,
            vec![
                json!({"id": 3, "status": "open"}),
                json!({"id": 2, "status": "open"})
            ]
        );
    }

    #[test]
    fn branch_read_constraints_sorts_nulls_like_postgres_defaults() {
        let mut asc_rows = vec![
            json!({"id": 1, "score": null}),
            json!({"id": 2, "score": 10}),
            json!({"id": 3}),
            json!({"id": 4, "score": 5}),
        ];

        apply_branch_read_constraints(
            &mut asc_rows,
            BranchReadConstraintInput {
                sort: Some("score:asc,id:asc"),
                ..branch_constraint_input()
            },
        )
        .unwrap();

        assert_eq!(
            asc_rows,
            vec![
                json!({"id": 4, "score": 5}),
                json!({"id": 2, "score": 10}),
                json!({"id": 1, "score": null}),
                json!({"id": 3}),
            ]
        );

        let mut desc_rows = vec![
            json!({"id": 2, "score": 10}),
            json!({"id": 4, "score": 5}),
            json!({"id": 1, "score": null}),
            json!({"id": 3}),
        ];

        apply_branch_read_constraints(
            &mut desc_rows,
            BranchReadConstraintInput {
                sort: Some("score:desc,id:asc"),
                ..branch_constraint_input()
            },
        )
        .unwrap();

        assert_eq!(
            desc_rows,
            vec![
                json!({"id": 1, "score": null}),
                json!({"id": 3}),
                json!({"id": 2, "score": 10}),
                json!({"id": 4, "score": 5}),
            ]
        );
    }

    #[test]
    fn branch_read_constraints_applies_distinct_after_overlay_sort() {
        let distinct_columns = vec!["status".to_string()];
        let mut rows = vec![
            json!({"id": 1, "status": "open", "priority": 10}),
            json!({"id": 2, "status": "open", "priority": 1}),
            json!({"id": 3, "status": "closed", "priority": 5}),
        ];

        apply_branch_read_constraints(
            &mut rows,
            BranchReadConstraintInput {
                sort: Some("status:asc,priority:asc"),
                distinct_columns: &distinct_columns,
                ..branch_constraint_input()
            },
        )
        .unwrap();

        assert_eq!(
            rows,
            vec![
                json!({"id": 3, "status": "closed", "priority": 5}),
                json!({"id": 2, "status": "open", "priority": 1}),
            ]
        );
    }

    #[test]
    fn branch_distinct_treats_missing_and_null_as_same_key() {
        let distinct_columns = vec!["status".to_string()];
        let mut rows = vec![
            json!({"id": 1, "status": null}),
            json!({"id": 2}),
            json!({"id": 3, "status": "open"}),
        ];

        apply_branch_distinct(&mut rows, &distinct_columns);

        assert_eq!(
            rows,
            vec![
                json!({"id": 1, "status": null}),
                json!({"id": 3, "status": "open"}),
            ]
        );
    }

    #[test]
    fn branch_base_fetch_limit_fetches_full_window_with_sentinel() {
        let fetch_limit = branch_base_fetch_limit(1_500, 50, 10_000).expect("window fits cap");

        assert_eq!(fetch_limit, 10_001);
    }

    #[test]
    fn branch_read_constraints_rejects_truncated_replay_window() {
        let mut rows = vec![json!({"id": 2})];

        let err = apply_branch_read_constraints(
            &mut rows,
            BranchReadConstraintInput {
                limit: 2,
                base_has_more: true,
                materialization_cap: 2,
                ..branch_constraint_input()
            },
        )
        .expect_err("short branch replay with more base rows must fail closed");

        assert_eq!(err.code, "BRANCH_REPLAY_WINDOW_TOO_LARGE");
    }

    #[test]
    fn branch_base_fetch_limit_rejects_windows_past_materialization_cap() {
        let err = branch_base_fetch_limit(9_990, 50, 10_000)
            .expect_err("branch cannot materialize this requested page exactly");

        assert_eq!(err.code, "BRANCH_PAGE_WINDOW_TOO_LARGE");
    }

    #[test]
    fn branch_policy_filter_fails_closed_on_unsupported_operator() {
        let row = json!({"id": 1, "region": "west"});
        let cages = vec![policy_cage("id", Operator::Gt, QailValue::Int(0))];

        let err = row_matches_policy_filter_cages(&row, &cages).unwrap_err();

        assert_eq!(err.status_code(), axum::http::StatusCode::FORBIDDEN);
    }

    #[test]
    fn branch_policy_filter_requires_each_or_cage_to_match() {
        let row = json!({"id": 1, "region": "west", "tier": "silver"});
        let cages = vec![
            Cage {
                kind: CageKind::Filter,
                conditions: vec![
                    Condition {
                        left: Expr::Named("region".to_string()),
                        op: Operator::Eq,
                        value: QailValue::String("west".to_string()),
                        is_array_unnest: false,
                    },
                    Condition {
                        left: Expr::Named("region".to_string()),
                        op: Operator::Eq,
                        value: QailValue::String("east".to_string()),
                        is_array_unnest: false,
                    },
                ],
                logical_op: LogicalOp::Or,
            },
            Cage {
                kind: CageKind::Filter,
                conditions: vec![
                    Condition {
                        left: Expr::Named("tier".to_string()),
                        op: Operator::Eq,
                        value: QailValue::String("gold".to_string()),
                        is_array_unnest: false,
                    },
                    Condition {
                        left: Expr::Named("tier".to_string()),
                        op: Operator::Eq,
                        value: QailValue::String("platinum".to_string()),
                        is_array_unnest: false,
                    },
                ],
                logical_op: LogicalOp::Or,
            },
        ];

        assert!(
            !row_matches_policy_filter_cages(&row, &cages).unwrap(),
            "matching one OR cage must not bypass another OR cage"
        );
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

    #[test]
    fn branch_replay_projection_adds_internal_constraint_columns_after_public_snapshot() {
        let filters = vec![(
            "status".to_string(),
            Operator::Eq,
            QailValue::String("open".to_string()),
        )];
        let distinct_columns = vec!["region".to_string()];
        let mut cmd = qail_core::ast::Qail::get("orders").columns(["name"]);
        let public_projection = branch_projection_columns_from_cmd(&cmd).unwrap().unwrap();

        ensure_branch_replay_columns_projected(
            &mut cmd,
            BranchReplayProjectionInput {
                filters: &filters,
                policy_filter_cages: &[],
                search: Some("needle"),
                search_columns: Some("description"),
                sort: Some("-priority"),
                default_sort_column: "id",
                distinct_columns: &distinct_columns,
                pk_column: "id",
                table_name: "orders",
                has_joins: false,
            },
        )
        .unwrap();

        assert_eq!(public_projection, vec!["name".to_string()]);
        assert_eq!(
            cmd.columns,
            vec![
                Expr::Named("name".to_string()),
                Expr::Named("id".to_string()),
                Expr::Named("status".to_string()),
                Expr::Named("description".to_string()),
                Expr::Named("priority".to_string()),
                Expr::Named("region".to_string()),
            ]
        );

        let mut rows = vec![json!({
            "name": "base",
            "id": "order-1",
            "status": "open",
            "description": "needle",
            "priority": 10,
            "region": "west"
        })];
        apply_branch_read_constraints(
            &mut rows,
            BranchReadConstraintInput {
                filters: &filters,
                search: Some("needle"),
                search_columns: Some("description"),
                sort: Some("-priority"),
                distinct_columns: &distinct_columns,
                ..branch_constraint_input()
            },
        )
        .unwrap();
        project_rows_to_selected_columns(&mut rows, &public_projection);

        assert_eq!(rows, vec![json!({"name": "base"})]);
    }

    #[test]
    fn branch_replay_projection_qualifies_internal_columns_for_joins() {
        let filters = vec![(
            "status".to_string(),
            Operator::Eq,
            QailValue::String("open".to_string()),
        )];
        let policy_cages = vec![policy_cage(
            "orders.tenant_id",
            Operator::Eq,
            QailValue::String("tenant-a".to_string()),
        )];
        let mut cmd = qail_core::ast::Qail::get("orders").columns(["orders.name"]);

        ensure_branch_replay_columns_projected(
            &mut cmd,
            BranchReplayProjectionInput {
                filters: &filters,
                policy_filter_cages: &policy_cages,
                search: None,
                search_columns: None,
                sort: None,
                default_sort_column: "id",
                distinct_columns: &[],
                pk_column: "id",
                table_name: "orders",
                has_joins: true,
            },
        )
        .unwrap();

        assert_eq!(
            cmd.columns,
            vec![
                Expr::Named("orders.name".to_string()),
                Expr::Named("orders.id".to_string()),
                Expr::Named("orders.status".to_string()),
                Expr::Named("orders.tenant_id".to_string()),
            ]
        );
    }

    #[test]
    fn branch_replay_projection_rejects_related_table_replay_columns() {
        let filters = vec![(
            "users.region".to_string(),
            Operator::Eq,
            QailValue::String("west".to_string()),
        )];
        let mut cmd = qail_core::ast::Qail::get("orders").columns(["orders.name"]);

        let err = ensure_branch_replay_columns_projected(
            &mut cmd,
            BranchReplayProjectionInput {
                filters: &filters,
                policy_filter_cages: &[],
                search: None,
                search_columns: None,
                sort: None,
                default_sort_column: "id",
                distinct_columns: &[],
                pk_column: "id",
                table_name: "orders",
                has_joins: true,
            },
        )
        .unwrap_err();

        assert_eq!(err.status_code(), axum::http::StatusCode::FORBIDDEN);
    }

    fn nested_projection_schema() -> SchemaRegistry {
        let mut schema = SchemaRegistry::new();
        schema
            .load_from_qail_str(
                r#"
table users {
    id uuid primary_key
    name text
}

table posts {
    id uuid primary_key
    user_id uuid references users(id)
    title text
}
"#,
            )
            .expect("schema should parse");
        schema
    }

    #[test]
    fn nested_projection_adds_forward_fk_key_without_leaking_it() {
        let schema = nested_projection_schema();
        let mut cmd = qail_core::ast::Qail::get("posts").columns(["title"]);

        let strip =
            ensure_nested_parent_key_columns_projected(&mut cmd, &schema, "posts", &["users"])
                .unwrap();

        assert_eq!(strip, vec!["user_id".to_string()]);
        assert_eq!(
            cmd.columns,
            vec![
                Expr::Named("title".to_string()),
                Expr::Named("user_id".to_string())
            ]
        );
    }

    #[test]
    fn nested_projection_adds_reverse_parent_key_without_leaking_it() {
        let schema = nested_projection_schema();
        let mut cmd = qail_core::ast::Qail::get("users").columns(["name"]);

        let strip =
            ensure_nested_parent_key_columns_projected(&mut cmd, &schema, "users", &["posts"])
                .unwrap();

        assert_eq!(strip, vec!["id".to_string()]);
        assert_eq!(
            cmd.columns,
            vec![
                Expr::Named("name".to_string()),
                Expr::Named("id".to_string())
            ]
        );
    }

    #[test]
    fn nested_projection_does_not_strip_user_selected_or_wildcard_keys() {
        let schema = nested_projection_schema();
        let mut selected_key = qail_core::ast::Qail::get("posts").columns(["title", "user_id"]);
        let mut wildcard = qail_core::ast::Qail::get("posts").columns(["*"]);

        let strip_selected = ensure_nested_parent_key_columns_projected(
            &mut selected_key,
            &schema,
            "posts",
            &["users"],
        )
        .unwrap();
        let strip_wildcard =
            ensure_nested_parent_key_columns_projected(&mut wildcard, &schema, "posts", &["users"])
                .unwrap();

        assert!(strip_selected.is_empty());
        assert!(strip_wildcard.is_empty());
        assert_eq!(
            selected_key.columns,
            vec![
                Expr::Named("title".to_string()),
                Expr::Named("user_id".to_string())
            ]
        );
        assert_eq!(wildcard.columns, vec![Expr::Named("*".to_string())]);
    }

    #[test]
    fn split_expand_relations_counts_nested_relations_against_limit() {
        let err = split_expand_relations("nested:items,nested:payments", 1).unwrap_err();

        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn split_expand_relations_deduplicates_flat_and_nested_relations() {
        let (flat, nested) =
            split_expand_relations("users, nested:items,users,nested:items,nested:payments", 3)
                .unwrap();

        assert_eq!(flat, vec!["users"]);
        assert_eq!(nested, vec!["items", "payments"]);
    }

    #[test]
    fn split_expand_relations_rejects_fail_open_inputs() {
        assert!(split_expand_relations("", 3).is_err());
        assert!(split_expand_relations("users,", 3).is_err());
        assert!(split_expand_relations("nested:", 3).is_err());
        assert!(split_expand_relations("users,bad-rel", 3).is_err());
        assert!(split_expand_relations("nested:bad-rel", 3).is_err());
    }
}
