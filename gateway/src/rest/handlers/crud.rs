//! CRUD handlers for REST endpoints.
//!
//! - `list_handler` — GET /api/{table}
//! - `aggregate_handler` — GET /api/{table}/aggregate
//! - `get_by_id_handler` — GET /api/{table}/:id
//! - `create_handler` — POST /api/{table}
//! - `update_handler` — PATCH /api/{table}/:id
//! - `delete_handler` — DELETE /api/{table}/:id

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{
        HeaderMap, StatusCode,
        header::{CONTENT_TYPE, HeaderValue},
    },
    response::{IntoResponse, Json, Response},
};
use qail_core::ast::{AggregateFunc, Expr, JoinKind, Operator, Value as QailValue};
use qail_core::transpiler::ToSql;
use serde_json::{Value, json};
use std::sync::Arc;
use uuid::Uuid;

use crate::GatewayState;
use crate::auth::authenticate_request;
use crate::handler::row_to_json;
use crate::middleware::ApiError;
use crate::policy::OperationType;

use super::super::branch::{apply_branch_overlay, redirect_to_overlay};
use super::super::filters::{
    apply_filters, apply_returning, apply_sorting, json_to_qail_value, parse_filters,
    parse_scalar_value,
};
use super::super::nested::expand_nested;
use super::super::types::*;
use super::super::{debug_sql, extract_branch_from_headers, extract_table_name, is_debug_request};
use super::{check_table_not_blocked, parse_prefer_header, primary_sort_for_cursor};

/// GET /api/{table} — list with pagination, sorting, filtering, column selection
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
    let tenant_id = auth.to_rls_context().operator_id.clone();
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
        if auth.role != "admin" && auth.role != "super_admin" {
            conn.release().await;
            return Err(ApiError::forbidden(
                "Admin role required for branch overlay reads",
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
    if let Some(ref tenant_id) = auth.tenant_id {
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

/// GET /api/{table}/aggregate — aggregation queries
///
/// `?func=count`                      → SELECT COUNT(*) FROM table
/// `?func=sum&column=price`           → SELECT SUM(price) FROM table
/// `?func=avg&column=price&group_by=status`  → SELECT status, AVG(price) FROM table GROUP BY status
/// `?name.eq=John`                    → with filters
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

    // Apply RLS
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;

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
    if let Some(ref tenant_id) = auth.tenant_id {
        let _proof = crate::tenant_guard::verify_tenant_boundary(
            &data,
            tenant_id,
            &state.config.tenant_column,
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

/// GET /api/{table}/:id — get single row by PK
pub(crate) async fn get_by_id_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    request: axum::extract::Request,
) -> Result<Json<SingleResponse>, ApiError> {
    let table_name =
        extract_table_name(request.uri()).ok_or_else(|| ApiError::not_found("table"))?;
    check_table_not_blocked(&state, &table_name)?;

    // F5: Accept any PK type (UUID, text, integer, serial, etc.)
    // Let Postgres validate the value against the actual column type.
    if id.is_empty() {
        return Err(ApiError::parse_error(
            "ID parameter cannot be empty".to_string(),
        ));
    }

    let table = state
        .schema
        .table(&table_name)
        .ok_or_else(|| ApiError::not_found(&table_name))?;

    let pk = table
        .primary_key
        .as_ref()
        .ok_or_else(|| ApiError::internal("Table has no primary key"))?;

    let auth = authenticate_request(state.as_ref(), &headers).await?;

    // Build: get table[pk = $id] — use String value; PG handles type coercion
    let mut cmd = qail_core::ast::Qail::get(&table_name)
        .filter(pk, Operator::Eq, QailValue::String(id.clone()))
        .limit(1);

    // Apply RLS
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;

    // Execute
    let mut conn = state
        .acquire_with_auth_rls_guarded(&auth, Some(&table_name))
        .await?;

    let rows = match conn.fetch_all_uncached(&cmd).await {
        Ok(rows) => rows,
        Err(e) => {
            conn.release().await;
            return Err(ApiError::from_pg_driver_error(&e, Some(&table_name)));
        }
    };

    let row = match rows.first() {
        Some(row) => row,
        None => {
            conn.release().await;
            return Err(ApiError::not_found(format!("{}/{}", table_name, id)));
        }
    };

    let mut data = row_to_json(row);

    // Branch overlay: check if this row is overridden on the branch — admin-gated
    let branch_ctx = extract_branch_from_headers(&headers);
    if let Some(branch_name) = branch_ctx.branch_name() {
        if auth.role != "admin" && auth.role != "super_admin" {
            conn.release().await;
            return Err(ApiError::forbidden(
                "Admin role required for branch overlay reads",
            ));
        }
        let sql = qail_pg::driver::branch_sql::read_overlay_sql(branch_name, &table_name);
        if let Ok(pg_conn) = conn.get_mut()
            && let Ok(overlay_rows) = pg_conn.simple_query(&sql).await
        {
            for orow in &overlay_rows {
                let row_pk = orow
                    .try_get_by_name::<String>("row_pk")
                    .ok()
                    .or_else(|| orow.get_string(0))
                    .unwrap_or_default();
                if row_pk == id {
                    let operation = orow
                        .try_get_by_name::<String>("operation")
                        .ok()
                        .or_else(|| orow.get_string(1))
                        .unwrap_or_default();
                    match operation.as_str() {
                        "delete" => {
                            conn.release().await;
                            return Err(ApiError::not_found(format!(
                                "{}/{} (deleted on branch)",
                                table_name, id
                            )));
                        }
                        "update" | "insert" => {
                            let row_data_str = orow
                                .try_get_by_name::<String>("row_data")
                                .ok()
                                .or_else(|| orow.get_string(2))
                                .unwrap_or_default();
                            if let Ok(val) = serde_json::from_str::<Value>(&row_data_str) {
                                data = val;
                            }
                        }
                        _ => {}
                    }
                    break;
                }
            }
        }
    }

    conn.release().await;

    // ── Tenant Boundary Invariant ────────────────────────────────────
    if let Some(ref tenant_id) = auth.tenant_id {
        let single = vec![data.clone()];
        let _proof = crate::tenant_guard::verify_tenant_boundary(
            &single,
            tenant_id,
            &state.config.tenant_column,
            &table_name,
            "rest_get_by_id",
        )
        .map_err(|v| {
            tracing::error!("{}", v);
            ApiError::internal("Data integrity error")
        })?;
    }

    Ok(Json(SingleResponse { data }))
}

/// POST /api/{table} — create from JSON body (single object or batch array)
///
/// Supports:
/// - Single: `{ "name": "Alice" }` → creates 1 row
/// - Batch:  `[{ "name": "Alice" }, { "name": "Bob" }]` → creates N rows
/// - Upsert: `?on_conflict=id` → INSERT ... ON CONFLICT (id) DO UPDATE
/// - Returning: `?returning=*` → RETURNING *
pub(crate) async fn create_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Query(mutation_params): Query<MutationParams>,
    request: axum::extract::Request,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let table_name =
        extract_table_name(request.uri()).ok_or_else(|| ApiError::not_found("table"))?;
    check_table_not_blocked(&state, &table_name)?;

    let table = state
        .schema
        .table(&table_name)
        .ok_or_else(|| ApiError::not_found(&table_name))?;

    let auth = authenticate_request(state.as_ref(), &headers).await?;
    let prefer = parse_prefer_header(&headers);

    // Validate required columns upfront (skip for upserts — conflict rows may exist)
    let required: Vec<String> = if prefer.wants_upsert() || prefer.wants_ignore_duplicates() {
        Vec::new() // Upsert: required columns may already exist in the row
    } else {
        table
            .required_columns()
            .iter()
            .map(|c| c.name.clone())
            // Skip tenant_column from required validation — it will be auto-injected
            // from the auth context if not provided by the client.
            .filter(|name| {
                if auth.tenant_id.is_some() && name == &state.config.tenant_column {
                    return false;
                }
                true
            })
            .collect()
    };

    // Parse JSON body
    let body = axum::body::to_bytes(request.into_body(), 1024 * 1024)
        .await
        .map_err(|e| ApiError::parse_error(e.to_string()))?;
    let body: Value =
        serde_json::from_slice(&body).map_err(|e| ApiError::parse_error(e.to_string()))?;

    // Detect batch vs single
    let is_batch = body.is_array();
    let objects: Vec<&serde_json::Map<String, Value>> = if is_batch {
        let arr = body
            .as_array()
            .ok_or_else(|| ApiError::parse_error("Expected JSON array body"))?;
        arr.iter()
            .map(|v| {
                v.as_object()
                    .ok_or_else(|| ApiError::parse_error("Batch items must be JSON objects"))
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![
            body.as_object()
                .ok_or_else(|| ApiError::parse_error("Expected JSON object or array"))?,
        ]
    };

    if objects.is_empty() {
        return Err(ApiError::parse_error("Empty request body"));
    }

    // Validate required columns for each object
    for (i, obj) in objects.iter().enumerate() {
        for col_name in &required {
            if !obj.contains_key(col_name) {
                return Err(ApiError::parse_error(format!(
                    "Missing required field '{}' in item {}",
                    col_name, i
                )));
            }
        }
    }
    // SECURITY: Fail closed on invalid JSON keys instead of silently skipping.
    // Skipping can produce unintended default-row inserts.
    for obj in &objects {
        for key in obj.keys() {
            if !crate::rest::filters::is_safe_identifier(key) {
                return Err(ApiError::parse_error(format!(
                    "Invalid field name '{}' in create payload",
                    key
                )));
            }
        }
    }

    // SECURITY: Check branch admin gate BEFORE acquiring connection
    let branch_ctx = extract_branch_from_headers(&headers);
    if branch_ctx.branch_name().is_some() && auth.role != "admin" && auth.role != "super_admin" {
        return Err(ApiError::forbidden(
            "Admin role required for branch overlay writes",
        ));
    }

    // Acquire connection
    let mut conn = state
        .acquire_with_auth_rls_guarded(&auth, Some(&table_name))
        .await?;

    // Branch CoW Write: redirect inserts to overlay table
    if let Some(branch_name) = branch_ctx.branch_name() {
        let mut all_results: Vec<Value> = Vec::with_capacity(objects.len());
        for obj in &objects {
            let row_data: Value = Value::Object((*obj).clone());
            let pk_col = table.primary_key.as_deref().unwrap_or("id");
            let row_pk = obj
                .get(pk_col)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| Uuid::new_v4().to_string());

            let overlay_result = redirect_to_overlay(
                &mut conn,
                branch_name,
                &table_name,
                &row_pk,
                "insert",
                &row_data,
            )
            .await;
            if let Err(e) = overlay_result {
                conn.release().await;
                return Err(e);
            }
            all_results.push(row_data);
        }

        conn.release().await;

        if is_batch {
            return Ok((
                StatusCode::CREATED,
                Json(
                    json!({ "data": all_results, "count": all_results.len(), "branch": branch_name }),
                ),
            ));
        } else {
            let data = all_results
                .into_iter()
                .next()
                .unwrap_or_else(|| json!({"created": true}));
            return Ok((
                StatusCode::CREATED,
                Json(json!({ "data": data, "branch": branch_name })),
            ));
        }
    }

    // Resolve PK column for Prefer: resolution=merge-duplicates
    let prefer_conflict_col: Option<String> =
        if prefer.wants_upsert() && mutation_params.on_conflict.is_none() {
            // Auto-resolve PK column from schema
            table.primary_key.clone()
        } else if prefer.wants_ignore_duplicates() && mutation_params.on_conflict.is_none() {
            table.primary_key.clone()
        } else {
            None
        };

    let mut all_results: Vec<Value> = Vec::with_capacity(objects.len());

    for obj in &objects {
        let mut cmd = qail_core::ast::Qail::add(&table_name);

        for (key, value) in *obj {
            let qail_val = json_to_qail_value(value);
            cmd = cmd.set_value(key, qail_val);
        }

        // Auto-inject tenant_id from auth context if not provided by client.
        // This ensures multi-tenant tables get the correct tenant_id without
        // requiring every frontend form to explicitly include it.
        if let Some(ref tid) = auth.tenant_id {
            let tc = &state.config.tenant_column;
            if !obj.contains_key(tc) {
                cmd = cmd.set_value(tc, QailValue::String(tid.clone()));
            }
        }

        // Upsert support: explicit on_conflict param takes precedence
        if let Some(ref conflict_col) = mutation_params.on_conflict {
            // SECURITY: Validate on_conflict column identifiers.
            let conflict_cols: Vec<&str> = conflict_col
                .split(',')
                .map(|s| s.trim())
                .filter(|s| crate::rest::filters::is_safe_identifier(s))
                .collect();
            let action = mutation_params
                .on_conflict_action
                .as_deref()
                .unwrap_or("update");

            if action == "nothing" {
                cmd = cmd.on_conflict_nothing(&conflict_cols);
            } else {
                // Default: update all provided columns on conflict
                // SECURITY: Filter update keys through identifier guard.
                let updates: Vec<(&str, Expr)> = obj
                    .keys()
                    .filter(|k| !conflict_cols.contains(&k.as_str()))
                    .filter(|k| crate::rest::filters::is_safe_identifier(k))
                    .map(|k| (k.as_str(), Expr::Named(format!("EXCLUDED.{}", k))))
                    .collect();
                cmd = cmd.on_conflict_update(&conflict_cols, &updates);
            }
        } else if prefer.wants_ignore_duplicates() {
            // Prefer: resolution=ignore-duplicates → DO NOTHING on PK
            if let Some(ref pk_col) = prefer_conflict_col {
                let cols: Vec<&str> = vec![pk_col.as_str()];
                cmd = cmd.on_conflict_nothing(&cols);
            }
        } else if let Some(ref pk_col) = prefer_conflict_col {
            // Prefer: resolution=merge-duplicates → DO UPDATE on all cols
            let conflict_cols: Vec<&str> = vec![pk_col.as_str()];
            // SECURITY: Filter update keys through identifier guard.
            let updates: Vec<(&str, Expr)> = obj
                .keys()
                .filter(|k| k.as_str() != pk_col.as_str())
                .filter(|k| crate::rest::filters::is_safe_identifier(k))
                .map(|k| (k.as_str(), Expr::Named(format!("EXCLUDED.{}", k))))
                .collect();
            cmd = cmd.on_conflict_update(&conflict_cols, &updates);
        }

        // Returning clause: Prefer return=representation forces RETURNING *
        if prefer.return_mode.as_deref() == Some("representation")
            && mutation_params.returning.is_none()
        {
            cmd = apply_returning(cmd, Some("*"));
        } else {
            cmd = apply_returning(cmd, mutation_params.returning.as_deref());
        }

        // Apply RLS
        if let Err(e) = state.policy_engine.apply_policies(&auth, &mut cmd) {
            conn.release().await;
            return Err(ApiError::forbidden(e.to_string()));
        }

        let rows = match conn.fetch_all_uncached(&cmd).await {
            Ok(rows) => rows,
            Err(e) => {
                conn.release().await;
                return Err(ApiError::from_pg_driver_error(&e, Some(&table_name)));
            }
        };

        if !rows.is_empty() {
            for row in &rows {
                all_results.push(row_to_json(row));
            }
        }
    }

    // Release connection before JSON processing
    conn.release().await;

    // Invalidate cache
    state.cache.invalidate_table(&table_name);

    // Prefer: return=minimal → 201 with no body
    if prefer.wants_minimal() {
        state.event_engine.fire(
            &table_name,
            OperationType::Create,
            Some(json!(all_results)),
            None,
        );
        return Ok((StatusCode::CREATED, Json(json!({}))));
    }

    if is_batch {
        let count = all_results.len();
        // Fire event triggers
        state.event_engine.fire(
            &table_name,
            OperationType::Create,
            Some(json!(all_results)),
            None,
        );
        Ok((
            StatusCode::CREATED,
            Json(json!({
                "data": all_results,
                "count": count,
            })),
        ))
    } else {
        let data = all_results
            .into_iter()
            .next()
            .unwrap_or_else(|| json!({"created": true}));
        // Fire event triggers
        state
            .event_engine
            .fire(&table_name, OperationType::Create, Some(data.clone()), None);
        Ok((StatusCode::CREATED, Json(json!({ "data": data }))))
    }
}

/// PATCH /api/{table}/:id — partial update
pub(crate) async fn update_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(mutation_params): Query<MutationParams>,
    request: axum::extract::Request,
) -> Result<Json<SingleResponse>, ApiError> {
    let table_name =
        extract_table_name(request.uri()).ok_or_else(|| ApiError::not_found("table"))?;
    check_table_not_blocked(&state, &table_name)?;

    // F5: Accept any PK type
    if id.is_empty() {
        return Err(ApiError::parse_error(
            "ID parameter cannot be empty".to_string(),
        ));
    }

    let table = state
        .schema
        .table(&table_name)
        .ok_or_else(|| ApiError::not_found(&table_name))?;

    let pk = table
        .primary_key
        .as_ref()
        .ok_or_else(|| ApiError::internal("Table has no primary key"))?
        .clone();

    let auth = authenticate_request(state.as_ref(), &headers).await?;

    // Parse JSON body
    let body = axum::body::to_bytes(request.into_body(), 1024 * 1024)
        .await
        .map_err(|e| ApiError::parse_error(e.to_string()))?;
    let body: Value =
        serde_json::from_slice(&body).map_err(|e| ApiError::parse_error(e.to_string()))?;
    let obj = body
        .as_object()
        .ok_or_else(|| ApiError::parse_error("Expected JSON object"))?;

    if obj.is_empty() {
        return Err(ApiError::parse_error("No fields to update"));
    }
    // SECURITY: Fail closed on invalid JSON keys instead of silently skipping.
    for key in obj.keys() {
        if !crate::rest::filters::is_safe_identifier(key) {
            return Err(ApiError::parse_error(format!(
                "Invalid field name '{}' in update payload",
                key
            )));
        }
    }

    // Build: set table { col1 = val1 } [pk = $id]
    let mut cmd = qail_core::ast::Qail::set(&table_name).filter(
        &pk,
        Operator::Eq,
        QailValue::String(id.clone()),
    );

    for (key, value) in obj {
        let qail_val = json_to_qail_value(value);
        cmd = cmd.set_value(key, qail_val);
    }

    // Returning clause
    cmd = apply_returning(cmd, mutation_params.returning.as_deref());

    // Apply RLS
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;

    // SECURITY: Check branch admin gate BEFORE acquiring connection
    let branch_ctx = extract_branch_from_headers(&headers);
    if branch_ctx.branch_name().is_some() && auth.role != "admin" && auth.role != "super_admin" {
        return Err(ApiError::forbidden(
            "Admin role required for branch overlay writes",
        ));
    }

    // Execute
    let mut conn = state
        .acquire_with_auth_rls_guarded(&auth, Some(&table_name))
        .await?;

    // Branch CoW Write: redirect updates to overlay
    if let Some(branch_name) = branch_ctx.branch_name() {
        let row_data: Value = Value::Object(obj.clone());
        let overlay_result = redirect_to_overlay(
            &mut conn,
            branch_name,
            &table_name,
            &id,
            "update",
            &row_data,
        )
        .await;
        if let Err(e) = overlay_result {
            conn.release().await;
            return Err(e);
        }
        conn.release().await;
        return Ok(Json(SingleResponse {
            data: json!({"updated": true, "branch": branch_name}),
        }));
    }

    let rows = match conn.fetch_all_uncached(&cmd).await {
        Ok(rows) => rows,
        Err(e) => {
            conn.release().await;
            return Err(ApiError::from_pg_driver_error(&e, Some(&table_name)));
        }
    };

    let data = rows
        .first()
        .map(row_to_json)
        .unwrap_or_else(|| json!({"updated": true}));

    // Release connection before event processing
    conn.release().await;

    // Invalidate cache
    state.cache.invalidate_table(&table_name);

    // Fire event triggers
    state
        .event_engine
        .fire(&table_name, OperationType::Update, Some(data.clone()), None);

    Ok(Json(SingleResponse { data }))
}

/// DELETE /api/{table}/:id — delete by PK
pub(crate) async fn delete_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    request: axum::extract::Request,
) -> Result<axum::http::StatusCode, ApiError> {
    let table_name =
        extract_table_name(request.uri()).ok_or_else(|| ApiError::not_found("table"))?;
    check_table_not_blocked(&state, &table_name)?;

    // F5: Accept any PK type
    if id.is_empty() {
        return Err(ApiError::parse_error(
            "ID parameter cannot be empty".to_string(),
        ));
    }

    let table = state
        .schema
        .table(&table_name)
        .ok_or_else(|| ApiError::not_found(&table_name))?;

    let pk = table
        .primary_key
        .as_ref()
        .ok_or_else(|| ApiError::internal("Table has no primary key"))?
        .clone();

    let auth = authenticate_request(state.as_ref(), &headers).await?;

    // Build: del table[pk = $id]
    let mut cmd = qail_core::ast::Qail::del(&table_name).filter(
        &pk,
        Operator::Eq,
        QailValue::String(id.clone()),
    );

    // Apply RLS
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;

    // SECURITY: Check branch admin gate BEFORE acquiring connection
    let branch_ctx = extract_branch_from_headers(&headers);
    if branch_ctx.branch_name().is_some() && auth.role != "admin" && auth.role != "super_admin" {
        return Err(ApiError::forbidden(
            "Admin role required for branch overlay writes",
        ));
    }

    // Execute
    let mut conn = state
        .acquire_with_auth_rls_guarded(&auth, Some(&table_name))
        .await?;

    // Branch CoW Write: redirect deletes to overlay (tombstone)
    if let Some(branch_name) = branch_ctx.branch_name() {
        let overlay_result = redirect_to_overlay(
            &mut conn,
            branch_name,
            &table_name,
            &id,
            "delete",
            &Value::Null,
        )
        .await;
        if let Err(e) = overlay_result {
            conn.release().await;
            return Err(e);
        }
        conn.release().await;
        return Ok(axum::http::StatusCode::NO_CONTENT);
    }

    match conn.fetch_all_uncached(&cmd).await {
        Ok(_) => {}
        Err(e) => {
            conn.release().await;
            return Err(ApiError::from_pg_driver_error(&e, Some(&table_name)));
        }
    };

    // Release connection before event processing
    conn.release().await;

    // Invalidate cache
    state.cache.invalidate_table(&table_name);

    // Fire event triggers
    state.event_engine.fire(
        &table_name,
        OperationType::Delete,
        None,
        Some(json!({"id": id})),
    );

    // F6: Return 204 No Content to match OpenAPI spec
    Ok(axum::http::StatusCode::NO_CONTENT)
}
