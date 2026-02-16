//! CRUD handlers for REST endpoints.
//!
//! - `list_handler` — GET /api/{table} with pagination, filtering, sorting, expand, streaming
//! - `aggregate_handler` — GET /api/{table}/aggregate
//! - `get_by_id_handler` — GET /api/{table}/:id
//! - `create_handler` — POST /api/{table}
//! - `update_handler` — PATCH /api/{table}/:id
//! - `delete_handler` — DELETE /api/{table}/:id

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json, Response},
};
use qail_core::ast::{AggregateFunc, Expr, JoinKind, Operator, Value as QailValue};
use qail_core::transpiler::ToSql;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

use crate::auth::extract_auth_from_headers;
use crate::handler::row_to_json;
use crate::middleware::ApiError;
use crate::policy::OperationType;
use crate::GatewayState;

use super::branch::{apply_branch_overlay, redirect_to_overlay};
use super::filters::{apply_filters, apply_sorting, apply_returning, json_to_qail_value, parse_filters, parse_scalar_value};
use super::nested::expand_nested;
use super::types::*;
use super::{extract_table_name, extract_branch_from_headers, is_debug_request, debug_sql};

/// GET /api/{table} — list with pagination, sorting, filtering, column selection
pub(crate) async fn list_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Query(params): Query<ListParams>,
    request: axum::extract::Request,
) -> Result<Response, ApiError> {
    let table_name =
        extract_table_name(request.uri()).ok_or_else(|| ApiError::not_found("table"))?;

    let _table = state
        .schema
        .table(&table_name)
        .ok_or_else(|| ApiError::not_found(&table_name))?;

    let auth = extract_auth_from_headers(&headers);

    // Build Qail AST
    let max_rows = state.config.max_result_rows.min(1000) as i64;
    let limit = params.limit.unwrap_or(50).clamp(1, max_rows);
    let offset = params.offset.unwrap_or(0).clamp(0, 100_000);

    let mut cmd = qail_core::ast::Qail::get(&table_name);

    // Column selection
    if let Some(ref select) = params.select {
        let cols: Vec<&str> = select.split(',').map(|s| s.trim()).collect();
        cmd = cmd.columns(cols);
    }

    // Sorting (multi-column) — default to `id ASC` for deterministic pagination
    if let Some(ref sort) = params.sort {
        cmd = apply_sorting(cmd, sort);
    } else {
        cmd = cmd.order_asc("id");
    }

    // Distinct
    if let Some(ref distinct) = params.distinct {
        let cols: Vec<&str> = distinct.split(',').map(|s| s.trim()).collect();
        cmd = cmd.distinct_on(cols);
    }

    // Expand FK relations via LEFT JOIN
    if let Some(ref expand) = params.expand {
        let relations: Vec<&str> = {
            let mut seen = std::collections::HashSet::new();
            expand.split(',').map(|s| s.trim()).filter(|s| !s.is_empty() && seen.insert(*s)).collect()
        };
        if relations.len() > state.config.max_expand_depth {
            return Err(ApiError::parse_error(format!(
                "Too many expand relations ({}). Maximum is {}",
                relations.len(), state.config.max_expand_depth
            )));
        }
        for rel in relations {
            // Try: this table references `rel` (forward: orders?expand=users)
            if let Some((fk_col, ref_col)) = state.schema.relation_for(&table_name, rel) {
                let left = format!("{}.{}", table_name, fk_col);
                let right = format!("{}.{}", rel, ref_col);
                cmd = cmd.join(JoinKind::Left, rel, &left, &right);
                continue;
            }
            // Try: `rel` references this table (reverse: users?expand=orders)
            if let Some((fk_col, ref_col)) = state.schema.relation_for(rel, &table_name) {
                let left = format!("{}.{}", table_name, ref_col);
                let right = format!("{}.{}", rel, fk_col);
                cmd = cmd.join(JoinKind::Left, rel, &left, &right);
                continue;
            }
            return Err(ApiError::parse_error(format!(
                "No relation between '{}' and '{}'",
                table_name, rel
            )));
        }
    }

    // Parse and apply filters from query string
    let query_string = request.uri().query().unwrap_or("");
    let filters = parse_filters(query_string);
    cmd = apply_filters(cmd, &filters);

    // Cursor-based pagination: filter rows after the cursor value
    if let Some(ref cursor) = params.cursor {
        // Cursor requires a sort column — use the first one, or default to PK
        if let Some(ref sort) = params.sort {
            let first_sort_col = sort.split(',').next().unwrap_or("id");
            let col = first_sort_col.split(':').next().unwrap_or("id");
            let dir = first_sort_col.split(':').nth(1).unwrap_or("asc");
            let cursor_val = parse_scalar_value(cursor);
            if dir == "desc" {
                cmd = cmd.lt(col, cursor_val);
            } else {
                cmd = cmd.gt(col, cursor_val);
            }
        }
    }

    // Full-text search
    if let Some(ref term) = params.search {
        let cols = params.search_columns.as_deref().unwrap_or("name");
        cmd = cmd.filter(cols, Operator::TextSearch, QailValue::String(term.clone()));
    }

    // Pagination
    cmd = cmd.limit(limit);
    cmd = cmd.offset(offset);

    // Apply RLS policies
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;

    // Build cache key from full URI + user identity
    let is_streaming = params.stream.unwrap_or(false);
    let has_branch = headers.get("x-branch-id").is_some();
    let has_nested = params.expand.as_deref().is_some_and(|e| e.contains("nested:"));
    let can_cache = !is_streaming && !has_branch && !has_nested;
    // SECURITY (E1): Include tenant_id to prevent cross-tenant cache poisoning.
    let tenant = auth.tenant_id.as_deref().unwrap_or("_anon");
    let cache_key = format!("rest:{}:{}:{}:{}", tenant, table_name, auth.user_id, request.uri());

    // Check cache for simple read queries
    if can_cache {
        if let Some(cached) = state.cache.get(&cache_key) {
            return Ok(Response::builder()
                .header("Content-Type", "application/json")
                .header("X-Cache", "HIT")
                .body(Body::from(cached))
                .unwrap());
        }
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
        .pool
        .acquire_with_rls_timeout(auth.to_rls_context(), state.config.statement_timeout_ms)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    // ── EXPLAIN Pre-check ──────────────────────────────────────────────
    // Run EXPLAIN (FORMAT JSON) for queries with expand depth ≥ threshold
    // to reject outrageously expensive queries before they consume resources.
    {
        use qail_pg::explain::{ExplainMode, check_estimate};
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let expand_depth = params.expand.as_deref()
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
                            return Err(ApiError::internal("EXPLAIN pre-check failed (enforce mode)"));
                        }
                        tracing::warn!(
                            table = %table_name,
                            sql = %sql_shape,
                            "EXPLAIN pre-check: failed to parse EXPLAIN output, allowing query"
                        );
                        qail_pg::explain::ExplainEstimate { total_cost: 0.0, plan_rows: 0 }
                    }
                    Err(e) => {
                        // SECURITY (E8): In Enforce mode, fail closed.
                        if matches!(state.explain_config.mode, ExplainMode::Enforce) {
                            tracing::warn!(
                                table = %table_name,
                                error = %e,
                                "EXPLAIN pre-check: EXPLAIN failed in Enforce mode — rejecting query"
                            );
                            return Err(ApiError::internal("EXPLAIN pre-check failed (enforce mode)"));
                        }
                        tracing::warn!(
                            table = %table_name,
                            error = %e,
                            "EXPLAIN pre-check: EXPLAIN query failed, allowing query"
                        );
                        qail_pg::explain::ExplainEstimate { total_cost: 0.0, plan_rows: 0 }
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
                let detail = decision.rejection_detail()
                    .expect("rejected decision always has detail");
                tracing::warn!(
                    table = %table_name,
                    cost = estimate.total_cost,
                    rows = estimate.plan_rows,
                    expand_depth,
                    "EXPLAIN pre-check REJECTED query"
                );
                return Err(ApiError::too_expensive(msg, detail));
            }
        }
    }

    let timer = crate::metrics::QueryTimer::new(&table_name, "select");
    let rows = conn
        .fetch_all_uncached(&cmd)
        .await
        .map_err(|e| ApiError::query_error(e.to_string()));
    timer.finish(rows.is_ok());

    // Release connection early — after this point only JSON processing remains.
    // Branch overlay still needs conn, so we do it before release.
    let mut data: Vec<Value> = match &rows {
        Ok(rows) => rows.iter().map(row_to_json).collect(),
        Err(_) => Vec::new(),
    };

    // Branch overlay merge (CoW Read)
    let branch_ctx = extract_branch_from_headers(&headers);
    if let Some(branch_name) = branch_ctx.branch_name() {
        apply_branch_overlay(&mut conn, branch_name, &table_name, &mut data, "id").await;
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
        ).map_err(|v| {
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
            expand_nested(
                &state,
                &table_name,
                &mut data,
                &nested_rels,
                &auth,
            )
            .await?;
        }
    }

    // NDJSON streaming: one JSON object per line
    if is_streaming {
        let mut body = String::new();
        for row in &data {
            body.push_str(&serde_json::to_string(row).unwrap_or_default());
            body.push('\n');
        }
        return Ok(Response::builder()
            .header("Content-Type", "application/x-ndjson")
            .body(Body::from(body))
            .unwrap());
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
    if can_cache {
        if let Ok(json) = serde_json::to_string(&response_body) {
            state.cache.set(&cache_key, &table_name, json);
        }
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

    let _table = state
        .schema
        .table(&table_name)
        .ok_or_else(|| ApiError::not_found(&table_name))?;

    let auth = extract_auth_from_headers(&headers);

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
            .map(|s| Expr::Named(s.trim().to_string()))
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
        .pool
        .acquire_with_rls_timeout(auth.to_rls_context(), state.config.statement_timeout_ms)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let rows = conn
        .fetch_all_uncached(&cmd)
        .await
        .map_err(|e| { ApiError::query_error(e.to_string()) })?;

    conn.release().await;

    let data: Vec<Value> = rows.iter().map(row_to_json).collect();
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

    // Validate UUID format before hitting the database
    let parsed_uuid = Uuid::parse_str(&id)
        .map_err(|_| ApiError::parse_error(format!("Invalid UUID: {}", id)))?;

    let table = state
        .schema
        .table(&table_name)
        .ok_or_else(|| ApiError::not_found(&table_name))?;

    let pk = table
        .primary_key
        .as_ref()
        .ok_or_else(|| ApiError::internal("Table has no primary key"))?;

    let auth = extract_auth_from_headers(&headers);

    // Build: get table[pk = $id] — use Uuid value for correct PG type matching
    let mut cmd = qail_core::ast::Qail::get(&table_name)
        .filter(pk, Operator::Eq, QailValue::Uuid(parsed_uuid))
        .limit(1);

    // Apply RLS
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;

    // Execute
    let mut conn = state
        .pool
        .acquire_with_rls_timeout(auth.to_rls_context(), state.config.statement_timeout_ms)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let rows = conn
        .fetch_all_uncached(&cmd)
        .await
        .map_err(|e| ApiError::query_error(e.to_string()))?;

    let row = rows
        .first()
        .ok_or_else(|| ApiError::not_found(format!("{}/{}", table_name, id)))?;

    let mut data = row_to_json(row);

    // Branch overlay: check if this row is overridden on the branch
    let branch_ctx = extract_branch_from_headers(&headers);
    if let Some(branch_name) = branch_ctx.branch_name() {
        let sql = qail_pg::driver::branch_sql::read_overlay_sql(branch_name, &table_name);
        if let Ok(overlay_rows) = conn.get_mut().simple_query(&sql).await {
            for orow in &overlay_rows {
                let row_pk = orow.get_string(0).unwrap_or_default();
                if row_pk == id {
                    let operation = orow.get_string(1).unwrap_or_default();
                    match operation.as_str() {
                        "delete" => {
                            return Err(ApiError::not_found(format!("{}/{} (deleted on branch)", table_name, id)));
                        }
                        "update" | "insert" => {
                            let row_data_str = orow.get_string(2).unwrap_or_default();
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

    let table = state
        .schema
        .table(&table_name)
        .ok_or_else(|| ApiError::not_found(&table_name))?;

    let auth = extract_auth_from_headers(&headers);

    // Validate required columns upfront
    let required: Vec<String> = table
        .required_columns()
        .iter()
        .map(|c| c.name.clone())
        .collect();

    // Parse JSON body
    let body = axum::body::to_bytes(request.into_body(), 1024 * 1024)
        .await
        .map_err(|e| ApiError::parse_error(e.to_string()))?;
    let body: Value =
        serde_json::from_slice(&body).map_err(|e| ApiError::parse_error(e.to_string()))?;

    // Detect batch vs single
    let is_batch = body.is_array();
    let objects: Vec<&serde_json::Map<String, Value>> = if is_batch {
        body.as_array()
            .unwrap()
            .iter()
            .map(|v| {
                v.as_object()
                    .ok_or_else(|| ApiError::parse_error("Batch items must be JSON objects"))
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![body
            .as_object()
            .ok_or_else(|| ApiError::parse_error("Expected JSON object or array"))?]
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

    // Acquire connection
    let mut conn = state
        .pool
        .acquire_with_rls_timeout(auth.to_rls_context(), state.config.statement_timeout_ms)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    // Branch CoW Write: redirect inserts to overlay table
    let branch_ctx = extract_branch_from_headers(&headers);
    if let Some(branch_name) = branch_ctx.branch_name() {
        let mut all_results: Vec<Value> = Vec::with_capacity(objects.len());
        for obj in &objects {
            let row_data: Value = Value::Object((*obj).clone());
            let row_pk = obj
                .get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| Uuid::new_v4().to_string());

            redirect_to_overlay(&mut conn, branch_name, &table_name, &row_pk, "insert", &row_data).await?;
            all_results.push(row_data);
        }

        if is_batch {
            return Ok((
                StatusCode::CREATED,
                Json(json!({ "data": all_results, "count": all_results.len(), "branch": branch_name })),
            ));
        } else {
            let data = all_results.into_iter().next().unwrap_or_else(|| json!({"created": true}));
            return Ok((StatusCode::CREATED, Json(json!({ "data": data, "branch": branch_name }))));
        }
    }

    let mut all_results: Vec<Value> = Vec::with_capacity(objects.len());

    for obj in &objects {
        let mut cmd = qail_core::ast::Qail::add(&table_name);

        for (key, value) in *obj {
            let qail_val = json_to_qail_value(value);
            cmd = cmd.set_value(key, qail_val);
        }

        // Upsert support
        if let Some(ref conflict_col) = mutation_params.on_conflict {
            let conflict_cols: Vec<&str> = conflict_col.split(',').map(|s| s.trim()).collect();
            let action = mutation_params
                .on_conflict_action
                .as_deref()
                .unwrap_or("update");

            if action == "nothing" {
                cmd = cmd.on_conflict_nothing(&conflict_cols);
            } else {
                // Default: update all provided columns on conflict
                let updates: Vec<(&str, Expr)> = obj
                    .keys()
                    .filter(|k| !conflict_cols.contains(&k.as_str()))
                    .map(|k| (k.as_str(), Expr::Named(format!("EXCLUDED.{}", k))))
                    .collect();
                cmd = cmd.on_conflict_update(&conflict_cols, &updates);
            }
        }

        // Returning clause
        cmd = apply_returning(cmd, mutation_params.returning.as_deref());

        // Apply RLS
        state
            .policy_engine
            .apply_policies(&auth, &mut cmd)
            .map_err(|e| ApiError::forbidden(e.to_string()))?;

        let rows = conn
            .fetch_all_uncached(&cmd)
            .await
            .map_err(|e| ApiError::query_error(e.to_string()))?;

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
        state.event_engine.fire(
            &table_name,
            OperationType::Create,
            Some(data.clone()),
            None,
        );
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

    // Validate UUID format
    Uuid::parse_str(&id)
        .map_err(|_| ApiError::parse_error(format!("Invalid UUID: {}", id)))?;

    let table = state
        .schema
        .table(&table_name)
        .ok_or_else(|| ApiError::not_found(&table_name))?;

    let pk = table
        .primary_key
        .as_ref()
        .ok_or_else(|| ApiError::internal("Table has no primary key"))?
        .clone();

    let auth = extract_auth_from_headers(&headers);

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

    // Build: set table { col1 = val1 } [pk = $id]
    let mut cmd = qail_core::ast::Qail::set(&table_name)
        .filter(&pk, Operator::Eq, QailValue::String(id.clone()));

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

    // Execute
    let mut conn = state
        .pool
        .acquire_with_rls_timeout(auth.to_rls_context(), state.config.statement_timeout_ms)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    // Branch CoW Write: redirect updates to overlay
    let branch_ctx = extract_branch_from_headers(&headers);
    if let Some(branch_name) = branch_ctx.branch_name() {
        let row_data: Value = Value::Object(obj.clone());
        redirect_to_overlay(&mut conn, branch_name, &table_name, &id, "update", &row_data).await?;
        return Ok(Json(SingleResponse { data: json!({"updated": true, "branch": branch_name}) }));
    }

    let rows = conn
        .fetch_all_uncached(&cmd)
        .await
        .map_err(|e| ApiError::query_error(e.to_string()))?;

    let data = rows
        .first()
        .map(row_to_json)
        .unwrap_or_else(|| json!({"updated": true}));

    // Release connection before event processing
    conn.release().await;

    // Invalidate cache
    state.cache.invalidate_table(&table_name);

    // Fire event triggers
    state.event_engine.fire(
        &table_name,
        OperationType::Update,
        Some(data.clone()),
        None,
    );

    Ok(Json(SingleResponse { data }))
}

/// DELETE /api/{table}/:id — delete by PK
pub(crate) async fn delete_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    request: axum::extract::Request,
) -> Result<Json<DeleteResponse>, ApiError> {
    let table_name =
        extract_table_name(request.uri()).ok_or_else(|| ApiError::not_found("table"))?;

    // Validate UUID format
    Uuid::parse_str(&id)
        .map_err(|_| ApiError::parse_error(format!("Invalid UUID: {}", id)))?;

    let table = state
        .schema
        .table(&table_name)
        .ok_or_else(|| ApiError::not_found(&table_name))?;

    let pk = table
        .primary_key
        .as_ref()
        .ok_or_else(|| ApiError::internal("Table has no primary key"))?
        .clone();

    let auth = extract_auth_from_headers(&headers);

    // Build: del table[pk = $id]
    let mut cmd =
        qail_core::ast::Qail::del(&table_name).filter(&pk, Operator::Eq, QailValue::String(id.clone()));

    // Apply RLS
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;

    // Execute
    let mut conn = state
        .pool
        .acquire_with_rls_timeout(auth.to_rls_context(), state.config.statement_timeout_ms)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    // Branch CoW Write: redirect deletes to overlay (tombstone)
    let branch_ctx = extract_branch_from_headers(&headers);
    if let Some(branch_name) = branch_ctx.branch_name() {
        redirect_to_overlay(&mut conn, branch_name, &table_name, &id, "delete", &Value::Null).await?;
        return Ok(Json(DeleteResponse { deleted: true }));
    }

    conn.fetch_all_uncached(&cmd)
        .await
        .map_err(|e| ApiError::query_error(e.to_string()))?;

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

    Ok(Json(DeleteResponse { deleted: true }))
}
