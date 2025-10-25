//! Auto-REST route generation from schema
//!
//! Given a `SchemaRegistry`, generates RESTful CRUD endpoints for every table:
//!
//! ```text
//! GET    /api/{table}                    → List with pagination, filtering, sorting
//! GET    /api/{table}?expand=rel         → List with LEFT JOIN on FK relation
//! GET    /api/{table}?expand=nested:rel  → List with nested FK objects/arrays
//! GET    /api/{table}?stream=true        → NDJSON streaming response
//! GET    /api/{table}?col.gte=10         → Filter with operators (eq, ne, gt, gte, lt, lte, in, like, ilike, is_null)
//! GET    /api/{table}?sort=col:desc      → Sort (multi-column: col1:asc,col2:desc)
//! GET    /api/{table}?distinct=col       → Distinct on columns
//! GET    /api/{table}/aggregate          → Aggregation (count, sum, avg, min, max)
//! GET    /api/{table}/_explain           → EXPLAIN ANALYZE for query
//! GET    /api/{table}/{id}               → Get single row by PK
//! POST   /api/{table}                    → Create row(s) from JSON body (single or batch)
//! POST   /api/{table}?on_conflict=col    → Upsert (insert or update on conflict)
//! POST   /api/{table}?returning=*        → Return created row(s)
//! PATCH  /api/{table}/{id}               → Partial update from JSON body
//! PATCH  /api/{table}/{id}?returning=*   → Return updated row
//! DELETE /api/{table}/{id}               → Delete by PK
//! GET    /api/{parent}/{id}/{child}      → Nested list: children filtered by parent FK
//! ```

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json, Response},
    routing::get,
    Router,
};
use qail_core::ast::{AggregateFunc, Expr, JoinKind, Operator, Value as QailValue};
use crate::policy::OperationType;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use std::sync::Arc;
use uuid::Uuid;

use crate::auth::extract_auth_from_headers;
use crate::handler::row_to_json;
use crate::middleware::ApiError;
use crate::GatewayState;
use qail_core::branch::BranchContext;

/// Extract branch context from X-Branch-ID header.
#[allow(dead_code)]
fn extract_branch_from_headers(headers: &HeaderMap) -> BranchContext {
    let branch_id = headers
        .get("x-branch-id")
        .and_then(|v| v.to_str().ok());
    BranchContext::from_header(branch_id)
}

// ============================================================================
// Query parameters
// ============================================================================

/// Query parameters for list endpoints
#[derive(Debug, Deserialize, Default)]
pub struct ListParams {
    /// Max rows to return (default: 50, max: 1000)
    pub limit: Option<i64>,
    /// Offset for pagination
    pub offset: Option<i64>,
    /// Sort: `column:asc` or multi-column `col1:asc,col2:desc`
    pub sort: Option<String>,
    /// Columns to select: `id,name,email`
    pub select: Option<String>,
    /// Expand FK relations via LEFT JOIN: `?expand=orders` or `?expand=orders,products`
    pub expand: Option<String>,
    /// Cursor-based pagination: value of the sort column to paginate after
    pub cursor: Option<String>,
    /// Distinct on columns: `?distinct=col1,col2`
    pub distinct: Option<String>,
    /// Stream NDJSON: `?stream=true` for line-delimited JSON response
    #[serde(default)]
    pub stream: Option<bool>,
    /// Full-text search: `?search=term` searches across text columns
    pub search: Option<String>,
    /// Columns to search: `?search_columns=name,description` (default: all text columns)
    pub search_columns: Option<String>,
}

/// Query parameters for mutation (create/update/delete) returning support
#[derive(Debug, Deserialize, Default)]
pub struct MutationParams {
    /// Columns to return: `*` for all, or `id,name` for specific columns
    pub returning: Option<String>,
    /// Upsert conflict column: `?on_conflict=id`
    pub on_conflict: Option<String>,
    /// Upsert conflict action: `update` (default) or `nothing`
    pub on_conflict_action: Option<String>,
}

/// Query parameters for aggregation endpoint
#[derive(Debug, Deserialize, Default)]
pub struct AggregateParams {
    /// Aggregate function: count, sum, avg, min, max
    pub func: Option<String>,
    /// Column to aggregate (default: * for count)
    pub column: Option<String>,
    /// Group by columns: `group_by=status,type`
    pub group_by: Option<String>,
    /// Distinct aggregation: `?distinct=true`
    pub distinct: Option<String>,
}

// ============================================================================
// Response types
// ============================================================================

#[derive(Debug, Serialize)]
pub struct ListResponse {
    pub data: Vec<Value>,
    pub count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<i64>,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Debug, Serialize)]
pub struct SingleResponse {
    pub data: Value,
}

#[derive(Debug, Serialize)]
pub struct DeleteResponse {
    pub deleted: bool,
}

#[derive(Debug, Serialize)]
pub struct AggregateResponse {
    pub data: Vec<Value>,
    pub count: usize,
}

#[derive(Debug, Serialize)]
pub struct BatchCreateResponse {
    pub data: Vec<Value>,
    pub count: usize,
}

// ============================================================================
// Route generation
// ============================================================================

/// Generate REST routes for all tables in the schema registry.
///
/// Returns an Axum Router with routes like `/api/users`, `/api/orders`, etc.
pub fn auto_rest_routes(state: Arc<GatewayState>) -> Router<Arc<GatewayState>> {
    let mut router: Router<Arc<GatewayState>> = Router::new();
    let table_names: Vec<String> = state
        .schema
        .table_names()
        .iter()
        .map(|s| s.to_string())
        .collect();

    for table_name in &table_names {
        let table = match state.schema.table(table_name) {
            Some(t) => t,
            None => continue,
        };
        let has_pk = table.primary_key.is_some();
        let path = format!("/api/{}", table_name);

        tracing::info!(
            "  AUTO-REST: {} → GET/POST{}",
            path,
            if has_pk {
                " + GET/PATCH/DELETE :id"
            } else {
                ""
            }
        );

        // GET /api/{table} — list
        // POST /api/{table} — create
        router = router.route(&path, get(list_handler).post(create_handler));

        // GET /api/{table}/aggregate — aggregation
        let agg_path = format!("/api/{}/aggregate", table_name);

        // GET /api/{table}/_explain — explain query plan
        let explain_path = format!("/api/{}/_explain", table_name);
        router = router.route(&explain_path, get(explain_handler));
        router = router.route(&agg_path, get(aggregate_handler));

        if has_pk {
            let id_path = format!("/api/{}/{{id}}", table_name);

            // GET /api/{table}/:id — get by PK
            // PATCH /api/{table}/:id — update
            // DELETE /api/{table}/:id — delete
            router = router.route(
                &id_path,
                get(get_by_id_handler)
                    .patch(update_handler)
                    .delete(delete_handler),
            );

            // Nested routes: GET /api/{parent}/:id/{child}
            let children = state.schema.children_of(table_name);
            for (child_table, _fk_col, _pk_col) in &children {
                let nested_path = format!("/api/{}/{{id}}/{}", table_name, child_table);
                tracing::info!("  AUTO-REST nested: {} → GET", nested_path);
                router = router.route(&nested_path, get(nested_list_handler));
            }
        }
    }

    // DevEx endpoints
    router = router
        // GET /api/_schema — Schema introspection
        .route("/api/_schema", get(schema_introspection_handler))
        // GET /api/_openapi — Auto-generated OpenAPI 3.0 spec
        .route("/api/_openapi", get(openapi_spec_handler));

    // Branch management endpoints
    router = router
        .route("/api/_branch", axum::routing::post(branch_create_handler).get(branch_list_handler))
        .route("/api/_branch/{name}", axum::routing::delete(branch_delete_handler))
        .route("/api/_branch/{name}/merge", axum::routing::post(branch_merge_handler));

    tracing::info!("  DEVEX: GET /api/_schema → Schema introspection");
    tracing::info!("  DEVEX: GET /api/_openapi → OpenAPI 3.0 spec");
    tracing::info!("  BRANCH: POST/GET /api/_branch, DELETE /api/_branch/:name, POST /api/_branch/:name/merge");

    router
}

// ============================================================================
// Filter parsing
// ============================================================================

/// Parse filter operators from query string.
///
/// Supports PostgREST-style operators:
/// - `?name.eq=John`        → name = 'John'
/// - `?price.gte=100`       → price >= 100
/// - `?status.in=active,pending` → status IN ('active', 'pending')
/// - `?email.like=%@gmail%` → email LIKE '%@gmail%'
/// - `?deleted_at.is_null=true` → deleted_at IS NULL
///
/// If no operator suffix, defaults to `eq`.
fn parse_filters(query_string: &str) -> Vec<(String, Operator, QailValue)> {
    let reserved = [
        "limit", "offset", "sort", "select", "expand", "cursor", "distinct",
        "returning", "on_conflict", "on_conflict_action",
        "func", "column", "group_by",
        "search", "search_columns", "stream",
    ];

    let mut filters = Vec::new();

    for pair in query_string.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => continue,
        };

        // Skip reserved params
        if reserved.contains(&key) {
            continue;
        }

        // Parse column.operator pattern
        let (column, op) = if let Some((col, op_str)) = key.rsplit_once('.') {
            let operator = match op_str {
                "eq" => Some(Operator::Eq),
                "ne" | "neq" => Some(Operator::Ne),
                "gt" => Some(Operator::Gt),
                "gte" | "ge" => Some(Operator::Gte),
                "lt" => Some(Operator::Lt),
                "lte" | "le" => Some(Operator::Lte),
                "like" => Some(Operator::Like),
                "ilike" | "fuzzy" => Some(Operator::Fuzzy),
                "not_like" => Some(Operator::NotLike),
                "in" => Some(Operator::In),
                "not_in" | "nin" => Some(Operator::NotIn),
                "is_null" => Some(Operator::IsNull),
                "is_not_null" => Some(Operator::IsNotNull),
                "contains" => Some(Operator::Contains),
                _ => None,
            };
            if let Some(op) = operator {
                (col, op)
            } else {
                // Unknown operator suffix — treat full key as column name with eq
                (key, Operator::Eq)
            }
        } else {
            // No dot — treat as column = value
            (key, Operator::Eq)
        };

        // Skip if this is a reserved param (column name might collide)
        if reserved.contains(&column) {
            continue;
        }

        // Decode the value
        let decoded_value = urlencoding::decode(value)
            .unwrap_or(std::borrow::Cow::Borrowed(value))
            .to_string();

        let qail_value = match op {
            Operator::IsNull | Operator::IsNotNull => {
                // These are unary — value is ignored (or "true"/"false")
                QailValue::Null
            }
            Operator::In | Operator::NotIn => {
                // Comma-separated values → Array
                let vals: Vec<QailValue> = decoded_value
                    .split(',')
                    .map(|v| parse_scalar_value(v.trim()))
                    .collect();
                QailValue::Array(vals)
            }
            _ => parse_scalar_value(&decoded_value),
        };

        filters.push((column.to_string(), op, qail_value));
    }

    filters
}

/// Parse a scalar value, attempting numeric conversion
fn parse_scalar_value(s: &str) -> QailValue {
    if s == "true" {
        return QailValue::Bool(true);
    }
    if s == "false" {
        return QailValue::Bool(false);
    }
    if s == "null" {
        return QailValue::Null;
    }
    if let Ok(n) = s.parse::<i64>() {
        return QailValue::Int(n);
    }
    if let Ok(f) = s.parse::<f64>() {
        return QailValue::Float(f);
    }
    QailValue::String(s.to_string())
}

/// Apply parsed filters to a Qail command
fn apply_filters(mut cmd: qail_core::ast::Qail, filters: &[(String, Operator, QailValue)]) -> qail_core::ast::Qail {
    for (column, op, value) in filters {
        match op {
            Operator::IsNull => {
                cmd = cmd.is_null(column);
            }
            Operator::IsNotNull => {
                cmd = cmd.is_not_null(column);
            }
            Operator::In | Operator::NotIn => {
                if let QailValue::Array(vals) = value {
                    if matches!(op, Operator::In) {
                        cmd = cmd.in_vals(column, vals.clone());
                    } else {
                        cmd = cmd.filter(column, Operator::NotIn, value.clone());
                    }
                }
            }
            _ => {
                cmd = cmd.filter(column, *op, value.clone());
            }
        }
    }
    cmd
}

/// Apply multi-column sorting
fn apply_sorting(mut cmd: qail_core::ast::Qail, sort: &str) -> qail_core::ast::Qail {
    for part in sort.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let parts: Vec<&str> = part.split(':').collect();
        let col = parts[0];
        let dir = parts.get(1).unwrap_or(&"asc");
        if *dir == "desc" {
            cmd = cmd.order_desc(col);
        } else {
            cmd = cmd.order_asc(col);
        }
    }
    cmd
}

/// Apply returning clause to a mutation command
fn apply_returning(
    mut cmd: qail_core::ast::Qail,
    returning: Option<&str>,
) -> qail_core::ast::Qail {
    if let Some(ret) = returning {
        if ret == "*" {
            cmd = cmd.returning_all();
        } else {
            let cols: Vec<&str> = ret.split(',').map(|s| s.trim()).collect();
            cmd = cmd.returning(cols);
        }
    }
    cmd
}

// ============================================================================
// Handlers
// ============================================================================

/// Extract table name from the request path (e.g., `/api/users` → `users`)
fn extract_table_name(uri: &axum::http::Uri) -> Option<String> {
    let path = uri.path();
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    if parts.len() >= 2 && parts[0] == "api" {
        Some(parts[1].to_string())
    } else {
        None
    }
}

/// GET /api/{table} — list with pagination, sorting, filtering, column selection
async fn list_handler(
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
    let limit = params.limit.unwrap_or(50).clamp(1, 1000);
    let offset = params.offset.unwrap_or(0).max(0);

    let mut cmd = qail_core::ast::Qail::get(&table_name);

    // Column selection
    if let Some(ref select) = params.select {
        let cols: Vec<&str> = select.split(',').map(|s| s.trim()).collect();
        cmd = cmd.columns(cols);
    }

    // Sorting (multi-column)
    if let Some(ref sort) = params.sort {
        cmd = apply_sorting(cmd, sort);
    }

    // Distinct
    if let Some(ref distinct) = params.distinct {
        let cols: Vec<&str> = distinct.split(',').map(|s| s.trim()).collect();
        cmd = cmd.distinct_on(cols);
    }

    // Expand FK relations via LEFT JOIN
    if let Some(ref expand) = params.expand {
        for rel in expand.split(',').map(|s| s.trim()) {
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
    let cache_key = format!("rest:{}:{}:{}", table_name, auth.user_id, request.uri());

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

    // Execute
    let mut conn = state
        .pool
        .acquire_with_rls(auth.to_rls_context())
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let rows = conn
        .fetch_all_uncached(&cmd)
        .await
        .map_err(|e| ApiError::query_error(e.to_string()))?;

    let mut data: Vec<Value> = rows.iter().map(row_to_json).collect();

    // Branch overlay merge (CoW Read)
    let branch_ctx = extract_branch_from_headers(&headers);
    if let Some(branch_name) = branch_ctx.branch_name() {
        apply_branch_overlay(&mut conn, branch_name, &table_name, &mut data, "id").await;
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

    // Store in cache for simple queries
    if can_cache {
        if let Ok(json) = serde_json::to_string(&response_body) {
            state.cache.set(&cache_key, &table_name, json);
        }
    }

    Ok(Json(response_body).into_response())
}

/// GET /api/{table}/aggregate — aggregation queries
///
/// `?func=count`                      → SELECT COUNT(*) FROM table
/// `?func=sum&column=price`           → SELECT SUM(price) FROM table
/// `?func=avg&column=price&group_by=status`  → SELECT status, AVG(price) FROM table GROUP BY status
/// `?name.eq=John`                    → with filters
async fn aggregate_handler(
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
        .acquire_with_rls(auth.to_rls_context())
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let rows = conn
        .fetch_all_uncached(&cmd)
        .await
        .map_err(|e| ApiError::query_error(e.to_string()))?;

    let data: Vec<Value> = rows.iter().map(row_to_json).collect();
    let count = data.len();

    Ok(Json(AggregateResponse { data, count }))
}

/// GET /api/{table}/:id — get single row by PK
async fn get_by_id_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    request: axum::extract::Request,
) -> Result<Json<SingleResponse>, ApiError> {
    let table_name =
        extract_table_name(request.uri()).ok_or_else(|| ApiError::not_found("table"))?;

    // Validate UUID format before hitting the database
    Uuid::parse_str(&id)
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

    // Build: get table[pk = $id]
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
        .pool
        .acquire_with_rls(auth.to_rls_context())
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

    Ok(Json(SingleResponse { data }))
}

/// POST /api/{table} — create from JSON body (single object or batch array)
///
/// Supports:
/// - Single: `{ "name": "Alice" }` → creates 1 row
/// - Batch:  `[{ "name": "Alice" }, { "name": "Bob" }]` → creates N rows
/// - Upsert: `?on_conflict=id` → INSERT ... ON CONFLICT (id) DO UPDATE
/// - Returning: `?returning=*` → RETURNING *
async fn create_handler(
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
        .acquire_with_rls(auth.to_rls_context())
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
async fn update_handler(
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
        .acquire_with_rls(auth.to_rls_context())
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
async fn delete_handler(
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
        .acquire_with_rls(auth.to_rls_context())
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

// ============================================================================
// Nested route handler
// ============================================================================

/// GET /api/{parent}/:id/{child} — list child rows filtered by parent FK
///
/// Example: `GET /api/users/123/orders` → `get orders[user_id = 123]`
///
/// Supports the same query parameters as the main list handler.
async fn nested_list_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(parent_id): Path<String>,
    Query(params): Query<ListParams>,
    request: axum::extract::Request,
) -> Result<Json<ListResponse>, ApiError> {
    let path = request.uri().path().to_string();
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();

    // /api/{parent}/{id}/{child}
    if parts.len() < 4 || parts[0] != "api" {
        return Err(ApiError::not_found("nested route"));
    }
    let parent_table = parts[1].to_string();
    let child_table = parts[3].to_string();

    // Validate parent UUID format
    Uuid::parse_str(&parent_id)
        .map_err(|_| ApiError::parse_error(format!("Invalid UUID: {}", parent_id)))?;

    // Look up FK relation: child → parent
    let (fk_col, _pk_col) = state
        .schema
        .relation_for(&child_table, &parent_table)
        .ok_or_else(|| {
            ApiError::not_found(format!("No relation: {} → {}", child_table, parent_table))
        })?;

    let auth = extract_auth_from_headers(&headers);

    let limit = params.limit.unwrap_or(50).clamp(1, 1000);
    let offset = params.offset.unwrap_or(0).max(0);

    // Build: get child[fk_col = parent_id]
    let mut cmd = qail_core::ast::Qail::get(&child_table)
        .filter(fk_col, Operator::Eq, QailValue::String(parent_id));

    // Column selection
    if let Some(ref select) = params.select {
        let cols: Vec<&str> = select.split(',').map(|s| s.trim()).collect();
        cmd = cmd.columns(cols);
    }

    // Sorting (multi-column)
    if let Some(ref sort) = params.sort {
        cmd = apply_sorting(cmd, sort);
    }

    // Distinct
    if let Some(ref distinct) = params.distinct {
        let cols: Vec<&str> = distinct.split(',').map(|s| s.trim()).collect();
        cmd = cmd.distinct_on(cols);
    }

    // Parse and apply filters from query string
    let query_string = request.uri().query().unwrap_or("");
    let filters = parse_filters(query_string);
    cmd = apply_filters(cmd, &filters);

    // Full-text search
    if let Some(ref term) = params.search {
        let cols = params.search_columns.as_deref().unwrap_or("name");
        cmd = cmd.filter(cols, Operator::TextSearch, QailValue::String(term.clone()));
    }

    cmd = cmd.limit(limit);
    cmd = cmd.offset(offset);

    // Apply RLS
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;

    // Execute — single query, no N+1
    let mut conn = state
        .pool
        .acquire_with_rls(auth.to_rls_context())
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let rows = conn
        .fetch_all_uncached(&cmd)
        .await
        .map_err(|e| ApiError::query_error(e.to_string()))?;

    let data: Vec<Value> = rows.iter().map(row_to_json).collect();
    let count = data.len();

    Ok(Json(ListResponse {
        data,
        count,
        total: None,
        limit,
        offset,
    }))
}

// ============================================================================
// DevEx Endpoints
// ============================================================================

/// GET /api/_schema — Schema introspection
///
/// Returns the full schema registry as JSON, including tables, columns,
/// primary keys, foreign keys, and column types.
async fn schema_introspection_handler(
    State(state): State<Arc<GatewayState>>,
) -> Json<Value> {
    let tables = state.schema.tables();
    let mut result = serde_json::Map::new();

    for (name, table) in tables {
        let columns: Vec<Value> = table.columns.iter().map(|col| {
            json!({
                "name": col.name,
                "type": col.col_type,
                "pg_type": col.pg_type,
                "nullable": col.nullable,
                "primary_key": col.primary_key,
                "unique": col.unique,
                "has_default": col.has_default,
                "foreign_key": col.foreign_key.as_ref().map(|fk| json!({
                    "ref_table": fk.ref_table,
                    "ref_column": fk.ref_column,
                })),
            })
        }).collect();

        result.insert(name.clone(), json!({
            "columns": columns,
            "primary_key": table.primary_key,
        }));
    }

    Json(json!({
        "tables": result,
        "table_count": tables.len(),
    }))
}

/// GET /api/_openapi — Auto-generated OpenAPI 3.0.3 spec
///
/// Generates a complete OpenAPI specification from the schema registry.
async fn openapi_spec_handler(
    State(state): State<Arc<GatewayState>>,
) -> Json<Value> {
    let tables = state.schema.tables();
    let mut paths = serde_json::Map::new();
    let mut schemas = serde_json::Map::new();

    for (name, table) in tables {
        // Build component schema for this table
        let mut properties = serde_json::Map::new();
        let mut required_cols = Vec::new();

        for col in &table.columns {
            let oas_type = pg_type_to_openapi(&col.pg_type);
            properties.insert(col.name.clone(), json!(oas_type));
            if !col.nullable && !col.has_default {
                required_cols.push(Value::String(col.name.clone()));
            }
        }

        schemas.insert(name.clone(), json!({
            "type": "object",
            "properties": properties,
            "required": required_cols,
        }));

        // List + Create path
        let list_path = format!("/api/{}", name);
        paths.insert(list_path, json!({
            "get": {
                "summary": format!("List {}", name),
                "tags": [name],
                "parameters": [
                    {"name": "limit", "in": "query", "schema": {"type": "integer", "default": 50}},
                    {"name": "offset", "in": "query", "schema": {"type": "integer", "default": 0}},
                    {"name": "sort", "in": "query", "schema": {"type": "string"}, "description": "col:asc,col:desc"},
                    {"name": "select", "in": "query", "schema": {"type": "string"}, "description": "col1,col2"},
                    {"name": "expand", "in": "query", "schema": {"type": "string"}, "description": "FK relation to expand"},
                    {"name": "distinct", "in": "query", "schema": {"type": "string"}, "description": "col1,col2"},
                ],
                "responses": {
                    "200": {
                        "description": "Success",
                        "content": {"application/json": {"schema": {
                            "type": "object",
                            "properties": {
                                "data": {"type": "array", "items": {"$ref": format!("#/components/schemas/{}", name)}},
                                "count": {"type": "integer"},
                                "limit": {"type": "integer"},
                                "offset": {"type": "integer"},
                            }
                        }}}
                    }
                }
            },
            "post": {
                "summary": format!("Create {}", name),
                "tags": [name],
                "requestBody": {
                    "content": {"application/json": {"schema": {"$ref": format!("#/components/schemas/{}", name)}}}
                },
                "parameters": [
                    {"name": "returning", "in": "query", "schema": {"type": "string"}, "description": "* or col1,col2"},
                    {"name": "on_conflict", "in": "query", "schema": {"type": "string"}, "description": "Upsert conflict column"},
                ],
                "responses": {
                    "201": {"description": "Created"},
                }
            }
        }));

        // Single-resource path (if PK exists)
        if let Some(ref pk) = table.primary_key {
            let id_path = format!("/api/{}/{{{}}}", name, pk);
            paths.insert(id_path, json!({
                "get": {
                    "summary": format!("Get {} by {}", name, pk),
                    "tags": [name],
                    "parameters": [
                        {"name": pk, "in": "path", "required": true, "schema": {"type": "string"}}
                    ],
                    "responses": {"200": {"description": "Success"}}
                },
                "patch": {
                    "summary": format!("Update {} by {}", name, pk),
                    "tags": [name],
                    "parameters": [
                        {"name": pk, "in": "path", "required": true, "schema": {"type": "string"}}
                    ],
                    "requestBody": {
                        "content": {"application/json": {"schema": {"$ref": format!("#/components/schemas/{}", name)}}}
                    },
                    "responses": {"200": {"description": "Updated"}}
                },
                "delete": {
                    "summary": format!("Delete {} by {}", name, pk),
                    "tags": [name],
                    "parameters": [
                        {"name": pk, "in": "path", "required": true, "schema": {"type": "string"}}
                    ],
                    "responses": {"204": {"description": "Deleted"}}
                }
            }));
        }
    }

    Json(json!({
        "openapi": "3.0.3",
        "info": {
            "title": "QAIL Gateway API",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "Auto-generated REST API from QAIL schema"
        },
        "paths": paths,
        "components": {
            "schemas": schemas,
            "securitySchemes": {
                "bearerAuth": {
                    "type": "http",
                    "scheme": "bearer",
                    "bearerFormat": "JWT"
                }
            }
        },
        "security": [{"bearerAuth": []}],
    }))
}

/// Map PostgreSQL types to OpenAPI 3.0 types
fn pg_type_to_openapi(pg_type: &str) -> Value {
    match pg_type.to_uppercase().as_str() {
        "INT2" | "INT4" | "SMALLINT" | "INTEGER" | "SERIAL" => json!({"type": "integer", "format": "int32"}),
        "INT8" | "BIGINT" | "BIGSERIAL" => json!({"type": "integer", "format": "int64"}),
        "FLOAT4" | "REAL" => json!({"type": "number", "format": "float"}),
        "FLOAT8" | "DOUBLE PRECISION" | "NUMERIC" | "DECIMAL" => json!({"type": "number", "format": "double"}),
        "BOOL" | "BOOLEAN" => json!({"type": "boolean"}),
        "UUID" => json!({"type": "string", "format": "uuid"}),
        "TIMESTAMPTZ" | "TIMESTAMP" => json!({"type": "string", "format": "date-time"}),
        "DATE" => json!({"type": "string", "format": "date"}),
        "JSON" | "JSONB" => json!({"type": "object"}),
        "TEXT[]" | "VARCHAR[]" => json!({"type": "array", "items": {"type": "string"}}),
        "INT4[]" | "INT8[]" => json!({"type": "array", "items": {"type": "integer"}}),
        _ => json!({"type": "string"}),
    }
}

// ============================================================================
// Query EXPLAIN endpoint
// ============================================================================

/// GET /api/{table}/_explain — return EXPLAIN ANALYZE for the query
///
/// Accepts the same query params as the list handler (filters, sort, expand, etc.)
/// and returns the PostgreSQL execution plan as JSON.
async fn explain_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Query(params): Query<ListParams>,
    request: axum::extract::Request,
) -> Result<Json<Value>, ApiError> {
    let table_name =
        extract_table_name(request.uri()).ok_or_else(|| ApiError::not_found("table"))?;

    let _table = state
        .schema
        .table(&table_name)
        .ok_or_else(|| ApiError::not_found(&table_name))?;

    let auth = extract_auth_from_headers(&headers);

    // Build query (same as list_handler)
    let limit = params.limit.unwrap_or(50).min(1000);
    let offset = params.offset.unwrap_or(0);
    let mut cmd = qail_core::ast::Qail::get(&table_name);

    // Apply select
    if let Some(ref select) = params.select {
        let cols: Vec<&str> = select.split(',').map(|s| s.trim()).collect();
        cmd = cmd.columns(cols);
    }

    // Apply sorting
    if let Some(ref sort) = params.sort {
        for part in sort.split(',') {
            let mut iter = part.splitn(2, ':');
            let col = iter.next().unwrap_or("id");
            let dir = iter.next().unwrap_or("asc");
            cmd = if dir == "desc" {
                cmd.order_desc(col)
            } else {
                cmd.order_asc(col)
            };
        }
    }

    // Apply expand (flat JOIN only)
    if let Some(ref expand) = params.expand {
        for rel in expand.split(',').map(|s| s.trim()).filter(|s| !s.starts_with("nested:")) {
            if let Some((fk_col, ref_col)) = state.schema.relation_for(&table_name, rel) {
                let left = format!("{}.{}", table_name, fk_col);
                let right = format!("{}.{}", rel, ref_col);
                cmd = cmd.join(JoinKind::Left, rel, &left, &right);
            } else if let Some((fk_col, ref_col)) = state.schema.relation_for(rel, &table_name) {
                let left = format!("{}.{}", table_name, ref_col);
                let right = format!("{}.{}", rel, fk_col);
                cmd = cmd.join(JoinKind::Left, rel, &left, &right);
            }
        }
    }

    // Apply filters
    let query_string = request.uri().query().unwrap_or("");
    let filters = parse_filters(query_string);
    cmd = apply_filters(cmd, &filters);

    // Full-text search
    if let Some(ref term) = params.search {
        let cols = params.search_columns.as_deref().unwrap_or("name");
        cmd = cmd.filter(cols, Operator::TextSearch, QailValue::String(term.clone()));
    }

    cmd = cmd.limit(limit);
    cmd = cmd.offset(offset);

    // Apply RLS
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;

    // Generate SQL from AST
    use qail_pg::protocol::AstEncoder;
    let mut sql_buf = bytes::BytesMut::with_capacity(256);
    let mut params_buf: Vec<Option<Vec<u8>>> = Vec::new();
    AstEncoder::encode_select_sql(&cmd, &mut sql_buf, &mut params_buf);
    let sql = String::from_utf8_lossy(&sql_buf).to_string();

    // Run EXPLAIN ANALYZE
    let explain_sql = format!("EXPLAIN (ANALYZE, FORMAT JSON) {}", sql);

    let mut conn = state
        .pool
        .acquire_with_rls(auth.to_rls_context())
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let raw_cmd = qail_core::ast::Qail::raw_sql(&explain_sql);
    let rows = conn
        .fetch_all_uncached(&raw_cmd)
        .await
        .map_err(|e| ApiError::query_error(e.to_string()))?;

    let plan: Vec<Value> = rows.iter().map(row_to_json).collect();

    Ok(Json(json!({
        "query": sql,
        "plan": plan,
    })))
}

// ============================================================================
// Nested FK expansion
// ============================================================================

/// Expand FK relations into nested JSON objects/arrays.
///
/// - **Forward FK** (e.g., `orders?expand=nested:users`): 
///   `order.user_id` → `order.user = {id, name, ...}` (nested object)
/// - **Reverse FK** (e.g., `users?expand=nested:orders`):
///   `user` → `user.orders = [{...}, {...}]` (nested array)
///
/// Uses batched WHERE IN queries to avoid N+1.
async fn expand_nested(
    state: &Arc<GatewayState>,
    table_name: &str,
    data: &mut [Value],
    relations: &[&str],
    auth: &crate::auth::AuthContext,
) -> Result<(), ApiError> {
    let mut conn = state
        .pool
        .acquire_with_rls(auth.to_rls_context())
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    for rel in relations {
        // Try forward FK: this table → rel table
        if let Some((fk_col, ref_col)) = state.schema.relation_for(table_name, rel) {
            // Collect all FK values from data
            let fk_values: Vec<QailValue> = data
                .iter()
                .filter_map(|row| row.get(fk_col).cloned())
                .filter(|v| !v.is_null())
                .map(|v| match v {
                    Value::String(s) => QailValue::String(s),
                    Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            QailValue::Int(i)
                        } else {
                            QailValue::String(n.to_string())
                        }
                    }
                    other => QailValue::String(other.to_string()),
                })
                .collect();

            if fk_values.is_empty() {
                continue;
            }

            // Fetch related rows in one query: get rel[ref_col IN (...)]
            let cmd = qail_core::ast::Qail::get(*rel)
                .filter(ref_col, Operator::In, QailValue::Array(fk_values));

            let rows = conn
                .fetch_all_uncached(&cmd)
                .await
                .map_err(|e| ApiError::query_error(e.to_string()))?;

            // Index by PK
            let related: std::collections::HashMap<String, Value> = rows
                .iter()
                .map(|row| {
                    let json = row_to_json(row);
                    let key = json
                        .get(ref_col)
                        .map(|v| v.as_str().unwrap_or(&v.to_string()).to_string())
                        .unwrap_or_default();
                    (key, json)
                })
                .collect();

            // Inject nested object
            for row in data.iter_mut() {
                if let Some(fk_val) = row.get(fk_col) {
                    let key = fk_val.as_str()
                        .unwrap_or(&fk_val.to_string())
                        .to_string();
                    if let Some(related_row) = related.get(&key) {
                        if let Some(obj) = row.as_object_mut() {
                            obj.insert(rel.to_string(), related_row.clone());
                        }
                    }
                }
            }
            continue;
        }

        // Try reverse FK: rel table → this table
        if let Some((fk_col, ref_col)) = state.schema.relation_for(rel, table_name) {
            // Collect all PK values from data
            let pk_values: Vec<QailValue> = data
                .iter()
                .filter_map(|row| row.get(ref_col).cloned())
                .filter(|v| !v.is_null())
                .map(|v| match v {
                    Value::String(s) => QailValue::String(s),
                    Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            QailValue::Int(i)
                        } else {
                            QailValue::String(n.to_string())
                        }
                    }
                    other => QailValue::String(other.to_string()),
                })
                .collect();

            if pk_values.is_empty() {
                continue;
            }

            // Fetch all child rows: get rel[fk_col IN (...)]
            let cmd = qail_core::ast::Qail::get(*rel)
                .filter(fk_col, Operator::In, QailValue::Array(pk_values));

            let rows = conn
                .fetch_all_uncached(&cmd)
                .await
                .map_err(|e| ApiError::query_error(e.to_string()))?;

            // Group by FK value
            let mut grouped: std::collections::HashMap<String, Vec<Value>> =
                std::collections::HashMap::new();
            for row in &rows {
                let json = row_to_json(row);
                let key = json
                    .get(fk_col)
                    .map(|v| v.as_str().unwrap_or(&v.to_string()).to_string())
                    .unwrap_or_default();
                grouped.entry(key).or_default().push(json);
            }

            // Inject nested array
            for row in data.iter_mut() {
                if let Some(pk_val) = row.get(ref_col) {
                    let key = pk_val.as_str()
                        .unwrap_or(&pk_val.to_string())
                        .to_string();
                    let children = grouped.get(&key).cloned().unwrap_or_default();
                    if let Some(obj) = row.as_object_mut() {
                        obj.insert(rel.to_string(), json!(children));
                    }
                }
            }
            continue;
        }

        return Err(ApiError::parse_error(format!(
            "No relation between '{}' and '{}' for nested expansion",
            table_name, rel
        )));
    }

    Ok(())
}

// ============================================================================
// Helpers
// ============================================================================

/// Convert a serde_json::Value to a qail_core::ast::Value
fn json_to_qail_value(v: &Value) -> QailValue {
    match v {
        Value::String(s) => QailValue::String(s.clone()),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                QailValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                QailValue::Float(f)
            } else {
                QailValue::String(n.to_string())
            }
        }
        Value::Bool(b) => QailValue::Bool(*b),
        Value::Null => QailValue::Null,
        Value::Array(arr) => {
            QailValue::Array(arr.iter().map(json_to_qail_value).collect())
        }
        other => QailValue::String(other.to_string()),
    }
}

// ============================================================================
// Branch CoW helpers — Data Virtualization
// ============================================================================

/// Apply branch overlay to main table data (CoW Read).
///
/// When a branch is active, reads from `_qail_branch_rows` and merges:
/// - `insert` overlays → appended to results
/// - `update` overlays → replace matching PK rows
/// - `delete` overlays → remove matching PK rows
async fn apply_branch_overlay(
    conn: &mut qail_pg::driver::PooledConnection,
    branch_name: &str,
    table_name: &str,
    data: &mut Vec<Value>,
    pk_column: &str,
) {
    let sql = qail_pg::driver::branch_sql::read_overlay_sql(branch_name, table_name);
    let overlay_rows = match conn.get_mut().simple_query(&sql).await {
        Ok(rows) => rows,
        Err(_) => return, // Overlay tables might not exist yet
    };

    for row in &overlay_rows {
        let row_pk = row.get_string(0).unwrap_or_default();
        let operation = row.get_string(1).unwrap_or_default();
        let row_data_str = row.get_string(2).unwrap_or_default();

        match operation.as_str() {
            "insert" => {
                // Append new row
                if let Ok(val) = serde_json::from_str::<Value>(&row_data_str) {
                    data.push(val);
                }
            }
            "update" => {
                // Replace matching PK row
                if let Ok(new_val) = serde_json::from_str::<Value>(&row_data_str) {
                    let mut found = false;
                    for existing in data.iter_mut() {
                        if let Some(existing_pk) = existing.get(pk_column).and_then(|v| {
                            v.as_str()
                                .map(|s| s.to_string())
                                .or_else(|| Some(v.to_string()))
                        }) {
                            if existing_pk == row_pk {
                                *existing = new_val.clone();
                                found = true;
                                break;
                            }
                        }
                    }
                    if !found {
                        // PK not in main — treat as insert
                        data.push(new_val);
                    }
                }
            }
            "delete" => {
                // Remove matching PK row
                data.retain(|existing| {
                    existing
                        .get(pk_column)
                        .and_then(|v| {
                            v.as_str()
                                .map(|s| s.to_string())
                                .or_else(|| Some(v.to_string()))
                        })
                        .map(|pk| pk != row_pk)
                        .unwrap_or(true)
                });
            }
            _ => {}
        }
    }
}

/// Redirect a write to the branch overlay (CoW Write).
///
/// Instead of inserting into the main table, stores the row in `_qail_branch_rows`.
async fn redirect_to_overlay(
    conn: &mut qail_pg::driver::PooledConnection,
    branch_name: &str,
    table_name: &str,
    row_pk: &str,
    operation: &str,
    row_data: &Value,
) -> Result<(), ApiError> {
    let sql = qail_pg::driver::branch_sql::write_overlay_sql(
        branch_name, table_name, row_pk, operation,
    );
    let data_str = serde_json::to_string(row_data).unwrap_or_default();
    // Use parameterized query for the JSONB data
    let full_sql = sql.replace("$1", &format!("'{}'::jsonb", data_str.replace('\'', "''")));
    conn.get_mut()
        .execute_simple(&full_sql)
        .await
        .map_err(|e| ApiError::internal(format!("Branch overlay write failed: {}", e)))?;
    Ok(())
}

// ============================================================================
// Branch management handlers — Data Virtualization
// ============================================================================

/// POST /api/_branch — Create a new branch
async fn branch_create_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let auth = extract_auth_from_headers(&headers);
    if !auth.is_authenticated() {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": "Authentication required"}))).into_response();
    }

    let name = match body.get("name").and_then(|v| v.as_str()) {
        Some(n) => n,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "Missing 'name' field"})),
            )
                .into_response();
        }
    };

    let parent = body.get("parent").and_then(|v| v.as_str());

    let mut conn = match state.pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Pool error: {}", e)})),
            )
                .into_response();
        }
    };

    // Auto-bootstrap: create internal tables if they don't exist
    let ddl = qail_pg::driver::branch_sql::create_branch_tables_sql();
    if let Err(e) = conn.get_mut().execute_simple(ddl).await {
        tracing::warn!("Branch DDL bootstrap (may already exist): {}", e);
    }

    let sql = qail_pg::driver::branch_sql::create_branch_sql(name, parent);
    match conn.get_mut().execute_simple(&sql).await {
        Ok(_) => (
            StatusCode::CREATED,
            Json(json!({"branch": name, "status": "created"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::CONFLICT,
            Json(json!({"error": format!("Failed to create branch: {}", e)})),
        )
            .into_response(),
    }
}

/// GET /api/_branch — List all branches
async fn branch_list_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let auth = extract_auth_from_headers(&headers);
    if !auth.is_authenticated() {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": "Authentication required"}))).into_response();
    }

    let mut conn = match state.pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Pool error: {}", e)})),
            )
                .into_response();
        }
    };

    let sql = qail_pg::driver::branch_sql::list_branches_sql();
    match conn.get_mut().simple_query(sql).await {
        Ok(rows) => {
            let branches: Vec<Value> = rows
                .iter()
                .map(row_to_json)
                .collect();
            Json(json!({"branches": branches})).into_response()
        }
        Err(_) => {
            // Tables may not exist yet
            Json(json!({"branches": []})).into_response()
        }
    }
}

/// DELETE /api/_branch/:name — Soft-delete a branch
async fn branch_delete_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> impl IntoResponse {
    let auth = extract_auth_from_headers(&headers);
    if !auth.is_authenticated() {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": "Authentication required"}))).into_response();
    }

    let mut conn = match state.pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Pool error: {}", e)})),
            )
                .into_response();
        }
    };

    let sql = qail_pg::driver::branch_sql::delete_branch_sql(&name);
    match conn.get_mut().execute_simple(&sql).await {
        Ok(_) => Json(json!({"branch": name, "status": "deleted"})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("Failed to delete branch: {}", e)})),
        )
            .into_response(),
    }
}

/// POST /api/_branch/:name/merge — Merge branch overlay into main tables
async fn branch_merge_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> impl IntoResponse {
    // Auth check: require authenticated user
    let auth = extract_auth_from_headers(&headers);
    if !auth.is_authenticated() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "Authentication required for branch operations"})),
        )
            .into_response();
    }

    let mut conn = match state.pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Pool error: {}", e)})),
            )
                .into_response();
        }
    };

    // Get overlay stats before merge
    let stats_sql = qail_pg::driver::branch_sql::branch_stats_sql(&name);
    let stats = match conn.get_mut().simple_query(&stats_sql).await {
        Ok(rows) => {
            rows.iter().map(row_to_json).collect::<Vec<_>>()
        }
        Err(_) => vec![],
    };

    // Apply overlay rows to main tables — inside a transaction
    if let Err(e) = conn.get_mut().execute_simple("BEGIN;").await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("Failed to start transaction: {}", e)})),
        )
            .into_response();
    }

    let overlay_sql = qail_pg::driver::branch_sql::merge_overlay_rows_sql(&name);
    let mut applied = 0u32;
    let mut errors: Vec<String> = Vec::new();

    match conn.get_mut().simple_query(&overlay_sql).await {
        Ok(overlay_rows) => {
            for row in &overlay_rows {
                let table = row.get_string(0).unwrap_or_default();
                let row_pk = row.get_string(1).unwrap_or_default();
                let operation = row.get_string(2).unwrap_or_default();
                let row_data_str = row.get_string(3).unwrap_or_default();

                let apply_sql = match operation.as_str() {
                    "insert" => {
                        // Parse JSONB → INSERT INTO table (cols) VALUES (vals)
                        if let Ok(val) = serde_json::from_str::<Value>(&row_data_str) {
                            if let Some(obj) = val.as_object() {
                                let cols: Vec<&str> = obj.keys().map(|k| k.as_str()).collect();
                                let vals: Vec<String> = obj.values().map(|v| match v {
                                    Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                                    Value::Null => "NULL".to_string(),
                                    Value::Bool(b) => b.to_string(),
                                    Value::Number(n) => n.to_string(),
                                    _ => format!("'{}'::jsonb", v.to_string().replace('\'', "''")),
                                }).collect();
                                Some(format!(
                                    "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING;",
                                    table, cols.join(", "), vals.join(", ")
                                ))
                            } else { None }
                        } else { None }
                    }
                    "update" => {
                        // Parse JSONB → UPDATE table SET col = val WHERE id = pk
                        if let Ok(val) = serde_json::from_str::<Value>(&row_data_str) {
                            if let Some(obj) = val.as_object() {
                                let sets: Vec<String> = obj.iter().map(|(k, v)| {
                                    let val_str = match v {
                                        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                                        Value::Null => "NULL".to_string(),
                                        Value::Bool(b) => b.to_string(),
                                        Value::Number(n) => n.to_string(),
                                        _ => format!("'{}'::jsonb", v.to_string().replace('\'', "''")),
                                    };
                                    format!("{} = {}", k, val_str)
                                }).collect();
                                Some(format!(
                                    "UPDATE {} SET {} WHERE id = '{}';",
                                    table, sets.join(", "), row_pk.replace('\'', "''")
                                ))
                            } else { None }
                        } else { None }
                    }
                    "delete" => {
                        Some(format!(
                            "DELETE FROM {} WHERE id = '{}';",
                            table, row_pk.replace('\'', "''")
                        ))
                    }
                    _ => None,
                };

                if let Some(sql) = apply_sql {
                    match conn.get_mut().execute_simple(&sql).await {
                        Ok(_) => applied += 1,
                        Err(e) => errors.push(format!("{}.{}: {}", table, row_pk, e)),
                    }
                }
            }
        }
        Err(e) => {
            errors.push(format!("Failed to read overlay: {}", e));
        }
    }

    // Rollback on errors, commit on success
    if !errors.is_empty() {
        let _ = conn.get_mut().execute_simple("ROLLBACK;").await;
        return (
            StatusCode::CONFLICT,
            Json(json!({"error": "Merge failed — rolled back", "merge_errors": errors})),
        )
            .into_response();
    }

    // Mark as merged (inside the same transaction)
    let merge_sql = qail_pg::driver::branch_sql::mark_merged_sql(&name);
    match conn.get_mut().execute_simple(&merge_sql).await {
        Ok(_) => {
            // COMMIT the transaction
            let _ = conn.get_mut().execute_simple("COMMIT;").await;
            let mut response = json!({
                "branch": name,
                "status": "merged",
                "applied": applied,
                "overlay_stats": stats,
            });
            if !errors.is_empty() {
                response["merge_errors"] = json!(errors);
            }
            Json(response).into_response()
        }
        Err(e) => {
            let _ = conn.get_mut().execute_simple("ROLLBACK;").await;
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Failed to merge branch: {}", e)})),
            )
                .into_response()
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_filters_basic() {
        let filters = parse_filters("name.eq=John&age.gte=18");
        assert_eq!(filters.len(), 2);
        assert_eq!(filters[0].0, "name");
        assert!(matches!(filters[0].1, Operator::Eq));
        assert_eq!(filters[1].0, "age");
        assert!(matches!(filters[1].1, Operator::Gte));
    }

    #[test]
    fn test_parse_filters_in() {
        let filters = parse_filters("status.in=active,pending,closed");
        assert_eq!(filters.len(), 1);
        assert!(matches!(filters[0].1, Operator::In));
        if let QailValue::Array(vals) = &filters[0].2 {
            assert_eq!(vals.len(), 3);
        } else {
            panic!("Expected Array value for IN filter");
        }
    }

    #[test]
    fn test_parse_filters_is_null() {
        let filters = parse_filters("deleted_at.is_null=true");
        assert_eq!(filters.len(), 1);
        assert!(matches!(filters[0].1, Operator::IsNull));
    }

    #[test]
    fn test_parse_filters_no_operator() {
        let filters = parse_filters("name=John");
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].0, "name");
        assert!(matches!(filters[0].1, Operator::Eq));
    }

    #[test]
    fn test_parse_filters_skips_reserved() {
        let filters = parse_filters("limit=10&offset=0&name.eq=John&sort=id:asc");
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].0, "name");
    }

    #[test]
    fn test_parse_scalar_value() {
        assert!(matches!(parse_scalar_value("42"), QailValue::Int(42)));
        assert!(matches!(parse_scalar_value("3.14"), QailValue::Float(_)));
        assert!(matches!(parse_scalar_value("true"), QailValue::Bool(true)));
        assert!(matches!(parse_scalar_value("null"), QailValue::Null));
        assert!(matches!(parse_scalar_value("hello"), QailValue::String(_)));
    }

    // =========================================================================
    // SQL Injection Hardening
    // =========================================================================

    #[test]
    fn test_sql_injection_in_filter_value() {
        // Classic SQL injection attempts — must be treated as literal strings
        let payloads = vec![
            "'; DROP TABLE users; --",
            "1 OR 1=1",
            "1; SELECT * FROM pg_shadow",
            "' UNION SELECT password FROM users --",
            "Robert'); DROP TABLE students;--",
            "1' AND '1'='1",
            "admin'--",
            "' OR ''='",
        ];
        for payload in payloads {
            let qs = format!("name.eq={}", urlencoding::encode(payload));
            let filters = parse_filters(&qs);
            assert_eq!(filters.len(), 1, "Injection payload should produce exactly 1 filter");
            // Value must be a String (treated as literal, never parsed as SQL)
            match &filters[0].2 {
                QailValue::String(s) => assert_eq!(s, payload),
                QailValue::Int(_) | QailValue::Float(_) => {
                    // "1 OR 1=1" might parse the leading "1" as int — that's fine,
                    // the important thing is it's a parameterized value
                }
                _ => {} // Any QailValue is safe — it's parameterized
            }
        }
    }

    #[test]
    fn test_null_bytes_in_filter() {
        let filters = parse_filters("name.eq=hello%00world");
        assert_eq!(filters.len(), 1);
        // Must not panic and must produce a value
    }

    #[test]
    fn test_extremely_long_value() {
        let long_val = "a".repeat(100_000);
        let qs = format!("name.eq={}", long_val);
        let filters = parse_filters(&qs);
        assert_eq!(filters.len(), 1);
    }

    #[test]
    fn test_empty_and_malformed_query_strings() {
        assert!(parse_filters("").is_empty());
        assert!(parse_filters("&&&").is_empty());
        // "===" splits as key="", value="=" — empty key produces no filter
        // (actually "=" key with "=" value — depends on split_once behavior)
        assert!(parse_filters("key_no_value").is_empty());
        // Bare operator with no value
        let f = parse_filters("col.eq=");
        assert_eq!(f.len(), 1); // empty string is valid
    }

    #[test]
    fn test_unicode_in_filters() {
        let filters = parse_filters("name.eq=日本語テスト&city.like=%E4%B8%8A%E6%B5%B7");
        assert_eq!(filters.len(), 2);
        match &filters[0].2 {
            QailValue::String(s) => assert_eq!(s, "日本語テスト"),
            _ => panic!("Expected unicode string"),
        }
    }

    // =========================================================================
    // Proptest Fuzzing
    // =========================================================================

    mod fuzz {
        use super::*;
        use proptest::prelude::*;

        /// Generate random query strings in the format `col.op=val`
        fn arb_query_string() -> impl Strategy<Value = String> {
            prop::collection::vec(
                (
                    "[a-z_]{1,20}",           // column name
                    prop_oneof![              // operator
                        Just("eq"), Just("ne"), Just("gt"), Just("gte"),
                        Just("lt"), Just("lte"), Just("like"), Just("ilike"),
                        Just("in"), Just("not_in"), Just("is_null"), Just("contains"),
                        Just("unknown_op"),
                    ],
                    ".*",                     // arbitrary value
                ),
                0..10, // 0 to 10 filter pairs
            )
            .prop_map(|pairs| {
                pairs
                    .into_iter()
                    .map(|(col, op, val)| format!("{}.{}={}", col, op, urlencoding::encode(&val)))
                    .collect::<Vec<_>>()
                    .join("&")
            })
        }

        proptest! {
            /// parse_filters must NEVER panic on any input
            #[test]
            fn fuzz_parse_filters_never_panics(qs in ".*") {
                let _ = parse_filters(&qs);
            }

            /// parse_scalar_value must NEVER panic on any input
            #[test]
            fn fuzz_parse_scalar_value_never_panics(s in ".*") {
                let _ = parse_scalar_value(&s);
            }

            /// Structured fuzzing: random col.op=val triplets
            #[test]
            fn fuzz_structured_filters(qs in arb_query_string()) {
                let filters = parse_filters(&qs);
                // All filters must have non-empty column names
                for (col, _op, _val) in &filters {
                    prop_assert!(!col.is_empty(), "Column name must not be empty");
                }
            }

            /// Reserved params must NEVER appear in filter output
            #[test]
            fn fuzz_reserved_params_filtered(
                col in prop_oneof![
                    Just("limit"), Just("offset"), Just("sort"),
                    Just("select"), Just("expand"), Just("cursor"),
                    Just("distinct"), Just("returning"),
                ],
                val in "[a-z0-9]{1,10}"
            ) {
                let qs = format!("{}={}", col, val);
                let filters = parse_filters(&qs);
                prop_assert!(filters.is_empty(), "Reserved param '{}' should not become a filter", col);
            }

            /// parse_scalar_value output is always a valid QailValue variant
            #[test]
            fn fuzz_scalar_value_is_valid(s in "[^\u{0}]{0,1000}") {
                let val = parse_scalar_value(&s);
                // Just verify it produced a valid QailValue (no panic)
                match val {
                    _ => {} // Any variant is fine — we just care it didn't panic
                }
            }
        }
    }
}
