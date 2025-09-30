//! Auto-REST route generation from schema
//!
//! Given a `SchemaRegistry`, generates RESTful CRUD endpoints for every table:
//!
//! ```text
//! GET    /api/{table}                → List with pagination, filtering, sorting
//! GET    /api/{table}?expand=rel     → List with LEFT JOIN on FK relation
//! GET    /api/{table}/{id}           → Get single row by PK
//! POST   /api/{table}                → Create row from JSON body
//! PATCH  /api/{table}/{id}           → Partial update from JSON body
//! DELETE /api/{table}/{id}           → Delete by PK
//! GET    /api/{parent}/{id}/{child}  → Nested list: children filtered by parent FK
//! ```

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::get,
    Router,
};
use qail_core::ast::{JoinKind, Operator, Value as QailValue};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;

use crate::auth::extract_auth_from_headers;
use crate::handler::row_to_json;
use crate::middleware::ApiError;
use crate::GatewayState;

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
    /// Sort: `column:asc` or `column:desc`
    pub sort: Option<String>,
    /// Columns to select: `id,name,email`
    pub select: Option<String>,
    /// Expand FK relations via LEFT JOIN: `?expand=orders` or `?expand=orders,products`
    pub expand: Option<String>,
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

        if has_pk {
            let id_path = format!("/api/{}/:id", table_name);

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
                let nested_path = format!("/api/{}/:id/{}", table_name, child_table);
                tracing::info!("  AUTO-REST nested: {} → GET", nested_path);
                router = router.route(&nested_path, get(nested_list_handler));
            }
        }
    }

    router
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

/// GET /api/{table} — list with pagination, sorting, column selection
async fn list_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Query(params): Query<ListParams>,
    request: axum::extract::Request,
) -> Result<Json<ListResponse>, ApiError> {
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

    // Sorting
    if let Some(ref sort) = params.sort {
        let parts: Vec<&str> = sort.split(':').collect();
        let col = parts[0];
        let dir = parts.get(1).unwrap_or(&"asc");
        if *dir == "desc" {
            cmd = cmd.order_desc(col);
        } else {
            cmd = cmd.order_asc(col);
        }
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

    // Pagination
    cmd = cmd.limit(limit);
    cmd = cmd.offset(offset);

    // Apply RLS policies
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

    Ok(Json(ListResponse {
        data,
        count,
        total: None,
        limit,
        offset,
    }))
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

    Ok(Json(SingleResponse {
        data: row_to_json(row),
    }))
}

/// POST /api/{table} — create from JSON body
async fn create_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    request: axum::extract::Request,
) -> Result<(StatusCode, Json<SingleResponse>), ApiError> {
    let table_name =
        extract_table_name(request.uri()).ok_or_else(|| ApiError::not_found("table"))?;

    let table = state
        .schema
        .table(&table_name)
        .ok_or_else(|| ApiError::not_found(&table_name))?;

    let auth = extract_auth_from_headers(&headers);

    // Validate required columns upfront (clone table info before body read)
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
    let obj = body
        .as_object()
        .ok_or_else(|| ApiError::parse_error("Expected JSON object"))?;

    // Validate required columns
    for col_name in &required {
        if !obj.contains_key(col_name) {
            return Err(ApiError::parse_error(format!(
                "Missing required field: {}",
                col_name
            )));
        }
    }

    // Build: add table { col1 = val1, col2 = val2, ... }
    let mut cmd = qail_core::ast::Qail::add(&table_name);
    for (key, value) in obj {
        let qail_val = json_to_qail_value(value);
        cmd = cmd.set_value(key, qail_val);
    }

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

    let data = rows
        .first()
        .map(row_to_json)
        .unwrap_or_else(|| json!({"created": true}));

    // Invalidate cache for this table
    state.cache.invalidate_table(&table_name);

    Ok((StatusCode::CREATED, Json(SingleResponse { data })))
}

/// PATCH /api/{table}/:id — partial update
async fn update_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    request: axum::extract::Request,
) -> Result<Json<SingleResponse>, ApiError> {
    let table_name =
        extract_table_name(request.uri()).ok_or_else(|| ApiError::not_found("table"))?;

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
        .filter(&pk, Operator::Eq, QailValue::String(id));

    for (key, value) in obj {
        let qail_val = json_to_qail_value(value);
        cmd = cmd.set_value(key, qail_val);
    }

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

    let data = rows
        .first()
        .map(row_to_json)
        .unwrap_or_else(|| json!({"updated": true}));

    // Invalidate cache
    state.cache.invalidate_table(&table_name);

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
        qail_core::ast::Qail::del(&table_name).filter(&pk, Operator::Eq, QailValue::String(id));

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

    conn.fetch_all_uncached(&cmd)
        .await
        .map_err(|e| ApiError::query_error(e.to_string()))?;

    // Invalidate cache
    state.cache.invalidate_table(&table_name);

    Ok(Json(DeleteResponse { deleted: true }))
}

// ============================================================================
// Nested route handler
// ============================================================================

/// GET /api/{parent}/:id/{child} — list child rows filtered by parent FK
///
/// Example: `GET /api/users/123/orders` → `get orders[user_id = 123]`
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

    // Sorting
    if let Some(ref sort) = params.sort {
        let parts: Vec<&str> = sort.split(':').collect();
        let col = parts[0];
        let dir = parts.get(1).unwrap_or(&"asc");
        if *dir == "desc" {
            cmd = cmd.order_desc(col);
        } else {
            cmd = cmd.order_asc(col);
        }
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
        other => QailValue::String(other.to_string()),
    }
}
