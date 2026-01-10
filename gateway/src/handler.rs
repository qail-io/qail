//! HTTP Request Handlers for QAIL Gateway
//!
//! Handles incoming requests and executes QAIL queries.

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::auth::extract_auth_from_headers;
use crate::GatewayState;

/// Health check response
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub pool_active: usize,
    pub pool_idle: usize,
}

/// Query response
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResponse {
    pub rows: Vec<serde_json::Value>,
    pub count: usize,
}

/// Batch query request
#[derive(Debug, Deserialize)]
pub struct BatchRequest {
    pub queries: Vec<String>,
    #[serde(default = "default_true")]
    pub transaction: bool,
}

fn default_true() -> bool { true }

/// Batch query response
#[derive(Debug, Serialize)]
pub struct BatchResponse {
    pub results: Vec<BatchQueryResult>,
    pub total: usize,
    pub success: usize,
}

/// Result for a single query in a batch
#[derive(Debug, Serialize)]
pub struct BatchQueryResult {
    pub index: usize,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
}

pub async fn health_check(
    State(state): State<Arc<GatewayState>>,
) -> Json<HealthResponse> {
    let stats = state.pool.stats().await;
    Json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        pool_active: stats.active,
        pool_idle: stats.idle,
    })
}

pub async fn execute_query(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    body: String,
) -> Result<Json<QueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    let query_text = body.trim();
    
    if query_text.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Empty query".to_string(),
                code: "EMPTY_QUERY".to_string(),
            }),
        ));
    }
    
    // Extract auth context from headers
    let auth = extract_auth_from_headers(&headers);
    
    tracing::info!("Executing text query: {} (user: {})", query_text, auth.user_id);
    
    // Parse the QAIL text into AST
    let mut cmd = match qail_core::parser::parse(query_text) {
        Ok(cmd) => cmd,
        Err(e) => {
            tracing::warn!("Parse error: {}", e);
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("Parse error: {}", e),
                    code: "PARSE_ERROR".to_string(),
                }),
            ));
        }
    };
    
    // Apply row-level security policies
    if let Err(e) = state.policy_engine.apply_policies(&auth, &mut cmd) {
        tracing::warn!("Policy error: {}", e);
        return Err((
            StatusCode::from_u16(e.status_code()).unwrap_or(StatusCode::FORBIDDEN),
            Json(ErrorResponse {
                error: e.to_string(),
                code: "POLICY_DENIED".to_string(),
            }),
        ));
    }
    
    execute_qail_cmd(&state, &cmd).await
}

/// Execute a QAIL query (BINARY format)
/// 
/// Accepts bincode-encoded QAIL AST and returns JSON results.
/// This is faster than text format since it skips parsing.
pub async fn execute_query_binary(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<QueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    if body.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Empty binary query".to_string(),
                code: "EMPTY_QUERY".to_string(),
            }),
        ));
    }
    
    // Extract auth context from headers
    let auth = extract_auth_from_headers(&headers);
    
    tracing::info!("Executing binary query ({} bytes, user: {})", body.len(), auth.user_id);
    
    // Deserialize the binary QAIL AST
    let mut cmd: qail_core::ast::Qail = match bincode::deserialize(&body) {
        Ok(cmd) => cmd,
        Err(e) => {
            tracing::warn!("Bincode decode error: {}", e);
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("Invalid binary format: {}", e),
                    code: "DECODE_ERROR".to_string(),
                }),
            ));
        }
    };
    
    // Apply row-level security policies
    if let Err(e) = state.policy_engine.apply_policies(&auth, &mut cmd) {
        tracing::warn!("Policy error: {}", e);
        return Err((
            StatusCode::from_u16(e.status_code()).unwrap_or(StatusCode::FORBIDDEN),
            Json(ErrorResponse {
                error: e.to_string(),
                code: "POLICY_DENIED".to_string(),
            }),
        ));
    }
    
    execute_qail_cmd(&state, &cmd).await
}

/// Common query execution logic
async fn execute_qail_cmd(
    state: &Arc<GatewayState>,
    cmd: &qail_core::ast::Qail,
) -> Result<Json<QueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    use qail_core::ast::Action;
    
    let table = &cmd.table;
    let is_read_query = matches!(cmd.action, Action::Get);
    
    // Generate cache key from command
    let cache_key = format!("{:?}", cmd);
    
    // Check cache for read queries
    if is_read_query {
        if let Some(cached) = state.cache.get(&cache_key) {
            tracing::debug!("Cache HIT for table '{}'", table);
            // Parse cached JSON back to response
            if let Ok(response) = serde_json::from_str::<QueryResponse>(&cached) {
                return Ok(Json(response));
            }
        }
    }
    
    // Acquire pooled connection and execute query
    let mut conn = state.pool.acquire().await.map_err(|e| {
        tracing::error!("Pool error: {}", e);
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "Database connection failed".to_string(),
                code: "CONNECTION_ERROR".to_string(),
            }),
        )
    })?;
    
    let rows = conn.fetch_all_uncached(cmd).await.map_err(|e| {
        tracing::error!("Query error: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Query failed: {}", e),
                code: "QUERY_ERROR".to_string(),
            }),
        )
    })?;
    
    // Convert rows to JSON
    let json_rows: Vec<serde_json::Value> = rows
        .iter()
        .map(row_to_json)
        .collect();
    
    let count = json_rows.len();
    
    let response = QueryResponse {
        rows: json_rows,
        count,
    };
    
    // Cache read query results
    if is_read_query {
        if let Ok(json) = serde_json::to_string(&response) {
            state.cache.set(&cache_key, table, json);
            tracing::debug!("Cache STORE for table '{}' ({} rows)", table, count);
        }
    } else {
        // Mutation - invalidate cache for this table
        state.cache.invalidate_table(table);
        tracing::debug!("Cache INVALIDATE for table '{}'", table);
    }
    
    Ok(Json(response))
}

pub fn row_to_json(row: &qail_pg::PgRow) -> serde_json::Value {
    let column_names: Vec<String> = if let Some(ref info) = row.column_info {
        let mut pairs: Vec<_> = info.name_to_index.iter().collect();
        pairs.sort_by_key(|(_, idx)| *idx);
        pairs.into_iter().map(|(name, _)| name.clone()).collect()
    } else {
        (0..row.columns.len()).map(|i| format!("col_{}", i)).collect()
    };
    
    let mut obj = serde_json::Map::new();
    
    for (i, col_name) in column_names.into_iter().enumerate() {
        let value = if let Some(s) = row.get_string(i) {
            if (s.starts_with('{') && s.ends_with('}')) || (s.starts_with('[') && s.ends_with(']')) {
                serde_json::from_str(&s).unwrap_or(serde_json::Value::String(s))
            } else {
                if let Ok(n) = s.parse::<i64>() {
                    serde_json::Value::Number(n.into())
                } else if let Ok(f) = s.parse::<f64>() {
                    if let Some(n) = serde_json::Number::from_f64(f) {
                        serde_json::Value::Number(n)
                    } else {
                        serde_json::Value::String(s)
                    }
                } else if s == "t" || s == "true" {
                    serde_json::Value::Bool(true)
                } else if s == "f" || s == "false" {
                    serde_json::Value::Bool(false)
                } else {
                    serde_json::Value::String(s)
                }
            }
        } else {
            serde_json::Value::Null
        };
        
        obj.insert(col_name, value);
    }
    
    serde_json::Value::Object(obj)
}

pub async fn execute_batch(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Json(request): Json<BatchRequest>,
) -> Result<Json<BatchResponse>, (StatusCode, Json<ErrorResponse>)> {
    if request.queries.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Empty query batch".to_string(),
                code: "EMPTY_BATCH".to_string(),
            }),
        ));
    }
    
    let auth = extract_auth_from_headers(&headers);
    tracing::info!("Executing batch of {} queries (user: {})", request.queries.len(), auth.user_id);
    
    let mut results = Vec::with_capacity(request.queries.len());
    let mut success_count = 0;
    
    // Acquire connection from pool
    let mut conn = state.pool.acquire().await.map_err(|e| {
        tracing::error!("Pool error: {}", e);
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "Database connection failed".to_string(),
                code: "CONNECTION_ERROR".to_string(),
            }),
        )
    })?;
    
    for (index, query_text) in request.queries.iter().enumerate() {
        let query_text = query_text.trim();
        
        // Parse query
        let mut cmd = match qail_core::parser::parse(query_text) {
            Ok(cmd) => cmd,
            Err(e) => {
                results.push(BatchQueryResult {
                    index,
                    success: false,
                    rows: None,
                    count: None,
                    error: Some(format!("Parse error: {}", e)),
                });
                continue;
            }
        };
        
        // Apply policies
        if let Err(e) = state.policy_engine.apply_policies(&auth, &mut cmd) {
            results.push(BatchQueryResult {
                index,
                success: false,
                rows: None,
                count: None,
                error: Some(e.to_string()),
            });
            continue;
        }
        
        // Execute query
        match conn.fetch_all_uncached(&cmd).await {
            Ok(rows) => {
                let json_rows: Vec<serde_json::Value> = rows.iter().map(row_to_json).collect();
                let count = json_rows.len();
                
                results.push(BatchQueryResult {
                    index,
                    success: true,
                    rows: Some(json_rows),
                    count: Some(count),
                    error: None,
                });
                success_count += 1;
            }
            Err(e) => {
                results.push(BatchQueryResult {
                    index,
                    success: false,
                    rows: None,
                    count: None,
                    error: Some(format!("Query error: {}", e)),
                });
            }
        }
    }
    
    let total = results.len();
    
    Ok(Json(BatchResponse {
        results,
        total,
        success: success_count,
    }))
}
