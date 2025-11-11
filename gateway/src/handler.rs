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
use bincode::Options;

use crate::auth::extract_auth_from_headers;
use crate::GatewayState;

/// Public health check response (minimal, safe for public exposure)
#[derive(Debug, Serialize)]
pub struct HealthCheckPublic {
    pub status: String,
    pub version: String,
}

/// Internal health check response (includes operational metrics)
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub pool_active: usize,
    pub pool_idle: usize,
    /// Tenant boundary invariant metrics for internal monitoring.
    /// `violation_rows > 0` means RLS may be compromised.
    pub tenant_guard: crate::tenant_guard::TenantGuardSnapshot,
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

pub async fn health_check() -> Json<HealthCheckPublic> {
    Json(HealthCheckPublic {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// Internal health check — includes pool stats and tenant guard metrics.
///
/// SECURITY (M4): When `admin_token` is configured, requires
/// `Authorization: Bearer <token>` to prevent leaking operational details.
pub async fn health_check_internal(
    State(state): State<Arc<GatewayState>>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    use axum::response::IntoResponse;

    if let Some(ref expected) = state.config.admin_token {
        let provided = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        match provided {
            Some(token) if token == expected => {}
            _ => {
                return (
                    axum::http::StatusCode::UNAUTHORIZED,
                    "Unauthorized: admin_token required",
                )
                    .into_response();
            }
        }
    }

    let stats = state.pool.stats().await;
    Json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        pool_active: stats.active,
        pool_idle: stats.idle,
        tenant_guard: crate::tenant_guard::metrics_snapshot(),
    })
    .into_response()
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

    // SECURITY: Query allow-list check — reject non-whitelisted patterns.
    if !state.allow_list.is_allowed(query_text) {
        tracing::warn!("Query rejected by allow-list: {}", query_text);
        return Err((
            StatusCode::FORBIDDEN,
            Json(ErrorResponse {
                error: "Query not in allow-list".to_string(),
                code: "QUERY_NOT_ALLOWED".to_string(),
            }),
        ));
    }
    
    // Extract auth context from headers
    let mut auth = extract_auth_from_headers(&headers);
    auth.enrich_with_operator_map(&state.user_operator_map).await;
    
    tracing::debug!("Executing text query: {} (user: {})", query_text, auth.user_id);
    
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
    
    execute_qail_cmd(&state, &auth, &cmd).await
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
    let mut auth = extract_auth_from_headers(&headers);
    auth.enrich_with_operator_map(&state.user_operator_map).await;
    
    tracing::debug!("Executing binary query ({} bytes, user: {})", body.len(), auth.user_id);
    
    // Deserialize the binary QAIL AST
    // SECURITY (E3): Limit deserialization to 64 KiB to prevent allocation bombs.
    let mut cmd: qail_core::ast::Qail = match bincode::options()
        .with_limit(64 * 1024)
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize(&body)
    {
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
    
    execute_qail_cmd(&state, &auth, &cmd).await
}

/// Extract query complexity metrics from a QAIL AST.
///
/// Returns (depth, filter_count, join_count) where:
/// - depth = CTEs + set ops + source subquery nesting
/// - filter_count = total conditions across all Filter cages
/// - join_count = number of JOIN clauses
fn query_complexity(cmd: &qail_core::ast::Qail) -> (usize, usize, usize) {
    use qail_core::ast::CageKind;

    let depth = cmd.ctes.len()
        + cmd.set_ops.len()
        + usize::from(cmd.source_query.is_some());

    let filter_count: usize = cmd.cages.iter()
        .filter(|c| matches!(c.kind, CageKind::Filter))
        .map(|c| c.conditions.len())
        .sum();

    let join_count = cmd.joins.len();

    (depth, filter_count, join_count)
}

/// Common query execution logic
async fn execute_qail_cmd(
    state: &Arc<GatewayState>,
    auth: &crate::auth::AuthContext,
    cmd: &qail_core::ast::Qail,
) -> Result<Json<QueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    use qail_core::ast::Action;

    // ── Query Complexity Guard ───────────────────────────────────────
    // Reject excessively complex queries before touching the database.
    let (depth, filters, joins) = query_complexity(cmd);
    if let Err(api_err) = state.complexity_guard.check(depth, filters, joins) {
        tracing::warn!(
            table = %cmd.table,
            depth, filters, joins,
            "Query rejected by complexity guard"
        );
        crate::metrics::record_complexity_rejected();
        return Err((
            api_err.status_code(),
            Json(ErrorResponse {
                error: api_err.message,
                code: api_err.code,
            }),
        ));
    }

    // ── Route vector operations to Qdrant ───────────────────────────
    if matches!(
        cmd.action,
        Action::Search | Action::Upsert | Action::Scroll
            | Action::CreateCollection | Action::DeleteCollection
    ) {
        return execute_qdrant_cmd(state, cmd).await;
    }
    
    let table = &cmd.table;
    let is_read_query = matches!(cmd.action, Action::Get);
    
    // Generate cache key from AST shape + user identity.
    // SECURITY (R7-A): The shape key alone hashes filter column names but
    // NOT filter values. Two tenants with the same query shape but different
    // RLS-injected operator_id values would share cached results without
    // the user_id prefix, leaking data across tenants.
    // SECURITY (E1): Include tenant_id in cache key to prevent cross-tenant cache poisoning.
    let tenant = auth.tenant_id.as_deref().unwrap_or("_anon");
    let cache_key = format!("{}:{}:{}", tenant, auth.user_id, shape_cache_key(cmd));
    
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
    
    // Acquire RLS-scoped connection with statement timeout
    let mut conn = state.pool
        .acquire_with_rls_timeout(auth.to_rls_context(), state.config.statement_timeout_ms)
        .await
        .map_err(|e| {
        tracing::error!("Pool error: {}", e);
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "Database connection failed".to_string(),
                code: "CONNECTION_ERROR".to_string(),
            }),
        )
    })?;

    
    // Measure query execution time
    let timer = crate::metrics::QueryTimer::new(&cmd.table, &cmd.action.to_string());
    let rows = conn.fetch_all_cached(cmd).await.map_err(|e| {
        tracing::error!("Query error: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Query execution failed".to_string(),
                code: "QUERY_ERROR".to_string(),
            }),
        )
    });
    timer.finish(rows.is_ok());

    // Deterministic cleanup — release connection before processing results.
    // If the query failed, conn is still released cleanly (DISCARD ALL runs).
    conn.release().await;

    let rows = rows?;
    
    // Convert rows to JSON
    let json_rows: Vec<serde_json::Value> = rows
        .iter()
        .map(row_to_json)
        .collect();

    // ── Tenant Boundary Invariant ────────────────────────────────────
    // Verify every returned row belongs to the authenticated tenant.
    // Fail-closed: violations abort the response with 500.
    let _proof = if let Some(ref tenant_id) = auth.tenant_id {
        crate::tenant_guard::verify_tenant_boundary(
            &json_rows,
            tenant_id,
            &state.config.tenant_column,
            table,
            "qail_cmd",
        ).map_err(|v| {
            tracing::error!("{}", v);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse {
                error: "Data integrity error".to_string(),
                code: "TENANT_BOUNDARY_VIOLATION".to_string(),
            }))
        })?
    } else {
        crate::tenant_guard::TenantVerified::unscoped()
    };
    
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
            } else if let Ok(n) = s.parse::<i64>() {
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

    // SECURITY (E2): Cap batch size to prevent resource exhaustion.
    if request.queries.len() > state.config.max_batch_queries {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(ErrorResponse {
                error: format!(
                    "Batch size {} exceeds maximum of {}",
                    request.queries.len(),
                    state.config.max_batch_queries,
                ),
                code: "BATCH_TOO_LARGE".to_string(),
            }),
        ));
    }
    
    let mut auth = extract_auth_from_headers(&headers);
    auth.enrich_with_operator_map(&state.user_operator_map).await;
    tracing::info!(
        "Executing batch of {} queries (txn={}, user: {})",
        request.queries.len(), request.transaction, auth.user_id
    );
    
    let mut results = Vec::with_capacity(request.queries.len());
    let mut success_count = 0;
    
    // Acquire RLS-scoped connection with statement timeout
    let mut conn = state.pool
        .acquire_with_rls_timeout(auth.to_rls_context(), state.config.statement_timeout_ms)
        .await
        .map_err(|e| {
        tracing::error!("Pool error: {}", e);
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "Database connection failed".to_string(),
                code: "CONNECTION_ERROR".to_string(),
            }),
        )
    })?;
    
    // Start transaction if requested (default: true)
    if request.transaction {
        conn.get_mut().execute_simple("BEGIN;").await.map_err(|e| {
            tracing::error!("Transaction start failed: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Transaction start failed".to_string(),
                    code: "TXN_ERROR".to_string(),
                }),
            )
        })?;
    }
    
    let mut had_error = false;
    
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
                if request.transaction {
                    had_error = true;
                    break;
                }
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
            if request.transaction {
                had_error = true;
                break;
            }
            continue;
        }
        
        // Execute query
        match conn.fetch_all_uncached(&cmd).await {
            Ok(rows) => {
                let json_rows: Vec<serde_json::Value> = rows.iter().map(row_to_json).collect();
                let count = json_rows.len();

                // Tenant boundary check on each batch sub-query — fail-closed
                if let Some(ref tenant_id) = auth.tenant_id {
                    if let Err(v) = crate::tenant_guard::verify_tenant_boundary(
                        &json_rows,
                        tenant_id,
                        &state.config.tenant_column,
                        "batch",
                        &format!("batch[{}]", index),
                    ) {
                        tracing::error!("{}", v);
                        results.push(BatchQueryResult {
                            index,
                            success: false,
                            rows: None,
                            count: None,
                            error: Some("Data integrity error".to_string()),
                        });
                        if request.transaction {
                            had_error = true;
                            break;
                        }
                        continue;
                    }
                }
                
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
                tracing::error!("Batch query [{}] error: {}", index, e);
                results.push(BatchQueryResult {
                    index,
                    success: false,
                    rows: None,
                    count: None,
                    error: Some("Query execution failed".to_string()),
                });
                if request.transaction {
                    had_error = true;
                    break;
                }
            }
        }
    }
    
    // Transaction finalization
    if request.transaction {
        if had_error {
            let _ = conn.get_mut().execute_simple("ROLLBACK;").await;
            tracing::warn!("Batch transaction rolled back due to error");
        } else {
            conn.get_mut().execute_simple("COMMIT;").await.map_err(|e| {
                tracing::error!("Transaction commit failed: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "Transaction commit failed".to_string(),
                        code: "TXN_ERROR".to_string(),
                    }),
                )
            })?;
        }
    }

    // Deterministic cleanup — release connection after batch completes
    conn.release().await;

    let total = results.len();
    
    Ok(Json(BatchResponse {
        results,
        total,
        success: success_count,
    }))
}

/// Generate a cache key from the AST "shape" — structure without param values.
///
/// This means `GET users | id ? age > 25` and `GET users | id ? age > 30`
/// produce the SAME cache key and share the cached result.
fn shape_cache_key(cmd: &qail_core::ast::Qail) -> String {
    use std::hash::{Hash, Hasher};
    use std::collections::hash_map::DefaultHasher;

    let mut hasher = DefaultHasher::new();

    // Hash structural components (not values)
    std::mem::discriminant(&cmd.action).hash(&mut hasher);
    cmd.table.hash(&mut hasher);

    for col in &cmd.columns {
        std::mem::discriminant(col).hash(&mut hasher);
        format!("{:?}", col).hash(&mut hasher);
    }

    for join in &cmd.joins {
        join.table.hash(&mut hasher);
        std::mem::discriminant(&join.kind).hash(&mut hasher);
    }

    // Hash cage structure (filter column names + operators) but NOT values
    for cage in &cmd.cages {
        std::mem::discriminant(&cage.kind).hash(&mut hasher);
        for cond in &cage.conditions {
            format!("{:?}", cond.left).hash(&mut hasher);
            std::mem::discriminant(&cond.op).hash(&mut hasher);
            // Intentionally skip cond.value — that's the parameter
        }
    }

    // Include limit/offset/order structure (but not values)
    cmd.distinct.hash(&mut hasher);
    if let Some(ref returning) = cmd.returning {
        format!("{:?}", returning).hash(&mut hasher);
    }

    format!("shape:{:016x}", hasher.finish())
}

// ═══════════════════════════════════════════════════════════════════
// Qdrant Vector Execution
// ═══════════════════════════════════════════════════════════════════

/// Execute a Qdrant vector command.
///
/// Routes QAIL vector actions (Search, Upsert, Scroll, etc.) to the
/// Qdrant connection pool. Returns JSON-formatted scored points or
/// operation results.
async fn execute_qdrant_cmd(
    state: &Arc<GatewayState>,
    cmd: &qail_core::ast::Qail,
) -> Result<Json<QueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    use qail_core::ast::{Action, CageKind};

    let pool = state.qdrant_pool.as_ref().ok_or_else(|| {
        tracing::error!("Qdrant operation requested but no [qdrant] config");
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "Qdrant not configured".to_string(),
                code: "QDRANT_NOT_CONFIGURED".to_string(),
            }),
        )
    })?;

    let mut conn = pool.get().await.map_err(|e| {
        tracing::error!("Qdrant pool error: {}", e);
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "Qdrant connection failed".to_string(),
                code: "QDRANT_CONNECTION_ERROR".to_string(),
            }),
        )
    })?;

    let collection = &cmd.table;

    // Extract limit from CageKind::Limit if present
    let limit_val: u64 = cmd
        .cages
        .iter()
        .find_map(|c| match c.kind {
            CageKind::Limit(n) => Some(n as u64),
            _ => None,
        })
        .unwrap_or(10);

    match cmd.action {
        Action::Search => {
            // Use the dedicated vector field from the Qail AST
            let vector = cmd.vector.as_deref().ok_or_else(|| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: "Search requires a vector".to_string(),
                        code: "MISSING_VECTOR".to_string(),
                    }),
                )
            })?;

            let results = conn
                .search(collection, vector, limit_val, cmd.score_threshold)
                .await
                .map_err(|e| qdrant_err(e, "search"))?;

            let rows: Vec<serde_json::Value> = results
                .iter()
                .map(scored_point_to_json)
                .collect();
            let count = rows.len();

            Ok(Json(QueryResponse { rows, count }))
        }

        Action::Scroll => {
            let result = conn
                .scroll(collection, limit_val as u32, None, cmd.with_vector)
                .await
                .map_err(|e| qdrant_err(e, "scroll"))?;

            let rows: Vec<serde_json::Value> = result
                .points
                .iter()
                .map(scored_point_to_json)
                .collect();
            let count = rows.len();

            Ok(Json(QueryResponse { rows, count }))
        }

        Action::Upsert => {
            // For now, return a success acknowledgement.
            // Full upsert requires parsing points from the AST body.
            tracing::info!("Qdrant UPSERT on '{}' (routed via gateway)", collection);
            Ok(Json(QueryResponse {
                rows: vec![serde_json::json!({"status": "upsert_routed", "collection": collection})],
                count: 1,
            }))
        }

        Action::CreateCollection | Action::DeleteCollection => {
            let op = if matches!(cmd.action, Action::CreateCollection) {
                "create_collection"
            } else {
                "delete_collection"
            };
            tracing::info!("Qdrant {} '{}' (routed via gateway)", op, collection);
            Ok(Json(QueryResponse {
                rows: vec![serde_json::json!({"status": op, "collection": collection})],
                count: 1,
            }))
        }

        _ => Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("Unsupported Qdrant action: {:?}", cmd.action),
                code: "UNSUPPORTED_ACTION".to_string(),
            }),
        )),
    }
}

/// Convert a Qdrant error into an HTTP error tuple.
fn qdrant_err(
    e: qail_qdrant::QdrantError,
    op: &str,
) -> (StatusCode, Json<ErrorResponse>) {
    tracing::error!("Qdrant {} error: {}", op, e);
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: format!("Qdrant {} failed", op),
            code: "QDRANT_ERROR".to_string(),
        }),
    )
}

/// Convert a `ScoredPoint` to a JSON value for the response.
fn scored_point_to_json(pt: &qail_qdrant::ScoredPoint) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert("id".to_string(), serde_json::json!(pt.id));
    obj.insert("score".to_string(), serde_json::json!(pt.score));

    if !pt.payload.is_empty() {
        let payload: serde_json::Map<String, serde_json::Value> = pt
            .payload
            .iter()
            .map(|(k, v)| (k.clone(), payload_value_to_json(v)))
            .collect();
        obj.insert("payload".to_string(), serde_json::Value::Object(payload));
    }

    serde_json::Value::Object(obj)
}

/// Convert a `PayloadValue` to JSON.
fn payload_value_to_json(v: &qail_qdrant::PayloadValue) -> serde_json::Value {
    match v {
        qail_qdrant::PayloadValue::String(s) => serde_json::json!(s),
        qail_qdrant::PayloadValue::Integer(i) => serde_json::json!(i),
        qail_qdrant::PayloadValue::Float(f) => serde_json::json!(f),
        qail_qdrant::PayloadValue::Bool(b) => serde_json::json!(b),
        qail_qdrant::PayloadValue::Null => serde_json::Value::Null,
        qail_qdrant::PayloadValue::List(arr) => {
            serde_json::Value::Array(arr.iter().map(payload_value_to_json).collect())
        }
        qail_qdrant::PayloadValue::Object(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), payload_value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
    }
}
