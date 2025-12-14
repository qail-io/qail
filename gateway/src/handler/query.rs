//! Qail AST query execution handlers.
//!
//! Text, binary, fast, and batch query endpoints.

use axum::{
    body::Bytes,
    extract::State,
    http::HeaderMap,
    response::Json,
};
use std::sync::Arc;
use bincode::Options;

use super::{
    BatchQueryResult, BatchRequest, BatchResponse,
    FastQueryResponse, QueryResponse,
};
use super::convert::{row_to_array, row_to_json};
use super::qdrant::execute_qdrant_cmd;
use crate::auth::extract_auth_from_headers;
use crate::middleware::ApiError;
use crate::GatewayState;

pub async fn execute_query(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    body: String,
) -> Result<Json<QueryResponse>, ApiError> {
    let query_text = body.trim();
    
    if query_text.is_empty() {
        return Err(ApiError::bad_request("EMPTY_QUERY", "Empty query"));
    }

    // SECURITY: Query allow-list check — reject non-whitelisted patterns.
    if !state.allow_list.is_allowed(query_text) {
        tracing::warn!("Query rejected by allow-list: {}", query_text);
        return Err(ApiError::with_code("QUERY_NOT_ALLOWED", "Query not in allow-list"));
    }
    
    // Extract auth context from headers
    let mut auth = extract_auth_from_headers(&headers);
    auth.enrich_with_operator_map(&state.user_operator_map).await;
    
    tracing::debug!("Executing text query: {} (user: {})", query_text, auth.user_id);
    
    // Parse the QAIL text into AST (cached — skip re-parse for repeated queries)
    let mut cmd = {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        query_text.hash(&mut h);
        let key = h.finish();

        if let Some(cached) = state.parse_cache.get(&key) {
            cached
        } else {
            match qail_core::parser::parse(query_text) {
                Ok(cmd) => {
                    state.parse_cache.insert(key, cmd.clone());
                    cmd
                }
                Err(e) => {
                    tracing::warn!("Parse error: {}", e);
                    return Err(ApiError::parse_error(format!("Parse error: {}", e)));
                }
            }
        }
    };
    
    // Apply row-level security policies
    if let Err(e) = state.policy_engine.apply_policies(&auth, &mut cmd) {
        tracing::warn!("Policy error: {}", e);
        return Err(ApiError::with_code("POLICY_DENIED", e.to_string()));
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
) -> Result<Json<QueryResponse>, ApiError> {
    if body.is_empty() {
        return Err(ApiError::bad_request("EMPTY_QUERY", "Empty binary query"));
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
            return Err(ApiError::bad_request("DECODE_ERROR", format!("Invalid binary format: {}", e)));
        }
    };
    
    // Apply row-level security policies
    if let Err(e) = state.policy_engine.apply_policies(&auth, &mut cmd) {
        tracing::warn!("Policy error: {}", e);
        return Err(ApiError::with_code("POLICY_DENIED", e.to_string()));
    }
    
    execute_qail_cmd(&state, &auth, &cmd).await
}

/// Execute a QAIL query (FAST — array-of-arrays response)
///
/// Returns rows as positional arrays instead of keyed objects.
/// Skips column metadata for maximum throughput.
/// Use for data pipelines and internal services that know the schema.
pub async fn execute_query_fast(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    body: String,
) -> Result<Json<FastQueryResponse>, ApiError> {
    let query_text = body.trim();
    
    if query_text.is_empty() {
        return Err(ApiError::bad_request("EMPTY_QUERY", "Empty query"));
    }

    if !state.allow_list.is_allowed(query_text) {
        return Err(ApiError::with_code("QUERY_NOT_ALLOWED", "Query not in allow-list"));
    }
    
    let mut auth = extract_auth_from_headers(&headers);
    auth.enrich_with_operator_map(&state.user_operator_map).await;
    
    let mut cmd = {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        query_text.hash(&mut h);
        let key = h.finish();

        if let Some(cached) = state.parse_cache.get(&key) {
            cached
        } else {
            match qail_core::parser::parse(query_text) {
                Ok(cmd) => {
                    state.parse_cache.insert(key, cmd.clone());
                    cmd
                }
                Err(e) => {
                    return Err(ApiError::parse_error(format!("Parse error: {}", e)));
                }
            }
        }
    };
    
    if let Err(e) = state.policy_engine.apply_policies(&auth, &mut cmd) {
        return Err(ApiError::with_code("POLICY_DENIED", e.to_string()));
    }
    
    execute_qail_cmd_fast(&state, &auth, &cmd).await
}

/// Fast query execution — uses fetch_all_fast (AST-native wire).
/// Returns array-of-arrays without column names.
async fn execute_qail_cmd_fast(
    state: &Arc<GatewayState>,
    auth: &crate::auth::AuthContext,
    cmd: &qail_core::ast::Qail,
) -> Result<Json<FastQueryResponse>, ApiError> {
    use qail_core::ast::Action;

    let (depth, filters, joins) = query_complexity(cmd);
    if let Err(api_err) = state.complexity_guard.check(depth, filters, joins) {
        crate::metrics::record_complexity_rejected();
        return Err(api_err);
    }

    if matches!(cmd.action, Action::Search | Action::Upsert | Action::Scroll
        | Action::CreateCollection | Action::DeleteCollection) {
        return Err(ApiError::bad_request(
            "UNSUPPORTED_ACTION",
            "Vector operations not supported on /qail/fast",
        ));
    }

    let mut conn = state.pool
        .acquire_with_rls_timeout(auth.to_rls_context(), state.config.statement_timeout_ms)
        .await
        .map_err(|e| ApiError::connection_error(e.to_string()))?;

    let timer = crate::metrics::QueryTimer::new(&cmd.table, &cmd.action.to_string());
    let rows = conn.fetch_all_fast(cmd).await.map_err(|e| {
        ApiError::query_error(e.to_string())
    });
    timer.finish(rows.is_ok());
    conn.release().await;
    let rows = rows?;
    
    let json_rows: Vec<Vec<serde_json::Value>> = rows
        .iter()
        .map(row_to_array)
        .collect();
    
    let count = json_rows.len();
    Ok(Json(FastQueryResponse { rows: json_rows, count }))
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
) -> Result<Json<QueryResponse>, ApiError> {
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
        return Err(api_err);
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
    
    // Acquire raw connection (no RLS setup yet — pipelined below)
    let mut conn = state.pool
        .acquire_raw()
        .await
        .map_err(|e| ApiError::connection_error(e.to_string()))?;

    // Generate RLS SQL for pipelining (BEGIN + SET LOCAL + set_config)
    let rls_sql = qail_pg::rls_sql_with_timeout(
        &auth.to_rls_context(),
        state.config.statement_timeout_ms,
    );
    
    // Measure query execution time
    let timer = crate::metrics::QueryTimer::new(&cmd.table, &cmd.action.to_string());
    let rows = conn.fetch_all_with_rls(cmd, &rls_sql).await.map_err(|e| {
        ApiError::query_error(e.to_string())
    });
    timer.finish(rows.is_ok());

    // Deterministic cleanup — release connection before processing results.
    // If the query failed, conn is still released cleanly (COMMIT runs).
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
            ApiError::with_code("TENANT_BOUNDARY_VIOLATION", "Data integrity error")
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
        tracing::debug!("Cache INVALIDATED for table '{}' (mutation)", table);
    }
    
    Ok(Json(response))
}

pub async fn execute_batch(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Json(request): Json<BatchRequest>,
) -> Result<Json<BatchResponse>, ApiError> {
    if request.queries.is_empty() {
        return Err(ApiError::bad_request("EMPTY_BATCH", "Empty query batch"));
    }

    // SECURITY (E2): Cap batch size to prevent resource exhaustion.
    if request.queries.len() > state.config.max_batch_queries {
        return Err(ApiError::bad_request(
            "BATCH_TOO_LARGE",
            format!(
                "Batch size {} exceeds maximum of {}",
                request.queries.len(),
                state.config.max_batch_queries,
            ),
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
        .map_err(|e| ApiError::connection_error(e.to_string()))?;
    
    // Start transaction if requested (default: true)
    if request.transaction {
        conn.get_mut().execute_simple("BEGIN;").await.map_err(|e| {
            tracing::error!("Transaction start failed: {}", e);
            ApiError::with_code("TXN_ERROR", "Transaction start failed")
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
        
        // Apply RLS policies
        if let Err(e) = state.policy_engine.apply_policies(&auth, &mut cmd) {
            results.push(BatchQueryResult {
                index,
                success: false,
                rows: None,
                count: None,
                error: Some(format!("Policy error: {}", e)),
            });
            if request.transaction {
                had_error = true;
                break;
            }
            continue;
        }

        // Query complexity check
        let (depth, filters, joins) = query_complexity(&cmd);
        if let Err(api_err) = state.complexity_guard.check(depth, filters, joins) {
            results.push(BatchQueryResult {
                index,
                success: false,
                rows: None,
                count: None,
                error: Some(api_err.message),
            });
            if request.transaction {
                had_error = true;
                break;
            }
            continue;
        }
        
        // Execute query
        let timer = crate::metrics::QueryTimer::new(&cmd.table, &cmd.action.to_string());
        match conn.fetch_all_uncached(&cmd).await {
            Ok(rows) => {
                timer.finish(true);
                let json_rows: Vec<serde_json::Value> = rows
                    .iter()
                    .map(row_to_json)
                    .collect();
                let count = json_rows.len();
                
                // Invalidate cache for mutations
                if !matches!(cmd.action, qail_core::ast::Action::Get) {
                    state.cache.invalidate_table(&cmd.table);
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
                ApiError::with_code("TXN_ERROR", "Transaction commit failed")
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
