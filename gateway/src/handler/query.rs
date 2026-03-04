//! Qail AST query execution handlers.
//!
//! Text, binary, fast, and batch query endpoints.

use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{Json, Response},
};
use std::sync::Arc;

use super::convert::{row_to_array, row_to_json};
#[cfg(feature = "qdrant")]
use super::qdrant::execute_qdrant_cmd;
use super::{BatchQueryResult, BatchRequest, BatchResponse, FastQueryResponse, QueryResponse};
use crate::GatewayState;
use crate::auth::authenticate_request;
use crate::middleware::ApiError;
use qail_core::ast::Action;

/// SECURITY (P0-2): Reject dangerous procedural/session actions on public query endpoints.
pub(crate) fn reject_dangerous_action(cmd: &qail_core::ast::Qail) -> Result<(), ApiError> {
    match cmd.action {
        Action::Call
        | Action::Do
        | Action::SessionSet
        | Action::SessionShow
        | Action::SessionReset => Err(ApiError::with_code(
            "ACTION_DENIED",
            format!(
                "Action {:?} is not allowed on public query endpoints",
                cmd.action
            ),
        )),
        _ => Ok(()),
    }
}

/// Execute a single Qail query (POST /qail).
pub async fn execute_query(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    body: String,
) -> Result<Json<QueryResponse>, ApiError> {
    let query_text = body.trim();

    if query_text.is_empty() {
        return Err(ApiError::bad_request("EMPTY_QUERY", "Empty query"));
    }

    // Extract + enforce auth (JWT/JWKS + allowed algorithms + strict mode + tenant rate limit)
    let auth = authenticate_request(state.as_ref(), &headers).await?;

    tracing::debug!(
        "Executing text query: {} (user: {})",
        query_text,
        auth.user_id
    );

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

    // SECURITY (P0-2): Reject dangerous actions
    reject_dangerous_action(&cmd)?;

    // SECURITY: Query allow-list check (raw text, canonical QAIL, or SQL).
    if !is_query_allowed(&state.allow_list, Some(query_text), &cmd) {
        tracing::warn!("Query rejected by allow-list: {}", query_text);
        return Err(ApiError::with_code(
            "QUERY_NOT_ALLOWED",
            "Query not in allow-list",
        ));
    }

    // Apply row-level security policies
    if let Err(e) = state.policy_engine.apply_policies(&auth, &mut cmd) {
        tracing::warn!("Policy error: {}", e);
        return Err(ApiError::with_code("POLICY_DENIED", e.to_string()));
    }

    // SECURITY: Clamp LIMIT at AST level so PostgreSQL stops scanning early.
    clamp_query_limit(&mut cmd, state.config.max_result_rows);

    execute_qail_cmd(&state, &auth, &cmd).await
}

/// Execute a streaming export query (POST /qail/export).
///
/// Accepts QAIL text that must compile to `Action::Export` and streams raw
/// COPY TO STDOUT chunks to the HTTP response body.
pub async fn execute_query_export(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    body: String,
) -> Result<Response, ApiError> {
    let query_text = body.trim();
    if query_text.is_empty() {
        return Err(ApiError::bad_request("EMPTY_QUERY", "Empty query"));
    }

    let auth = authenticate_request(state.as_ref(), &headers).await?;

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

    if cmd.action != Action::Export {
        return Err(ApiError::bad_request(
            "EXPORT_ONLY",
            "Endpoint /qail/export only accepts QAIL export commands",
        ));
    }

    // Reuse the same public endpoint action deny-list for consistency.
    reject_dangerous_action(&cmd)?;

    if !is_query_allowed(&state.allow_list, Some(query_text), &cmd) {
        tracing::warn!("Export query rejected by allow-list: {}", query_text);
        return Err(ApiError::with_code(
            "QUERY_NOT_ALLOWED",
            "Query not in allow-list",
        ));
    }

    if let Err(e) = state.policy_engine.apply_policies(&auth, &mut cmd) {
        tracing::warn!("Policy error: {}", e);
        return Err(ApiError::with_code("POLICY_DENIED", e.to_string()));
    }

    let (depth, filters, joins) = query_complexity(&cmd);
    if let Err(api_err) = state.complexity_guard.check(depth, filters, joins) {
        crate::metrics::record_complexity_rejected();
        return Err(api_err);
    }

    let mut conn = state
        .acquire_with_auth_rls_guarded(&auth, Some(&cmd.table))
        .await?;
    let cmd_for_stream = cmd.clone();
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(8);

    tokio::spawn(async move {
        let tx_for_chunks = tx.clone();
        let result = conn
            .copy_export_stream_raw(&cmd_for_stream, move |chunk| {
                let tx = tx_for_chunks.clone();
                async move {
                    tx.send(Ok(Bytes::from(chunk))).await.map_err(|_| {
                        qail_pg::PgError::Query(
                            "export stream receiver dropped before completion".to_string(),
                        )
                    })
                }
            })
            .await;

        if let Err(e) = result {
            let _ = tx
                .send(Err(std::io::Error::other(format!(
                    "export stream failed: {}",
                    e
                ))))
                .await;
        }
        conn.release().await;
    });

    let stream = futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });

    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    Ok(response)
}

/// Execute a QAIL query (BINARY format)
///
/// Accepts postcard-encoded QAIL AST and returns JSON results.
/// This is faster than text format since it skips parsing.
pub async fn execute_query_binary(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<QueryResponse>, ApiError> {
    if body.is_empty() {
        return Err(ApiError::bad_request("EMPTY_QUERY", "Empty binary query"));
    }

    // Extract + enforce auth (JWT/JWKS + allowed algorithms + strict mode + tenant rate limit)
    let auth = authenticate_request(state.as_ref(), &headers).await?;

    tracing::debug!(
        "Executing binary query ({} bytes, user: {})",
        body.len(),
        auth.user_id
    );

    // SECURITY (E3): Limit deserialization to 64 KiB to prevent allocation bombs.
    if body.len() > 64 * 1024 {
        return Err(ApiError::bad_request(
            "PAYLOAD_TOO_LARGE",
            "Binary query exceeds 64 KiB limit",
        ));
    }

    // Deserialize the binary QAIL AST (postcard format)
    let mut cmd: qail_core::ast::Qail = match postcard::from_bytes(&body) {
        Ok(cmd) => cmd,
        Err(e) => {
            tracing::warn!("Postcard decode error: {}", e);
            return Err(ApiError::bad_request(
                "DECODE_ERROR",
                format!("Invalid binary format: {}", e),
            ));
        }
    };

    // SECURITY: Validate AST identifiers — reject SQL injection in table/column names,
    // Expr::Raw nodes, and dangerous procedural actions (Call, Do, SessionSet).
    if let Err(e) = qail_core::sanitize::validate_ast(&cmd) {
        tracing::warn!("Binary AST rejected by structural validation: {}", e);
        return Err(ApiError::bad_request(
            "AST_VALIDATION_FAILED",
            format!("Invalid AST: {}", e),
        ));
    }

    // SECURITY: When binary_requires_allow_list is true (default), reject binary
    // queries if no allow-list is loaded. This prevents the binary endpoint from
    // being a completely unguarded entry point when operators haven't configured
    // query restrictions.
    if state.config.binary_requires_allow_list && !state.allow_list.is_enabled() {
        tracing::warn!(
            "Binary query rejected: binary_requires_allow_list=true but no allow-list is loaded. \
             Set QAIL_BINARY_REQUIRES_ALLOW_LIST=false or configure an allow-list file."
        );
        return Err(ApiError::with_code(
            "BINARY_REQUIRES_ALLOW_LIST",
            "Binary endpoint requires a query allow-list to be configured",
        ));
    }

    // SECURITY (P0-2): Reject dangerous actions
    reject_dangerous_action(&cmd)?;

    // SECURITY: Enforce allow-list for binary endpoint too.
    if !is_query_allowed(&state.allow_list, None, &cmd) {
        tracing::warn!("Binary query rejected by allow-list");
        return Err(ApiError::with_code(
            "QUERY_NOT_ALLOWED",
            "Query not in allow-list",
        ));
    }

    // Apply row-level security policies
    if let Err(e) = state.policy_engine.apply_policies(&auth, &mut cmd) {
        tracing::warn!("Policy error: {}", e);
        return Err(ApiError::with_code("POLICY_DENIED", e.to_string()));
    }

    // SECURITY: Clamp LIMIT at AST level so PostgreSQL stops scanning early.
    clamp_query_limit(&mut cmd, state.config.max_result_rows);

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

    // Extract + enforce auth (JWT/JWKS + allowed algorithms + strict mode + tenant rate limit)
    let auth = authenticate_request(state.as_ref(), &headers).await?;

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

    // SECURITY (P0-2): Reject dangerous actions
    reject_dangerous_action(&cmd)?;

    if !is_query_allowed(&state.allow_list, Some(query_text), &cmd) {
        tracing::warn!("Fast query rejected by allow-list: {}", query_text);
        return Err(ApiError::with_code(
            "QUERY_NOT_ALLOWED",
            "Query not in allow-list",
        ));
    }

    if let Err(e) = state.policy_engine.apply_policies(&auth, &mut cmd) {
        return Err(ApiError::with_code("POLICY_DENIED", e.to_string()));
    }

    // SECURITY: Clamp LIMIT at AST level so PostgreSQL stops scanning early.
    clamp_query_limit(&mut cmd, state.config.max_result_rows);

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

    if matches!(
        cmd.action,
        Action::Search
            | Action::Upsert
            | Action::Scroll
            | Action::CreateCollection
            | Action::DeleteCollection
    ) {
        return Err(ApiError::bad_request(
            "UNSUPPORTED_ACTION",
            "Vector operations not supported on /qail/fast",
        ));
    }

    let mut conn = state
        .acquire_with_auth_rls_guarded(auth, Some(&cmd.table))
        .await?;

    let timer = crate::metrics::QueryTimer::new(&cmd.table, &cmd.action.to_string());
    let rows = conn
        .fetch_all_fast(cmd)
        .await
        .map_err(|e| ApiError::from_pg_driver_error(&e, Some(&cmd.table)));
    timer.finish(rows.is_ok());
    conn.release().await;
    let rows = rows?;

    let json_rows: Vec<Vec<serde_json::Value>> = rows.iter().map(row_to_array).collect();

    let count = json_rows.len();
    Ok(Json(FastQueryResponse {
        rows: json_rows,
        count,
    }))
}

/// Extract query complexity metrics from a QAIL AST.
///
/// Returns (depth, filter_count, join_count) where:
/// - depth = CTEs + set ops + source subquery nesting
/// - filter_count = total conditions across all Filter cages
/// - join_count = number of JOIN clauses
pub(crate) fn query_complexity(cmd: &qail_core::ast::Qail) -> (usize, usize, usize) {
    use qail_core::ast::CageKind;

    let depth = cmd.ctes.len() + cmd.set_ops.len() + usize::from(cmd.source_query.is_some());

    let filter_count: usize = cmd
        .cages
        .iter()
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
        Action::Search
            | Action::Upsert
            | Action::Scroll
            | Action::CreateCollection
            | Action::DeleteCollection
    ) {
        #[cfg(feature = "qdrant")]
        {
            return execute_qdrant_cmd(state, cmd).await;
        }
        #[cfg(not(feature = "qdrant"))]
        {
            return Err(ApiError::bad_request(
                "QDRANT_DISABLED",
                "Vector operations require the 'qdrant' feature",
            ));
        }
    }

    let table = &cmd.table;
    let is_read_query = matches!(cmd.action, Action::Get);

    // Generate cache key from full AST payload + identity.
    // SECURITY: Include tenant/user identity plus value-sensitive AST hash to
    // prevent stale collisions between different filter values.
    let tenant = auth.tenant_id.as_deref().unwrap_or("_anon");
    let cache_key = format!("{}:{}:{}", tenant, auth.user_id, exact_cache_key(cmd));

    // Check cache for read queries
    if is_read_query && let Some(cached) = state.cache.get(&cache_key) {
        tracing::debug!("Cache HIT for table '{}'", table);
        // Parse cached JSON back to response
        if let Ok(response) = serde_json::from_str::<QueryResponse>(&cached) {
            return Ok(Json(response));
        }
    }

    // Acquire raw connection (no RLS setup yet — pipelined below)
    let mut conn = state
        .acquire_raw_with_auth_guarded(auth, Some(&cmd.table))
        .await?;

    // Generate RLS SQL for pipelining (BEGIN + SET LOCAL + set_config)
    let rls_sql = qail_pg::rls_sql_with_timeouts(
        &auth.to_rls_context(),
        state.config.statement_timeout_ms,
        state.config.lock_timeout_ms,
    );

    // Measure query execution time
    let timer = crate::metrics::QueryTimer::new(&cmd.table, &cmd.action.to_string());
    let rows = conn
        .fetch_all_with_rls(cmd, &rls_sql)
        .await
        .map_err(|e| ApiError::from_pg_driver_error(&e, Some(&cmd.table)));
    timer.finish(rows.is_ok());

    // Deterministic cleanup — release connection before processing results.
    // If the query failed, conn is still released cleanly (COMMIT runs).
    conn.release().await;

    let rows = rows?;

    // Convert rows to JSON
    // Note: max_result_rows is enforced via AST LIMIT injection (clamp_query_limit)
    // before execution, so PostgreSQL stops scanning early — no OOM risk.
    let json_rows: Vec<serde_json::Value> = rows.iter().map(row_to_json).collect();

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
        )
        .map_err(|v| {
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

/// Execute a batch of Qail queries (POST /qail/batch).
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

    // Extract + enforce auth (JWT/JWKS + allowed algorithms + strict mode + tenant rate limit)
    let auth = authenticate_request(state.as_ref(), &headers).await?;

    tracing::info!(
        "Executing batch of {} queries (txn={}, user: {})",
        request.queries.len(),
        request.transaction,
        auth.user_id
    );

    let mut results = Vec::with_capacity(request.queries.len());
    let mut success_count = 0;

    // Acquire RLS-scoped connection with statement timeout
    let mut conn = state.acquire_with_auth_rls_guarded(&auth, None).await?;

    // Start transaction if requested (default: true)
    if request.transaction {
        match conn.get_mut() {
            Ok(pg_conn) => {
                if let Err(e) = pg_conn.execute_simple("BEGIN;").await {
                    tracing::error!("Transaction start failed: {}", e);
                    conn.release().await;
                    return Err(ApiError::with_code("TXN_ERROR", "Transaction start failed"));
                }
            }
            Err(e) => {
                conn.release().await;
                return Err(ApiError::from_pg_driver_error(&e, None));
            }
        }
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

        // SECURITY (P0-2): Reject dangerous actions in batch
        if let Err(e) = reject_dangerous_action(&cmd) {
            results.push(BatchQueryResult {
                index,
                success: false,
                rows: None,
                count: None,
                error: Some(e.message.clone()),
            });
            if request.transaction {
                had_error = true;
                break;
            }
            continue;
        }

        if !is_query_allowed(&state.allow_list, Some(query_text), &cmd) {
            results.push(BatchQueryResult {
                index,
                success: false,
                rows: None,
                count: None,
                error: Some("Query not in allow-list".to_string()),
            });
            if request.transaction {
                had_error = true;
                break;
            }
            continue;
        }

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
                error: Some(api_err.message.clone()),
            });
            if request.transaction {
                had_error = true;
                break;
            }
            continue;
        }

        // SECURITY: Clamp LIMIT at AST level so PostgreSQL stops scanning early.
        clamp_query_limit(&mut cmd, state.config.max_result_rows);

        // Execute query
        let timer = crate::metrics::QueryTimer::new(&cmd.table, &cmd.action.to_string());
        match conn.fetch_all_uncached(&cmd).await {
            Ok(rows) => {
                timer.finish(true);
                let json_rows: Vec<serde_json::Value> = rows.iter().map(row_to_json).collect();
                let count = json_rows.len();

                // SECURITY (P0-R6): Tenant boundary verification in batch results.
                if matches!(cmd.action, qail_core::ast::Action::Get)
                    && let Some(ref tenant_id) = auth.tenant_id
                    && let Err(v) = crate::tenant_guard::verify_tenant_boundary(
                        &json_rows,
                        tenant_id,
                        &state.config.tenant_column,
                        &cmd.table,
                        "batch_query",
                    )
                {
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
                timer.finish(false);
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
                // Non-transactional batch: the failed query put PG into
                // aborted-transaction state (because acquire_with_rls uses BEGIN).
                // Reset the connection so subsequent batch queries can execute.
                if let Ok(pg_conn) = conn.get_mut() {
                    let rls_sql = qail_pg::rls_sql_with_timeouts(
                        &auth.to_rls_context(),
                        state.config.statement_timeout_ms,
                        state.config.lock_timeout_ms,
                    );
                    let reset_sql = format!("ROLLBACK; {}", rls_sql);
                    if let Err(re) = pg_conn.execute_simple(&reset_sql).await {
                        tracing::warn!(
                            "Batch non-txn reset failed after query error: {}; \
                             remaining queries will fail",
                            re
                        );
                    }
                }
            }
        }
    }

    // Transaction finalization
    if request.transaction {
        if had_error {
            if let Ok(pg_conn) = conn.get_mut() {
                let _ = pg_conn.execute_simple("ROLLBACK;").await;
            }
            tracing::warn!("Batch transaction rolled back due to error");
        } else {
            match conn.get_mut() {
                Ok(pg_conn) => {
                    if let Err(e) = pg_conn.execute_simple("COMMIT;").await {
                        tracing::error!("Transaction commit failed: {}", e);
                        conn.release().await;
                        return Err(ApiError::with_code(
                            "TXN_ERROR",
                            "Transaction commit failed",
                        ));
                    }
                }
                Err(e) => {
                    conn.release().await;
                    return Err(ApiError::from_pg_driver_error(&e, None));
                }
            }
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

/// Generate a cache key from the full AST payload (including filter values).
fn exact_cache_key(cmd: &qail_core::ast::Qail) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let payload = serde_json::to_string(cmd).unwrap_or_else(|_| format!("{cmd:?}"));
    let mut hasher = DefaultHasher::new();
    payload.hash(&mut hasher);
    format!("full:{:016x}", hasher.finish())
}

/// Clamp the LIMIT on a Qail command to at most `max_rows`.
///
/// SECURITY: This must be called **before** execution so PostgreSQL's planner can
/// use the LIMIT to cut off scanning. Post-fetch truncation does not prevent
/// memory exhaustion because all rows are already materialized.
///
/// - If the AST has no LIMIT cage, one is injected.
/// - If the existing LIMIT is higher than `max_rows`, it is lowered.
/// - If the existing LIMIT is already ≤ `max_rows`, nothing changes.
///
/// Only applies to read queries (Get/With/Cnt) — mutations are left untouched.
pub(crate) fn clamp_query_limit(cmd: &mut qail_core::ast::Qail, max_rows: usize) {
    use qail_core::ast::{Action, Cage, CageKind, LogicalOp};

    // Only clamp read actions — writes have RETURNING which is typically small.
    if !matches!(cmd.action, Action::Get | Action::With | Action::Cnt) {
        return;
    }

    // Find existing Limit cage
    for cage in &mut cmd.cages {
        if let CageKind::Limit(ref mut n) = cage.kind {
            if *n > max_rows {
                *n = max_rows;
            }
            return; // Already has a limit, clamped or already fine.
        }
    }

    // No limit cage — inject one.
    cmd.cages.push(Cage {
        kind: CageKind::Limit(max_rows),
        conditions: vec![],
        logical_op: LogicalOp::And,
    });
}

/// Check allow-list against multiple canonical forms.
pub(crate) fn is_query_allowed(
    allow_list: &crate::middleware::QueryAllowList,
    raw_query: Option<&str>,
    cmd: &qail_core::ast::Qail,
) -> bool {
    use qail_core::transpiler::ToSql;

    // Fast path: allow-list disabled.
    if !allow_list.is_enabled() {
        return true;
    }

    if let Some(raw) = raw_query
        && allow_list.is_allowed(raw)
    {
        return true;
    }

    // Canonical QAIL formatter (Display impl).
    let canonical_qail = cmd.to_string();
    if allow_list.is_allowed(&canonical_qail) {
        return true;
    }

    // SQL fallback for deployments that store SQL patterns.
    let sql = cmd.to_sql();
    allow_list.is_allowed(&sql)
}

#[cfg(test)]
mod tests {
    use super::{exact_cache_key, execute_query_export, is_query_allowed};
    use crate::GatewayState;
    use crate::cache::QueryCache;
    use crate::concurrency::TenantSemaphore;
    use crate::config::GatewayConfig;
    use crate::event::EventTriggerEngine;
    use crate::middleware::{QueryAllowList, QueryComplexityGuard, RateLimiter};
    use crate::policy::PolicyEngine;
    use crate::schema::SchemaRegistry;
    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use axum::routing::post;
    use jsonwebtoken::Algorithm;
    use metrics_exporter_prometheus::PrometheusBuilder;
    use qail_pg::{PgPool, PoolConfig};
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::RwLock;
    use tower::util::ServiceExt;

    async fn build_test_state(
        config: GatewayConfig,
        allow_list: QueryAllowList,
    ) -> Arc<GatewayState> {
        let pool = PgPool::connect(
            PoolConfig::new_dev("127.0.0.1", 5432, "qail", "qail")
                .min_connections(0)
                .max_connections(1)
                .connect_timeout(Duration::from_millis(25))
                .acquire_timeout(Duration::from_millis(25)),
        )
        .await
        .expect("pool should initialize lazily with min_connections=0");

        let explain_config = config.explain_config();
        let explain_cache = qail_pg::explain::ExplainCache::new(explain_config.cache_ttl);
        let tenant_semaphore = Arc::new(TenantSemaphore::with_limits(
            config.max_concurrent_queries,
            config.max_tenants,
            Duration::from_secs(config.tenant_idle_timeout_secs),
        ));
        let db_backpressure = Arc::new(crate::db_backpressure::DbBackpressure::new(
            config.db_max_waiters_global,
            config.db_max_waiters_per_tenant,
            config.max_tenants,
        ));
        let prometheus_handle = {
            let recorder = PrometheusBuilder::new().build_recorder();
            Arc::new(recorder.handle())
        };
        let txn_max = if config.txn_max_sessions > 0 {
            config.txn_max_sessions
        } else {
            std::cmp::max(pool.max_connections() / 2, 2)
        };

        Arc::new(GatewayState {
            pool,
            policy_engine: PolicyEngine::new(),
            event_engine: EventTriggerEngine::new(),
            schema: SchemaRegistry::new(),
            cache: QueryCache::new(config.cache_config()),
            config: config.clone(),
            rate_limiter: RateLimiter::new(config.rate_limit_rate, config.rate_limit_burst),
            tenant_rate_limiter: RateLimiter::new(
                config.tenant_rate_limit_rate,
                config.tenant_rate_limit_burst,
            ),
            explain_cache,
            explain_config,
            tenant_semaphore,
            db_backpressure,
            user_operator_map: Arc::new(RwLock::new(HashMap::new())),
            #[cfg(feature = "qdrant")]
            qdrant_pool: None,
            prometheus_handle,
            complexity_guard: QueryComplexityGuard::new(
                config.max_query_depth,
                config.max_query_filters,
                config.max_query_joins,
            ),
            allow_list,
            rpc_allow_list: None,
            rpc_signature_cache: moka::sync::Cache::builder()
                .max_capacity(64)
                .time_to_live(Duration::from_secs(60))
                .build(),
            parse_cache: moka::sync::Cache::builder()
                .max_capacity(64)
                .time_to_live(Duration::from_secs(60))
                .build(),
            idempotency_store: crate::idempotency::IdempotencyStore::production(),
            jwks_store: None,
            jwt_allowed_algorithms: Vec::<Algorithm>::new(),
            blocked_tables: HashSet::new(),
            allowed_tables: HashSet::new(),
            transaction_manager: Arc::new(crate::transaction::TransactionSessionManager::new(
                txn_max,
                config.txn_session_timeout_secs,
                config.txn_max_lifetime_secs,
                config.txn_max_statements_per_session,
            )),
        })
    }

    fn parse_error_code(bytes: &[u8]) -> String {
        let value: serde_json::Value = serde_json::from_slice(bytes).expect("valid JSON error");
        value
            .get("code")
            .and_then(serde_json::Value::as_str)
            .expect("error code")
            .to_string()
    }

    #[test]
    fn cache_key_includes_filter_values() {
        let a = qail_core::ast::Qail::get("users").eq("age", 25);
        let b = qail_core::ast::Qail::get("users").eq("age", 30);
        assert_ne!(
            exact_cache_key(&a),
            exact_cache_key(&b),
            "cache key must differ when filter values differ"
        );
    }

    #[test]
    fn allow_list_disabled_allows_query() {
        let allow_list = QueryAllowList::new();
        let cmd = qail_core::ast::Qail::get("users");
        assert!(is_query_allowed(&allow_list, None, &cmd));
    }

    #[test]
    fn allow_list_accepts_canonical_qail() {
        let cmd = qail_core::ast::Qail::get("users")
            .columns(["id"])
            .eq("active", true);
        let mut allow_list = QueryAllowList::new();
        allow_list.allow(&cmd.to_string());
        assert!(is_query_allowed(&allow_list, None, &cmd));
    }

    #[test]
    fn allow_list_rejects_unlisted_query() {
        let cmd = qail_core::ast::Qail::get("users").columns(["id"]);
        let mut allow_list = QueryAllowList::new();
        allow_list.allow("get other_table");
        assert!(!is_query_allowed(&allow_list, None, &cmd));
    }

    // ── Regression: query_complexity is pub(crate) for WS parity ─────

    #[test]
    fn query_complexity_simple_query() {
        let cmd = qail_core::ast::Qail::get("users")
            .columns(["id"])
            .eq("active", true);
        let (depth, filters, joins) = super::query_complexity(&cmd);
        assert_eq!(depth, 0);
        assert_eq!(filters, 1);
        assert_eq!(joins, 0);
    }

    #[test]
    fn query_complexity_with_joins() {
        use qail_core::ast::JoinKind;
        let cmd = qail_core::ast::Qail::get("orders")
            .join(JoinKind::Left, "users", "orders.user_id", "users.id")
            .eq("status", "active")
            .eq("visible", true);
        let (depth, filters, joins) = super::query_complexity(&cmd);
        assert_eq!(depth, 0);
        assert_eq!(filters, 2);
        assert_eq!(joins, 1);
    }

    #[tokio::test]
    async fn export_handler_rejects_empty_query() {
        let _serial = crate::metrics::txn_test_serial_guard().await;
        let mut config = GatewayConfig::default();
        config.production_strict = false;

        let state = build_test_state(config, QueryAllowList::new()).await;
        let app = Router::new()
            .route("/qail/export", post(execute_query_export))
            .with_state(Arc::clone(&state));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/qail/export")
                    .header("content-type", "text/plain")
                    .body(Body::from("   "))
                    .expect("request should build"),
            )
            .await
            .expect("request should execute");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        assert_eq!(parse_error_code(&body), "EMPTY_QUERY");
    }

    #[tokio::test]
    async fn export_handler_rejects_non_export_query() {
        let _serial = crate::metrics::txn_test_serial_guard().await;
        let mut config = GatewayConfig::default();
        config.production_strict = false;

        let state = build_test_state(config, QueryAllowList::new()).await;
        let app = Router::new()
            .route("/qail/export", post(execute_query_export))
            .with_state(Arc::clone(&state));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/qail/export")
                    .header("content-type", "text/plain")
                    .body(Body::from("get users"))
                    .expect("request should build"),
            )
            .await
            .expect("request should execute");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        assert_eq!(parse_error_code(&body), "EXPORT_ONLY");
    }

    #[tokio::test]
    async fn export_handler_enforces_allow_list_before_db_acquire() {
        let _serial = crate::metrics::txn_test_serial_guard().await;
        let mut config = GatewayConfig::default();
        config.production_strict = false;
        let mut allow_list = QueryAllowList::new();
        allow_list.enable();

        let state = build_test_state(config, allow_list).await;
        let app = Router::new()
            .route("/qail/export", post(execute_query_export))
            .with_state(Arc::clone(&state));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/qail/export")
                    .header("content-type", "text/plain")
                    .body(Body::from("export users fields id"))
                    .expect("request should build"),
            )
            .await
            .expect("request should execute");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        assert_eq!(parse_error_code(&body), "QUERY_NOT_ALLOWED");
    }
}
