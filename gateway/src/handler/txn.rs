//! Transaction session REST endpoint handlers.
//!
//! Provides multi-statement transaction support via the gateway HTTP API:
//! - `POST /txn/begin` — start a new transaction session
//! - `POST /txn/query` — execute a query within a transaction
//! - `POST /txn/commit` — commit and close a transaction
//! - `POST /txn/rollback` — rollback and close a transaction
//! - `POST /txn/savepoint` — savepoint operations within a transaction

use axum::{extract::State, http::HeaderMap, response::Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::GatewayState;
use crate::auth::authenticate_request;
use crate::middleware::ApiError;

// ── Request/Response types ──

/// Response from `POST /txn/begin`.
#[derive(Debug, Serialize)]
pub struct TxnBeginResponse {
    /// Unique session ID. Include as `X-Transaction-Id` in subsequent requests.
    pub txn_id: String,
}

/// Response from `POST /txn/commit` or `POST /txn/rollback`.
#[derive(Debug, Serialize)]
pub struct TxnEndResponse {
    /// Action performed: "committed" or "rolled_back".
    pub status: String,
}

/// Request body for `POST /txn/savepoint`.
#[derive(Debug, Deserialize)]
pub struct SavepointRequest {
    /// Savepoint action: "create", "rollback", or "release".
    pub action: String,
    /// Savepoint name.
    pub name: String,
}

/// Response from `POST /txn/savepoint`.
#[derive(Debug, Serialize)]
pub struct SavepointResponse {
    /// Action performed.
    pub action: String,
    /// Savepoint name.
    pub name: String,
}

// ── Helper ──

/// Extract the transaction session ID from headers.
fn extract_txn_id(headers: &HeaderMap) -> Result<String, ApiError> {
    headers
        .get("x-transaction-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .ok_or_else(|| ApiError::bad_request("MISSING_TXN_ID", "Missing X-Transaction-Id header"))
}

// ── Handlers ──

/// `POST /txn/begin` — Start a new transaction session.
///
/// Acquires a connection from the pool, sets RLS context, and issues BEGIN.
/// Returns a session ID to use in subsequent `/txn/*` requests.
pub async fn txn_begin(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
) -> Result<Json<TxnBeginResponse>, ApiError> {
    let auth = authenticate_request(&state, &headers).await?;

    let tenant_id = auth.tenant_id.clone().unwrap_or_default();
    let user_id = Some(auth.user_id.clone());

    let limits = state.config.effective_limits(&auth.role);

    let txn_id = state
        .create_txn_session_guarded(
            &auth,
            tenant_id,
            user_id,
            limits.statement_timeout_ms,
            limits.lock_timeout_ms,
        )
        .await
        .map_err(txn_err_to_api)?;

    Ok(Json(TxnBeginResponse { txn_id }))
}

/// `POST /txn/query` — Execute a query within an existing transaction session.
///
/// Requires `X-Transaction-Id` header. The query runs on the pinned connection
/// bound to that session with full RLS context.
pub async fn txn_query(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    body: String,
) -> Result<Json<super::QueryResponse>, ApiError> {
    let auth = authenticate_request(&state, &headers).await?;
    let txn_id = extract_txn_id(&headers)?;
    let tenant_id = auth.tenant_id.clone().unwrap_or_default();

    // Parse the query
    let mut cmd = qail_core::parser::parse(&body)
        .map_err(|e| ApiError::bad_request("PARSE_ERROR", format!("Parse error: {}", e)))?;

    // Security: reject dangerous actions
    super::query::reject_dangerous_action(&cmd)?;

    // Security: reject DDL inside transactions
    reject_ddl_in_transaction(&cmd)?;

    // Enforce query allow-list parity with non-transaction endpoints.
    if !crate::handler::is_query_allowed(&state.allow_list, Some(&body), &cmd) {
        return Err(ApiError::with_code(
            "QUERY_NOT_ALLOWED",
            "Query not in allow-list",
        ));
    }

    // Apply policy filters/rewrites before execution.
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::with_code("POLICY_DENIED", e.to_string()))?;

    // Clamp LIMIT to prevent oversized result sets in long-lived txn sessions.
    crate::handler::clamp_query_limit(&mut cmd, state.config.max_result_rows);

    // Complexity guard parity with /qail.
    let (depth, filters, joins) = crate::handler::query::query_complexity(&cmd);
    if let Err(api_err) = state.complexity_guard.check(depth, filters, joins) {
        crate::metrics::record_complexity_rejected();
        return Err(api_err);
    }

    let cmd_table = cmd.table.clone();

    // Execute within the pinned session
    let rows = state
        .transaction_manager
        .with_session(&txn_id, &tenant_id, |session| {
            Box::pin(async move {
                use super::convert::row_to_json;
                let conn = session
                    .conn
                    .as_mut()
                    .ok_or(crate::transaction::TransactionError::SessionNotFound)?;
                let result =
                    conn.fetch_all_uncached(&cmd)
                        .await
                        .map_err(|e: qail_pg::PgError| {
                            crate::transaction::TransactionError::Database(e.to_string())
                        })?;

                let json_rows: Vec<serde_json::Value> = result.iter().map(row_to_json).collect();

                Ok(json_rows)
            })
        })
        .await
        .map_err(txn_err_to_api)?;

    if let Some(ref tenant_id) = auth.tenant_id {
        let _proof = crate::tenant_guard::verify_tenant_boundary(
            &rows,
            tenant_id,
            &state.config.tenant_column,
            &cmd_table,
            "txn_query",
        )
        .map_err(|v| {
            tracing::error!("{}", v);
            ApiError::internal("Data integrity error")
        })?;
    }

    let count = rows.len();
    Ok(Json(super::QueryResponse { rows, count }))
}

/// `POST /txn/commit` — Commit and close a transaction session.
///
/// Requires `X-Transaction-Id` header. The pinned connection is released
/// back to the pool after COMMIT.
pub async fn txn_commit(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
) -> Result<Json<TxnEndResponse>, ApiError> {
    let auth = authenticate_request(&state, &headers).await?;
    let txn_id = extract_txn_id(&headers)?;
    let tenant_id = auth.tenant_id.clone().unwrap_or_default();

    state
        .transaction_manager
        .close_session(&txn_id, &tenant_id, true)
        .await
        .map_err(txn_err_to_api)?;

    Ok(Json(TxnEndResponse {
        status: "committed".to_string(),
    }))
}

/// `POST /txn/rollback` — Rollback and close a transaction session.
///
/// Requires `X-Transaction-Id` header. The pinned connection is released
/// back to the pool after ROLLBACK.
pub async fn txn_rollback(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
) -> Result<Json<TxnEndResponse>, ApiError> {
    let auth = authenticate_request(&state, &headers).await?;
    let txn_id = extract_txn_id(&headers)?;
    let tenant_id = auth.tenant_id.clone().unwrap_or_default();

    state
        .transaction_manager
        .close_session(&txn_id, &tenant_id, false)
        .await
        .map_err(txn_err_to_api)?;

    Ok(Json(TxnEndResponse {
        status: "rolled_back".to_string(),
    }))
}

/// `POST /txn/savepoint` — Savepoint operations within a transaction.
///
/// Requires `X-Transaction-Id` header and JSON body.
/// Actions: "create", "rollback", "release".
pub async fn txn_savepoint(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Json(request): Json<SavepointRequest>,
) -> Result<Json<SavepointResponse>, ApiError> {
    let auth = authenticate_request(&state, &headers).await?;
    let txn_id = extract_txn_id(&headers)?;
    let tenant_id = auth.tenant_id.clone().unwrap_or_default();

    // Validate savepoint name (alphanumeric + underscore only)
    if !request
        .name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_')
    {
        return Err(ApiError::bad_request(
            "INVALID_SAVEPOINT_NAME",
            "Savepoint name must be alphanumeric (with underscores)",
        ));
    }

    if request.name.is_empty() || request.name.len() > 63 {
        return Err(ApiError::bad_request(
            "INVALID_SAVEPOINT_NAME",
            "Savepoint name must be 1-63 characters",
        ));
    }

    let action = request.action.clone();
    let name = request.name.clone();

    state
        .transaction_manager
        .with_session(&txn_id, &tenant_id, |session| {
            let action = action.clone();
            let name = name.clone();
            Box::pin(async move {
                let conn = session
                    .conn
                    .as_mut()
                    .ok_or(crate::transaction::TransactionError::SessionNotFound)?;
                match action.as_str() {
                    "create" => conn.savepoint(&name).await.map_err(|e: qail_pg::PgError| {
                        crate::transaction::TransactionError::Database(e.to_string())
                    }),
                    "rollback" => conn
                        .rollback_to(&name)
                        .await
                        .map_err(|e: qail_pg::PgError| {
                            crate::transaction::TransactionError::Database(e.to_string())
                        }),
                    "release" => {
                        conn.release_savepoint(&name)
                            .await
                            .map_err(|e: qail_pg::PgError| {
                                crate::transaction::TransactionError::Database(e.to_string())
                            })
                    }
                    _ => Err(crate::transaction::TransactionError::Rejected(format!(
                        "Invalid savepoint action '{}'. Use 'create', 'rollback', or 'release'",
                        action
                    ))),
                }
            })
        })
        .await
        .map_err(txn_err_to_api)?;

    Ok(Json(SavepointResponse {
        action: request.action,
        name: request.name,
    }))
}

/// Reject DDL actions inside transactions. Only DML is allowed.
fn reject_ddl_in_transaction(cmd: &qail_core::ast::Qail) -> Result<(), ApiError> {
    use qail_core::ast::Action;
    match cmd.action {
        Action::Get
        | Action::Set
        | Action::Add
        | Action::Del
        | Action::Put
        | Action::With
        | Action::Cnt
        | Action::Over
        | Action::Upsert => Ok(()),
        _ => Err(ApiError::bad_request(
            "UNSUPPORTED_ACTION",
            format!(
                "Action {} is not allowed inside a transaction session. Only DML operations (get/set/add/del/put/with/cnt/over/upsert) are permitted.",
                cmd.action
            ),
        )),
    }
}

/// Convert a `TransactionError` to an `ApiError`.
fn txn_err_to_api(err: crate::transaction::TransactionError) -> ApiError {
    use crate::transaction::TransactionError;
    match err {
        TransactionError::SessionLimitReached(_) => {
            ApiError::with_code("TXN_SESSION_LIMIT", err.to_string())
        }
        TransactionError::SessionNotFound => ApiError::not_found("Transaction session"),
        TransactionError::TenantMismatch => ApiError::forbidden(err.to_string()),
        TransactionError::Pool(e) => ApiError::connection_error(e),
        TransactionError::Database(e) => ApiError::internal(e),
        TransactionError::Rejected(e) => ApiError::bad_request("TXN_REJECTED", e),
        TransactionError::SessionLifetimeExceeded(_) => {
            ApiError::with_code("TXN_SESSION_EXPIRED", err.to_string())
        }
        TransactionError::StatementLimitReached(_) => {
            ApiError::with_code("TXN_STATEMENT_LIMIT", err.to_string())
        }
        TransactionError::Aborted => ApiError::with_code("TXN_ABORTED", err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    use std::time::Duration;
    use tokio::sync::RwLock;
    use tower::util::ServiceExt;

    #[test]
    fn test_reject_ddl_allows_dml() {
        let cmd = qail_core::ast::Qail::get("users");
        assert!(reject_ddl_in_transaction(&cmd).is_ok());

        let cmd = qail_core::ast::Qail::add("users")
            .columns(["name"])
            .values(["Alice"]);
        assert!(reject_ddl_in_transaction(&cmd).is_ok());

        let cmd = qail_core::ast::Qail::set("users")
            .columns(["name"])
            .values(["Bob"]);
        assert!(reject_ddl_in_transaction(&cmd).is_ok());

        let cmd = qail_core::ast::Qail::del("users");
        assert!(reject_ddl_in_transaction(&cmd).is_ok());
    }

    #[test]
    fn test_reject_ddl_blocks_ddl() {
        use qail_core::ast::{Action, Qail};
        let mut cmd = Qail::get("users");
        cmd.action = Action::Make;
        assert!(reject_ddl_in_transaction(&cmd).is_err());

        cmd.action = Action::Truncate;
        assert!(reject_ddl_in_transaction(&cmd).is_err());
    }

    #[test]
    fn test_savepoint_name_validation() {
        // Valid names
        assert!("sp1".chars().all(|c| c.is_alphanumeric() || c == '_'));
        assert!(
            "my_savepoint"
                .chars()
                .all(|c| c.is_alphanumeric() || c == '_')
        );

        // Invalid names
        assert!(!"sp;DROP".chars().all(|c| c.is_alphanumeric() || c == '_'));
        assert!(!"sp name".chars().all(|c| c.is_alphanumeric() || c == '_'));
    }

    async fn build_test_state(config: GatewayConfig) -> Arc<GatewayState> {
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
            allow_list: QueryAllowList::new(),
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
        let value: serde_json::Value =
            serde_json::from_slice(bytes).expect("valid JSON error body");
        value
            .get("code")
            .and_then(serde_json::Value::as_str)
            .expect("error code field")
            .to_string()
    }

    #[tokio::test]
    async fn txn_query_returns_conflict_when_session_lifetime_exceeded() {
        let _serial = crate::metrics::txn_test_serial_guard().await;
        let mut config = GatewayConfig::default();
        config.production_strict = false;
        config.txn_max_sessions = 4;
        config.txn_max_lifetime_secs = 1;
        config.txn_max_statements_per_session = 100;

        let state = build_test_state(config).await;
        state
            .transaction_manager
            .insert_test_session_no_conn(
                "txn-expired-1",
                "",
                Duration::from_secs(5),
                Duration::from_secs(0),
                0,
            )
            .await;

        let app = Router::new()
            .route("/txn/query", post(txn_query))
            .with_state(Arc::clone(&state));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/txn/query")
                    .header("x-transaction-id", "txn-expired-1")
                    .header("content-type", "text/plain")
                    .body(Body::from("get users"))
                    .expect("request should build"),
            )
            .await
            .expect("request should execute");

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        assert_eq!(parse_error_code(&body), "TXN_SESSION_EXPIRED");
    }

    #[tokio::test]
    async fn txn_savepoint_returns_conflict_when_statement_limit_exceeded() {
        let _serial = crate::metrics::txn_test_serial_guard().await;
        let mut config = GatewayConfig::default();
        config.production_strict = false;
        config.txn_max_sessions = 4;
        config.txn_max_lifetime_secs = 600;
        config.txn_max_statements_per_session = 1;

        let state = build_test_state(config).await;
        state
            .transaction_manager
            .insert_test_session_no_conn(
                "txn-stmt-limit-1",
                "",
                Duration::from_secs(0),
                Duration::from_secs(0),
                1,
            )
            .await;

        let app = Router::new()
            .route("/txn/savepoint", post(txn_savepoint))
            .with_state(Arc::clone(&state));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/txn/savepoint")
                    .header("x-transaction-id", "txn-stmt-limit-1")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"action":"create","name":"sp1"}"#))
                    .expect("request should build"),
            )
            .await
            .expect("request should execute");

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        assert_eq!(parse_error_code(&body), "TXN_STATEMENT_LIMIT");
    }
}
