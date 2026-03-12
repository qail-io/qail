use super::*;
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
    let value: serde_json::Value = serde_json::from_slice(bytes).expect("valid JSON error body");
    value
        .get("code")
        .and_then(serde_json::Value::as_str)
        .expect("error code field")
        .to_string()
}

#[tokio::test]
async fn txn_query_returns_conflict_when_session_lifetime_exceeded() {
    let _serial = crate::metrics::txn_test_serial_guard().await;
    let config = GatewayConfig {
        production_strict: false,
        txn_max_sessions: 4,
        txn_max_lifetime_secs: 1,
        txn_max_statements_per_session: 100,
        ..GatewayConfig::default()
    };

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
    let config = GatewayConfig {
        production_strict: false,
        txn_max_sessions: 4,
        txn_max_lifetime_secs: 600,
        txn_max_statements_per_session: 1,
        ..GatewayConfig::default()
    };

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
