use super::rules::exact_cache_key;
use super::{execute_query_binary, execute_query_export, is_query_allowed};
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

async fn build_test_state(config: GatewayConfig, allow_list: QueryAllowList) -> Arc<GatewayState> {
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
        user_tenant_map: Arc::new(RwLock::new(HashMap::new())),
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

fn parse_error_message(bytes: &[u8]) -> String {
    let value: serde_json::Value = serde_json::from_slice(bytes).expect("valid JSON error");
    value
        .get("message")
        .and_then(serde_json::Value::as_str)
        .expect("error message")
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
    let config = GatewayConfig {
        production_strict: false,
        require_auth: false,
        ..GatewayConfig::default()
    };

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
    let config = GatewayConfig {
        production_strict: false,
        require_auth: false,
        ..GatewayConfig::default()
    };

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
    let config = GatewayConfig {
        production_strict: false,
        require_auth: false,
        ..GatewayConfig::default()
    };
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

#[tokio::test]
async fn binary_handler_accepts_qwb2_then_enforces_binary_allow_list_gate() {
    let _serial = crate::metrics::txn_test_serial_guard().await;
    let config = GatewayConfig {
        production_strict: false,
        require_auth: false,
        ..GatewayConfig::default()
    };

    let state = build_test_state(config, QueryAllowList::new()).await;
    let app = Router::new()
        .route("/qail/binary", post(execute_query_binary))
        .with_state(Arc::clone(&state));

    let payload = qail_core::wire::encode_cmd_binary(&qail_core::ast::Qail::get("users").limit(1));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/qail/binary")
                .header("content-type", "application/octet-stream")
                .body(Body::from(payload))
                .expect("request should build"),
        )
        .await
        .expect("request should execute");

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    assert_eq!(parse_error_code(&body), "BINARY_REQUIRES_ALLOW_LIST");
}

#[tokio::test]
async fn binary_handler_rejects_invalid_binary_payload() {
    let _serial = crate::metrics::txn_test_serial_guard().await;
    let config = GatewayConfig {
        production_strict: false,
        require_auth: false,
        ..GatewayConfig::default()
    };

    let state = build_test_state(config, QueryAllowList::new()).await;
    let app = Router::new()
        .route("/qail/binary", post(execute_query_binary))
        .with_state(Arc::clone(&state));

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/qail/binary")
                .header("content-type", "application/octet-stream")
                .body(Body::from(vec![0x01, 0x02, 0x03]))
                .expect("request should build"),
        )
        .await
        .expect("request should execute");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    assert_eq!(parse_error_code(&body), "DECODE_ERROR");
}

#[tokio::test]
async fn binary_handler_rejects_raw_text_payload_without_qwb2_header() {
    let _serial = crate::metrics::txn_test_serial_guard().await;
    let config = GatewayConfig {
        production_strict: false,
        require_auth: false,
        ..GatewayConfig::default()
    };

    let state = build_test_state(config, QueryAllowList::new()).await;
    let app = Router::new()
        .route("/qail/binary", post(execute_query_binary))
        .with_state(Arc::clone(&state));

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/qail/binary")
                .header("content-type", "application/octet-stream")
                .body(Body::from("get users limit 1"))
                .expect("request should build"),
        )
        .await
        .expect("request should execute");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    assert_eq!(parse_error_code(&body), "DECODE_ERROR");
}

#[tokio::test]
async fn binary_handler_rejects_legacy_qwb1_text_payload() {
    let _serial = crate::metrics::txn_test_serial_guard().await;
    let config = GatewayConfig {
        production_strict: false,
        require_auth: false,
        ..GatewayConfig::default()
    };

    let state = build_test_state(config, QueryAllowList::new()).await;
    let app = Router::new()
        .route("/qail/binary", post(execute_query_binary))
        .with_state(Arc::clone(&state));

    let legacy_text = b"get users limit 1";
    let mut legacy_payload = Vec::new();
    legacy_payload.extend_from_slice(b"QWB1");
    legacy_payload.extend_from_slice(&(legacy_text.len() as u32).to_be_bytes());
    legacy_payload.extend_from_slice(legacy_text);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/qail/binary")
                .header("content-type", "application/octet-stream")
                .body(Body::from(legacy_payload))
                .expect("request should build"),
        )
        .await
        .expect("request should execute");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    assert_eq!(parse_error_code(&body), "DECODE_ERROR");
    assert!(
        parse_error_message(&body).contains("legacy QWB1"),
        "error message should indicate legacy wire rejection"
    );
}

#[tokio::test]
async fn binary_handler_rejects_legacy_postcard_like_payload() {
    let _serial = crate::metrics::txn_test_serial_guard().await;
    let config = GatewayConfig {
        production_strict: false,
        require_auth: false,
        ..GatewayConfig::default()
    };

    let state = build_test_state(config, QueryAllowList::new()).await;
    let app = Router::new()
        .route("/qail/binary", post(execute_query_binary))
        .with_state(Arc::clone(&state));

    // Legacy postcard-style payloads do not carry QWB2 framing and must be rejected.
    let legacy_payload = vec![0x82, 0xA6, 0x61, 0x63, 0x74, 0x69, 0x6F, 0x6E];
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/qail/binary")
                .header("content-type", "application/octet-stream")
                .body(Body::from(legacy_payload))
                .expect("request should build"),
        )
        .await
        .expect("request should execute");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    assert_eq!(parse_error_code(&body), "DECODE_ERROR");
    assert!(
        parse_error_message(&body).contains("Invalid binary format"),
        "error message should indicate wire decode failure"
    );
}
