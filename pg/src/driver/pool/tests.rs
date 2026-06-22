//! Pool unit tests.

use crate::driver::pool::churn::*;
use crate::driver::pool::config::*;
use crate::driver::pool::gss::*;
use crate::driver::pool::lifecycle::*;
use crate::driver::pool::{PgPool, PoolConfig, PooledConnection};
use crate::driver::{AuthSettings, GssEncMode, PgConnection, PgError, TlsMode};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

#[test]
fn test_pool_config() {
    let config = PoolConfig::new("localhost", 5432, "user", "testdb")
        .password("secret123")
        .max_connections(20)
        .min_connections(5);

    assert_eq!(config.host, "localhost");
    assert_eq!(config.port, 5432);
    assert_eq!(config.user, "user");
    assert_eq!(config.database, "testdb");
    assert_eq!(config.password, Some("secret123".to_string()));
    assert_eq!(config.max_connections, 20);
    assert_eq!(config.min_connections, 5);
}

#[test]
fn test_pool_config_defaults() {
    let config = PoolConfig::new("localhost", 5432, "user", "testdb");
    assert_eq!(config.max_connections, 10);
    assert_eq!(config.min_connections, 1);
    assert_eq!(config.idle_timeout, Duration::from_secs(600));
    assert_eq!(config.acquire_timeout, Duration::from_secs(30));
    assert_eq!(config.connect_timeout, Duration::from_secs(10));
    assert_eq!(config.leaked_cleanup_queue, 64);
    assert!(config.password.is_none());
    assert_eq!(config.tls_mode, TlsMode::Prefer);
    assert!(config.tls_ca_cert_pem.is_none());
    assert!(config.mtls.is_none());
    assert!(config.auth_settings.allow_scram_sha_256);
    assert!(!config.auth_settings.allow_md5_password);
    assert!(!config.auth_settings.allow_cleartext_password);
    assert_eq!(config.gss_connect_retries, 2);
    assert_eq!(config.gss_retry_base_delay, Duration::from_millis(150));
    assert_eq!(config.gss_circuit_breaker_threshold, 8);
    assert_eq!(config.gss_circuit_breaker_window, Duration::from_secs(30));
    assert_eq!(config.gss_circuit_breaker_cooldown, Duration::from_secs(15));
    assert_eq!(config.gss_enc_mode, GssEncMode::Disable);
    assert!(!config.io_uring);
}

#[test]
fn test_gss_enc_mode_parse() {
    assert_eq!(
        GssEncMode::parse_gssencmode("disable"),
        Some(GssEncMode::Disable)
    );
    assert_eq!(
        GssEncMode::parse_gssencmode("prefer"),
        Some(GssEncMode::Prefer)
    );
    assert_eq!(
        GssEncMode::parse_gssencmode("require"),
        Some(GssEncMode::Require)
    );
    assert_eq!(
        GssEncMode::parse_gssencmode("PREFER"),
        Some(GssEncMode::Prefer)
    );
    assert_eq!(
        GssEncMode::parse_gssencmode("  Require  "),
        Some(GssEncMode::Require)
    );
    assert_eq!(GssEncMode::parse_gssencmode(""), None);
    assert_eq!(GssEncMode::parse_gssencmode("invalid"), None);
    assert_eq!(GssEncMode::parse_gssencmode("allow"), None);
}

#[test]
fn test_gss_enc_mode_default() {
    assert_eq!(GssEncMode::default(), GssEncMode::Disable);
}

#[test]
fn test_url_gssencmode_disable() {
    let mut config = PoolConfig::new("localhost", 5432, "u", "db");
    apply_url_query_params(&mut config, "gssencmode=disable", "localhost").unwrap();
    assert_eq!(config.gss_enc_mode, GssEncMode::Disable);
}

#[test]
fn test_url_gssencmode_prefer() {
    let mut config = PoolConfig::new("localhost", 5432, "u", "db");
    apply_url_query_params(&mut config, "gssencmode=prefer", "localhost").unwrap();
    assert_eq!(config.gss_enc_mode, GssEncMode::Prefer);
}

#[test]
fn test_url_gssencmode_require() {
    let mut config = PoolConfig::new("localhost", 5432, "u", "db");
    apply_url_query_params(&mut config, "gssencmode=require", "localhost").unwrap();
    assert_eq!(config.gss_enc_mode, GssEncMode::Require);
}

#[test]
fn test_url_gssencmode_invalid() {
    let mut config = PoolConfig::new("localhost", 5432, "u", "db");
    let err = apply_url_query_params(&mut config, "gssencmode=bogus", "localhost");
    assert!(err.is_err());
}

#[test]
fn test_url_io_uring_opt_in() {
    let mut config = PoolConfig::new("localhost", 5432, "u", "db");

    apply_url_query_params(&mut config, "io_uring=true", "localhost").unwrap();

    assert!(config.io_uring);
}

#[test]
fn test_url_io_uring_rejects_invalid_bool() {
    let mut config = PoolConfig::new("localhost", 5432, "u", "db");
    let err = apply_url_query_params(&mut config, "io_uring=auto", "localhost");

    assert!(err.is_err());
}

#[test]
fn test_url_gssencmode_with_sslmode() {
    let mut config = PoolConfig::new("localhost", 5432, "u", "db");
    apply_url_query_params(
        &mut config,
        "gssencmode=prefer&sslmode=require",
        "localhost",
    )
    .unwrap();
    assert_eq!(config.gss_enc_mode, GssEncMode::Prefer);
    assert_eq!(config.tls_mode, TlsMode::Require);
}

#[test]
fn test_url_query_params_are_percent_decoded() {
    let mut config = PoolConfig::new("localhost", 5432, "u", "db");

    apply_url_query_params(&mut config, "%73slmode=re%71uire", "localhost").unwrap();

    assert_eq!(config.tls_mode, TlsMode::Require);
}

#[test]
fn test_url_query_params_reject_invalid_percent_encoded_utf8() {
    let mut config = PoolConfig::new("localhost", 5432, "u", "db");

    let err = apply_url_query_params(&mut config, "sslmode=%FF", "localhost")
        .expect_err("invalid percent-decoded UTF-8 must fail");

    assert!(err.to_string().contains("percent-encoding"));
}

#[test]
fn test_url_gssencmode_require_sslmode_require_is_valid() {
    // libpq allows this — negotiation resolves precedence, not config parsing.
    let mut config = PoolConfig::new("localhost", 5432, "u", "db");
    apply_url_query_params(
        &mut config,
        "gssencmode=require&sslmode=require",
        "localhost",
    )
    .unwrap();
    assert_eq!(config.gss_enc_mode, GssEncMode::Require);
    assert_eq!(config.tls_mode, TlsMode::Require);
}

#[test]
fn test_pool_config_builder_chaining() {
    let config = PoolConfig::new("db.example.com", 5433, "admin", "prod")
        .password("p@ss")
        .max_connections(50)
        .min_connections(10)
        .idle_timeout(Duration::from_secs(300))
        .acquire_timeout(Duration::from_secs(5))
        .connect_timeout(Duration::from_secs(3))
        .max_lifetime(Duration::from_secs(3600))
        .leaked_cleanup_queue(32)
        .gss_connect_retries(4)
        .gss_retry_base_delay(Duration::from_millis(250))
        .gss_circuit_breaker_threshold(12)
        .gss_circuit_breaker_window(Duration::from_secs(45))
        .gss_circuit_breaker_cooldown(Duration::from_secs(20))
        .test_on_acquire(false)
        .io_uring(true);

    assert_eq!(config.host, "db.example.com");
    assert_eq!(config.port, 5433);
    assert_eq!(config.max_connections, 50);
    assert_eq!(config.min_connections, 10);
    assert_eq!(config.idle_timeout, Duration::from_secs(300));
    assert_eq!(config.acquire_timeout, Duration::from_secs(5));
    assert_eq!(config.connect_timeout, Duration::from_secs(3));
    assert_eq!(config.max_lifetime, Some(Duration::from_secs(3600)));
    assert_eq!(config.leaked_cleanup_queue, 32);
    assert_eq!(config.gss_connect_retries, 4);
    assert_eq!(config.gss_retry_base_delay, Duration::from_millis(250));
    assert_eq!(config.gss_circuit_breaker_threshold, 12);
    assert!(config.io_uring);
    assert_eq!(config.gss_circuit_breaker_window, Duration::from_secs(45));
    assert_eq!(config.gss_circuit_breaker_cooldown, Duration::from_secs(20));
    assert!(!config.test_on_acquire);
}

#[test]
fn test_validate_pool_config_rejects_zero_max_connections() {
    let config = PoolConfig::new("localhost", 5432, "user", "db")
        .max_connections(0)
        .min_connections(0);
    let err = validate_pool_config(&config).expect_err("expected invalid max_connections");
    assert!(err.to_string().contains("max_connections must be >= 1"));
}

#[test]
fn test_validate_pool_config_rejects_min_greater_than_max() {
    let config = PoolConfig::new("localhost", 5432, "user", "db")
        .max_connections(2)
        .min_connections(3);
    let err = validate_pool_config(&config).expect_err("expected invalid min/max");
    assert!(
        err.to_string()
            .contains("min_connections (3) must be <= max_connections (2)")
    );
}

#[test]
fn test_validate_pool_config_rejects_zero_acquire_timeout() {
    let config = PoolConfig::new("localhost", 5432, "user", "db").acquire_timeout(Duration::ZERO);
    let err = validate_pool_config(&config).expect_err("expected invalid acquire_timeout");
    assert!(err.to_string().contains("acquire_timeout must be > 0"));
}

#[test]
fn test_validate_pool_config_rejects_zero_connect_timeout() {
    let config = PoolConfig::new("localhost", 5432, "user", "db").connect_timeout(Duration::ZERO);
    let err = validate_pool_config(&config).expect_err("expected invalid connect_timeout");
    assert!(err.to_string().contains("connect_timeout must be > 0"));
}

#[test]
fn test_validate_pool_config_rejects_zero_leaked_cleanup_queue() {
    let config = PoolConfig::new("localhost", 5432, "user", "db").leaked_cleanup_queue(0);
    let err = validate_pool_config(&config).expect_err("expected invalid leaked_cleanup_queue");
    assert!(
        err.to_string()
            .contains("leaked_cleanup_queue must be >= 1")
    );
}

#[tokio::test]
async fn test_close_graceful_waits_for_active_connections_to_drain() {
    let pool = PgPool::connect(
        PoolConfig::new_dev("localhost", 5432, "user", "db")
            .min_connections(0)
            .max_connections(1),
    )
    .await
    .expect("pool should initialize without dialing with min_connections=0");

    pool.inner.active_count.store(1, Ordering::Relaxed);
    let pool_clone = pool.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(60)).await;
        pool_clone.inner.active_count.store(0, Ordering::Relaxed);
    });

    let started = Instant::now();
    pool.close_graceful(Duration::from_millis(200)).await;
    assert!(
        started.elapsed() >= Duration::from_millis(50),
        "close_graceful should wait for active connections to drain"
    );
    assert!(pool.is_closed());
}

#[tokio::test]
async fn test_close_graceful_unblocks_waiting_acquire() {
    let pool = PgPool::connect(
        PoolConfig::new_dev("localhost", 5432, "user", "db")
            .min_connections(0)
            .max_connections(1),
    )
    .await
    .expect("pool should initialize without dialing with min_connections=0");

    let permit = pool
        .inner
        .semaphore
        .acquire()
        .await
        .expect("semaphore permit");
    permit.forget();

    let pool_clone = pool.clone();
    let waiter = tokio::spawn(async move { pool_clone.acquire_raw().await });
    tokio::time::sleep(Duration::from_millis(30)).await;

    pool.close_graceful(Duration::from_millis(200)).await;

    let res = tokio::time::timeout(Duration::from_millis(200), waiter)
        .await
        .expect("waiting acquire should unblock quickly after close")
        .expect("join handle");
    assert!(matches!(res, Err(PgError::PoolClosed)));
}

#[cfg(unix)]
#[tokio::test]
async fn test_release_drops_desynced_connection_without_commit() {
    use crate::driver::connection::StatementCache;
    use crate::driver::stream::PgStream;
    use bytes::BytesMut;
    use std::collections::{HashMap, VecDeque};
    use std::num::NonZeroUsize;
    use tokio::net::UnixStream;

    let (unix_stream, _peer) = UnixStream::pair().expect("unix stream pair");
    let conn = PgConnection {
        stream: PgStream::Unix(unix_stream),
        buffer: BytesMut::with_capacity(1024),
        write_buf: BytesMut::with_capacity(1024),
        sql_buf: BytesMut::with_capacity(256),
        params_buf: Vec::new(),
        prepared_statements: HashMap::new(),
        stmt_cache: StatementCache::new(NonZeroUsize::new(16).expect("non-zero")),
        column_info_cache: HashMap::new(),
        process_id: 0,
        cancel_key_bytes: Vec::new(),
        requested_protocol_minor: PgConnection::default_protocol_minor(),
        negotiated_protocol_minor: PgConnection::default_protocol_minor(),
        notifications: VecDeque::new(),
        replication_stream_active: false,
        replication_mode_enabled: false,
        last_replication_wal_end: None,
        io_desynced: true,
        pending_statement_closes: Vec::new(),
        draining_statement_closes: false,
    };

    let pool = PgPool::connect(
        PoolConfig::new_dev("localhost", 5432, "user", "db")
            .min_connections(0)
            .max_connections(1),
    )
    .await
    .expect("pool init");

    // Simulate an acquired slot: consume one permit and mark active.
    let permit = pool
        .inner
        .semaphore
        .acquire()
        .await
        .expect("semaphore permit");
    permit.forget();
    pool.inner.active_count.store(1, Ordering::Relaxed);

    let pooled = PooledConnection {
        conn: Some(conn),
        pool: std::sync::Arc::clone(&pool.inner),
        rls_dirty: true,
        created_at: Instant::now(),
    };
    pooled.release().await;

    assert_eq!(pool.inner.active_count.load(Ordering::Relaxed), 0);
    assert_eq!(pool.inner.semaphore.available_permits(), 1);
    assert_eq!(pool.inner.connections.lock().await.len(), 0);
}

#[cfg(unix)]
#[tokio::test]
async fn test_release_raw_rolls_back_before_returning_connection() {
    use crate::driver::connection::StatementCache;
    use crate::driver::stream::PgStream;
    use bytes::BytesMut;
    use std::collections::{HashMap, VecDeque};
    use std::num::NonZeroUsize;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixStream;

    fn backend_frame(msg_type: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(msg_type);
        out.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    fn command_complete(tag: &str) -> Vec<u8> {
        let mut payload = Vec::from(tag.as_bytes());
        payload.push(0);
        backend_frame(b'C', &payload)
    }

    let (unix_stream, mut peer) = UnixStream::pair().expect("unix stream pair");
    let conn = PgConnection {
        stream: PgStream::Unix(unix_stream),
        buffer: BytesMut::with_capacity(1024),
        write_buf: BytesMut::with_capacity(1024),
        sql_buf: BytesMut::with_capacity(256),
        params_buf: Vec::new(),
        prepared_statements: HashMap::new(),
        stmt_cache: StatementCache::new(NonZeroUsize::new(16).expect("non-zero")),
        column_info_cache: HashMap::new(),
        process_id: 0,
        cancel_key_bytes: Vec::new(),
        requested_protocol_minor: PgConnection::default_protocol_minor(),
        negotiated_protocol_minor: PgConnection::default_protocol_minor(),
        notifications: VecDeque::new(),
        replication_stream_active: false,
        replication_mode_enabled: false,
        last_replication_wal_end: None,
        io_desynced: false,
        pending_statement_closes: Vec::new(),
        draining_statement_closes: false,
    };

    let pool = PgPool::connect(
        PoolConfig::new_dev("localhost", 5432, "user", "db")
            .min_connections(0)
            .max_connections(1),
    )
    .await
    .expect("pool init");

    let permit = pool
        .inner
        .semaphore
        .acquire()
        .await
        .expect("semaphore permit");
    permit.forget();
    pool.inner.active_count.store(1, Ordering::Relaxed);

    let peer_task = tokio::spawn(async move {
        let mut head = [0u8; 5];
        peer.read_exact(&mut head).await.unwrap();
        assert_eq!(head[0], b'Q');
        let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
        let mut payload = vec![0u8; len - 4];
        peer.read_exact(&mut payload).await.unwrap();
        assert_eq!(payload, b"ROLLBACK\0");

        peer.write_all(&command_complete("ROLLBACK")).await.unwrap();
        peer.write_all(&backend_frame(b'Z', b"I")).await.unwrap();
        peer.flush().await.unwrap();
    });

    let pooled = PooledConnection {
        conn: Some(conn),
        pool: std::sync::Arc::clone(&pool.inner),
        rls_dirty: false,
        created_at: Instant::now(),
    };
    pooled
        .release_checked()
        .await
        .expect("release should succeed");
    peer_task.await.unwrap();

    assert_eq!(pool.inner.active_count.load(Ordering::Relaxed), 0);
    assert_eq!(pool.inner.semaphore.available_permits(), 1);
    assert_eq!(pool.inner.connections.lock().await.len(), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn test_execute_simple_with_timeout_marks_connection_desynced() {
    use crate::driver::connection::StatementCache;
    use crate::driver::stream::PgStream;
    use bytes::BytesMut;
    use std::collections::{HashMap, VecDeque};
    use std::num::NonZeroUsize;
    use tokio::net::UnixStream;

    let (unix_stream, _peer) = UnixStream::pair().expect("unix stream pair");
    let mut conn = PgConnection {
        stream: PgStream::Unix(unix_stream),
        buffer: BytesMut::with_capacity(1024),
        write_buf: BytesMut::with_capacity(1024),
        sql_buf: BytesMut::with_capacity(256),
        params_buf: Vec::new(),
        prepared_statements: HashMap::new(),
        stmt_cache: StatementCache::new(NonZeroUsize::new(16).expect("non-zero")),
        column_info_cache: HashMap::new(),
        process_id: 0,
        cancel_key_bytes: Vec::new(),
        requested_protocol_minor: PgConnection::default_protocol_minor(),
        negotiated_protocol_minor: PgConnection::default_protocol_minor(),
        notifications: VecDeque::new(),
        replication_stream_active: false,
        replication_mode_enabled: false,
        last_replication_wal_end: None,
        io_desynced: false,
        pending_statement_closes: Vec::new(),
        draining_statement_closes: false,
    };

    let err = execute_simple_with_timeout(
        &mut conn,
        "SELECT 1",
        Duration::from_millis(1),
        "unit-test timeout",
    )
    .await
    .expect_err("expected timeout");
    assert!(matches!(err, PgError::Timeout(_)));
    assert!(conn.is_io_desynced());
}

#[test]
fn test_hot_preprepare_message_tracks_parse_complete_and_ready() {
    let mut parse_complete_count = 0usize;
    let mut error: Option<PgError> = None;

    let done = handle_hot_preprepare_message(
        &crate::protocol::BackendMessage::ParseComplete,
        &mut parse_complete_count,
        &mut error,
    )
    .expect("parse complete accepted");
    assert!(!done);
    assert_eq!(parse_complete_count, 1);
    assert!(error.is_none());

    let done = handle_hot_preprepare_message(
        &crate::protocol::BackendMessage::ReadyForQuery(crate::protocol::TransactionStatus::Idle),
        &mut parse_complete_count,
        &mut error,
    )
    .expect("ready accepted");
    assert!(done);
}

#[test]
fn test_hot_preprepare_message_captures_error() {
    let mut parse_complete_count = 0usize;
    let mut error: Option<PgError> = None;
    let err_fields = crate::protocol::ErrorFields {
        severity: "ERROR".to_string(),
        code: "42601".to_string(),
        message: "syntax error".to_string(),
        detail: None,
        hint: None,
    };

    let done = handle_hot_preprepare_message(
        &crate::protocol::BackendMessage::ErrorResponse(err_fields),
        &mut parse_complete_count,
        &mut error,
    )
    .expect("error response accepted for drain");
    assert!(!done);
    assert_eq!(parse_complete_count, 0);
    assert!(matches!(error, Some(PgError::QueryServer(_))));
}

#[test]
fn test_hot_preprepare_message_rejects_unexpected_data_row() {
    let mut parse_complete_count = 0usize;
    let mut error: Option<PgError> = None;
    let err = handle_hot_preprepare_message(
        &crate::protocol::BackendMessage::DataRow(vec![]),
        &mut parse_complete_count,
        &mut error,
    )
    .expect_err("unexpected DataRow should fail");
    assert!(err.to_string().contains("Unexpected backend message"));
}

#[test]
fn test_parse_pg_url_strips_query_string() {
    let (host, port, user, db, password) = parse_pg_url(
        "postgresql://alice:secret@db.internal:5433/app?sslmode=require&channel_binding=require",
    )
    .unwrap();
    assert_eq!(host, "db.internal");
    assert_eq!(port, 5433);
    assert_eq!(user, "alice");
    assert_eq!(db, "app");
    assert_eq!(password, Some("secret".to_string()));
}

#[test]
fn test_pool_config_from_url_applies_auth_and_query_params() {
    let config =
        PoolConfig::from_url("postgresql://alice:s%40cret@db.internal:5433/app?sslmode=require")
            .unwrap();

    assert_eq!(config.host, "db.internal");
    assert_eq!(config.port, 5433);
    assert_eq!(config.user, "alice");
    assert_eq!(config.database, "app");
    assert_eq!(config.password, Some("s@cret".to_string()));
    assert_eq!(config.tls_mode, TlsMode::Require);
    assert!(!config.io_uring);
}

#[test]
fn test_pool_config_from_url_applies_io_uring_query_param() {
    let config =
        PoolConfig::from_url("postgresql://alice@db.internal:5433/app?io_uring=true").unwrap();

    assert!(config.io_uring);
}

#[test]
fn test_from_qail_config_maps_io_uring() {
    let mut qail = qail_core::config::QailConfig::default();
    qail.postgres.io_uring = true;

    let config = PoolConfig::from_qail_config(&qail).expect("expected valid config");

    assert!(config.io_uring);
}

#[test]
fn test_parse_pg_url_defaults_port_only_when_omitted() {
    let (host, port, user, db, password) =
        parse_pg_url("postgresql://alice:secret@db.internal/app").unwrap();
    assert_eq!(host, "db.internal");
    assert_eq!(port, 5432);
    assert_eq!(user, "alice");
    assert_eq!(db, "app");
    assert_eq!(password, Some("secret".to_string()));
}

#[test]
fn test_parse_pg_url_decodes_percent_encoded_user_password_and_database() {
    let (host, port, user, db, password) =
        parse_pg_url("postgresql://us%40er:p%40ss%2Fword@db.internal/my%2Fdb").unwrap();
    assert_eq!(host, "db.internal");
    assert_eq!(port, 5432);
    assert_eq!(user, "us@er");
    assert_eq!(db, "my/db");
    assert_eq!(password, Some("p@ss/word".to_string()));
}

#[test]
fn test_parse_pg_url_splits_credentials_at_last_at_sign() {
    let (host, port, user, db, password) =
        parse_pg_url("postgresql://alice:p@ss@db.internal:5433/app").unwrap();

    assert_eq!(host, "db.internal");
    assert_eq!(port, 5433);
    assert_eq!(user, "alice");
    assert_eq!(db, "app");
    assert_eq!(password, Some("p@ss".to_string()));
}

#[test]
fn test_parse_pg_url_decodes_utf8_percent_encoding() {
    let (host, _port, user, db, password) =
        parse_pg_url("postgresql://caf%C3%A9:p%C3%A9ss@db.internal/app_%E2%9C%93").unwrap();
    assert_eq!(host, "db.internal");
    assert_eq!(user, "café");
    assert_eq!(db, "app_✓");
    assert_eq!(password, Some("péss".to_string()));
}

#[test]
fn test_parse_pg_url_rejects_invalid_percent_encoded_utf8() {
    let err = parse_pg_url("postgresql://caf%C3%A9:%FF@db.internal/app")
        .expect_err("invalid percent-decoded UTF-8 must fail");

    assert!(err.to_string().contains("percent-encoding"));
}

#[test]
fn test_parse_pg_url_rejects_malformed_percent_encoding() {
    let err = parse_pg_url("postgresql://app:bad%ZZ@db.internal/app")
        .expect_err("malformed percent escape must fail");
    assert!(err.to_string().contains("two hex digits"));

    let err = parse_pg_url("postgresql://app:bad%@db.internal/app")
        .expect_err("trailing percent escape must fail");
    assert!(err.to_string().contains("two hex digits"));
}

#[test]
fn test_parse_pg_url_supports_bracketed_ipv6() {
    let (host, port, user, db, password) =
        parse_pg_url("postgresql://alice:secret@[::1]:5544/app").unwrap();
    assert_eq!(host, "[::1]");
    assert_eq!(port, 5544);
    assert_eq!(user, "alice");
    assert_eq!(db, "app");
    assert_eq!(password, Some("secret".to_string()));

    let err = match parse_pg_url("postgresql://alice@[::1:5544/app") {
        Ok(_) => panic!("malformed IPv6 URL must be rejected"),
        Err(PgError::Connection(msg)) => msg,
        Err(other) => panic!("unexpected error: {other:?}"),
    };
    assert!(err.contains("IPv6"));
}

#[test]
fn test_parse_pg_url_rejects_non_postgres_scheme_and_missing_host() {
    let err = match parse_pg_url("mysql://alice:secret@db.internal/app") {
        Ok(_) => panic!("non-postgres URL scheme must be rejected"),
        Err(PgError::Connection(msg)) => msg,
        Err(other) => panic!("unexpected error: {other:?}"),
    };
    assert!(err.contains("postgres://"));

    let err = match parse_pg_url("postgres://:5432/app") {
        Ok(_) => panic!("missing host must be rejected"),
        Err(PgError::Connection(msg)) => msg,
        Err(other) => panic!("unexpected error: {other:?}"),
    };
    assert!(err.contains("missing host"));

    let err = match parse_pg_url("postgres://db.internal/") {
        Ok(_) => panic!("missing database must be rejected"),
        Err(PgError::Connection(msg)) => msg,
        Err(other) => panic!("unexpected error: {other:?}"),
    };
    assert!(err.contains("missing database"));

    let err = match parse_pg_url("postgres://@db.internal/app") {
        Ok(_) => panic!("missing user must be rejected"),
        Err(PgError::Connection(msg)) => msg,
        Err(other) => panic!("unexpected error: {other:?}"),
    };
    assert!(err.contains("missing user"));
}

#[test]
fn test_from_qail_config_rejects_non_numeric_postgres_port() {
    let mut qail = qail_core::config::QailConfig::default();
    qail.postgres.url = "postgres://app@db.example.com:notaport/app".to_string();

    let err = match PoolConfig::from_qail_config(&qail) {
        Ok(_) => panic!("non-numeric URL port must be rejected"),
        Err(PgError::Connection(msg)) => msg,
        Err(other) => panic!("unexpected error: {other:?}"),
    };

    assert!(err.contains("Invalid PostgreSQL URL port 'notaport'"));
}

#[test]
fn test_from_qail_config_rejects_out_of_range_postgres_port() {
    let mut qail = qail_core::config::QailConfig::default();
    qail.postgres.url = "postgres://app@db.example.com:70000/app".to_string();

    let err = match PoolConfig::from_qail_config(&qail) {
        Ok(_) => panic!("out-of-range URL port must be rejected"),
        Err(PgError::Connection(msg)) => msg,
        Err(other) => panic!("unexpected error: {other:?}"),
    };

    assert!(err.contains("Invalid PostgreSQL URL port '70000'"));
}

#[test]
fn test_parse_bool_param_variants() {
    assert_eq!(parse_bool_param("true"), Some(true));
    assert_eq!(parse_bool_param("YES"), Some(true));
    assert_eq!(parse_bool_param("0"), Some(false));
    assert_eq!(parse_bool_param("off"), Some(false));
    assert_eq!(parse_bool_param("invalid"), None);
}

#[test]
fn test_from_qail_config_rejects_invalid_gss_provider() {
    let mut qail = qail_core::config::QailConfig::default();
    qail.postgres.url =
        "postgres://alice:secret@db.internal:5432/app?gss_provider=unknown".to_string();

    let err = match PoolConfig::from_qail_config(&qail) {
        Ok(_) => panic!("expected invalid gss_provider error"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("Invalid gss_provider value"));
}

#[test]
fn test_from_qail_config_rejects_empty_gss_service() {
    let mut qail = qail_core::config::QailConfig::default();
    qail.postgres.url = "postgres://alice:secret@db.internal:5432/app?gss_service=".to_string();

    let err = match PoolConfig::from_qail_config(&qail) {
        Ok(_) => panic!("expected empty gss_service error"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("gss_service must not be empty"));
}

#[test]
fn test_from_qail_config_rejects_empty_krbsrvname_alias() {
    let mut qail = qail_core::config::QailConfig::default();
    qail.postgres.url = "postgres://alice:secret@db.internal:5432/app?krbsrvname=".to_string();

    let err = match PoolConfig::from_qail_config(&qail) {
        Ok(_) => panic!("expected empty krbsrvname alias error"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("gss_service must not be empty"));
}

#[test]
fn test_from_qail_config_rejects_empty_gsshostname_alias() {
    let mut qail = qail_core::config::QailConfig::default();
    qail.postgres.url = "postgres://alice:secret@db.internal:5432/app?gsshostname=".to_string();

    let err = match PoolConfig::from_qail_config(&qail) {
        Ok(_) => panic!("expected empty gsshostname alias error"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("gss_target must not be empty"));
}

#[test]
fn test_from_qail_config_accepts_valid_gsslib_values() {
    let mut qail = qail_core::config::QailConfig::default();
    qail.postgres.url = "postgres://alice:secret@db.internal:5432/app?gsslib=gssapi".to_string();
    assert!(PoolConfig::from_qail_config(&qail).is_ok());

    qail.postgres.url = "postgres://alice:secret@db.internal:5432/app?gsslib=sspi".to_string();
    assert!(PoolConfig::from_qail_config(&qail).is_ok());
}

#[test]
fn test_from_qail_config_rejects_invalid_gsslib_value() {
    let mut qail = qail_core::config::QailConfig::default();
    qail.postgres.url = "postgres://alice:secret@db.internal:5432/app?gsslib=kerberos".to_string();

    let err = match PoolConfig::from_qail_config(&qail) {
        Ok(_) => panic!("expected invalid gsslib error"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("Invalid gsslib value"));
}

#[test]
fn test_from_qail_config_parses_gss_retry_settings() {
    let mut qail = qail_core::config::QailConfig::default();
    qail.postgres.url =
            "postgres://alice@db.internal:5432/app?gss_connect_retries=5&gss_retry_base_ms=400&gss_circuit_threshold=9&gss_circuit_window_ms=60000&gss_circuit_cooldown_ms=12000".to_string();

    let cfg = PoolConfig::from_qail_config(&qail).expect("expected valid config");
    assert_eq!(cfg.gss_connect_retries, 5);
    assert_eq!(cfg.gss_retry_base_delay, Duration::from_millis(400));
    assert_eq!(cfg.gss_circuit_breaker_threshold, 9);
    assert_eq!(cfg.gss_circuit_breaker_window, Duration::from_secs(60));
    assert_eq!(cfg.gss_circuit_breaker_cooldown, Duration::from_secs(12));
}

#[test]
fn test_from_qail_config_rejects_invalid_gss_retry_base() {
    let mut qail = qail_core::config::QailConfig::default();
    qail.postgres.url = "postgres://alice@db.internal:5432/app?gss_retry_base_ms=0".to_string();

    let err = match PoolConfig::from_qail_config(&qail) {
        Ok(_) => panic!("expected invalid gss_retry_base_ms error"),
        Err(e) => e,
    };
    assert!(
        err.to_string()
            .contains("gss_retry_base_ms must be greater than 0")
    );
}

#[test]
fn test_from_qail_config_rejects_invalid_gss_connect_retries() {
    let mut qail = qail_core::config::QailConfig::default();
    qail.postgres.url = "postgres://alice@db.internal:5432/app?gss_connect_retries=100".to_string();

    let err = match PoolConfig::from_qail_config(&qail) {
        Ok(_) => panic!("expected invalid gss_connect_retries error"),
        Err(e) => e,
    };
    assert!(
        err.to_string()
            .contains("gss_connect_retries must be <= 20")
    );
}

#[test]
fn test_from_qail_config_rejects_invalid_gss_circuit_threshold() {
    let mut qail = qail_core::config::QailConfig::default();
    qail.postgres.url =
        "postgres://alice@db.internal:5432/app?gss_circuit_threshold=500".to_string();

    let err = match PoolConfig::from_qail_config(&qail) {
        Ok(_) => panic!("expected invalid gss_circuit_threshold error"),
        Err(e) => e,
    };
    assert!(
        err.to_string()
            .contains("gss_circuit_threshold must be <= 100")
    );
}

#[test]
fn test_from_qail_config_rejects_invalid_gss_circuit_window() {
    let mut qail = qail_core::config::QailConfig::default();
    qail.postgres.url = "postgres://alice@db.internal:5432/app?gss_circuit_window_ms=0".to_string();

    let err = match PoolConfig::from_qail_config(&qail) {
        Ok(_) => panic!("expected invalid gss_circuit_window_ms error"),
        Err(e) => e,
    };
    assert!(
        err.to_string()
            .contains("gss_circuit_window_ms must be greater than 0")
    );
}

#[test]
fn test_from_qail_config_rejects_invalid_gss_circuit_cooldown() {
    let mut qail = qail_core::config::QailConfig::default();
    qail.postgres.url =
        "postgres://alice@db.internal:5432/app?gss_circuit_cooldown_ms=0".to_string();

    let err = match PoolConfig::from_qail_config(&qail) {
        Ok(_) => panic!("expected invalid gss_circuit_cooldown_ms error"),
        Err(e) => e,
    };
    assert!(
        err.to_string()
            .contains("gss_circuit_cooldown_ms must be greater than 0")
    );
}

#[cfg(not(all(feature = "enterprise-gssapi", target_os = "linux")))]
#[test]
fn test_from_qail_config_linux_krb5_requires_feature_on_linux() {
    let mut qail = qail_core::config::QailConfig::default();
    qail.postgres.url = "postgres://alice@db.internal:5432/app?gss_provider=linux_krb5".to_string();

    let err = match PoolConfig::from_qail_config(&qail) {
        Ok(_) => panic!("expected linux_krb5 feature-gate error"),
        Err(e) => e,
    };
    assert!(
        err.to_string()
            .contains("requires qail-pg feature enterprise-gssapi on Linux")
    );
}

#[test]
fn test_timeout_error_display() {
    let err = PgError::Timeout("pool acquire after 30s (10 max connections)".to_string());
    let msg = err.to_string();
    assert!(msg.contains("Timeout"));
    assert!(msg.contains("30s"));
    assert!(msg.contains("10 max connections"));
}

#[test]
fn test_should_retry_gss_connect_error_transient_auth() {
    let config = PoolConfig::new("localhost", 5432, "user", "db")
        .auth_settings(AuthSettings::gssapi_only())
        .gss_connect_retries(3);
    let err = PgError::Auth("temporary kerberos service unavailable".to_string());
    assert!(should_retry_gss_connect_error(&config, 0, &err));
}

#[test]
fn test_should_retry_gss_connect_error_non_transient_auth() {
    let config = PoolConfig::new("localhost", 5432, "user", "db")
        .auth_settings(AuthSettings::gssapi_only())
        .gss_connect_retries(3);
    let err = PgError::Auth(
        "Kerberos V5 authentication requested but no GSS token provider is configured".to_string(),
    );
    assert!(!should_retry_gss_connect_error(&config, 0, &err));
}

#[test]
fn test_should_retry_gss_connect_error_respects_retry_limit() {
    let config = PoolConfig::new("localhost", 5432, "user", "db")
        .auth_settings(AuthSettings::gssapi_only())
        .gss_connect_retries(1);
    let err = PgError::Connection("temporary network is unreachable".to_string());
    assert!(should_retry_gss_connect_error(&config, 0, &err));
    assert!(!should_retry_gss_connect_error(&config, 1, &err));
}

#[test]
fn test_gss_retry_delay_has_bounded_jitter() {
    let delay = gss_retry_delay(Duration::from_millis(100), 2);
    assert!(delay >= Duration::from_millis(400));
    assert!(delay <= Duration::from_millis(480));
}

#[test]
fn test_gss_circuit_opens_and_resets_on_success() {
    let config = PoolConfig::new("circuit.test", 5432, "user", "db_circuit")
        .auth_settings(AuthSettings::gssapi_only())
        .gss_circuit_breaker_threshold(2)
        .gss_circuit_breaker_window(Duration::from_secs(30))
        .gss_circuit_breaker_cooldown(Duration::from_secs(5));

    gss_circuit_record_success(&config);
    assert!(gss_circuit_remaining_open(&config).is_none());

    gss_circuit_record_failure(&config);
    assert!(gss_circuit_remaining_open(&config).is_none());

    gss_circuit_record_failure(&config);
    assert!(gss_circuit_remaining_open(&config).is_some());

    gss_circuit_record_success(&config);
    assert!(gss_circuit_remaining_open(&config).is_none());
}

fn unique_pool_host(prefix: &str) -> String {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("{}.{}", prefix, id)
}

#[test]
fn test_decrement_active_count_saturating() {
    let counter = AtomicUsize::new(0);
    decrement_active_count_saturating(&counter);
    assert_eq!(counter.load(Ordering::Relaxed), 0);

    counter.store(2, Ordering::Relaxed);
    decrement_active_count_saturating(&counter);
    assert_eq!(counter.load(Ordering::Relaxed), 1);
    decrement_active_count_saturating(&counter);
    assert_eq!(counter.load(Ordering::Relaxed), 0);
    decrement_active_count_saturating(&counter);
    assert_eq!(counter.load(Ordering::Relaxed), 0);
}

#[test]
fn test_maintenance_backfill_deficit_respects_capacity() {
    assert_eq!(maintenance_backfill_deficit(10, 5, 5, 0), 0);
    assert_eq!(maintenance_backfill_deficit(10, 5, 3, 0), 2);
    assert_eq!(maintenance_backfill_deficit(10, 5, 0, 8), 2);
    assert_eq!(maintenance_backfill_deficit(1, 1, 0, 1), 0);
    assert_eq!(maintenance_backfill_deficit(4, 10, 0, 0), 4);
}

#[test]
fn test_pool_churn_circuit_opens_after_threshold() {
    let host = unique_pool_host("pool-churn");
    let config = PoolConfig::new(&host, 5432, "user", "db");

    assert!(pool_churn_remaining_open(&config).is_none());
    for _ in 0..POOL_CHURN_THRESHOLD {
        pool_churn_record_destroy(&config, "unit_test_churn");
    }
    assert!(pool_churn_remaining_open(&config).is_some());

    // Cleanup isolated registry state for this test key.
    if let Ok(mut registry) = pool_churn_registry().lock() {
        registry.remove(&pool_churn_key(&config));
    }
}

#[test]
fn test_pool_closed_error_display() {
    let err = PgError::PoolClosed;
    assert_eq!(err.to_string(), "Connection pool is closed");
}

#[test]
fn test_pool_exhausted_error_display() {
    let err = PgError::PoolExhausted { max: 20 };
    let msg = err.to_string();
    assert!(msg.contains("exhausted"));
    assert!(msg.contains("20"));
}

#[test]
fn test_io_error_source_chaining() {
    use std::error::Error;
    let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "peer reset");
    let pg_err = PgError::Io(io_err);
    // source() should return the inner io::Error
    let source = pg_err.source().expect("Io variant should have source");
    assert!(source.to_string().contains("peer reset"));
}

#[test]
fn test_non_io_errors_have_no_source() {
    use std::error::Error;
    assert!(PgError::Connection("test".into()).source().is_none());
    assert!(PgError::Query("test".into()).source().is_none());
    assert!(PgError::Timeout("test".into()).source().is_none());
    assert!(PgError::PoolClosed.source().is_none());
    assert!(PgError::NoRows.source().is_none());
}

#[test]
fn test_io_error_from_conversion() {
    let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken");
    let pg_err: PgError = io_err.into();
    assert!(matches!(pg_err, PgError::Io(_)));
    assert!(pg_err.to_string().contains("broken"));
}

#[test]
fn test_error_variants_are_distinct() {
    // Ensure we can match on each variant for programmatic error handling
    let errors: Vec<PgError> = vec![
        PgError::Connection("conn".into()),
        PgError::Protocol("proto".into()),
        PgError::Auth("auth".into()),
        PgError::Query("query".into()),
        PgError::QueryServer(crate::driver::PgServerError {
            severity: "ERROR".to_string(),
            code: "23505".to_string(),
            message: "duplicate key value violates unique constraint".to_string(),
            detail: None,
            hint: None,
        }),
        PgError::NoRows,
        PgError::Io(std::io::Error::other("io")),
        PgError::Encode("enc".into()),
        PgError::Timeout("timeout".into()),
        PgError::PoolExhausted { max: 10 },
        PgError::PoolClosed,
    ];
    // All variants produce non-empty display strings
    for err in &errors {
        assert!(!err.to_string().is_empty());
    }
    assert_eq!(errors.len(), 11);
}
