use super::*;
use crate::metrics::{reset_txn_test_metrics, txn_test_metrics_snapshot};

const TEST_AUTH_FINGERPRINT: &str = "test-auth-scope";

async fn insert_test_session(
    mgr: &TransactionSessionManager,
    id: &str,
    tenant: &str,
    created_ago: Duration,
    last_used_ago: Duration,
    statements_executed: usize,
) {
    let now = Instant::now();
    let session = TransactionSession {
        conn: None,
        tenant_id: tenant.to_string(),
        user_id: Some("test-user".to_string()),
        auth_fingerprint: TEST_AUTH_FINGERPRINT.to_string(),
        created_at: now - created_ago,
        last_used: now - last_used_ago,
        closed: false,
        statements_executed,
        pg_aborted: false,
        mutated_tables: std::collections::HashSet::new(),
    };
    let mut sessions = mgr.sessions.lock().await;
    sessions.insert(id.to_string(), Arc::new(Mutex::new(session)));
    crate::metrics::record_txn_active_sessions(sessions.len());
}

async fn test_pooled_connection_from_env() -> Option<qail_pg::PooledConnection> {
    let database_url = std::env::var("DATABASE_URL").ok()?;
    let parsed = url::Url::parse(&database_url).ok()?;
    let host = parsed.host_str()?;
    let port = parsed.port().unwrap_or(5432);
    let user = parsed.username();
    if user.is_empty() {
        return None;
    }
    let database = parsed.path().trim_start_matches('/');
    if database.is_empty() {
        return None;
    }

    let mut config = qail_pg::PoolConfig::new_dev(host, port, user, database)
        .min_connections(0)
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(2))
        .connect_timeout(Duration::from_secs(2));
    if let Some(password) = parsed.password() {
        config = config.password(password);
    }

    let pool = qail_pg::PgPool::connect(config).await.ok()?;
    pool.acquire_raw().await.ok()
}

#[test]
fn test_transaction_error_display() {
    let err = TransactionError::SessionLimitReached(10);
    assert!(err.to_string().contains("limit reached"));
    assert!(err.to_string().contains("10"));

    let err = TransactionError::SessionNotFound;
    assert!(err.to_string().contains("not found"));

    let err = TransactionError::TenantMismatch;
    assert!(err.to_string().contains("different tenant"));

    let err = TransactionError::UserMismatch;
    assert!(err.to_string().contains("different user"));

    let err = TransactionError::AuthScopeMismatch;
    assert!(err.to_string().contains("different auth scope"));

    let err = TransactionError::Backpressure("Database acquire queue is saturated".to_string());
    assert!(err.to_string().contains("Backpressure"));

    let err = TransactionError::SessionLifetimeExceeded(900);
    assert!(err.to_string().contains("900"));

    let err = TransactionError::StatementLimitReached(1000);
    assert!(err.to_string().contains("1000"));
}

#[tokio::test]
async fn test_session_manager_respects_limit() {
    let mgr = TransactionSessionManager::new(2, 30, 900, 1000);
    assert_eq!(mgr.active_count().await, 0);
}

#[tokio::test]
async fn test_reap_expired_empty() {
    let mgr = TransactionSessionManager::new(10, 30, 900, 1000);
    // Should not panic on empty sessions
    mgr.reap_expired().await;
}

#[tokio::test]
async fn test_with_session_rejects_user_mismatch() {
    let mgr = TransactionSessionManager::new(10, 30, 900, 1000);
    insert_test_session(
        &mgr,
        "s_user_mismatch",
        "tenant_um",
        Duration::from_secs(0),
        Duration::from_secs(0),
        0,
    )
    .await;

    let result = mgr
        .with_session(
            "s_user_mismatch",
            "tenant_um",
            Some("other-user"),
            TEST_AUTH_FINGERPRINT,
            |_session| Box::pin(async move { Ok(()) }),
        )
        .await;

    assert!(matches!(result, Err(TransactionError::UserMismatch)));
}

#[tokio::test]
async fn test_with_session_rejects_auth_scope_mismatch() {
    let mgr = TransactionSessionManager::new(10, 30, 900, 1000);
    insert_test_session(
        &mgr,
        "s_auth_scope_mismatch",
        "tenant_scope",
        Duration::from_secs(0),
        Duration::from_secs(0),
        0,
    )
    .await;

    let result = mgr
        .with_session(
            "s_auth_scope_mismatch",
            "tenant_scope",
            Some("test-user"),
            "different-auth-scope",
            |_session| Box::pin(async move { Ok(()) }),
        )
        .await;

    assert!(matches!(result, Err(TransactionError::AuthScopeMismatch)));
}

#[tokio::test]
async fn test_with_session_enforces_lifetime_limit_and_records_metrics() {
    let _serial = crate::metrics::txn_test_serial_guard().await;
    reset_txn_test_metrics();
    let mgr = TransactionSessionManager::new(10, 30, 1, 1000);
    insert_test_session(
        &mgr,
        "s_lifetime",
        "tenant_a",
        Duration::from_secs(5),
        Duration::from_secs(0),
        0,
    )
    .await;

    let result = mgr
        .with_session(
            "s_lifetime",
            "tenant_a",
            Some("test-user"),
            TEST_AUTH_FINGERPRINT,
            |_session| Box::pin(async move { Ok(()) }),
        )
        .await;

    assert!(matches!(
        result,
        Err(TransactionError::SessionLifetimeExceeded(1))
    ));
    assert_eq!(mgr.active_count().await, 0);

    let snapshot = txn_test_metrics_snapshot();
    assert_eq!(snapshot.expired, 1);
    assert_eq!(snapshot.forced_lifetime, 1);
    assert_eq!(snapshot.active, 0);
}

#[tokio::test]
async fn test_close_session_rejects_commit_after_lifetime_limit_and_records_metrics() {
    let _serial = crate::metrics::txn_test_serial_guard().await;
    reset_txn_test_metrics();
    let mgr = TransactionSessionManager::new(10, 30, 1, 1000);
    insert_test_session(
        &mgr,
        "s_commit_lifetime",
        "tenant_commit",
        Duration::from_secs(5),
        Duration::from_secs(0),
        0,
    )
    .await;

    let result = mgr
        .close_session(
            "s_commit_lifetime",
            "tenant_commit",
            Some("test-user"),
            TEST_AUTH_FINGERPRINT,
            true,
        )
        .await;

    assert!(matches!(
        result,
        Err(TransactionError::SessionLifetimeExceeded(1))
    ));
    assert_eq!(mgr.active_count().await, 0);

    let snapshot = txn_test_metrics_snapshot();
    assert_eq!(snapshot.expired, 1);
    assert_eq!(snapshot.forced_lifetime, 1);
    assert_eq!(snapshot.active, 0);
}

#[tokio::test]
async fn test_with_session_enforces_statement_limit_and_records_metrics() {
    let _serial = crate::metrics::txn_test_serial_guard().await;
    reset_txn_test_metrics();
    let mgr = TransactionSessionManager::new(10, 30, 900, 1);
    insert_test_session(
        &mgr,
        "s_stmt",
        "tenant_b",
        Duration::from_secs(0),
        Duration::from_secs(0),
        1,
    )
    .await;

    let result = mgr
        .with_session(
            "s_stmt",
            "tenant_b",
            Some("test-user"),
            TEST_AUTH_FINGERPRINT,
            |_session| Box::pin(async move { Ok(()) }),
        )
        .await;

    assert!(matches!(
        result,
        Err(TransactionError::StatementLimitReached(1))
    ));
    assert_eq!(mgr.active_count().await, 0);

    let snapshot = txn_test_metrics_snapshot();
    assert_eq!(snapshot.statement_limit_hit, 1);
    assert_eq!(snapshot.forced_statement, 1);
    assert_eq!(snapshot.active, 0);
}

#[tokio::test]
async fn test_reap_expired_records_idle_timeout_metrics() {
    let _serial = crate::metrics::txn_test_serial_guard().await;
    reset_txn_test_metrics();
    let mgr = TransactionSessionManager::new(10, 1, 900, 1000);
    insert_test_session(
        &mgr,
        "s_idle",
        "tenant_c",
        Duration::from_secs(10),
        Duration::from_secs(5),
        3,
    )
    .await;

    mgr.reap_expired().await;

    assert_eq!(mgr.active_count().await, 0);
    let snapshot = txn_test_metrics_snapshot();
    assert_eq!(snapshot.forced_idle, 1);
    assert_eq!(snapshot.active, 0);
}

#[tokio::test]
async fn test_with_session_allow_aborted_enables_recovery_flow() {
    let mgr = TransactionSessionManager::new(10, 30, 900, 1000);
    insert_test_session(
        &mgr,
        "s_aborted",
        "tenant_d",
        Duration::from_secs(0),
        Duration::from_secs(0),
        0,
    )
    .await;

    {
        let sessions = mgr.sessions.lock().await;
        let session = std::sync::Arc::clone(sessions.get("s_aborted").expect("session exists"));
        drop(sessions);
        let mut guard = session.lock().await;
        guard.pg_aborted = true;
    }

    let blocked = mgr
        .with_session(
            "s_aborted",
            "tenant_d",
            Some("test-user"),
            TEST_AUTH_FINGERPRINT,
            |_session| Box::pin(async move { Ok(()) }),
        )
        .await;
    assert!(matches!(blocked, Err(TransactionError::Aborted)));

    let recovered = mgr
        .with_session_allow_aborted(
            "s_aborted",
            "tenant_d",
            Some("test-user"),
            TEST_AUTH_FINGERPRINT,
            |_session| Box::pin(async move { Ok(()) }),
        )
        .await;
    // Test helper sessions have no pinned connection. Reaching SessionNotFound
    // here confirms we passed the aborted-state guard.
    assert!(matches!(recovered, Err(TransactionError::SessionNotFound)));
}

#[tokio::test]
async fn test_with_session_refreshes_last_used_after_success_when_database_url_set() {
    let Some(conn) = test_pooled_connection_from_env().await else {
        eprintln!(
            "DATABASE_URL unavailable or unreachable; skipping live transaction timestamp test"
        );
        return;
    };

    let mgr = TransactionSessionManager::new(10, 30, 900, 1000);
    let now = Instant::now();
    let session = TransactionSession {
        conn: Some(conn),
        tenant_id: "tenant_touch".to_string(),
        user_id: Some("test-user".to_string()),
        auth_fingerprint: TEST_AUTH_FINGERPRINT.to_string(),
        created_at: now,
        last_used: now - Duration::from_secs(60),
        closed: false,
        statements_executed: 0,
        pg_aborted: false,
        mutated_tables: std::collections::HashSet::new(),
    };
    {
        let mut sessions = mgr.sessions.lock().await;
        sessions.insert("s_touch".to_string(), Arc::new(Mutex::new(session)));
        crate::metrics::record_txn_active_sessions(sessions.len());
    }

    let operation_finished_at = Arc::new(Mutex::new(None));
    let operation_finished_at_for_closure = Arc::clone(&operation_finished_at);
    let result = mgr
        .with_session(
            "s_touch",
            "tenant_touch",
            Some("test-user"),
            TEST_AUTH_FINGERPRINT,
            move |_session| {
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    *operation_finished_at_for_closure.lock().await = Some(Instant::now());
                    Ok(())
                })
            },
        )
        .await;
    assert!(result.is_ok());

    let operation_finished_at = operation_finished_at
        .lock()
        .await
        .expect("closure should record its finish time");
    let session = {
        let sessions = mgr.sessions.lock().await;
        Arc::clone(sessions.get("s_touch").expect("session remains open"))
    };
    let mut guard = session.lock().await;
    assert!(
        guard.last_used >= operation_finished_at,
        "last_used must be refreshed after the operation completes"
    );
    let conn = guard.conn.take();
    drop(guard);
    TransactionSessionManager::rollback_and_release(conn).await;
}
