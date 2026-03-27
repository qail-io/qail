use super::*;
use crate::metrics::{reset_txn_test_metrics, txn_test_metrics_snapshot};

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
        created_at: now - created_ago,
        last_used: now - last_used_ago,
        closed: false,
        statements_executed,
        pg_aborted: false,
    };
    let mut sessions = mgr.sessions.lock().await;
    sessions.insert(id.to_string(), Arc::new(Mutex::new(session)));
    crate::metrics::record_txn_active_sessions(sessions.len());
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
            |_session| Box::pin(async move { Ok(()) }),
        )
        .await;

    assert!(matches!(result, Err(TransactionError::UserMismatch)));
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
        .with_session("s_lifetime", "tenant_a", Some("test-user"), |_session| {
            Box::pin(async move { Ok(()) })
        })
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
        .with_session("s_stmt", "tenant_b", Some("test-user"), |_session| {
            Box::pin(async move { Ok(()) })
        })
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
        let session = sessions.get("s_aborted").expect("session exists").clone();
        drop(sessions);
        let mut guard = session.lock().await;
        guard.pg_aborted = true;
    }

    let blocked = mgr
        .with_session("s_aborted", "tenant_d", Some("test-user"), |_session| {
            Box::pin(async move { Ok(()) })
        })
        .await;
    assert!(matches!(blocked, Err(TransactionError::Aborted)));

    let recovered = mgr
        .with_session_allow_aborted("s_aborted", "tenant_d", Some("test-user"), |_session| {
            Box::pin(async move { Ok(()) })
        })
        .await;
    // Test helper sessions have no pinned connection. Reaching SessionNotFound
    // here confirms we passed the aborted-state guard.
    assert!(matches!(recovered, Err(TransactionError::SessionNotFound)));
}
