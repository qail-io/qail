use super::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[test]
fn global_cap_sheds() {
    let bp = Arc::new(DbBackpressure::new(1, 10, 10));
    let p1 = bp.enter("t1:u1").expect("first waiter should pass");
    let r2 = bp.enter("t2:u2");
    assert!(matches!(r2, Err(RejectReason::Global)));
    drop(p1);
    assert!(bp.enter("t2:u2").is_ok());
}

#[test]
fn tenant_cap_sheds() {
    let bp = Arc::new(DbBackpressure::new(10, 1, 10));
    let p1 = bp.enter("tenant:user").expect("first waiter should pass");
    let r2 = bp.enter("tenant:user");
    assert!(matches!(r2, Err(RejectReason::Tenant)));
    drop(p1);
    assert!(bp.enter("tenant:user").is_ok());
}

#[test]
fn tenant_map_cap_sheds_new_keys() {
    let bp = Arc::new(DbBackpressure::new(10, 10, 1));
    let p1 = bp.enter("t1:u1").expect("first waiter should pass");
    let r2 = bp.enter("t2:u2");
    assert!(matches!(r2, Err(RejectReason::TenantMapSaturated)));
    drop(p1);
    assert!(bp.enter("t2:u2").is_ok());
}

#[test]
fn waiter_key_uses_tenant_scope_when_present() {
    let auth = AuthContext {
        user_id: "u1".to_string(),
        role: "operator".to_string(),
        tenant_id: Some("tenant_a".to_string()),
        claims: HashMap::new(),
    };
    assert_eq!(waiter_key_for_auth(&auth), "tenant:tenant_a");
}

#[test]
fn same_tenant_different_users_share_waiter_scope() {
    let auth_a = AuthContext {
        user_id: "u1".to_string(),
        role: "operator".to_string(),
        tenant_id: Some("tenant_a".to_string()),
        claims: HashMap::new(),
    };
    let auth_b = AuthContext {
        user_id: "u2".to_string(),
        role: "operator".to_string(),
        tenant_id: Some("tenant_a".to_string()),
        claims: HashMap::new(),
    };
    assert_eq!(waiter_key_for_auth(&auth_a), waiter_key_for_auth(&auth_b));
}

#[test]
fn waiter_key_falls_back_to_user_scope_when_tenant_missing() {
    let auth = AuthContext {
        user_id: "u1".to_string(),
        role: "operator".to_string(),
        tenant_id: None,
        claims: HashMap::new(),
    };
    assert_eq!(waiter_key_for_auth(&auth), "user:u1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chaos_many_users_same_tenant_hard_caps_per_tenant_waiters() {
    let tenant_cap = 8usize;
    let total_users = 96usize;
    let bp = Arc::new(DbBackpressure::new(10_000, tenant_cap, 1_000));

    let start = Arc::new(tokio::sync::Barrier::new(total_users + 1));
    let release = Arc::new(tokio::sync::Barrier::new(total_users + 1));
    let successes = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::with_capacity(total_users);
    for i in 0..total_users {
        let bp = Arc::clone(&bp);
        let start = Arc::clone(&start);
        let release = Arc::clone(&release);
        let successes = Arc::clone(&successes);

        handles.push(tokio::spawn(async move {
            let auth = AuthContext {
                user_id: format!("user_{}", i),
                role: "operator".to_string(),
                tenant_id: Some("tenant_chaos".to_string()),
                claims: HashMap::new(),
            };
            let key = waiter_key_for_auth(&auth);

            start.wait().await;

            let permit = bp.enter(&key).ok();
            if permit.is_some() {
                successes.fetch_add(1, Ordering::Relaxed);
            }

            // Keep successful permits alive until all contenders attempted.
            release.wait().await;
        }));
    }

    start.wait().await;
    tokio::time::timeout(Duration::from_secs(2), release.wait())
        .await
        .expect("all concurrent contenders should reach release barrier");

    for handle in handles {
        handle.await.expect("worker should complete");
    }

    assert_eq!(
        successes.load(Ordering::Relaxed),
        tenant_cap,
        "per-tenant waiter cap must hold even with many users in same tenant"
    );
}
