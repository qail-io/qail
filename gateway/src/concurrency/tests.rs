use super::*;

#[tokio::test]
async fn test_permits_are_per_tenant() {
    let sem = TenantSemaphore::new(2);

    let _a1 = sem.try_acquire("tenant-a").await.unwrap();
    let _a2 = sem.try_acquire("tenant-a").await.unwrap();
    assert!(
        sem.try_acquire("tenant-a").await.is_none(),
        "Tenant A should be full"
    );

    let _b1 = sem.try_acquire("tenant-b").await.unwrap();
    assert!(
        sem.try_acquire("tenant-b").await.is_some(),
        "Tenant B should have capacity"
    );
}

#[tokio::test]
async fn test_permit_release_frees_slot() {
    let sem = TenantSemaphore::new(1);

    let permit = sem.try_acquire("t1").await.unwrap();
    assert!(sem.try_acquire("t1").await.is_none(), "Should be full");

    drop(permit);
    assert!(
        sem.try_acquire("t1").await.is_some(),
        "Should be free after drop"
    );
}

#[tokio::test]
async fn test_tenant_count() {
    let sem = TenantSemaphore::new(5);
    let _a = sem.try_acquire("a").await;
    let _b = sem.try_acquire("b").await;
    assert_eq!(sem.tenant_count(), 2);
}

#[tokio::test]
async fn redteam_100_concurrent_acquires_same_tenant() {
    let sem = Arc::new(TenantSemaphore::new(5));
    let acquired = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let rejected = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..100 {
        let sem = sem.clone();
        let acquired = acquired.clone();
        let rejected = rejected.clone();
        handles.push(tokio::spawn(async move {
            match sem.try_acquire("hot-tenant").await {
                Some(permit) => {
                    acquired.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    drop(permit);
                }
                None => {
                    rejected.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let total = acquired.load(std::sync::atomic::Ordering::Relaxed)
        + rejected.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(total, 100, "All tasks must complete");
    assert!(
        rejected.load(std::sync::atomic::Ordering::Relaxed) > 0,
        "Some tasks must be rejected when max_permits=5 with 100 concurrent"
    );
}

#[tokio::test]
async fn redteam_50_tenants_concurrent() {
    let sem = Arc::new(TenantSemaphore::new(2));
    let mut handles = Vec::new();

    for i in 0..50 {
        let sem = sem.clone();
        handles.push(tokio::spawn(async move {
            let tenant = format!("tenant-{}", i);
            let p1 = sem.try_acquire(&tenant).await;
            let p2 = sem.try_acquire(&tenant).await;
            assert!(p1.is_some(), "First permit for {} must succeed", tenant);
            assert!(p2.is_some(), "Second permit for {} must succeed", tenant);
            assert!(
                sem.try_acquire(&tenant).await.is_none(),
                "Third permit for {} must fail",
                tenant
            );
            drop(p1);
            drop(p2);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(sem.tenant_count(), 50);
}

#[tokio::test]
async fn redteam_same_tenant_5_ips_burst() {
    let sem = Arc::new(TenantSemaphore::new(3));
    let acquired = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ip in 0..5 {
        for _ in 0..10 {
            let sem = sem.clone();
            let acquired = acquired.clone();
            handles.push(tokio::spawn(async move {
                if let Some(_permit) = sem.try_acquire("burst-tenant").await {
                    acquired.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            }));
        }
    }
    for h in handles {
        h.await.unwrap();
    }
    let count = acquired.load(std::sync::atomic::Ordering::Relaxed);
    assert!(count <= 50, "Cannot exceed total tasks");
    assert!(count >= 3, "At least max_permits should succeed initially");
}
