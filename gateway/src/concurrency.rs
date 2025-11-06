//! Per-tenant concurrency guard.
//!
//! Limits the number of concurrent database queries per tenant (operator)
//! to prevent a single tenant from monopolising the connection pool.
//! Also caps the total number of tracked tenants to prevent memory exhaustion.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Semaphore, OwnedSemaphorePermit};

/// Entry tracking a tenant's semaphore and last usage time.
struct TenantEntry {
    semaphore: Arc<Semaphore>,
    last_used: Instant,
}

/// Per-tenant concurrency limiter with bounded growth.
///
/// Each tenant (identified by operator_id) gets its own semaphore with
/// `max_permits` concurrent slots. When all slots are occupied, additional
/// requests receive a 429 response instead of queuing unboundedly.
///
/// The tenant map is capped at `max_tenants` entries. Idle entries are
/// evicted by a background sweeper task.
#[derive(Debug)]
pub struct TenantSemaphore {
    max_permits: usize,
    max_tenants: usize,
    idle_timeout: Duration,
    semaphores: RwLock<HashMap<String, TenantEntry>>,
}

// Manual Debug since TenantEntry contains Instant
impl std::fmt::Debug for TenantEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantEntry")
            .field("permits", &self.semaphore.available_permits())
            .field("age_secs", &self.last_used.elapsed().as_secs())
            .finish()
    }
}

impl TenantSemaphore {
    /// Create a new per-tenant semaphore with bounded growth.
    pub fn new(max_permits: usize) -> Self {
        Self::with_limits(max_permits, 10_000, Duration::from_secs(300))
    }

    /// Create with explicit limits.
    pub fn with_limits(max_permits: usize, max_tenants: usize, idle_timeout: Duration) -> Self {
        Self {
            max_permits,
            max_tenants,
            idle_timeout,
            semaphores: RwLock::new(HashMap::new()),
        }
    }

    /// Start the background idle sweeper task.
    ///
    /// Runs every 60 seconds, evicting entries that have been idle longer
    /// than `idle_timeout` and have all permits available (no active queries).
    pub fn start_sweeper(self: &Arc<Self>) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let mut map = this.semaphores.write().await;
                let before = map.len();
                map.retain(|_tenant_id, entry| {
                    // Keep if: recently used OR has active permits (queries in-flight)
                    let is_idle = entry.last_used.elapsed() > this.idle_timeout;
                    let all_free = entry.semaphore.available_permits() == this.max_permits;
                    !(is_idle && all_free)
                });
                let evicted = before - map.len();
                if evicted > 0 {
                    eprintln!(
                        "[INFO] tenant_semaphore_sweep: evicted {} idle tenants ({} remaining)",
                        evicted,
                        map.len()
                    );
                }
            }
        });
    }

    /// Try to acquire a permit for the given tenant.
    ///
    /// Returns `Some(permit)` if a slot is available, `None` if all slots
    /// are occupied or the tenant map is full (caller should return 429).
    pub async fn try_acquire(&self, tenant_id: &str) -> Option<OwnedSemaphorePermit> {
        // Fast path: check if semaphore already exists
        {
            let map = self.semaphores.read().await;
            if let Some(entry) = map.get(tenant_id) {
                return entry.semaphore.clone().try_acquire_owned().ok();
            }
        }

        // Slow path: create semaphore for new tenant
        let mut map = self.semaphores.write().await;

        // Check if another task created it while we waited for write lock
        if let Some(entry) = map.get_mut(tenant_id) {
            entry.last_used = Instant::now();
            return entry.semaphore.clone().try_acquire_owned().ok();
        }

        // Reject if at tenant capacity — prevents memory exhaustion
        if map.len() >= self.max_tenants {
            eprintln!(
                "[WARN] tenant_semaphore_full: rejecting new tenant '{}' (cap: {})",
                tenant_id, self.max_tenants
            );
            return None;
        }

        let entry = map.entry(tenant_id.to_string()).or_insert(TenantEntry {
            semaphore: Arc::new(Semaphore::new(self.max_permits)),
            last_used: Instant::now(),
        });
        entry.semaphore.clone().try_acquire_owned().ok()
    }

    /// Number of tracked tenants (for metrics / debugging).
    pub fn tenant_count(&self) -> usize {
        // Non-async best-effort via try_read
        self.semaphores
            .try_read()
            .map(|m| m.len())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_permits_are_per_tenant() {
        let sem = TenantSemaphore::new(2);

        // Tenant A takes 2 permits → full
        let _a1 = sem.try_acquire("tenant-a").await.unwrap();
        let _a2 = sem.try_acquire("tenant-a").await.unwrap();
        assert!(sem.try_acquire("tenant-a").await.is_none(), "Tenant A should be full");

        // Tenant B still has capacity
        let _b1 = sem.try_acquire("tenant-b").await.unwrap();
        assert!(sem.try_acquire("tenant-b").await.is_some(), "Tenant B should have capacity");
    }

    #[tokio::test]
    async fn test_permit_release_frees_slot() {
        let sem = TenantSemaphore::new(1);

        let permit = sem.try_acquire("t1").await.unwrap();
        assert!(sem.try_acquire("t1").await.is_none(), "Should be full");

        drop(permit);
        assert!(sem.try_acquire("t1").await.is_some(), "Should be free after drop");
    }

    #[tokio::test]
    async fn test_tenant_count() {
        let sem = TenantSemaphore::new(5);
        let _a = sem.try_acquire("a").await;
        let _b = sem.try_acquire("b").await;
        assert_eq!(sem.tenant_count(), 2);
    }

    // ══════════════════════════════════════════════════════════════════
    // RED-TEAM: Concurrency Race Tests (#12 from adversarial checklist)
    // ══════════════════════════════════════════════════════════════════

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
                        // Hold permit briefly to simulate query execution
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                        drop(permit);
                    }
                    None => {
                        rejected.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }));
        }
        for h in handles { h.await.unwrap(); }
        
        let total = acquired.load(std::sync::atomic::Ordering::Relaxed) 
                  + rejected.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(total, 100, "All tasks must complete");
        assert!(rejected.load(std::sync::atomic::Ordering::Relaxed) > 0, 
            "Some tasks must be rejected when max_permits=5 with 100 concurrent");
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
                // Third must fail (max_permits = 2)
                assert!(sem.try_acquire(&tenant).await.is_none(), 
                    "Third permit for {} must fail", tenant);
                drop(p1);
                drop(p2);
            }));
        }
        for h in handles { h.await.unwrap(); }
        assert_eq!(sem.tenant_count(), 50);
    }

    #[tokio::test]
    async fn redteam_same_tenant_5_ips_burst() {
        // Simulates same tenant from 5 IPs, each sending 10 requests
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
        for h in handles { h.await.unwrap(); }
        // With 50 tasks and max_permits=3, most should be rejected
        let count = acquired.load(std::sync::atomic::Ordering::Relaxed);
        assert!(count <= 50, "Cannot exceed total tasks");
        // At least some should succeed
        assert!(count >= 3, "At least max_permits should succeed initially");
    }
}
