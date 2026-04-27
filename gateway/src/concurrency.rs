//! Per-tenant concurrency guard.
//!
//! Limits the number of concurrent database queries per tenant (operator)
//! to prevent a single tenant from monopolising the connection pool.
//! Also caps the total number of tracked tenants to prevent memory exhaustion.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};

/// Entry tracking a tenant's semaphore and last usage time.
struct TenantEntry {
    semaphore: Arc<Semaphore>,
    last_used: Instant,
}

/// Per-tenant concurrency limiter with bounded growth.
///
/// Each tenant (identified by tenant_id) gets its own semaphore with
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
                    tracing::info!(
                        evicted = evicted,
                        remaining = map.len(),
                        "tenant_semaphore_sweep: evicted idle tenants"
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
                return Arc::clone(&entry.semaphore).try_acquire_owned().ok();
            }
        }

        // Slow path: create semaphore for new tenant
        let mut map = self.semaphores.write().await;

        // Check if another task created it while we waited for write lock
        if let Some(entry) = map.get_mut(tenant_id) {
            entry.last_used = Instant::now();
            return Arc::clone(&entry.semaphore).try_acquire_owned().ok();
        }

        // Reject if at tenant capacity — prevents memory exhaustion
        if map.len() >= self.max_tenants {
            tracing::warn!(
                tenant_id = tenant_id,
                cap = self.max_tenants,
                "tenant_semaphore_full: rejecting new tenant"
            );
            return None;
        }

        let entry = map.entry(tenant_id.to_string()).or_insert(TenantEntry {
            semaphore: Arc::new(Semaphore::new(self.max_permits)),
            last_used: Instant::now(),
        });
        Arc::clone(&entry.semaphore).try_acquire_owned().ok()
    }

    /// Number of tracked tenants (for metrics / debugging).
    pub fn tenant_count(&self) -> usize {
        // Non-async best-effort via try_read
        self.semaphores.try_read().map(|m| m.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests;
