//! DB acquire backpressure guard.
//!
//! Caps waiting acquires globally and per tenant to prevent queue blow-up
//! under overload and return deterministic 503 shedding.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use qail_pg::{PgError, PooledConnection};

use crate::auth::AuthContext;
use crate::middleware::ApiError;

mod state;

pub(crate) const POOL_BACKPRESSURE_MSG_GLOBAL: &str = "Database acquire queue is saturated";
pub(crate) const POOL_BACKPRESSURE_MSG_TENANT: &str = "Tenant database acquire queue is saturated";
pub(crate) const POOL_BACKPRESSURE_MSG_TENANT_MAP: &str =
    "Database queue tenant-tracker is saturated";

/// Translate a pool-backpressure message into stable response-header metadata.
pub(crate) fn backpressure_response_metadata(message: &str) -> (&'static str, &'static str, u64) {
    match message {
        POOL_BACKPRESSURE_MSG_GLOBAL => ("global", "global_waiters_exceeded", 1),
        POOL_BACKPRESSURE_MSG_TENANT => ("tenant", "tenant_waiters_exceeded", 1),
        POOL_BACKPRESSURE_MSG_TENANT_MAP => ("tenant_map", "tenant_tracker_saturated", 1),
        _ => ("unknown", "queue_saturated", 1),
    }
}

#[derive(Debug)]
pub struct DbBackpressure {
    max_waiters_global: usize,
    max_waiters_per_tenant: usize,
    max_tracked_tenants: usize,
    global_waiters: AtomicUsize,
    per_tenant_waiters: Mutex<HashMap<String, usize>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RejectReason {
    Global,
    Tenant,
    TenantMapSaturated,
}

impl DbBackpressure {
    pub fn new(
        max_waiters_global: usize,
        max_waiters_per_tenant: usize,
        max_tracked_tenants: usize,
    ) -> Self {
        Self {
            max_waiters_global,
            max_waiters_per_tenant,
            max_tracked_tenants,
            global_waiters: AtomicUsize::new(0),
            per_tenant_waiters: Mutex::new(HashMap::new()),
        }
    }

    fn enabled(&self) -> bool {
        self.max_waiters_global > 0 && self.max_waiters_per_tenant > 0
    }

    fn tracked_tenants_len(&self) -> usize {
        let guard = self
            .per_tenant_waiters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.len()
    }

    fn enter(self: &Arc<Self>, tenant_key: &str) -> Result<DbWaitPermit, RejectReason> {
        if !self.enabled() {
            return Ok(DbWaitPermit {
                owner: None,
                tenant_key: String::new(),
            });
        }

        let global_now = self.global_waiters.fetch_add(1, Ordering::Relaxed) + 1;
        if global_now > self.max_waiters_global {
            self.global_waiters.fetch_sub(1, Ordering::Relaxed);
            crate::metrics::record_db_waiters(
                self.global_waiters.load(Ordering::Relaxed),
                self.tracked_tenants_len(),
            );
            return Err(RejectReason::Global);
        }

        let mut tenants = self
            .per_tenant_waiters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(waiters) = tenants.get_mut(tenant_key) {
            if *waiters >= self.max_waiters_per_tenant {
                drop(tenants);
                self.global_waiters.fetch_sub(1, Ordering::Relaxed);
                crate::metrics::record_db_waiters(
                    self.global_waiters.load(Ordering::Relaxed),
                    self.tracked_tenants_len(),
                );
                return Err(RejectReason::Tenant);
            }
            *waiters += 1;
        } else {
            if tenants.len() >= self.max_tracked_tenants {
                drop(tenants);
                self.global_waiters.fetch_sub(1, Ordering::Relaxed);
                crate::metrics::record_db_waiters(
                    self.global_waiters.load(Ordering::Relaxed),
                    self.tracked_tenants_len(),
                );
                return Err(RejectReason::TenantMapSaturated);
            }
            tenants.insert(tenant_key.to_string(), 1);
        }

        let tracked = tenants.len();
        drop(tenants);
        crate::metrics::record_db_waiters(global_now, tracked);

        Ok(DbWaitPermit {
            owner: Some(Arc::clone(self)),
            tenant_key: tenant_key.to_string(),
        })
    }
}

struct DbWaitPermit {
    owner: Option<Arc<DbBackpressure>>,
    tenant_key: String,
}

impl Drop for DbWaitPermit {
    fn drop(&mut self) {
        let Some(owner) = self.owner.as_ref() else {
            return;
        };

        let prev_global = owner.global_waiters.fetch_sub(1, Ordering::Relaxed);
        let global_now = prev_global.saturating_sub(1);

        let mut tenants = owner
            .per_tenant_waiters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(waiters) = tenants.get_mut(&self.tenant_key) {
            if *waiters > 1 {
                *waiters -= 1;
            } else {
                tenants.remove(&self.tenant_key);
            }
        }
        let tracked = tenants.len();
        drop(tenants);
        crate::metrics::record_db_waiters(global_now, tracked);
    }
}

fn waiter_key_for_auth(auth: &AuthContext) -> String {
    // Enforce per-tenant fairness for multi-tenant deployments.
    // Fallback to user scope when tenant_id is unavailable (single-tenant/dev).
    match auth.tenant_id.as_deref() {
        Some(tid) if !tid.is_empty() => format!("tenant:{}", tid),
        _ => format!("user:{}", auth.user_id),
    }
}

fn backpressure_api_error(reason: RejectReason) -> ApiError {
    let message = match reason {
        RejectReason::Global => POOL_BACKPRESSURE_MSG_GLOBAL,
        RejectReason::Tenant => POOL_BACKPRESSURE_MSG_TENANT,
        RejectReason::TenantMapSaturated => POOL_BACKPRESSURE_MSG_TENANT_MAP,
    };
    crate::metrics::record_db_acquire_shed(match reason {
        RejectReason::Global => "global",
        RejectReason::Tenant => "tenant",
        RejectReason::TenantMapSaturated => "tenant_map",
    });
    ApiError::with_code("POOL_BACKPRESSURE", message)
}

fn record_acquire_outcome(wait_started: Instant, result: &Result<PooledConnection, PgError>) {
    let wait_ms = wait_started.elapsed().as_secs_f64() * 1000.0;
    match result {
        Ok(_) => crate::metrics::record_db_acquire_wait(wait_ms, "success"),
        Err(PgError::Timeout(_)) => {
            crate::metrics::record_db_acquire_timeout();
            crate::metrics::record_db_acquire_wait(wait_ms, "timeout");
        }
        Err(PgError::PoolExhausted { .. }) => {
            crate::metrics::record_db_acquire_shed("pool_exhausted");
            crate::metrics::record_db_acquire_wait(wait_ms, "exhausted");
        }
        Err(_) => crate::metrics::record_db_acquire_wait(wait_ms, "error"),
    }
}

#[cfg(test)]
mod tests;
