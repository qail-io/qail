//! DB acquire backpressure guard.
//!
//! Caps waiting acquires globally and per tenant to prevent queue blow-up
//! under overload and return deterministic 503 shedding.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use qail_core::rls::RlsContext;
use qail_pg::{PgError, PooledConnection};

use crate::GatewayState;
use crate::auth::AuthContext;
use crate::middleware::ApiError;

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

impl GatewayState {
    /// Acquire RLS-scoped connection with queue backpressure guards.
    pub async fn acquire_with_rls_timeouts_guarded(
        &self,
        waiter_key: &str,
        rls_ctx: RlsContext,
        statement_timeout_ms: u32,
        lock_timeout_ms: u32,
        table: Option<&str>,
    ) -> Result<PooledConnection, ApiError> {
        let _waiter = self
            .db_backpressure
            .enter(waiter_key)
            .map_err(backpressure_api_error)?;

        let started = Instant::now();
        let result = self
            .pool
            .acquire_with_rls_timeouts(rls_ctx, statement_timeout_ms, lock_timeout_ms)
            .await;
        record_acquire_outcome(started, &result);
        result.map_err(|e| ApiError::from_pg_driver_error(&e, table))
    }

    /// Acquire RLS-scoped connection (uses gateway default timeouts).
    pub async fn acquire_with_auth_rls_guarded(
        &self,
        auth: &AuthContext,
        table: Option<&str>,
    ) -> Result<PooledConnection, ApiError> {
        self.acquire_with_auth_rls_timeouts_guarded(
            auth,
            self.config.statement_timeout_ms,
            self.config.lock_timeout_ms,
            table,
        )
        .await
    }

    /// Acquire RLS-scoped connection with explicit timeouts and auth-derived waiter key.
    pub async fn acquire_with_auth_rls_timeouts_guarded(
        &self,
        auth: &AuthContext,
        statement_timeout_ms: u32,
        lock_timeout_ms: u32,
        table: Option<&str>,
    ) -> Result<PooledConnection, ApiError> {
        let waiter_key = waiter_key_for_auth(auth);
        self.acquire_with_rls_timeouts_guarded(
            &waiter_key,
            auth.to_rls_context(),
            statement_timeout_ms,
            lock_timeout_ms,
            table,
        )
        .await
    }

    /// Acquire raw connection with backpressure guards.
    pub async fn acquire_raw_with_auth_guarded(
        &self,
        auth: &AuthContext,
        table: Option<&str>,
    ) -> Result<PooledConnection, ApiError> {
        let waiter_key = waiter_key_for_auth(auth);
        let _waiter = self
            .db_backpressure
            .enter(&waiter_key)
            .map_err(backpressure_api_error)?;

        let started = Instant::now();
        let result = self.pool.acquire_raw().await;
        record_acquire_outcome(started, &result);
        result.map_err(|e| ApiError::from_pg_driver_error(&e, table))
    }

    /// Acquire system-scoped connection with backpressure guards.
    pub async fn acquire_system_guarded(
        &self,
        scope: &str,
        table: Option<&str>,
    ) -> Result<PooledConnection, ApiError> {
        let waiter_key = format!("_system:{}", scope);
        let _waiter = self
            .db_backpressure
            .enter(&waiter_key)
            .map_err(backpressure_api_error)?;

        let started = Instant::now();
        let result = self.pool.acquire_system().await;
        record_acquire_outcome(started, &result);
        result.map_err(|e| ApiError::from_pg_driver_error(&e, table))
    }

    /// Create a transaction session with backpressure guarding on initial pool acquire.
    pub async fn create_txn_session_guarded(
        &self,
        auth: &AuthContext,
        tenant_id: String,
        user_id: Option<String>,
        statement_timeout_ms: u32,
        lock_timeout_ms: u32,
    ) -> Result<String, crate::transaction::TransactionError> {
        let waiter_key = waiter_key_for_auth(auth);
        let _waiter = self.db_backpressure.enter(&waiter_key).map_err(|reason| {
            backpressure_api_error(reason);
            crate::transaction::TransactionError::Pool(
                "transaction begin shed by DB backpressure".to_string(),
            )
        })?;

        let started = Instant::now();
        let result = self
            .transaction_manager
            .create_session(
                &self.pool,
                auth.to_rls_context(),
                tenant_id,
                user_id,
                statement_timeout_ms,
                lock_timeout_ms,
            )
            .await;

        let wait_ms = started.elapsed().as_secs_f64() * 1000.0;
        match &result {
            Ok(_) => crate::metrics::record_db_acquire_wait(wait_ms, "success"),
            Err(crate::transaction::TransactionError::Pool(msg))
                if msg.to_ascii_lowercase().contains("timeout") =>
            {
                crate::metrics::record_db_acquire_timeout();
                crate::metrics::record_db_acquire_wait(wait_ms, "timeout");
            }
            Err(crate::transaction::TransactionError::Pool(_)) => {
                crate::metrics::record_db_acquire_wait(wait_ms, "error");
            }
            Err(_) => crate::metrics::record_db_acquire_wait(wait_ms, "error"),
        }
        result
    }
}

#[cfg(test)]
mod tests {
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
}
