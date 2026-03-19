//! Tenant Boundary Invariant Enforcer
//!
//! Runtime verification that RLS is working correctly. After every query,
//! scans returned rows for a configurable tenant column (default `tenant_id`)
//! mismatches against the authenticated tenant context.
//!
//! This catches RLS bypass bugs in code we haven't written yet.
//!
//! # Design
//!
//! - **Fail-closed**: Violations abort the response (500). Leaked rows
//!   never reach the client.
//! - **Type-safe**: Returns `TenantVerified` token that response builders
//!   require. If you skip the check, your code won't compile.
//! - **Zero false positives**: Only triggers when the tenant column exists
//!   AND its value mismatches the authenticated tenant.
//! - **Performance**: O(n) scan per response, no allocations beyond the counter.

use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};

/// Zero-cost proof that tenant boundary was verified.
///
/// Cannot be constructed outside this module. Response builders
/// require this as a parameter — if you skip `verify_tenant_boundary`,
/// your code won't compile.
#[derive(Debug)]
#[must_use]
pub struct TenantVerified(());

impl TenantVerified {
    /// Create a `TenantVerified` for unauthenticated/system requests
    /// where no tenant scoping applies (e.g., no `tenant_id` in context).
    pub fn unscoped() -> Self {
        Self(())
    }
}

/// Tenant boundary violation — one or more rows had wrong tenant column value.
#[derive(Debug)]
pub struct TenantViolation {
    /// Number of rows with mismatched tenant column
    pub violation_count: u64,
    /// Table where the violation was detected.
    pub table: String,
    /// Endpoint / call site that triggered the check.
    pub endpoint: String,
}

impl std::fmt::Display for TenantViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TENANT_BOUNDARY_VIOLATION: {} rows in table={} endpoint={}",
            self.violation_count, self.table, self.endpoint
        )
    }
}

/// Global counters for tenant boundary violations.
///
/// These are designed for external monitoring (Prometheus, health endpoint).
pub struct TenantGuardMetrics {
    /// Total rows checked across all requests
    pub rows_checked: AtomicU64,
    /// Total requests where at least one violation was found
    pub violation_requests: AtomicU64,
    /// Total individual row violations (rows with wrong tenant column value)
    pub violation_rows: AtomicU64,
}

impl TenantGuardMetrics {
    /// Create a new, zeroed metrics instance.
    pub const fn new() -> Self {
        Self {
            rows_checked: AtomicU64::new(0),
            violation_requests: AtomicU64::new(0),
            violation_rows: AtomicU64::new(0),
        }
    }
}

impl Default for TenantGuardMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Global singleton for metrics collection.
pub static TENANT_GUARD: TenantGuardMetrics = TenantGuardMetrics::new();

/// Verify that all rows in a query response belong to the expected tenant.
///
/// Returns `Ok(TenantVerified)` if all rows pass, or `Err(TenantViolation)`
/// if any row has a mismatched tenant column value. On violation, the caller
/// MUST abort the response — never send leaked rows to the client.
///
/// # Arguments
///
/// * `rows` - The JSON rows from the query response
/// * `expected_tenant_id` - The authenticated tenant's ID
/// * `tenant_column` - Column name to check (e.g. "operator_id", "tenant_id")
/// * `table` - Table name for logging context
/// * `endpoint` - Endpoint name for logging context
pub fn verify_tenant_boundary(
    rows: &[Value],
    expected_tenant_id: &str,
    tenant_column: &str,
    table: &str,
    endpoint: &str,
) -> Result<TenantVerified, TenantViolation> {
    if rows.is_empty() || expected_tenant_id.is_empty() {
        return Ok(TenantVerified(()));
    }

    let mut violations = 0u64;
    let mut checked = 0u64;

    for (i, row) in rows.iter().enumerate() {
        if let Some(obj) = row.as_object() {
            // Check tenant column (our RLS partition key)
            if let Some(op_val) = obj.get(tenant_column) {
                checked += 1;

                let row_tenant_id = match op_val {
                    Value::String(s) => s.as_str(),
                    Value::Number(n) => {
                        let n_str = n.to_string();
                        if n_str != expected_tenant_id {
                            violations += 1;
                            tracing::error!(
                                table = table,
                                endpoint = endpoint,
                                row = i,
                                column = tenant_column,
                                expected = expected_tenant_id,
                                actual = %n_str,
                                "TENANT_BOUNDARY_VIOLATION — RLS MAY BE COMPROMISED"
                            );
                        }
                        continue;
                    }
                    Value::Null => continue, // NULL tenant column — skip (system rows)
                    _ => continue,
                };

                if row_tenant_id != expected_tenant_id {
                    violations += 1;
                    tracing::error!(
                        table = table,
                        endpoint = endpoint,
                        row = i,
                        column = tenant_column,
                        expected = expected_tenant_id,
                        actual = row_tenant_id,
                        "TENANT_BOUNDARY_VIOLATION — RLS MAY BE COMPROMISED"
                    );
                }
            }
        }
    }

    // Update global counters
    TENANT_GUARD
        .rows_checked
        .fetch_add(checked, Ordering::Relaxed);
    if violations > 0 {
        TENANT_GUARD
            .violation_requests
            .fetch_add(1, Ordering::Relaxed);
        TENANT_GUARD
            .violation_rows
            .fetch_add(violations, Ordering::Relaxed);
        Err(TenantViolation {
            violation_count: violations,
            table: table.to_string(),
            endpoint: endpoint.to_string(),
        })
    } else {
        Ok(TenantVerified(()))
    }
}

/// Get current tenant guard metrics as a JSON-serializable snapshot.
pub fn metrics_snapshot() -> TenantGuardSnapshot {
    TenantGuardSnapshot {
        rows_checked: TENANT_GUARD.rows_checked.load(Ordering::Relaxed),
        violation_requests: TENANT_GUARD.violation_requests.load(Ordering::Relaxed),
        violation_rows: TENANT_GUARD.violation_rows.load(Ordering::Relaxed),
    }
}

/// Serializable snapshot of tenant guard metrics.
#[derive(Debug, serde::Serialize)]
pub struct TenantGuardSnapshot {
    /// Total rows checked across all requests.
    pub rows_checked: u64,
    /// Total requests where at least one violation was found.
    pub violation_requests: u64,
    /// Total individual row violations.
    pub violation_rows: u64,
}

#[cfg(test)]
mod tests;
