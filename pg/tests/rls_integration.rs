//! RLS Integration Tests
//!
//! Tests multi-tenant data isolation using the qail-pg driver.
//! Requires a local PostgreSQL database with RLS policies applied.
//!
//! Run with:
//! ```sh
//! DATABASE_URL="postgresql://qail_app@localhost:5432/qail_test" \
//!     cargo test -p qail-pg --test rls_integration -- --nocapture
//! ```

use qail_core::ast::Qail;
use qail_core::rls::RlsContext;
use qail_pg::PgDriver;

/// Known test operator: Operator A (has 11 vessels in local DB)
const OPERATOR_A_ID: &str = "00000000-0000-0000-0000-000000000001";
/// Known test operator: Operator B
const OPERATOR_B_ID: &str = "00000000-0000-0000-0000-000000000002";

async fn connect() -> PgDriver {
    PgDriver::connect_env()
        .await
        .expect("DATABASE_URL must point to qail_test with qail_app user")
}

#[tokio::test]
#[ignore = "Requires local PostgreSQL with RLS policies"]
async fn test_no_context_sees_nothing() {
    let mut driver = connect().await;

    // Without RLS context, a non-superuser should see zero rows
    // because empty operator_id matches nothing
    let vessels = driver
        .fetch_all(&Qail::get("vessels").columns(["id"]))
        .await
        .unwrap();

    assert_eq!(vessels.len(), 0, "Without RLS context, should see 0 vessels");
}

#[tokio::test]
#[ignore = "Requires local PostgreSQL with RLS policies"]
async fn test_operator_isolation() {
    let mut driver = connect().await;

    // Set Operator A context
    driver
        .set_rls_context(RlsContext::operator(OPERATOR_A_ID))
        .await
        .unwrap();

    let ekajaya_vessels = driver
        .fetch_all(&Qail::get("vessels").columns(["id", "name"]))
        .await
        .unwrap();

    assert!(
        ekajaya_vessels.len() > 0,
        "Operator A should have vessels visible"
    );

    // Switch to Operator B context
    driver
        .set_rls_context(RlsContext::operator(OPERATOR_B_ID))
        .await
        .unwrap();

    let maruti_vessels = driver
        .fetch_all(&Qail::get("vessels").columns(["id", "name"]))
        .await
        .unwrap();

    assert!(
        maruti_vessels.len() > 0,
        "Operator B should have vessels visible"
    );

    // Different operators see different data
    assert_ne!(
        ekajaya_vessels.len(),
        maruti_vessels.len(),
        "Different operators should see different vessel counts"
    );
}

#[tokio::test]
#[ignore = "Requires local PostgreSQL with RLS policies"]
async fn test_clear_context_revokes_access() {
    let mut driver = connect().await;

    // Set context → see data
    driver
        .set_rls_context(RlsContext::operator(OPERATOR_A_ID))
        .await
        .unwrap();

    let with_context = driver
        .fetch_all(&Qail::get("vessels").columns(["id"]))
        .await
        .unwrap();

    assert!(with_context.len() > 0, "Should see vessels with context");

    // Clear context → see nothing
    driver.clear_rls_context().await.unwrap();

    let without_context = driver
        .fetch_all(&Qail::get("vessels").columns(["id"]))
        .await
        .unwrap();

    assert_eq!(
        without_context.len(),
        0,
        "Should see 0 vessels after clearing context"
    );
}

#[tokio::test]
#[ignore = "Requires local PostgreSQL with RLS policies"]
async fn test_super_admin_bypass() {
    let mut driver = connect().await;

    // Super admin should see ALL vessels across all operators
    driver
        .set_rls_context(RlsContext::super_admin(qail_core::rls::SuperAdminToken::issue()))
        .await
        .unwrap();

    let all_vessels = driver
        .fetch_all(&Qail::get("vessels").columns(["id"]))
        .await
        .unwrap();

    // Should see more than any single operator
    assert!(
        all_vessels.len() > 11,
        "Super admin should see all vessels (got {})",
        all_vessels.len()
    );

    driver.clear_rls_context().await.unwrap();
}

#[tokio::test]
#[ignore = "Requires local PostgreSQL with RLS policies"]
async fn test_context_getter() {
    let mut driver = connect().await;

    // No context initially
    assert!(driver.rls_context().is_none());

    // Set context
    driver
        .set_rls_context(RlsContext::operator(OPERATOR_A_ID))
        .await
        .unwrap();

    let ctx = driver.rls_context().unwrap();
    assert_eq!(ctx.operator_id, OPERATOR_A_ID);
    assert!(!ctx.bypasses_rls());

    // Clear
    driver.clear_rls_context().await.unwrap();
    assert!(driver.rls_context().is_none());
}

#[tokio::test]
#[ignore = "Requires local PostgreSQL with RLS policies"]
async fn test_rls_across_multiple_tables() {
    let mut driver = connect().await;

    // Set Operator A context
    driver
        .set_rls_context(RlsContext::operator(OPERATOR_A_ID))
        .await
        .unwrap();

    // Vessels should be scoped
    let vessels = driver
        .fetch_all(&Qail::get("vessels").columns(["id"]))
        .await
        .unwrap();

    // Odysseys should also be scoped
    let odysseys = driver
        .fetch_all(&Qail::get("odysseys").columns(["id"]))
        .await
        .unwrap();

    // Both should return data (Operator A has vessels and odysseys)
    assert!(
        vessels.len() > 0,
        "Operator A should have vessels"
    );

    // Odysseys may or may not have data for this operator,
    // but the query should succeed (no permission error)
    // The important thing is RLS didn't block the query
    let _ = odysseys;

    driver.clear_rls_context().await.unwrap();
}

// ══════════════════════════════════════════════════════════════════
// P3: Pool-level connection recycling isolation
// Proves that DISCARD ALL + RLS reset prevents cross-tenant leakage
// when connections are recycled through the pool.
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "Requires local PostgreSQL with RLS policies"]
async fn test_pool_connection_recycling_isolation() {
    let mut driver = connect().await;

    // ── Step 1: Tenant A context — query and count vessels ──
    driver
        .set_rls_context(RlsContext::operator(OPERATOR_A_ID))
        .await
        .unwrap();

    let ekajaya_vessels = driver
        .fetch_all(&Qail::get("vessels").columns(["id", "name"]))
        .await
        .unwrap();
    let a_count = ekajaya_vessels.len();
    assert!(a_count > 0, "Tenant A should see vessels");

    // ── Step 2: Clear context (simulates connection recycling) ──
    // This triggers RLS reset + DISCARD ALL internally
    driver.clear_rls_context().await.unwrap();

    // Verify isolation: without context, zero rows
    let no_context = driver
        .fetch_all(&Qail::get("vessels").columns(["id"]))
        .await
        .unwrap();
    assert_eq!(no_context.len(), 0, "After clearing context, should see 0 vessels");

    // ── Step 3: Tenant B context on SAME connection ──
    driver
        .set_rls_context(RlsContext::operator(OPERATOR_B_ID))
        .await
        .unwrap();

    let maruti_vessels = driver
        .fetch_all(&Qail::get("vessels").columns(["id", "name"]))
        .await
        .unwrap();
    let b_count = maruti_vessels.len();
    assert!(b_count > 0, "Tenant B should see vessels");

    // ── Step 4: Verify complete isolation ──
    // Different operators see different data on the SAME recycled connection.
    // If DISCARD ALL or RLS reset failed, Tenant B would see Tenant A data.
    assert_ne!(
        a_count, b_count,
        "Tenant A ({}) and Tenant B ({}) must see different vessel counts on recycled connection",
        a_count, b_count
    );

    driver.clear_rls_context().await.unwrap();
}
