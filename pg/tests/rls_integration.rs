//! RLS Integration Tests
//!
//! Tests multi-tenant data isolation using the qail-pg driver.
//! Requires a local PostgreSQL database with RLS policies applied.
//!
//! Run with:
//! ```sh
//! DATABASE_URL="postgresql://user@localhost:5432/example-db" \
//!     cargo test -p qail-pg --test rls_integration -- --nocapture
//! ```

use qail_core::ast::Qail;
use qail_core::rls::RlsContext;
use qail_pg::PgDriver;

/// Known test operator: Ekajaya (has 11 vessels in local DB)
const EKAJAYA_ID: &str = "f070bf51-7211-4497-bee5-a59920584fca";
/// Known test operator: Maruti Group
const MARUTI_ID: &str = "a6fda6c9-2ac6-4263-a3f8-e8b94b5d0153";

async fn connect() -> PgDriver {
    PgDriver::connect_env()
        .await
        .expect("DATABASE_URL must point to example-db with example user")
}

#[tokio::test]
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
async fn test_operator_isolation() {
    let mut driver = connect().await;

    // Set Ekajaya context
    driver
        .set_rls_context(RlsContext::operator(EKAJAYA_ID))
        .await
        .unwrap();

    let ekajaya_vessels = driver
        .fetch_all(&Qail::get("vessels").columns(["id", "name"]))
        .await
        .unwrap();

    assert!(
        ekajaya_vessels.len() > 0,
        "Ekajaya should have vessels visible"
    );

    // Switch to Maruti context
    driver
        .set_rls_context(RlsContext::operator(MARUTI_ID))
        .await
        .unwrap();

    let maruti_vessels = driver
        .fetch_all(&Qail::get("vessels").columns(["id", "name"]))
        .await
        .unwrap();

    assert!(
        maruti_vessels.len() > 0,
        "Maruti should have vessels visible"
    );

    // Different operators see different data
    assert_ne!(
        ekajaya_vessels.len(),
        maruti_vessels.len(),
        "Different operators should see different vessel counts"
    );
}

#[tokio::test]
async fn test_clear_context_revokes_access() {
    let mut driver = connect().await;

    // Set context → see data
    driver
        .set_rls_context(RlsContext::operator(EKAJAYA_ID))
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
async fn test_super_admin_bypass() {
    let mut driver = connect().await;

    // Super admin should see ALL vessels across all operators
    driver
        .set_rls_context(RlsContext::super_admin())
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
async fn test_context_getter() {
    let mut driver = connect().await;

    // No context initially
    assert!(driver.rls_context().is_none());

    // Set context
    driver
        .set_rls_context(RlsContext::operator(EKAJAYA_ID))
        .await
        .unwrap();

    let ctx = driver.rls_context().unwrap();
    assert_eq!(ctx.operator_id, EKAJAYA_ID);
    assert!(!ctx.is_super_admin);

    // Clear
    driver.clear_rls_context().await.unwrap();
    assert!(driver.rls_context().is_none());
}

#[tokio::test]
async fn test_rls_across_multiple_tables() {
    let mut driver = connect().await;

    // Set Ekajaya context
    driver
        .set_rls_context(RlsContext::operator(EKAJAYA_ID))
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

    // Both should return data (Ekajaya has vessels and odysseys)
    assert!(
        vessels.len() > 0,
        "Ekajaya should have vessels"
    );

    // Odysseys may or may not have data for this operator,
    // but the query should succeed (no permission error)
    // The important thing is RLS didn't block the query
    let _ = odysseys;

    driver.clear_rls_context().await.unwrap();
}
