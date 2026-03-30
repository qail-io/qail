//! EXPLAIN Pre-check Integration Test
//!
//! Tests the EXPLAIN cost estimation against a live PostgreSQL database,
//! using the same PgDriver pattern as rls_integration.rs.
//!
//! Run with:
//! ```sh
//! DATABASE_URL="postgresql://qail_user@localhost:5432/qail_test" \
//!     cargo test -p qail-pg --test explain_integration -- --nocapture --ignored
//! ```
//!
//! Note: Uses `qail_user` (superuser) because EXPLAIN requires SELECT permission
//! on the target tables. The `qail_app` role is an operator-level app user
//! with restricted grants.
#![cfg(feature = "legacy-raw-examples")]

use qail_core::ast::{JoinKind, Qail};
use qail_core::rls::RlsContext;
use qail_core::transpiler::ToSql;
use qail_pg::PgDriver;
use qail_pg::explain::{ExplainCache, ExplainConfig, ExplainMode, check_estimate};

use std::time::Duration;

/// Known test operator: Operator A (has vessels in local DB)
const OPERATOR_A_ID: &str = "00000000-0000-0000-0000-000000000001";

async fn connect() -> PgDriver {
    PgDriver::connect_env()
        .await
        .expect("DATABASE_URL must point to qail_test with qail_user (superuser)")
}

/// Run EXPLAIN (FORMAT JSON) via driver's AST-native explain API.
async fn run_explain(
    driver: &mut PgDriver,
    cmd: &Qail,
) -> Option<qail_pg::explain::ExplainEstimate> {
    driver.explain_estimate(cmd).await.unwrap()
}

#[tokio::test]
#[ignore = "Requires local PostgreSQL with live data"]
async fn test_explain_simple_select() {
    let mut driver = connect().await;

    // Set operator context for RLS
    driver
        .set_rls_context(RlsContext::tenant(OPERATOR_A_ID))
        .await
        .unwrap();

    // Simple SELECT — should be cheap
    let cmd = Qail::get("vessels").columns(["id", "name"]).limit(10);
    println!("SQL: {}", cmd.to_sql());

    let est = run_explain(&mut driver, &cmd)
        .await
        .expect("Should parse EXPLAIN output");

    println!("✅ Simple SELECT vessels LIMIT 10");
    println!("   Cost: {:.2}, Rows: {}", est.total_cost, est.plan_rows);

    // Should be well under default thresholds
    let config = ExplainConfig::default();
    let decision = check_estimate(&est, &config);
    assert!(
        !decision.is_rejected(),
        "Simple limited query should be ALLOWED"
    );
    println!("   Decision: ALLOW ✓");
}

#[tokio::test]
#[ignore = "Requires local PostgreSQL with live data"]
async fn test_explain_multi_join() {
    let mut driver = connect().await;

    // Operator A context
    driver
        .set_rls_context(RlsContext::tenant(OPERATOR_A_ID))
        .await
        .unwrap();

    // Multi-table join: orders + agents (2 JOINs)
    let cmd = Qail::get("orders")
        .join(JoinKind::Left, "agents", "orders.agent_id", "agents.id")
        .join(
            JoinKind::Left,
            "destinations",
            "orders.id",
            "destinations.id",
        );

    println!("SQL: {}", cmd.to_sql());

    let est = run_explain(&mut driver, &cmd)
        .await
        .expect("Should parse EXPLAIN output");

    println!("✅ Multi-join: orders + agents + destinations");
    println!("   Cost: {:.2}, Rows: {}", est.total_cost, est.plan_rows);

    // With very strict thresholds → should be rejected
    let strict_config = ExplainConfig {
        mode: ExplainMode::Enforce,
        depth_threshold: 1,
        max_total_cost: 1.0, // Extremely low
        max_plan_rows: 1,    // Extremely low
        cache_ttl: Duration::from_secs(60),
    };
    let decision = check_estimate(&est, &strict_config);
    assert!(
        decision.is_rejected(),
        "Multi-join with strict=1.0 should be REJECTED"
    );
    println!("   Strict(max_cost=1.0) → REJECTED ✓");
    if let Some(msg) = decision.rejection_message() {
        println!("   Message: {}", msg);
    }

    // With default thresholds — show the result
    let default_config = ExplainConfig::default();
    let default_decision = check_estimate(&est, &default_config);
    println!(
        "   Default(max_cost={:.0}) → {}",
        default_config.max_total_cost,
        if default_decision.is_rejected() {
            "REJECTED"
        } else {
            "ALLOW"
        }
    );
}

#[tokio::test]
#[ignore = "Requires local PostgreSQL with live data"]
async fn test_explain_cache_roundtrip() {
    let mut driver = connect().await;

    driver
        .set_rls_context(RlsContext::tenant(OPERATOR_A_ID))
        .await
        .unwrap();

    let cache = ExplainCache::new(Duration::from_secs(300));
    let cmd = Qail::get("vessels").columns(["id"]).limit(5);

    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let sql = cmd.to_sql();
    let mut hasher = DefaultHasher::new();
    sql.hash(&mut hasher);
    let shape_hash = hasher.finish();

    // Cache miss
    assert!(
        cache.get(shape_hash, None).is_none(),
        "Should be cache miss initially"
    );

    // Run EXPLAIN and cache
    let est = run_explain(&mut driver, &cmd).await.unwrap();
    cache.insert(shape_hash, est.clone());

    // Cache hit
    let cached = cache.get(shape_hash, None).unwrap();
    assert!((cached.total_cost - est.total_cost).abs() < 0.01);
    assert_eq!(cached.plan_rows, est.plan_rows);

    println!("✅ Cache roundtrip verified");
    println!("   MISS → EXPLAIN → INSERT → HIT");
    println!(
        "   cost={:.2}, rows={}",
        cached.total_cost, cached.plan_rows
    );
}

#[tokio::test]
#[ignore = "Requires local PostgreSQL with live data"]
async fn test_explain_full_table_scan() {
    let mut driver = connect().await;

    // Operator context — RLS will filter, but no LIMIT = seq scan
    driver
        .set_rls_context(RlsContext::tenant(OPERATOR_A_ID))
        .await
        .unwrap();

    // SELECT * FROM orders — no filters, no limit
    let cmd = Qail::get("orders");
    println!("SQL: {}", cmd.to_sql());

    let est = run_explain(&mut driver, &cmd)
        .await
        .expect("Should parse EXPLAIN output");

    println!("✅ Full scan: SELECT * FROM orders (no filters, no limit)");
    println!("   Cost: {:.2}, Rows: {}", est.total_cost, est.plan_rows);

    // With tight thresholds, full scan should be rejected
    let config = ExplainConfig {
        mode: ExplainMode::Enforce,
        depth_threshold: 0,
        max_total_cost: 5.0, // Very tight
        max_plan_rows: 5,    // Very tight
        cache_ttl: Duration::from_secs(60),
    };
    let decision = check_estimate(&est, &config);
    println!(
        "   Tight threshold(5.0 cost, 5 rows) → {}",
        if decision.is_rejected() {
            "REJECTED ✓"
        } else {
            "ALLOW"
        }
    );
    if let Some(msg) = decision.rejection_message() {
        println!("   Message: {}", msg);
    }
}
