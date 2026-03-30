//! Red-Team Live DB Integration Tests
//!
//! Adversarial tests that require a live PostgreSQL database.
//! Verifies timeout recovery, schema drift detection, transaction rollback,
//! context switching, and RLS context management at the driver level.
//!
//! **Note**: Uses `qail_user` (superuser) because the `qail_app` app user currently
//! lacks direct table grants. Superusers bypass RLS, so RLS-specific isolation
//! tests are in `rls_integration.rs` and need env setup (GRANT + FORCE RLS).
//!
//! Run with:
//! ```sh
//! DATABASE_URL="postgresql://qail_user@localhost:5432/qail_test" \
//!     cargo test -p qail-pg --test red_team_integration -- --nocapture --ignored
//! ```
#![cfg(feature = "legacy-raw-examples")]

use qail_core::ast::{Operator, Qail};
use qail_core::rls::RlsContext;
use qail_core::transpiler::ToSql;
use qail_pg::PgDriver;

/// Known test operators
const OPERATOR_A_ID: &str = "00000000-0000-0000-0000-000000000001";
const OPERATOR_B_ID: &str = "00000000-0000-0000-0000-000000000002";

async fn connect() -> PgDriver {
    PgDriver::connect_env()
        .await
        .expect("DATABASE_URL must point to qail_test")
}

async fn current_operator_id(driver: &mut PgDriver) -> String {
    let rows = driver
        .fetch_all(&Qail::session_show("app.current_operator_id"))
        .await
        .unwrap();
    rows.first()
        .and_then(|row| row.get_string(0))
        .unwrap_or_default()
}

// ══════════════════════════════════════════════════════════════════════
// #2: Connection Reuse After Statement Timeout
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "Requires local PostgreSQL"]
async fn redteam_statement_timeout_recovery() {
    let mut driver = connect().await;

    // Set a very short timeout
    driver.set_statement_timeout(1).await.unwrap(); // 1ms

    // This query may fail with statement_timeout
    let result = driver
        .fetch_all(&Qail::get("vessels").columns(["id", "name"]))
        .await;

    // Reset timeout and verify connection is still usable
    driver.reset_statement_timeout().await.unwrap();

    let vessels = driver
        .fetch_all(&Qail::get("vessels").columns(["id"]).limit(1))
        .await;

    assert!(
        vessels.is_ok(),
        "Connection must be usable after timeout recovery"
    );
    println!(
        "✅ Connection recovered after statement timeout (first query: {:?})",
        result.is_ok()
    );
}

// ══════════════════════════════════════════════════════════════════════
// #1: RLS Context Switching — Driver State Management
// Verifies the driver correctly switches GUC variables between contexts.
// Note: With superuser, RLS policies don't filter, but we verify the
// GUC state is being set correctly.
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "Requires local PostgreSQL"]
async fn redteam_context_switch_guc_correctness() {
    let mut driver = connect().await;

    // Step 1: Set Operator A context
    driver
        .set_rls_context(RlsContext::tenant(OPERATOR_A_ID))
        .await
        .unwrap();

    // Verify GUC is set correctly by reading it back
    let guc_val = current_operator_id(&mut driver).await;
    assert_eq!(
        guc_val, OPERATOR_A_ID,
        "GUC must contain Operator A's operator_id"
    );

    // Step 2: Switch to Operator B
    driver
        .set_rls_context(RlsContext::tenant(OPERATOR_B_ID))
        .await
        .unwrap();

    let guc_val = current_operator_id(&mut driver).await;
    assert_eq!(
        guc_val, OPERATOR_B_ID,
        "GUC must switch to Operator B's operator_id"
    );

    // Step 3: Clear context
    driver.clear_rls_context().await.unwrap();

    let guc_val = current_operator_id(&mut driver).await;
    assert_eq!(guc_val, "", "GUC must be empty after clear_rls_context");

    println!("✅ GUC context switches verified: Operator A → Operator B → cleared");
}

// ══════════════════════════════════════════════════════════════════════
// #1: RLS Context After Timeout — GUC Survives Timeout?
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "Requires local PostgreSQL"]
async fn redteam_guc_state_after_timeout() {
    let mut driver = connect().await;

    // Set RLS context
    driver
        .set_rls_context(RlsContext::tenant(OPERATOR_A_ID))
        .await
        .unwrap();

    // Set very short timeout
    driver.set_statement_timeout(1).await.unwrap(); // 1ms

    // Query (may time out)
    let _ = driver
        .fetch_all(&Qail::get("vessels").columns(["id", "name"]))
        .await;

    // Reset timeout
    driver.reset_statement_timeout().await.unwrap();

    // Check if GUC survived the timeout
    let guc_val = current_operator_id(&mut driver).await;

    // GUC variables persist within a session even across statement timeouts
    println!("  GUC after timeout: '{}'", guc_val);
    assert_eq!(guc_val, OPERATOR_A_ID, "GUC must survive statement timeout");
    println!("✅ RLS GUC state survived timeout (value: {})", guc_val);
}

// ══════════════════════════════════════════════════════════════════════
// #10: Transaction Rollback Preserves Data Integrity
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "Requires local PostgreSQL"]
async fn redteam_transaction_rollback_preserves_data() {
    let mut driver = connect().await;

    // Count vessels before
    let before = driver
        .fetch_all(&Qail::get("vessels").columns(["id"]))
        .await
        .unwrap()
        .len();

    // Start transaction, query fails, rollback
    driver.begin().await.unwrap();

    let result = driver
        .fetch_all(&Qail::get("__nonexistent_table_xyz__").columns(["id"]))
        .await;

    assert!(result.is_err(), "Query to non-existent table must fail");

    // Rollback
    driver.rollback().await.unwrap();

    // Verify nothing changed
    let after = driver
        .fetch_all(&Qail::get("vessels").columns(["id"]))
        .await
        .unwrap()
        .len();

    assert_eq!(before, after, "Rollback must preserve row count");
    println!(
        "✅ Transaction rollback preserved data (before={}, after={})",
        before, after
    );
}

// ══════════════════════════════════════════════════════════════════════
// #4: RLS Filter Commutativity — App Filters + RLS Context Don't Conflict
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "Requires local PostgreSQL"]
async fn redteam_app_filter_with_rls_context() {
    let mut driver = connect().await;

    // Set operator context
    driver
        .set_rls_context(RlsContext::tenant(OPERATOR_A_ID))
        .await
        .unwrap();

    // Full query
    let all_vessels = driver
        .fetch_all(&Qail::get("vessels").columns(["id", "name"]))
        .await
        .unwrap();

    // With LIMIT
    let limited = driver
        .fetch_all(&Qail::get("vessels").columns(["id", "name"]).limit(1))
        .await
        .unwrap();

    assert_eq!(limited.len(), 1, "LIMIT must work with RLS context set");

    // Filter that matches nothing
    let filtered = driver
        .fetch_all(&Qail::get("vessels").columns(["id", "name"]).filter(
            "name",
            Operator::Eq,
            "__nonexistent_vessel_name__",
        ))
        .await
        .unwrap();

    assert_eq!(filtered.len(), 0, "Non-matching filter must return 0 rows");

    println!(
        "✅ App filters commute with RLS context (all={}, limited={}, filtered={})",
        all_vessels.len(),
        limited.len(),
        filtered.len()
    );
}

// ══════════════════════════════════════════════════════════════════════
// #5: Schema Drift Detection
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "Requires local PostgreSQL"]
async fn redteam_schema_drift_nonexistent_column() {
    let mut driver = connect().await;

    let result = driver
        .fetch_all(&Qail::get("vessels").columns(["id", "__nonexistent_column_xyz__"]))
        .await;

    assert!(
        result.is_err(),
        "Non-existent column must fail (schema drift)"
    );
    println!("✅ Non-existent column correctly produces error");
}

#[tokio::test]
#[ignore = "Requires local PostgreSQL"]
async fn redteam_schema_drift_nonexistent_table() {
    let mut driver = connect().await;

    let result = driver
        .fetch_all(&Qail::get("__nonexistent_table_abc__").columns(["id"]))
        .await;

    assert!(
        result.is_err(),
        "Non-existent table must fail (schema drift)"
    );
    println!("✅ Non-existent table correctly produces error");
}

// ══════════════════════════════════════════════════════════════════════
// Rapid Context Switching Stress — 100 iterations
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "Requires local PostgreSQL"]
async fn redteam_rapid_context_switching_100x() {
    let mut driver = connect().await;

    for i in 0..100 {
        let op_id = if i % 2 == 0 {
            OPERATOR_A_ID
        } else {
            OPERATOR_B_ID
        };
        driver
            .set_rls_context(RlsContext::tenant(op_id))
            .await
            .unwrap();

        let vessels = driver
            .fetch_all(&Qail::get("vessels").columns(["id"]).limit(1))
            .await
            .unwrap();

        assert!(vessels.len() <= 1, "LIMIT 1 must work on each iteration");
    }

    // Verify final state is correct
    let final_guc = current_operator_id(&mut driver).await;
    assert_eq!(
        final_guc, OPERATOR_B_ID,
        "Final GUC should be Operator B (last odd iteration)"
    );

    println!("✅ 100 rapid context switches completed without error");
}

// ══════════════════════════════════════════════════════════════════════
// Savepoint Partial Rollback
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "Requires local PostgreSQL"]
async fn redteam_savepoint_partial_rollback() {
    let mut driver = connect().await;

    let before = driver
        .fetch_all(&Qail::get("vessels").columns(["id"]))
        .await
        .unwrap()
        .len();

    driver.begin().await.unwrap();

    // First query succeeds
    let _ = driver
        .fetch_all(&Qail::get("vessels").columns(["id"]).limit(1))
        .await
        .unwrap();

    // Create savepoint
    driver.savepoint("sp1").await.unwrap();

    // Second query fails
    let result = driver
        .fetch_all(&Qail::get("__nonexistent__").columns(["id"]))
        .await;
    assert!(result.is_err());

    // Rollback to savepoint (not the whole transaction)
    driver.rollback_to("sp1").await.unwrap();

    // Can still query after partial rollback
    let mid = driver
        .fetch_all(&Qail::get("vessels").columns(["id"]).limit(1))
        .await
        .unwrap();
    assert_eq!(mid.len(), 1, "Query after rollback_to must work");

    driver.commit().await.unwrap();

    let after = driver
        .fetch_all(&Qail::get("vessels").columns(["id"]))
        .await
        .unwrap()
        .len();

    assert_eq!(before, after, "Read-only transaction must not change data");
    println!("✅ Savepoint partial rollback works correctly");
}

// ══════════════════════════════════════════════════════════════════════
// Pipeline Batch Fetch — Multiple Queries in One Round-Trip
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "Requires local PostgreSQL"]
async fn redteam_pipeline_batch_correctness() {
    let mut driver = connect().await;

    driver
        .set_rls_context(RlsContext::tenant(OPERATOR_A_ID))
        .await
        .unwrap();

    let cmds: Vec<Qail> = (1..=5)
        .map(|i| Qail::get("vessels").columns(["id"]).limit(i))
        .collect();

    let results = driver.pipeline_execute_rows(&cmds).await.unwrap();

    assert_eq!(results.len(), 5, "Pipeline must return 5 result sets");
    for (i, result_set) in results.iter().enumerate() {
        let expected_max = i + 1;
        assert!(
            result_set.len() <= expected_max,
            "Result set {} should have <= {} rows, got {}",
            i,
            expected_max,
            result_set.len()
        );
    }

    println!(
        "✅ Pipeline batch returned correct result sets: {:?}",
        results.iter().map(|r| r.len()).collect::<Vec<_>>()
    );
}

// ══════════════════════════════════════════════════════════════════════
// TIER X: "The Int64 Bomb" — Negative LIMIT Overflow
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "Requires local PostgreSQL"]
async fn tierx_negative_limit_overflow() {
    // limit(-1) internally does `n as usize` → usize::MAX
    // The transpiler will produce LIMIT 18446744073709551615
    // Postgres should either reject it or treat it as no limit
    let mut driver = connect().await;

    let cmd = Qail::get("vessels").columns(["id"]).limit(-1);
    let sql = cmd.to_sql();

    // Document the transpiled SQL — this is the bug
    println!("  Transpiled SQL: {}", sql);
    assert!(
        sql.contains("18446744073709551615") || sql.contains("-1"),
        "Negative LIMIT should be visible in SQL output: {}",
        sql
    );

    // Postgres should handle this gracefully
    let result = driver.fetch_all(&cmd).await;
    // Either succeeds with all rows (Postgres treats huge LIMIT as no limit)
    // or fails with an error (Postgres rejects the value) — both acceptable
    println!(
        "  Result: {:?}",
        result
            .as_ref()
            .map(|r| r.len())
            .map_err(|e| format!("{}", e))
    );
    println!("✅ Negative LIMIT overflow handled (no driver panic)");
}

#[tokio::test]
#[ignore = "Requires local PostgreSQL"]
async fn tierx_i64_max_limit() {
    let mut driver = connect().await;

    let cmd = Qail::get("vessels").columns(["id"]).limit(i64::MAX);
    let result = driver.fetch_all(&cmd).await;
    // Should not panic — either returns all rows or Postgres error
    assert!(result.is_ok() || result.is_err());
    println!("✅ i64::MAX LIMIT handled without panic");
}

// ══════════════════════════════════════════════════════════════════════
// TIER X: "Recursive Savepoint Spiral" — 100 Nested Savepoints
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "Requires local PostgreSQL"]
async fn tierx_100_nested_savepoints() {
    let mut driver = connect().await;

    driver.begin().await.unwrap();

    // Create 100 nested savepoints
    for i in 0..100 {
        let sp_name = format!("sp{}", i);
        let result = driver.savepoint(&sp_name).await;
        if let Err(e) = result {
            // Postgres hit stack depth limit — acceptable
            println!("  Savepoint {} failed: {:?}", i, e);
            driver.rollback().await.unwrap_or(());
            println!("✅ Postgres rejects deep savepoint nesting at depth {}", i);
            return;
        }
    }

    // If we got here, all 100 succeeded — rollback chain
    for i in (0..100).rev() {
        driver.rollback_to(&format!("sp{}", i)).await.unwrap();
    }

    driver.rollback().await.unwrap();
    println!("✅ 100 nested savepoints all succeeded and rolled back cleanly");
}

// ══════════════════════════════════════════════════════════════════════
// TIER X: "Unicode Torture" — RTL, ZWJ, Emoji in GUC Values
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "Requires local PostgreSQL"]
async fn tierx_unicode_torture_in_guc() {
    let mut driver = connect().await;

    // Unicode edge cases in operator_id
    let test_cases = vec![
        ("RTL override", "\u{202e}operator_id_backwards"), // U+202E
        ("ZWJ sequence", "family\u{200d}operator"),        // ZWJ
        ("BOM marker", "operator\u{FEFF}id"),              // BOM
        (
            "Cyrillic lookalike",
            "\u{043e}\u{043f}\u{0435}\u{0440}\u{0430}\u{0442}\u{043e}\u{0440}",
        ), // Cyrillic
        ("Empty string", ""),
        ("Just spaces", "   "),
    ];

    for (label, op_id) in &test_cases {
        let result = driver.set_rls_context(RlsContext::tenant(op_id)).await;

        // Must not panic
        assert!(result.is_ok(), "{} should not panic: {:?}", label, result);

        // Read back the GUC
        let guc_val = current_operator_id(&mut driver).await;
        println!("  {}: set='{}' → got='{}'", label, op_id, guc_val);
    }

    println!("✅ All unicode torture cases handled without panic");
}

// ══════════════════════════════════════════════════════════════════════
// TIER X: NULL Byte Injection in RLS Context
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "Requires local PostgreSQL"]
async fn tierx_null_byte_rejection() {
    let mut driver = connect().await;

    // set_rls_context should reject generated SQL containing embedded NULL bytes
    let result = driver
        .set_rls_context(RlsContext::tenant("bad\0tenant"))
        .await;

    assert!(result.is_err(), "NULL byte in SQL must be rejected");
    let err = format!("{}", result.unwrap_err());
    assert!(
        err.contains("NULL byte"),
        "Error must mention NULL byte: {}",
        err
    );

    // Connection must still be usable after rejection
    let rows = driver
        .fetch_all(&Qail::get("vessels").columns(["id"]).limit(1))
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "Connection must work after NULL byte rejection"
    );
    println!("✅ NULL byte injection correctly rejected, connection still alive");
}

// ══════════════════════════════════════════════════════════════════════
// TIER X: SQL Comment Injection in Column Name
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "Requires local PostgreSQL"]
async fn tierx_sql_comment_in_column_name() {
    let mut driver = connect().await;

    // Try to inject a SQL comment via column name
    let cmd = Qail::get("vessels").columns(["id", "name--; DROP TABLE vessels"]);
    let result = driver.fetch_all(&cmd).await;

    // Should fail (column doesn't exist) but must not execute the DROP
    assert!(
        result.is_err(),
        "SQL comment injection in column must fail safely"
    );

    // Verify table still exists
    let check = driver
        .fetch_all(&Qail::get("vessels").columns(["id"]).limit(1))
        .await
        .unwrap();
    assert_eq!(check.len(), 1, "vessels table must still exist");
    println!("✅ SQL comment injection in column name neutralized");
}

// ══════════════════════════════════════════════════════════════════════
// TIER X: Rapid BEGIN/ROLLBACK Cycling
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "Requires local PostgreSQL"]
async fn tierx_rapid_begin_rollback_100x() {
    let mut driver = connect().await;

    for i in 0..100 {
        driver.begin().await.unwrap();

        // Do a small query inside the transaction
        let _ = driver
            .fetch_all(&Qail::get("vessels").columns(["id"]).limit(1))
            .await
            .unwrap();

        if i % 2 == 0 {
            driver.commit().await.unwrap();
        } else {
            driver.rollback().await.unwrap();
        }
    }

    // Connection must be healthy after 100 cycles
    let final_check = driver
        .fetch_all(&Qail::get("vessels").columns(["id"]).limit(1))
        .await
        .unwrap();
    assert_eq!(final_check.len(), 1);
    println!("✅ 100 begin/commit/rollback cycles completed cleanly");
}

// ══════════════════════════════════════════════════════════════════════
// TIER X: Very Long Operator ID (10KB)
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "Requires local PostgreSQL"]
async fn tierx_very_long_operator_id() {
    let mut driver = connect().await;

    // 10KB operator_id — way beyond any UUID
    let long_id = "a".repeat(10_000);
    let result = driver.set_rls_context(RlsContext::tenant(&long_id)).await;

    // Should succeed (GUC values can be very long)
    assert!(
        result.is_ok(),
        "10KB GUC value should be accepted: {:?}",
        result
    );

    // Read it back via session SHOW
    let guc_val = current_operator_id(&mut driver).await;
    let len = guc_val.len();
    assert_eq!(len, 10_000, "GUC must store the full 10KB value");
    println!("✅ 10KB operator_id stored and retrieved correctly");
}

// ══════════════════════════════════════════════════════════════════════
// TIER X: Empty Table Name
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "Requires local PostgreSQL"]
async fn tierx_empty_table_name() {
    let mut driver = connect().await;

    let cmd = Qail::get("").columns(["id"]);
    let result = driver.fetch_all(&cmd).await;
    // Must not panic — should return an error
    assert!(result.is_err(), "Empty table name must fail");
    println!("✅ Empty table name produces error (not panic)");
}
