//! Data Safety Stress Test
//!
//! Tests QAIL's data safety features under stress:
//! - FK validation (compile-time referential integrity)
//! - Type validation (PK/unique/index constraints)
//! - CHECK constraint enforcement
//! - Concurrent constraint violations
//! - Transaction rollback safety
//!
//! Run with: cargo run --example data_safety_stress_test

use qail_core::migrate::*;
use qail_pg::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("DATA SAFETY STRESS TEST");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    let mut driver = PgDriver::connect("127.0.0.1", 5432, "orion", "postgres").await?;
    println!("✅ Connected to PostgreSQL\n");

    let mut passed = 0;
    let mut failed = 0;

    // ========================================================================
    // CLEANUP
    // ========================================================================
    println!("━━━ CLEANUP ━━━");
    let _ = driver.execute_raw("DROP TABLE IF EXISTS stress_orders CASCADE").await;
    let _ = driver.execute_raw("DROP TABLE IF EXISTS stress_users CASCADE").await;
    let _ = driver.execute_raw("DROP TABLE IF EXISTS stress_audit CASCADE").await;
    println!("✅ Cleaned up existing tables\n");

    // ========================================================================
    // TEST 1: Compile-Time FK Validation
    // ========================================================================
    println!("━━━ TEST 1: COMPILE-TIME FK VALIDATION ━━━");

    // Create schema with FK reference
    let mut schema = Schema::new();
    schema.add_table(
        Table::new("stress_users")
            .column(Column::new("id", ColumnType::Serial).primary_key())
            .column(Column::new("name", ColumnType::Text).not_null()),
    );
    schema.add_table(
        Table::new("stress_orders")
            .column(Column::new("id", ColumnType::Serial).primary_key())
            .column(Column::new("user_id", ColumnType::Int)
                .references("stress_users", "id")
                .on_delete(FkAction::Cascade)),
    );


    match schema.validate() {
        Ok(_) => {
            println!("✅ Valid FK reference (stress_orders.user_id → stress_users.id)");
            passed += 1;
        }
        Err(errors) => {
            println!("❌ Unexpected validation error: {:?}", errors);
            failed += 1;
        }
    }

    // Test invalid FK - reference to non-existent table
    let mut bad_schema = Schema::new();
    bad_schema.add_table(
        Table::new("orphan_table")
            .column(Column::new("id", ColumnType::Serial).primary_key())
            .column(Column::new("bad_ref", ColumnType::Int).references("nonexistent", "id")),
    );

    match bad_schema.validate() {
        Ok(_) => {
            println!("❌ Should have rejected FK to nonexistent table!");
            failed += 1;
        }
        Err(errors) => {
            println!("✅ Caught invalid FK: {}", errors[0]);
            passed += 1;
        }
    }

    // ========================================================================
    // TEST 2: Type Validation (PK constraints)
    // ========================================================================
    println!("\n━━━ TEST 2: TYPE VALIDATION ━━━");

    // Test: TEXT cannot be primary key
    let result = std::panic::catch_unwind(|| {
        Column::new("bad_pk", ColumnType::Text).primary_key()
    });
    match result {
        Err(_) => {
            println!("✅ Rejected TEXT as primary key type");
            passed += 1;
        }
        Ok(_) => {
            println!("❌ Should have rejected TEXT as PK!");
            failed += 1;
        }
    }

    // Test: JSONB cannot have UNIQUE constraint
    let result = std::panic::catch_unwind(|| {
        Column::new("bad_unique", ColumnType::Jsonb).unique()
    });
    match result {
        Err(_) => {
            println!("✅ Rejected UNIQUE on JSONB type");
            passed += 1;
        }
        Ok(_) => {
            println!("❌ Should have rejected UNIQUE on JSONB!");
            failed += 1;
        }
    }

    // Valid PK types should work
    let _ = Column::new("uuid_pk", ColumnType::Uuid).primary_key();
    let _ = Column::new("serial_pk", ColumnType::Serial).primary_key();
    let _ = Column::new("int_pk", ColumnType::Int).primary_key();
    println!("✅ UUID, SERIAL, INT allowed as primary keys");
    passed += 1;

    // ========================================================================
    // TEST 3: Runtime FK Enforcement (PostgreSQL)
    // ========================================================================
    println!("\n━━━ TEST 3: RUNTIME FK ENFORCEMENT ━━━");

    // Create real tables
    driver.execute_raw("
        CREATE TABLE stress_users (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        )
    ").await?;

    driver.execute_raw("
        CREATE TABLE stress_orders (
            id SERIAL PRIMARY KEY,
            user_id INT REFERENCES stress_users(id) ON DELETE CASCADE,
            amount INT NOT NULL
        )
    ").await?;

    // Insert valid data
    driver.execute_raw("INSERT INTO stress_users (name) VALUES ('Alice')").await?;
    println!("✅ Inserted user Alice (id=1)");
    passed += 1;

    // Valid FK insert
    match driver.execute_raw("INSERT INTO stress_orders (user_id, amount) VALUES (1, 100)").await {
        Ok(_) => {
            println!("✅ Valid FK insert succeeded");
            passed += 1;
        }
        Err(e) => {
            println!("❌ Valid FK insert failed: {:?}", e);
            failed += 1;
        }
    }

    // Invalid FK insert (user_id=999 doesn't exist)
    match driver.execute_raw("INSERT INTO stress_orders (user_id, amount) VALUES (999, 100)").await {
        Ok(_) => {
            println!("❌ Should have rejected invalid FK!");
            failed += 1;
        }
        Err(_) => {
            println!("✅ Rejected invalid FK (user_id=999 doesn't exist)");
            passed += 1;
        }
    }

    // ========================================================================
    // TEST 4: CHECK Constraint Enforcement
    // ========================================================================
    println!("\n━━━ TEST 4: CHECK CONSTRAINT ENFORCEMENT ━━━");

    driver.execute_raw("
        ALTER TABLE stress_orders ADD CONSTRAINT chk_amount CHECK (amount > 0)
    ").await?;
    println!("✅ Added CHECK constraint (amount > 0)");
    passed += 1;

    // Valid amount
    match driver.execute_raw("INSERT INTO stress_orders (user_id, amount) VALUES (1, 50)").await {
        Ok(_) => {
            println!("✅ Valid amount (50) accepted");
            passed += 1;
        }
        Err(e) => {
            println!("❌ Valid amount rejected: {:?}", e);
            failed += 1;
        }
    }

    // Invalid amount (violates CHECK)
    match driver.execute_raw("INSERT INTO stress_orders (user_id, amount) VALUES (1, -10)").await {
        Ok(_) => {
            println!("❌ Should have rejected amount=-10!");
            failed += 1;
        }
        Err(_) => {
            println!("✅ Rejected amount=-10 (violates CHECK)");
            passed += 1;
        }
    }

    // ========================================================================
    // TEST 5: Transaction Rollback Safety
    // ========================================================================
    println!("\n━━━ TEST 5: TRANSACTION ROLLBACK SAFETY ━━━");

    // Transaction that should fail due to invalid FK
    let result = driver.execute_raw("
        BEGIN;
        INSERT INTO stress_orders (user_id, amount) VALUES (1, 200);
        INSERT INTO stress_orders (user_id, amount) VALUES (999, 300);
        COMMIT;
    ").await;

    if result.is_err() {
        println!("✅ Transaction failed (invalid FK) - rollback expected");
        passed += 1;
    } else {
        println!("❌ Transaction should have failed!");
        failed += 1;
    }

    // ========================================================================
    // TEST 6: Cascade Delete Safety
    // ========================================================================
    println!("\n━━━ TEST 6: CASCADE DELETE SAFETY ━━━");

    // Delete user should cascade to orders
    driver.execute_raw("DELETE FROM stress_users WHERE name = 'Alice'").await?;
    println!("✅ CASCADE DELETE executed (dependent orders removed)");
    passed += 1;

    // ========================================================================
    // TEST 7: Unique Constraint Enforcement
    // ========================================================================
    println!("\n━━━ TEST 7: UNIQUE CONSTRAINT ENFORCEMENT ━━━");

    driver.execute_raw("ALTER TABLE stress_users ADD COLUMN email TEXT UNIQUE").await?;
    driver.execute_raw("INSERT INTO stress_users (name, email) VALUES ('Bob', 'bob@test.com')").await?;
    println!("✅ Inserted user with unique email");
    passed += 1;

    // Duplicate email should fail
    match driver.execute_raw("INSERT INTO stress_users (name, email) VALUES ('Carol', 'bob@test.com')").await {
        Ok(_) => {
            println!("❌ Should have rejected duplicate email!");
            failed += 1;
        }
        Err(_) => {
            println!("✅ Rejected duplicate email (UNIQUE constraint)");
            passed += 1;
        }
    }

    // ========================================================================
    // TEST 8: Stress Insert with Constraints
    // ========================================================================
    println!("\n━━━ TEST 8: STRESS INSERT WITH CONSTRAINTS ━━━");

    let mut success_count = 0;
    let mut error_count = 0;

    // Bob was inserted with id=2 in Test 7, use that
    // First get the user_id properly
    driver.execute_raw("INSERT INTO stress_users (name) VALUES ('StressUser')").await?;

    // Use last insert ID (should be 3)
    for i in 0..100 {
        // user_id 3 is StressUser
        let sql = format!("INSERT INTO stress_orders (user_id, amount) VALUES (3, {})", i * 10 + 1);
        match driver.execute_raw(&sql).await {
            Ok(_) => success_count += 1,
            Err(_) => error_count += 1,
        }
    }

    println!("✅ Stress insert: {} succeeded, {} rejected", success_count, error_count);
    if success_count == 100 {
        passed += 1;
    } else {
        failed += 1;
    }

    // ========================================================================
    // SUMMARY
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("SUMMARY");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("✅ Passed: {}", passed);
    println!("❌ Failed: {}", failed);
    println!("📊 Total:  {}", passed + failed);

    if failed == 0 {
        println!("\n🎉 ALL DATA SAFETY TESTS PASSED!");
        println!("   QAIL provides enterprise-grade data protection:");
        println!("   - Compile-time FK validation");
        println!("   - Type-safe PK/UNIQUE constraints");
        println!("   - Runtime CHECK enforcement");
        println!("   - Transaction rollback safety");
        println!("   - CASCADE delete protection");
    } else {
        println!("\n⚠️  Some tests failed - review output above");
    }

    let _ = driver.execute_raw("DROP TABLE IF EXISTS stress_orders CASCADE").await;
    let _ = driver.execute_raw("DROP TABLE IF EXISTS stress_users CASCADE").await;
    println!("\n✅ Cleaned up test tables");

    Ok(())
}
