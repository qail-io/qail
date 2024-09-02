//! Migration DDL Battle Test
//!
//! Tests all new DDL features against real PostgreSQL:
//! - CHECK constraints
//! - DEFERRABLE foreign keys
//! - GENERATED columns
//! - Advanced indexes (GIN, partial, CONCURRENTLY)
//! - ALTER TABLE operations
//! - ARRAY/ENUM types
//!
//! Run with: cargo run --example migration_battle_test

use qail_core::migrate::*;
use qail_pg::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("MIGRATION DDL BATTLE TEST");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    // Connect to PostgreSQL
    let mut driver = PgDriver::connect("127.0.0.1", 5432, "orion", "postgres").await?;
    println!("✅ Connected to PostgreSQL\n");

    let mut passed = 0;
    let mut failed = 0;

    // Helper to test SQL execution
    async fn test_sql(
        driver: &mut PgDriver,
        name: &str,
        sql: &str,
        passed: &mut i32,
        failed: &mut i32,
    ) {
        match driver.execute_raw(sql).await {
            Ok(_) => {
                println!("✅ {} - SQL: {}", name, &sql[..sql.len().min(60)]);
                *passed += 1;
            }
            Err(e) => {
                println!("❌ {} - Error: {:?}", name, e);
                println!("   SQL: {}", sql);
                *failed += 1;
            }
        }
    }

    // ========================================================================
    // CLEANUP
    // ========================================================================
    println!("━━━ CLEANUP ━━━");
    let _ = driver.execute_raw("DROP TABLE IF EXISTS battle_posts CASCADE").await;
    let _ = driver.execute_raw("DROP TABLE IF EXISTS battle_users CASCADE").await;
    let _ = driver.execute_raw("DROP TYPE IF EXISTS order_status CASCADE").await;
    println!("✅ Cleaned up existing tables\n");

    // ========================================================================
    // Phase 1: CHECK Constraints
    // ========================================================================
    println!("━━━ PHASE 1: CHECK CONSTRAINTS ━━━");
    
    // Build schema with CHECK constraint (showing AST usage)
    let _users = Table::new("battle_users")
        .column(Column::new("id", ColumnType::Serial).primary_key())
        .column(Column::new("name", ColumnType::Text).not_null())
        .column(Column::new("age", ColumnType::Int)
            .check(CheckExpr::Between { column: "age".into(), low: 0, high: 150 }))
        .column(Column::new("email", ColumnType::Text)
            .check(CheckExpr::Regex { column: "email".into(), pattern: ".*@.*".into() }));
    
    // Generate DDL SQL (this would come from transpiler in production)
    let create_users_sql = r#"
        CREATE TABLE battle_users (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            age INT CHECK (age >= 0 AND age <= 150),
            email TEXT CHECK (email ~ '.*@.*')
        )
    "#;
    test_sql(&mut driver, "CREATE TABLE with CHECK", create_users_sql, &mut passed, &mut failed).await;

    // Test CHECK constraint works
    let insert_valid = "INSERT INTO battle_users (name, age, email) VALUES ('Alice', 25, 'alice@example.com')";
    test_sql(&mut driver, "INSERT valid data", insert_valid, &mut passed, &mut failed).await;

    // This should fail due to CHECK constraint
    let insert_invalid = "INSERT INTO battle_users (name, age, email) VALUES ('Bob', 200, 'bob@example.com')";
    match driver.execute_raw(insert_invalid).await {
        Ok(_) => {
            println!("❌ CHECK constraint should have rejected age=200");
            failed += 1;
        }
        Err(_) => {
            println!("✅ CHECK constraint rejected age=200");
            passed += 1;
        }
    }

    // ========================================================================
    // Phase 2: DEFERRABLE Foreign Keys
    // ========================================================================
    println!("\n━━━ PHASE 2: DEFERRABLE FOREIGN KEYS ━━━");
    
    let create_posts_sql = r#"
        CREATE TABLE battle_posts (
            id SERIAL PRIMARY KEY,
            user_id INT REFERENCES battle_users(id) DEFERRABLE INITIALLY DEFERRED,
            title TEXT NOT NULL
        )
    "#;
    test_sql(&mut driver, "CREATE TABLE with DEFERRABLE FK", create_posts_sql, &mut passed, &mut failed).await;

    // Test deferred constraint (insert post before user in same transaction)
    let deferred_test = r#"
        BEGIN;
        INSERT INTO battle_posts (id, user_id, title) VALUES (100, 999, 'Orphan Post');
        INSERT INTO battle_users (id, name, age, email) VALUES (999, 'Deferred User', 30, 'deferred@test.com');
        COMMIT;
    "#;
    test_sql(&mut driver, "DEFERRABLE FK allows out-of-order insert", deferred_test, &mut passed, &mut failed).await;

    // ========================================================================
    // Phase 3: GENERATED Columns
    // ========================================================================
    println!("\n━━━ PHASE 3: GENERATED COLUMNS ━━━");
    
    let alter_generated = r#"
        ALTER TABLE battle_users 
        ADD COLUMN full_info TEXT GENERATED ALWAYS AS (name || ' (' || COALESCE(email, 'no email') || ')') STORED
    "#;
    test_sql(&mut driver, "ADD GENERATED STORED column", alter_generated, &mut passed, &mut failed).await;

    // Verify generated column works
    let check_generated = "SELECT full_info FROM battle_users WHERE name = 'Alice'";
    match driver.execute_raw(check_generated).await {
        Ok(_) => {
            println!("✅ GENERATED column returns data");
            passed += 1;
        }
        Err(e) => {
            println!("❌ GENERATED column error: {:?}", e);
            failed += 1;
        }
    }

    // ========================================================================
    // Phase 4: Advanced Indexes
    // ========================================================================
    println!("\n━━━ PHASE 4: ADVANCED INDEXES ━━━");
    
    // Add JSONB column for GIN index test
    let _ = driver.execute_raw("ALTER TABLE battle_users ADD COLUMN metadata JSONB DEFAULT '{}'").await;
    
    let gin_index = "CREATE INDEX CONCURRENTLY idx_users_metadata ON battle_users USING GIN (metadata)";
    test_sql(&mut driver, "CREATE INDEX USING GIN CONCURRENTLY", gin_index, &mut passed, &mut failed).await;

    let partial_index = "CREATE INDEX idx_users_active ON battle_users (name) WHERE age > 18";
    test_sql(&mut driver, "CREATE partial index", partial_index, &mut passed, &mut failed).await;

    let covering_index = "CREATE INDEX idx_users_covering ON battle_users (name) INCLUDE (email, age)";
    test_sql(&mut driver, "CREATE covering index (INCLUDE)", covering_index, &mut passed, &mut failed).await;

    // ========================================================================
    // Phase 5: ALTER TABLE Operations
    // ========================================================================
    println!("\n━━━ PHASE 5: ALTER TABLE OPERATIONS ━━━");
    
    let add_column = "ALTER TABLE battle_users ADD COLUMN bio TEXT";
    test_sql(&mut driver, "ADD COLUMN", add_column, &mut passed, &mut failed).await;

    let set_not_null = "ALTER TABLE battle_users ALTER COLUMN bio SET DEFAULT 'No bio yet'";
    test_sql(&mut driver, "SET DEFAULT", set_not_null, &mut passed, &mut failed).await;

    let rename_column = "ALTER TABLE battle_users RENAME COLUMN bio TO biography";
    test_sql(&mut driver, "RENAME COLUMN", rename_column, &mut passed, &mut failed).await;

    let add_constraint = "ALTER TABLE battle_users ADD CONSTRAINT chk_name_len CHECK (LENGTH(name) >= 2)";
    test_sql(&mut driver, "ADD CONSTRAINT", add_constraint, &mut passed, &mut failed).await;

    // ========================================================================
    // Phase 6: ARRAY/ENUM Types
    // ========================================================================
    println!("\n━━━ PHASE 6: ARRAY/ENUM TYPES ━━━");
    
    let create_enum = "CREATE TYPE order_status AS ENUM ('pending', 'shipped', 'delivered', 'cancelled')";
    test_sql(&mut driver, "CREATE ENUM type", create_enum, &mut passed, &mut failed).await;

    let add_enum_col = "ALTER TABLE battle_users ADD COLUMN status order_status DEFAULT 'pending'";
    test_sql(&mut driver, "ADD ENUM column", add_enum_col, &mut passed, &mut failed).await;

    let add_array_col = "ALTER TABLE battle_users ADD COLUMN tags TEXT[] DEFAULT '{}'";
    test_sql(&mut driver, "ADD ARRAY column", add_array_col, &mut passed, &mut failed).await;

    // Test ARRAY operations
    let update_array = "UPDATE battle_users SET tags = ARRAY['admin', 'vip'] WHERE name = 'Alice'";
    test_sql(&mut driver, "UPDATE ARRAY column", update_array, &mut passed, &mut failed).await;

    let query_array = "SELECT * FROM battle_users WHERE 'admin' = ANY(tags)";
    test_sql(&mut driver, "Query ARRAY with ANY", query_array, &mut passed, &mut failed).await;

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
        println!("\n🎉 ALL MIGRATION DDL TESTS PASSED!");
    } else {
        println!("\n⚠️  Some tests failed - review output above");
    }

    // Cleanup
    let _ = driver.execute_raw("DROP TABLE IF EXISTS battle_posts CASCADE").await;
    let _ = driver.execute_raw("DROP TABLE IF EXISTS battle_users CASCADE").await;
    let _ = driver.execute_raw("DROP TYPE IF EXISTS order_status CASCADE").await;
    println!("\n✅ Cleaned up test tables");

    Ok(())
}
