//! Battle test: QAIL queries against real PostgreSQL
//! 
//! Run with: cargo run --example battle_test

use qail_pg::driver::PgDriver;
use qail_core::prelude::{Qail, Operator, SortOrder};
use qail_core::transpiler::ToSql;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔥 QAIL Battle Test");
    println!("==================\n");

    let mut driver = PgDriver::connect("127.0.0.1", 5432, "orion", "qail_test_migration").await?;

    // =========== INSERT TESTS ===========
    println!("📝 INSERT Tests");
    println!("---------------");

    // Test 1: Simple INSERT
    let insert = Qail::add("inquiries")
        .columns(["name", "email", "service", "message"])
        .values(["Alice", "alice@test.com", "wedding", "Hello from QAIL!"]);
    
    match driver.execute(&insert).await {
        Ok(_) => println!("  ✓ INSERT simple: success"),
        Err(e) => println!("  ✗ INSERT simple: {}", e),
    }

    // Test 2: INSERT with special characters
    let insert2 = Qail::add("inquiries")
        .columns(["name", "email", "service", "message"])
        .values(["Bob's Place", "bob@test.com", "corporate", "Special chars: <>&'\""]);
    
    match driver.execute(&insert2).await {
        Ok(_) => println!("  ✓ INSERT special chars: success"),
        Err(e) => println!("  ✗ INSERT special chars: {}", e),
    }

    // =========== SELECT TESTS ===========
    println!("\n📖 SELECT Tests");
    println!("----------------");

    // Test 3: Simple SELECT
    let select = Qail::get("inquiries").columns(["id", "name", "email"]);
    match driver.fetch_all(&select).await {
        Ok(rows) => println!("  ✓ SELECT simple: {} rows", rows.len()),
        Err(e) => println!("  ✗ SELECT simple: {}", e),
    }

    // Test 4: SELECT with WHERE filter
    let select_filter = Qail::get("inquiries")
        .columns(["id", "name"])
        .filter("name", Operator::Eq, "Alice");
    
    match driver.fetch_all(&select_filter).await {
        Ok(rows) => println!("  ✓ SELECT with filter: {} rows", rows.len()),
        Err(e) => println!("  ✗ SELECT with filter: {}", e),
    }

    // Test 5: SELECT with LIKE
    let select_like = Qail::get("inquiries")
        .columns(["id", "name"])
        .filter("name", Operator::Like, "%Bob%");
    
    match driver.fetch_all(&select_like).await {
        Ok(rows) => println!("  ✓ SELECT with LIKE: {} rows", rows.len()),
        Err(e) => println!("  ✗ SELECT with LIKE: {}", e),
    }

    // Test 6: SELECT with ORDER BY
    let select_order = Qail::get("inquiries")
        .columns(["id", "name"])
        .order_by("id", SortOrder::Desc)
        .limit(5);
    
    match driver.fetch_all(&select_order).await {
        Ok(rows) => println!("  ✓ SELECT with ORDER BY: {} rows", rows.len()),
        Err(e) => println!("  ✗ SELECT with ORDER BY: {}", e),
    }

    // =========== UPDATE TESTS ===========
    println!("\n✏️  UPDATE Tests");
    println!("----------------");

    // Test 7: UPDATE single row
    let update = Qail::set("inquiries")
        .columns(["status"])
        .values(["read"])
        .filter("name", Operator::Eq, "Alice");
    
    match driver.execute(&update).await {
        Ok(_) => println!("  ✓ UPDATE single: success"),
        Err(e) => println!("  ✗ UPDATE single: {}", e),
    }

    // =========== AGGREGATE TESTS ===========
    println!("\n📊 Aggregate Tests");
    println!("------------------");

    // Test 8: Aggregate - skipped (requires direct Expr push)
    // TODO: Add .select_expr() method for complex expressions
    println!("  ⏭ COUNT(*): skipped (needs API enhancement)");

    // =========== DELETE TESTS ===========
    println!("\n🗑️  DELETE Tests");
    println!("----------------");

    // Test 9: DELETE with filter
    let delete = Qail::del("inquiries")
        .filter("name", Operator::Like, "%Bob%");
    
    match driver.execute(&delete).await {
        Ok(_) => println!("  ✓ DELETE with filter: success"),
        Err(e) => println!("  ✗ DELETE with filter: {}", e),
    }

    // =========== CLEANUP ===========
    println!("\n🧹 Cleanup");
    println!("-----------");

    let cleanup = Qail::del("inquiries")
        .filter("name", Operator::Like, "%Alice%");
    
    match driver.execute(&cleanup).await {
        Ok(_) => println!("  ✓ Cleanup: success"),
        Err(e) => println!("  ✗ Cleanup: {}", e),
    }

    println!("\n✅ Battle test complete!");

    Ok(())
}
