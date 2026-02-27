//! Battle test: QAIL queries against real PostgreSQL
//!
//! Run with: cargo run --example battle_test

use qail_core::prelude::{JoinKind, Operator, Qail, SortOrder, Value};
use qail_pg::driver::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔥 QAIL Deep Battle Test");
    println!("========================\n");

    let mut driver = PgDriver::connect("127.0.0.1", 5432, "orion", "qail_test_migration").await?;

    // =========== SETUP TEST DATA ===========
    println!("� Setup Test Data");
    println!("------------------");

    // Clean slate
    let _ = driver.execute(&Qail::del("inquiries")).await;

    // Insert test data with various values
    for (name, email, service, status) in [
        ("Alice", "alice@test.com", "wedding", "new"),
        ("Bob", "bob@test.com", "corporate", "read"),
        ("Charlie", "charlie@test.com", "wedding", "new"),
        ("Diana", "diana@test.com", "birthday", "replied"),
        ("Eve", "eve@test.com", "wedding", "new"),
    ] {
        let insert = Qail::add("inquiries")
            .columns(["name", "email", "service", "status", "message"])
            .values([name, email, service, status, "Test message"]);
        driver.execute(&insert).await?;
    }
    println!("  ✓ Inserted 5 test rows");

    // =========== BASIC SELECT TESTS ===========
    println!("\n📖 SELECT Tests - Basic");
    println!("------------------------");

    // SELECT *
    let select_all = Qail::get("inquiries").select_all();
    match driver.fetch_all(&select_all).await {
        Ok(rows) => println!("  ✓ SELECT *: {} rows", rows.len()),
        Err(e) => println!("  ✗ SELECT *: {}", e),
    }

    // SELECT with multiple columns
    let select_cols = Qail::get("inquiries").columns(["id", "name", "email", "status"]);
    match driver.fetch_all(&select_cols).await {
        Ok(rows) => println!("  ✓ SELECT multi-column: {} rows", rows.len()),
        Err(e) => println!("  ✗ SELECT multi-column: {}", e),
    }

    // =========== WHERE CLAUSE OPERATORS ===========
    println!("\n� WHERE Operators");
    println!("------------------");

    // Equals
    let eq = Qail::get("inquiries")
        .columns(["name"])
        .filter("status", Operator::Eq, "new");
    match driver.fetch_all(&eq).await {
        Ok(rows) => println!("  ✓ WHERE = : {} rows (expect 3)", rows.len()),
        Err(e) => println!("  ✗ WHERE = : {}", e),
    }

    // Not Equals
    let ne = Qail::get("inquiries")
        .columns(["name"])
        .filter("status", Operator::Ne, "new");
    match driver.fetch_all(&ne).await {
        Ok(rows) => println!("  ✓ WHERE != : {} rows (expect 2)", rows.len()),
        Err(e) => println!("  ✗ WHERE != : {}", e),
    }

    // LIKE
    let like = Qail::get("inquiries")
        .columns(["name"])
        .filter("name", Operator::Like, "A%");
    match driver.fetch_all(&like).await {
        Ok(rows) => println!("  ✓ WHERE LIKE: {} rows (expect 1: Alice)", rows.len()),
        Err(e) => println!("  ✗ WHERE LIKE: {}", e),
    }

    // ILIKE (case-insensitive)
    let ilike = Qail::get("inquiries")
        .columns(["name"])
        .filter("name", Operator::ILike, "%LI%");
    match driver.fetch_all(&ilike).await {
        Ok(rows) => println!("  ✓ WHERE ILIKE: {} rows (Alice, Charlie)", rows.len()),
        Err(e) => println!("  ✗ WHERE ILIKE: {}", e),
    }

    // IN operator
    let in_op = Qail::get("inquiries").columns(["name"]).filter(
        "service",
        Operator::In,
        Value::Array(vec![
            Value::String("wedding".into()),
            Value::String("birthday".into()),
        ]),
    );
    match driver.fetch_all(&in_op).await {
        Ok(rows) => println!("  ✓ WHERE IN: {} rows (expect 4)", rows.len()),
        Err(e) => println!("  ✗ WHERE IN: {}", e),
    }

    // IS NULL (test on optional field)
    let is_null = Qail::get("inquiries")
        .columns(["name"])
        .filter("phone", Operator::IsNull, "");
    match driver.fetch_all(&is_null).await {
        Ok(rows) => println!("  ✓ WHERE IS NULL: {} rows", rows.len()),
        Err(e) => println!("  ✗ WHERE IS NULL: {}", e),
    }

    // =========== JOIN TESTS ===========
    println!("\n🔗 JOIN Tests");
    println!("--------------");

    // LEFT JOIN (self-join for testing)
    let left_join = Qail::get("inquiries")
        .columns(["inquiries.id", "inquiries.name"])
        .join(
            JoinKind::Left,
            "inquiries AS i2",
            "inquiries.service",
            "i2.service",
        )
        .limit(5);
    match driver.fetch_all(&left_join).await {
        Ok(rows) => println!("  ✓ LEFT JOIN: {} rows", rows.len()),
        Err(e) => println!("  ✗ LEFT JOIN: {}", e),
    }

    // =========== ORDER BY + LIMIT + OFFSET ===========
    println!("\n📊 Pagination Tests");
    println!("-------------------");

    // ORDER BY DESC
    let order_desc = Qail::get("inquiries")
        .columns(["id", "name"])
        .order_by("id", SortOrder::Desc)
        .limit(3);
    match driver.fetch_all(&order_desc).await {
        Ok(rows) => println!("  ✓ ORDER BY DESC LIMIT 3: {} rows", rows.len()),
        Err(e) => println!("  ✗ ORDER BY DESC: {}", e),
    }

    // LIMIT + OFFSET
    let paginated = Qail::get("inquiries")
        .columns(["id", "name"])
        .order_by("id", SortOrder::Asc)
        .limit(2)
        .offset(2);
    match driver.fetch_all(&paginated).await {
        Ok(rows) => println!("  ✓ LIMIT 2 OFFSET 2: {} rows", rows.len()),
        Err(e) => println!("  ✗ LIMIT OFFSET: {}", e),
    }

    // =========== UPDATE TESTS ===========
    println!("\n✏️  UPDATE Tests");
    println!("----------------");

    // Single column UPDATE
    let update_single = Qail::set("inquiries")
        .columns(["status"])
        .values(["archived"])
        .filter("name", Operator::Eq, "Eve");
    match driver.execute(&update_single).await {
        Ok(_) => println!("  ✓ UPDATE single column: success"),
        Err(e) => println!("  ✗ UPDATE single column: {}", e),
    }

    // Multi-column UPDATE
    let update_multi = Qail::set("inquiries")
        .columns(["status", "message"])
        .values(["contacted", "Updated via QAIL"])
        .filter("name", Operator::Eq, "Diana");
    match driver.execute(&update_multi).await {
        Ok(_) => println!("  ✓ UPDATE multi-column: success"),
        Err(e) => println!("  ✗ UPDATE multi-column: {}", e),
    }

    // UPDATE with LIKE in WHERE
    let update_like = Qail::set("inquiries")
        .columns(["status"])
        .values(["bulk_updated"])
        .filter("service", Operator::Eq, "wedding");
    match driver.execute(&update_like).await {
        Ok(_) => println!("  ✓ UPDATE with complex WHERE: success"),
        Err(e) => println!("  ✗ UPDATE with complex WHERE: {}", e),
    }

    // =========== DELETE TESTS ===========
    println!("\n🗑️  DELETE Tests");
    println!("----------------");

    // DELETE with specific filter
    let delete_one = Qail::del("inquiries").filter("name", Operator::Eq, "Bob");
    match driver.execute(&delete_one).await {
        Ok(_) => println!("  ✓ DELETE specific row: success"),
        Err(e) => println!("  ✗ DELETE specific row: {}", e),
    }

    // Verify deletion
    let verify = Qail::get("inquiries").columns(["name"]);
    match driver.fetch_all(&verify).await {
        Ok(rows) => println!("  ✓ Remaining rows: {} (expect 4)", rows.len()),
        Err(e) => println!("  ✗ Verify: {}", e),
    }

    // =========== DISTINCT TEST ===========
    println!("\n🎯 DISTINCT Test");
    println!("-----------------");

    // DISTINCT uses a field, set it directly
    let mut distinct = Qail::get("inquiries").columns(["service"]);
    distinct.distinct = true;
    match driver.fetch_all(&distinct).await {
        Ok(rows) => println!("  ✓ SELECT DISTINCT: {} unique services", rows.len()),
        Err(e) => println!("  ✗ SELECT DISTINCT: {}", e),
    }

    // =========== CLEANUP ===========
    println!("\n🧹 Cleanup");
    println!("-----------");

    let cleanup = Qail::del("inquiries");
    match driver.execute(&cleanup).await {
        Ok(_) => println!("  ✓ Cleanup: all test rows deleted"),
        Err(e) => println!("  ✗ Cleanup: {}", e),
    }

    println!("\n✅ Deep battle test complete!");

    Ok(())
}
