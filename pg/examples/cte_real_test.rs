//! CTE Test - Complex WhatsApp Conversations Query
//!
//! Tests QAIL's CTE support with a real-world complex query.

use qail_core::ast::{Qail, Operator};
use qail_pg::PgDriver;

fn print_cache_stats(driver: &PgDriver, label: &str) {
    let (size, cap) = driver.cache_stats();
    println!("  📊 Cache [{label}]: {size}/{cap} statements cached");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔗 Connecting to swb_staging_local...");
    
    let mut driver = PgDriver::connect("localhost", 5432, "orion", "swb_staging_local").await?;
    
    println!("✅ Connected! Testing queries...\n");
    print_cache_stats(&driver, "initial");

    // Test 1: Simple query - Latest messages
    println!("\n=== Test 1: Simple Query ===");
    let query = Qail::get("whatsapp_messages")
        .column("phone_number")
        .column("content")
        .column("direction")
        .order_desc("created_at")
        .limit(5);
    
    let rows = driver.fetch_all(&query).await?;
    println!("Rows returned: {}", rows.len());
    print_cache_stats(&driver, "after test 1");
    
    // Test 2: DISTINCT ON - Latest message per phone
    println!("\n=== Test 2: DISTINCT ON ===");
    let latest_per_phone = Qail::get("whatsapp_messages")
        .column("phone_number")
        .column("content")
        .column("created_at")
        .distinct_on(["phone_number"])
        .order_desc("phone_number")
        .order_desc("created_at")
        .limit(10);
    
    let rows = driver.fetch_all(&latest_per_phone).await?;
    println!("Rows returned: {}", rows.len());
    print_cache_stats(&driver, "after test 2");
    
    // Test 3: Filter with condition
    println!("\n=== Test 3: Filter (inbound messages) ===");
    let inbound = Qail::get("whatsapp_messages")
        .column("phone_number")
        .column("content")
        .filter("direction", Operator::Eq, "inbound")
        .order_desc("created_at")
        .limit(5);
    
    let rows = driver.fetch_all(&inbound).await?;
    println!("Rows returned: {}", rows.len());
    print_cache_stats(&driver, "after test 3");
    
    // Test 4: CTE with WITH clause
    println!("\n=== Test 4: CTE with WITH ===");
    
    // Build CTE subquery
    let latest_cte = Qail::get("whatsapp_messages")
        .column("phone_number")
        .column("content")
        .column("created_at")
        .distinct_on(["phone_number"])
        .order_desc("phone_number")
        .order_desc("created_at");
    
    // Main query using CTE
    let conversations = Qail::get("latest_messages")
        .with("latest_messages", latest_cte)
        .column("phone_number")
        .column("content")
        .order_desc("created_at")
        .limit(5);
    
    let rows = driver.fetch_all(&conversations).await?;
    println!("Rows returned: {}", rows.len());
    print_cache_stats(&driver, "after test 4");
    
    // Test 5: Run same query again (should hit cache)
    println!("\n=== Test 5: Cache Hit Test (repeat test 1) ===");
    let rows = driver.fetch_all(&query).await?;
    println!("Rows returned: {}", rows.len());
    print_cache_stats(&driver, "after cache hit");
    
    // Final summary
    println!("\n=== 📊 FINAL CACHE REPORT ===");
    let (size, cap) = driver.cache_stats();
    println!("  Statements cached: {}/{}", size, cap);
    println!("  Cache capacity: {} (reduced from 1000 to prevent OOM)", cap);
    
    println!("\n✅ All tests passed!");
    Ok(())
}
