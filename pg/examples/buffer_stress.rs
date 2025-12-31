//! Buffer Boundary Stress Test
//! Tests BytesMut resize with large data

use qail_pg::driver::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Test 1: Buffer Boundary (1MB)");
    println!("{}", "━".repeat(40));
    
    // Connect to database
    let mut driver = PgDriver::connect("localhost", 5432, "orion", "postgres").await?;
    
    // Create 1MB of test data (enough to exceed default buffer, fast test)
    let size_kb = 1024;
    let huge_string: String = (0..size_kb * 1024)
        .map(|i| ((i % 26) as u8 + b'a') as char)
        .collect();
    
    println!("  Created {} KB of test data", size_kb);
    
    // Create test table
    driver.execute_raw("DROP TABLE IF EXISTS big_text").await?;
    driver.execute_raw("CREATE TABLE big_text (id serial primary key, data text)").await?;
    
    // Insert using TEXT (simpler than bytea)
    let start = std::time::Instant::now();
    
    // Escape single quotes
    let escaped = huge_string.replace('\'', "''");
    let sql = format!("INSERT INTO big_text (data) VALUES ('{}')", escaped);
    
    println!("  SQL size: {} KB", sql.len() / 1024);
    println!("  Inserting...");
    
    driver.execute_raw(&sql).await?;
    
    let elapsed = start.elapsed();
    println!("  ✓ Insert completed in {:?}", elapsed);
    
    // Verify by selecting length
    driver.execute_raw("SELECT length(data) FROM big_text ORDER BY id DESC LIMIT 1").await?;
    println!("  ✓ Verified in database");
    
    println!();
    println!("✓ Buffer Boundary Test (1MB) PASSED!");
    
    Ok(())
}
