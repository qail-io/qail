//! NULL byte rejection test
use qail_pg::driver::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut driver = PgDriver::connect("localhost", 5432, "orion", "postgres").await?;
    
    // Test: Embedded NULL byte in SQL string
    println!("Testing NULL byte rejection...");
    let evil_sql = "SELECT 'hello\x00world'";
    println!("  SQL bytes: {:?}", evil_sql.as_bytes());
    
    match driver.execute_raw(evil_sql).await {
        Ok(_) => println!("  ❌ NULL byte ACCEPTED (should reject!)"),
        Err(e) => println!("  ✓ NULL byte REJECTED: {}", e),
    }
    
    // Test: Normal SQL should still work
    println!("\nTesting normal query...");
    match driver.execute_raw("SELECT 'hello world'").await {
        Ok(_) => println!("  ✓ Normal query works"),
        Err(e) => println!("  ❌ Normal query failed: {}", e),
    }
    
    Ok(())
}
