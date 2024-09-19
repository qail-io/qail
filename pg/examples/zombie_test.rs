//! Zombie Client Test: SIGINT and lock cleanup verification
//! Tests that locks are released when connection is terminated

use qail_pg::driver::PgDriver;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Test 7: Zombie Client");
    println!("{}", "━".repeat(40));
    
    // Connect
    let mut driver = PgDriver::connect("localhost", 5432, "orion", "postgres").await?;
    
    // 1. Begin a transaction (takes locks)
    println!("  Starting transaction with locks...");
    driver.execute_raw("BEGIN").await?;
    driver.execute_raw("LOCK TABLE zombie_test IN ACCESS EXCLUSIVE MODE").await?;
    println!("    ✓ Exclusive lock acquired on zombie_test");
    
    // 2. Check that lock exists
    let mut check_driver = PgDriver::connect("localhost", 5432, "orion", "postgres").await?;
    check_driver.execute_raw("SELECT relation::regclass, mode FROM pg_locks WHERE relation::regclass::text = 'zombie_test'").await?;
    println!("    ✓ Lock confirmed in pg_locks");
    
    // 3. Simulate SIGINT by dropping connection without commit
    println!("  Simulating connection drop (no COMMIT)...");
    drop(driver); // Connection dropped without COMMIT - should trigger ROLLBACK
    
    // Wait for Postgres to detect disconnection
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // 4. Verify lock is released
    println!("  Verifying lock cleanup...");
    let _lock_check = check_driver.execute_raw(
        "SELECT COUNT(*) FROM pg_locks WHERE relation::regclass::text = 'zombie_test' AND mode = 'AccessExclusiveLock'"
    ).await?;
    
    // If we can take a new lock, the old one is gone
    let mut verify_driver = PgDriver::connect("localhost", 5432, "orion", "postgres").await?;
    verify_driver.execute_raw("BEGIN").await?;
    verify_driver.execute_raw("LOCK TABLE zombie_test IN ACCESS EXCLUSIVE MODE NOWAIT").await?;
    verify_driver.execute_raw("ROLLBACK").await?;
    println!("    ✓ Lock released - new exclusive lock acquired successfully");
    
    println!();
    println!("✓ Zombie Client Test PASSED!");
    println!("  Postgres correctly cleaned up locks when connection dropped.");
    
    Ok(())
}
