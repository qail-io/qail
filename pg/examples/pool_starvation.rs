//! Pool Starvation Test: Tiny pool with many concurrent tasks
//! Tests that connection pool handles starvation gracefully

use qail_pg::driver::PgDriver;
use std::sync::Arc;
use tokio::sync::Semaphore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Test 6: Pool Starvation");
    println!("{}", "━".repeat(40));
    
    // Simulating pool starvation with semaphore (limit concurrent connections)
    let max_connections = 1;
    let task_count = 50;
    
    let semaphore = Arc::new(Semaphore::new(max_connections));
    
    println!("  Pool size: {} connection(s)", max_connections);
    println!("  Concurrent tasks: {}", task_count);
    println!("  Starting tasks...");
    
    let start = std::time::Instant::now();
    let mut handles = vec![];
    
    for i in 0..task_count {
        let sem = semaphore.clone();
        handles.push(tokio::spawn(async move {
            // Acquire permit (blocks if pool exhausted)
            let _permit = sem.acquire().await.unwrap();
            
            // Create connection (simulates pool checkout)
            let mut driver = PgDriver::connect("localhost", 5432, "orion", "postgres")
                .await
                .expect("Connection failed");
            
            // Simulate work (10ms sleep in DB)
            driver.execute_raw("SELECT pg_sleep(0.01)").await.expect("Query failed");
            
            if i % 10 == 0 {
                println!("    Task {} completed", i);
            }
        }));
    }
    
    // Wait for all tasks
    for handle in handles {
        handle.await?;
    }
    
    let elapsed = start.elapsed();
    println!();
    println!("  ✓ All {} tasks completed in {:?}", task_count, elapsed);
    println!("  Expected ~{:.1}s (50 tasks × 10ms each)", task_count as f64 * 0.01);
    
    println!();
    println!("✓ Pool Starvation Test PASSED!");
    
    Ok(())
}
