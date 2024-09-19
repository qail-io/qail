//! Pool Overhead Analysis: 1000 tasks to measure scaling
//! Expected: 10s. If 14s+, pool is broken.

use qail_pg::driver::PgDriver;
use std::sync::Arc;
use tokio::sync::Semaphore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔬 Pool Overhead Analysis (1000 tasks)");
    println!("{}", "━".repeat(40));
    
    let max_connections = 1;
    let task_count = 1000;
    
    let semaphore = Arc::new(Semaphore::new(max_connections));
    
    println!("  Pool size: {} connection(s)", max_connections);
    println!("  Tasks: {}", task_count);
    println!("  Expected: {}s ({}ms × {} tasks)", 
        task_count as f64 * 0.01,
        10,
        task_count
    );
    println!();
    
    let start = std::time::Instant::now();
    let mut handles = vec![];
    
    for i in 0..task_count {
        let sem = semaphore.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            
            let mut driver = PgDriver::connect("localhost", 5432, "orion", "postgres")
                .await
                .expect("Connection failed");
            
            driver.execute_raw("SELECT pg_sleep(0.01)").await.expect("Query failed");
            
            if i % 100 == 0 {
                println!("    Task {} done", i);
            }
        }));
    }
    
    for handle in handles {
        handle.await?;
    }
    
    let elapsed = start.elapsed();
    let overhead_ms = elapsed.as_millis() as f64 - (task_count as f64 * 10.0);
    let overhead_per_task = overhead_ms / task_count as f64;
    
    println!();
    println!("  Total time: {:?}", elapsed);
    println!("  Expected:   {}ms", task_count * 10);
    println!("  Overhead:   {}ms ({:.2}ms/task)", overhead_ms, overhead_per_task);
    println!("  Waste:      {:.1}%", (overhead_ms / (task_count as f64 * 10.0)) * 100.0);
    
    if overhead_per_task > 5.0 {
        println!("\n❌ Pool overhead scales linearly - implementation is broken");
    } else if overhead_per_task > 1.0 {
        println!("\n⚠️ Pool overhead is high but acceptable");
    } else {
        println!("\n✓ Pool overhead is minimal");
    }
    
    Ok(())
}
