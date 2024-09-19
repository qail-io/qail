//! Pool Overhead Test
//! Measures pure acquire/release time without DB work

use qail_core::ast::Qail;
use qail_pg::{PgPool, PoolConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔬 Pool Overhead Test (Pure Acquire/Release)");
    println!("{}", "━".repeat(40));
    
    // Create pool with 1 connection
    let config = PoolConfig::new("localhost", 5432, "orion", "postgres")
        .max_connections(1)
        .min_connections(1)
        .acquire_timeout(Duration::from_secs(60));
    
    let pool = PgPool::connect(config).await?;
    
    let iterations = 10000;
    
    println!("  Pool size: 1 connection");
    println!("  Iterations: {}", iterations);
    println!();
    
    // Test 1: Pure acquire/release (no query)
    println!("  Test 1: Pure acquire/release");
    let start = std::time::Instant::now();
    
    for _ in 0..iterations {
        let conn = pool.acquire().await?;
        drop(conn); // Return to pool
    }
    
    let elapsed = start.elapsed();
    let per_op = elapsed.as_nanos() as f64 / iterations as f64;
    println!("    Total: {:?}", elapsed);
    println!("    Per acquire+release: {:.0}ns ({:.3}μs)", per_op, per_op / 1000.0);
    
    // Test 2: With minimal query
    println!();
    println!("  Test 2: With query (SELECT 1 via pipeline)");
    let query = Qail::raw_sql("SELECT 1");
    
    let start = std::time::Instant::now();
    
    for _ in 0..1000 {
        let mut conn = pool.acquire().await?;
        conn.pipeline_ast_fast(&[query.clone()]).await?;
        drop(conn);
    }
    
    let elapsed = start.elapsed();
    let per_op = elapsed.as_micros() as f64 / 1000.0;
    println!("    Total: {:?}", elapsed);
    println!("    Per query (incl pool): {:.1}μs", per_op);
    
    println!();
    if per_op < 100.0 {
        println!("✓ Pool overhead is excellent");
    } else if per_op < 500.0 {
        println!("✓ Pool overhead is acceptable");
    } else {
        println!("⚠️ Pool overhead needs investigation");
    }
    
    Ok(())
}
