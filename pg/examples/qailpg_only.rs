//! QAIL-pg 1-MINUTE benchmark with result consumption verification

use qail_pg::PgConnection;
use std::time::{Duration, Instant};

const BATCH_SIZE: usize = 10_000;
const TARGET_DURATION: Duration = Duration::from_secs(60);

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🏁 QAIL-pg 1-MINUTE Stability Benchmark");
    println!("========================================\n");
    
    let mut conn = PgConnection::connect("127.0.0.1", 5432, "orion", "postgres").await?;
    
    let stmt = conn.prepare("SELECT id, name FROM harbors LIMIT $1").await?;
    
    let params_batch: Vec<Vec<Option<Vec<u8>>>> = (1..=BATCH_SIZE)
        .map(|i| {
            let limit = ((i % 10) + 1).to_string();
            vec![Some(limit.into_bytes())]
        })
        .collect();
    
    println!("Query: SELECT id, name FROM harbors LIMIT $1");
    println!("Target: 60 seconds, batch size: {}\n", BATCH_SIZE);
    
    let start = Instant::now();
    let mut total_queries: usize = 0;
    let mut total_rows: usize = 0;
    let mut batch_count = 0;
    
    while start.elapsed() < TARGET_DURATION {
        // Use pipeline_prepared_results to ensure rows are consumed
        let results = conn.pipeline_prepared_results(&stmt, &params_batch).await?;
        
        // Count actual rows consumed
        for result_set in &results {
            total_rows += result_set.len();
        }
        total_queries += results.len();
        batch_count += 1;
        
        // Progress every 10 seconds
        if batch_count % 100 == 0 {
            let elapsed = start.elapsed().as_secs_f64();
            let qps = total_queries as f64 / elapsed;
            println!("  {:.0}s: {} queries, {} rows, {:.0} q/s", elapsed, total_queries, total_rows, qps);
        }
    }
    
    let elapsed = start.elapsed();
    let qps = total_queries as f64 / elapsed.as_secs_f64();
    
    println!("\n=== FINAL RESULTS ===");
    println!("  Duration: {:.2}s", elapsed.as_secs_f64());
    println!("  Queries:  {}", total_queries);
    println!("  Rows:     {} (consumed)", total_rows);
    println!("  📈 Average: {:.0} q/s", qps);
    
    Ok(())
}
