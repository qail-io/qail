//! QAIL-pg Pool + Pipeline 60-second benchmark
//! 10 connections running pipelined queries in parallel

use qail_pg::{PgPool, PoolConfig};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

const POOL_SIZE: usize = 10;
const BATCH_SIZE: usize = 10_000;
const TARGET_DURATION: Duration = Duration::from_secs(60);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🏁 QAIL-pg POOL + PIPELINE 60-Second Benchmark");
    println!("================================================\n");
    
    let config = PoolConfig::new("127.0.0.1", 5432, "orion", "postgres")
        .max_connections(POOL_SIZE)
        .min_connections(POOL_SIZE);
    
    let pool = PgPool::connect(config).await?;
    
    println!("Pool: {} connections", POOL_SIZE);
    println!("Query: SELECT id, name FROM harbors LIMIT $1");
    println!("Target: 60 seconds, batch size: {}\n", BATCH_SIZE);
    
    let total_queries = Arc::new(AtomicUsize::new(0));
    let total_rows = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();
    
    let params_batch: Vec<Vec<Option<Vec<u8>>>> = (1..=BATCH_SIZE)
        .map(|i| {
            let limit = ((i % 10) + 1).to_string();
            vec![Some(limit.into_bytes())]
        })
        .collect();
    let params_batch = Arc::new(params_batch);
    
    let mut tasks = JoinSet::new();
    
    // Spawn 10 parallel workers
    for worker_id in 0..POOL_SIZE {
        let pool = pool.clone();
        let total_queries = Arc::clone(&total_queries);
        let total_rows = Arc::clone(&total_rows);
        let params_batch = Arc::clone(&params_batch);
        
        tasks.spawn(async move {
            let mut conn = pool.acquire().await.unwrap();
            let stmt = conn.prepare("SELECT id, name FROM harbors LIMIT $1").await.unwrap();
            
            while start.elapsed() < TARGET_DURATION {
                let results = conn.pipeline_prepared_results(&stmt, &params_batch).await.unwrap();
                
                let mut batch_rows = 0;
                for result_set in &results {
                    batch_rows += result_set.len();
                }
                
                total_queries.fetch_add(results.len(), Ordering::Relaxed);
                total_rows.fetch_add(batch_rows, Ordering::Relaxed);
            }
            
            worker_id
        });
    }
    
    // Progress reporter
    let total_queries_clone = Arc::clone(&total_queries);
    let total_rows_clone = Arc::clone(&total_rows);
    let progress_task = tokio::spawn(async move {
        let mut last_report = Instant::now();
        while start.elapsed() < TARGET_DURATION {
            tokio::time::sleep(Duration::from_secs(5)).await;
            if last_report.elapsed().as_secs() >= 5 {
                let elapsed = start.elapsed().as_secs_f64();
                let queries = total_queries_clone.load(Ordering::Relaxed);
                let rows = total_rows_clone.load(Ordering::Relaxed);
                let qps = queries as f64 / elapsed;
                println!("  {:.0}s: {} queries, {} rows, {:.0} q/s", elapsed, queries, rows, qps);
                last_report = Instant::now();
            }
        }
    });
    
    // Wait for all workers
    while let Some(_) = tasks.join_next().await {}
    progress_task.abort();
    
    let elapsed = start.elapsed();
    let queries = total_queries.load(Ordering::Relaxed);
    let rows = total_rows.load(Ordering::Relaxed);
    let qps = queries as f64 / elapsed.as_secs_f64();
    
    println!("\n=== FINAL RESULTS ===");
    println!("  Pool Size: {} connections", POOL_SIZE);
    println!("  Duration:  {:.2}s", elapsed.as_secs_f64());
    println!("  Queries:   {}", queries);
    println!("  Rows:      {} (consumed)", rows);
    println!("  📈 Average: {:.0} q/s", qps);
    
    Ok(())
}
