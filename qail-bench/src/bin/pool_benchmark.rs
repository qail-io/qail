//! QAIL Pool Benchmark - FAIR COMPARISON with C libpq
//!
//! Uses PREPARED STATEMENTS (same as C) for fair comparison.
//!
//! ## Run
//!
//! ```bash
//! cargo run --release --bin pool_benchmark
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use std::env;
use qail_pg::{PgPool, PoolConfig};

const TOTAL_QUERIES: usize = 150_000_000;
const NUM_WORKERS: usize = 10;
const POOL_SIZE: usize = 10;
const QUERIES_PER_BATCH: usize = 100;

fn get_env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = get_env_or("PG_HOST", "127.0.0.1");
    let port: u16 = get_env_or("PG_PORT", "5432").parse()?;
    let user = get_env_or("PG_USER", "postgres");
    let database = get_env_or("PG_DATABASE", "postgres");
    
    println!("🔌 Connecting to {}:{} as {}", host, port, user);
    
    // Create pool
    let pool = PgPool::connect(
        PoolConfig::new(&host, port, &user, &database)
            .max_connections(POOL_SIZE)
            .min_connections(POOL_SIZE)
    ).await?;
    
    println!("🚀 QAIL POOL BENCHMARK (PREPARED STATEMENTS)");
    println!("============================================");
    println!("Total queries:    {:>15}", TOTAL_QUERIES);
    println!("Workers:          {:>15}", NUM_WORKERS);
    println!("Pool size:        {:>15}", POOL_SIZE);
    println!("Batch size:       {:>15}", QUERIES_PER_BATCH);
    println!();
    
    let batches_per_worker = TOTAL_QUERIES / NUM_WORKERS / QUERIES_PER_BATCH;
    let counter = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();
    
    // Spawn workers
    let mut handles = Vec::new();
    for worker_id in 0..NUM_WORKERS {
        let pool = pool.clone();
        let counter = Arc::clone(&counter);
        
        handles.push(tokio::spawn(async move {
            // Acquire connection for this worker
            let mut conn = pool.acquire().await.expect("Failed to acquire");
            
            // Prepare statement ONCE (same as C)
            let stmt = conn.prepare("SELECT id, name FROM harbors LIMIT $1")
                .await
                .expect("Prepare failed");
            
            // Build params batch
            let params_batch: Vec<Vec<Option<Vec<u8>>>> = (1..=QUERIES_PER_BATCH)
                .map(|i| {
                    let limit = ((i % 10) + 1).to_string();
                    vec![Some(limit.into_bytes())]
                })
                .collect();
            
            for _ in 0..batches_per_worker {
                // Execute batch with prepared statement (same as C)
                let results = conn.pipeline_prepared_ultra(&stmt, &params_batch)
                    .await
                    .expect("Query failed");
                
                // Count results
                for rows in &results {
                    for (_id, _name) in rows {
                        // Consume data
                    }
                }
                
                counter.fetch_add(QUERIES_PER_BATCH, Ordering::Relaxed);
            }
            
            worker_id
        }));
    }
    
    // Progress reporter
    let counter_clone = Arc::clone(&counter);
    let progress = tokio::spawn(async move {
        let start = Instant::now();
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let count = counter_clone.load(Ordering::Relaxed);
            if count >= TOTAL_QUERIES { break; }
            
            let elapsed = start.elapsed().as_secs_f64();
            let qps = count as f64 / elapsed;
            let remaining = TOTAL_QUERIES - count;
            let eta = remaining as f64 / qps;
            
            println!("   {:>6} queries | {:>8.0} q/s | ETA: {:.0}s", count, qps, eta);
        }
    });
    
    // Wait for all workers
    for handle in handles {
        handle.await?;
    }
    progress.abort();
    
    let elapsed = start.elapsed();
    let qps = TOTAL_QUERIES as f64 / elapsed.as_secs_f64();
    
    println!("\n📈 FINAL RESULTS:");
    println!("┌──────────────────────────────────────────────────┐");
    println!("│ QAIL POOL BENCHMARK (PREPARED)                   │");
    println!("├──────────────────────────────────────────────────┤");
    println!("│ Total Time:               {:>15.1}s │", elapsed.as_secs_f64());
    println!("│ Queries/Second:           {:>15.0} │", qps);
    println!("│ Workers:                  {:>15} │", NUM_WORKERS);
    println!("│ Pool Size:                {:>15} │", POOL_SIZE);
    println!("│ Queries Completed:        {:>15} │", counter.load(Ordering::Relaxed));
    println!("└──────────────────────────────────────────────────┘");
    
    Ok(())
}
