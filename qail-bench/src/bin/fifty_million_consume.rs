//! QAIL 10 Million Query Benchmark - ULTRA-FAST RESULT CONSUMPTION
//!
//! Uses prepared statement + pipeline with ULTRA-OPTIMIZED 2-column API.
//! - Zero-copy Bytes
//! - Fixed tuple instead of Vec per row
//! - Inline column parsing
//!
//! ## Run
//!
//! ```bash
//! cargo run --release --bin fifty_million_consume
//! ```

use std::time::Instant;
use std::env;
use qail_pg::PgConnection;

const TOTAL_QUERIES: usize = 10_000_000;
const QUERIES_PER_BATCH: usize = 1_000;
const BATCHES: usize = TOTAL_QUERIES / QUERIES_PER_BATCH;

fn get_env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = get_env_or("PG_HOST", "127.0.0.1");
    let port: u16 = get_env_or("PG_PORT", "5432").parse()?;
    let user = get_env_or("PG_USER", "postgres");
    let database = get_env_or("PG_DATABASE", "postgres");
    
    println!("🔌 Connecting to {}:{} as {}", host, port, user);
    
    let mut conn = PgConnection::connect(&host, port, &user, &database).await?;
    
    println!("🚀 10 MILLION QUERY BENCHMARK - ULTRA-FAST RESULT CONSUMPTION");
    println!("==============================================================");
    println!("Total queries:    {:>15}", TOTAL_QUERIES);
    println!("Batch size:       {:>15}", QUERIES_PER_BATCH);
    println!("Batches:          {:>15}", BATCHES);
    println!("\n⚠️  ULTRA-FAST API: Fixed 2-column tuples, zero-copy Bytes\n");
    
    let stmt = conn.prepare("SELECT id, name FROM harbors LIMIT $1").await?;
    println!("✅ Statement prepared: {}", stmt.name());
    
    let params_batch: Vec<Vec<Option<Vec<u8>>>> = (1..=QUERIES_PER_BATCH)
        .map(|i| {
            let limit = ((i % 10) + 1).to_string();
            vec![Some(limit.into_bytes())]
        })
        .collect();
    println!("✅ Params pre-built\n");
    
    println!("📊 Executing {} queries with ULTRA-FAST result consumption...\n", TOTAL_QUERIES);
    
    let start = Instant::now();
    let mut successful_queries: usize = 0;
    let mut total_rows_read: usize = 0;
    let mut last_report = Instant::now();
    
    for batch in 0..BATCHES {
        // ULTRA-FAST API
        let results = conn.pipeline_prepared_ultra(&stmt, &params_batch).await?;
        
        // Consume results - (col0, col1) tuples
        for query_rows in &results {
            successful_queries += 1;
            for (id_bytes, name_bytes) in query_rows {
                // Parse directly from Bytes tuple
                let _id: i64 = std::str::from_utf8(id_bytes)
                    .unwrap_or("0")
                    .parse()
                    .unwrap_or(0);
                let _name = std::str::from_utf8(name_bytes).unwrap_or("");
                total_rows_read += 1;
            }
        }
        
        if successful_queries % 1_000_000 == 0 || last_report.elapsed().as_secs() >= 5 {
            let elapsed = start.elapsed();
            let qps = successful_queries as f64 / elapsed.as_secs_f64();
            let remaining = TOTAL_QUERIES - successful_queries;
            let eta = remaining as f64 / qps;
            
            println!("{:>6}M queries | {:>8.0} q/s | ETA: {:.0}s | Rows: {} | Batch {}/{}",
                successful_queries / 1_000_000,
                qps,
                eta,
                total_rows_read,
                batch + 1,
                BATCHES
            );
            last_report = Instant::now();
        }
    }
    
    let elapsed = start.elapsed();
    let qps = TOTAL_QUERIES as f64 / elapsed.as_secs_f64();
    let per_query_ns = (elapsed.as_nanos() as f64) / (TOTAL_QUERIES as f64);
    
    println!("\n📈 FINAL RESULTS:");
    println!("┌──────────────────────────────────────────────────┐");
    println!("│ 10M QUERY - QAIL ULTRA-FAST CONSUMPTION          │");
    println!("├──────────────────────────────────────────────────┤");
    println!("│ Total Time:               {:>15.1}s │", elapsed.as_secs_f64());
    println!("│ Queries/Second:           {:>15.0} │", qps);
    println!("│ Per Query:                {:>12.0}ns │", per_query_ns);
    println!("│ Successful:               {:>15} │", successful_queries);
    println!("│ Rows Parsed:              {:>15} │", total_rows_read);
    println!("│ Avg Rows/Query:           {:>15.1} │", total_rows_read as f64 / successful_queries as f64);
    println!("└──────────────────────────────────────────────────┘");
    
    println!("\n🦀 ULTRA-FAST OPTIMIZATIONS:");
    println!("   - Zero-copy Bytes (ref-counted slices)");
    println!("   - Fixed tuple (col0, col1) - no Vec per row");
    println!("   - Inline column parsing - no function calls");
    
    Ok(())
}
