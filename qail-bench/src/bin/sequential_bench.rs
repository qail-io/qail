//! QAIL Sequential Query Benchmark
//!
//! Fair comparison: sequential queries, no pipelining.

use std::time::Instant;
use qail_pg::PgConnection;

const TOTAL_QUERIES: usize = 1_000_000;
const REPORT_INTERVAL: usize = 100_000;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔌 Connecting...");
    
    let mut conn = PgConnection::connect("127.0.0.1", 5432, "orion", "postgres").await?;
    println!("✅ Connected");
    
    println!("\n🚀 RUST SEQUENTIAL QUERY BENCHMARK");
    println!("=======================================================");
    println!("Total queries:    {:>15}", TOTAL_QUERIES);
    println!("\n⚠️  Sequential execution (no pipelining)\n");
    
    println!("📊 Executing queries...\n");
    
    let start = Instant::now();
    let mut successful: usize = 0;
    
    // Params for cached query
    let params: Vec<Option<Vec<u8>>> = vec![Some(b"10".to_vec())];
    
    for i in 0..TOTAL_QUERIES {
        // Execute one query at a time using cached prepared statement
        let _rows = conn.query_cached("SELECT id, name FROM harbors LIMIT $1", &params).await?;
        successful += 1;
        
        if (i + 1) % REPORT_INTERVAL == 0 {
            let elapsed = start.elapsed().as_secs_f64();
            let qps = successful as f64 / elapsed;
            let remaining = TOTAL_QUERIES - successful;
            let eta = remaining as f64 / qps;
            let pct = successful * 100 / TOTAL_QUERIES;
            
            println!("   {:>8} queries | {:>8.0} q/s | ETA: {:.0}s | {}%",
                successful, qps, eta, pct);
        }
    }
    
    let elapsed = start.elapsed();
    let qps = TOTAL_QUERIES as f64 / elapsed.as_secs_f64();
    let per_query_ns = elapsed.as_nanos() as f64 / TOTAL_QUERIES as f64;
    
    println!("\n📈 FINAL RESULTS:");
    println!("┌──────────────────────────────────────────────────┐");
    println!("│ SEQUENTIAL QUERIES (native Rust)                 │");
    println!("├──────────────────────────────────────────────────┤");
    println!("│ Total Time:        {:>20.1}s │", elapsed.as_secs_f64());
    println!("│ Queries/Second:    {:>20.0} │", qps);
    println!("│ Per Query:         {:>17.0}ns │", per_query_ns);
    println!("│ Successful:        {:>20} │", successful);
    println!("│ Mode: Sequential (no pipelining)                 │");
    println!("└──────────────────────────────────────────────────┘");
    
    Ok(())
}
