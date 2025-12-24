//! MILLION Query Pipelining Benchmark
//!
//! Tests 1 MILLION queries using pipelining.
//! Serial would take ~10 hours so we skip it.
//!
//! Setup: ssh -L 15432:localhost:5432 sailtix -N -f
//! Run: STAGING_DB_PASSWORD="password" cargo run -p qail-pg --example million --release

use std::time::Instant;

const QUERIES_PER_BATCH: usize = 1000;
const BATCHES: usize = 1000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let password = std::env::var("STAGING_DB_PASSWORD")
        .expect("Set STAGING_DB_PASSWORD");
    
    let total_queries = BATCHES * QUERIES_PER_BATCH;
    
    println!("🚀 ONE MILLION QUERY BENCHMARK");
    println!("==============================");
    println!("Total queries: {:>12}", format_number(total_queries));
    println!("Batch size: {:>15}", format_number(QUERIES_PER_BATCH));
    println!("Batches: {:>18}\n", format_number(BATCHES));
    
    // Note: Serial would take ~10 hours at 37ms/query
    let estimated_serial = total_queries as f64 * 0.037;
    println!("⚠️  Skipping serial test (would take ~{:.0} hours)", estimated_serial / 3600.0);
    
    let mut conn = qail_pg::PgConnection::connect_with_password(
        "127.0.0.1", 15432, "sailtix", "swb-staging", Some(&password)
    ).await?;
    
    // Warmup
    for _ in 0..5 {
        let _ = conn.query("SELECT 1", &[]).await?;
    }
    
    // ===== PIPELINED QUERIES =====
    println!("\n📊 Pipelining {} queries in {} batches...", 
        format_number(total_queries), format_number(BATCHES));
    
    let pipeline_start = Instant::now();
    let mut successful_queries = 0;
    
    for batch in 0..BATCHES {
        if batch % 100 == 0 {
            println!("   Batch {}/{}", batch, BATCHES);
        }
        
        // Build batch of queries
        let params: Vec<Vec<u8>> = (1..=QUERIES_PER_BATCH)
            .map(|i| format!("{}", (i % 36) + 1).into_bytes()) // LIMIT 1-36
            .collect();
        
        let queries: Vec<(&str, Vec<Option<Vec<u8>>>)> = params.iter()
            .map(|p| ("SELECT id, name FROM vessels LIMIT $1", vec![Some(p.clone())]))
            .collect();
        
        let query_refs: Vec<(&str, &[Option<Vec<u8>>])> = queries.iter()
            .map(|(sql, params)| (*sql, params.as_slice()))
            .collect();
        
        let results = conn.query_pipeline(&query_refs).await?;
        successful_queries += results.len();
    }
    
    let pipeline_time = pipeline_start.elapsed();
    
    // ===== SUMMARY =====
    let qps = total_queries as f64 / pipeline_time.as_secs_f64();
    let per_query_ns = pipeline_time.as_nanos() / total_queries as u128;
    
    println!("\n📈 Results:");
    println!("┌──────────────────────────────────────────┐");
    println!("│ ONE MILLION QUERIES                      │");
    println!("├──────────────────────────────────────────┤");
    println!("│ Total Time:     {:>23.2?} │", pipeline_time);
    println!("│ Queries/Second: {:>23} │", format_number(qps as usize));
    println!("│ Per Query:      {:>20}ns │", format_number(per_query_ns as usize));
    println!("│ Successful:     {:>23} │", format_number(successful_queries));
    println!("└──────────────────────────────────────────┘");
    
    // Compare to theoretical serial
    let theoretical_serial_secs = total_queries as f64 * 0.037;
    let speedup = theoretical_serial_secs / pipeline_time.as_secs_f64();
    
    println!("\n🏆 vs Serial (37ms/query):");
    println!("   Serial estimate:  {:.0} seconds ({:.1} hours)", 
        theoretical_serial_secs, theoretical_serial_secs / 3600.0);
    println!("   Pipeline actual:  {:.1} seconds", pipeline_time.as_secs_f64());
    println!("   Speedup:          {:.0}x faster!", speedup);
    
    Ok(())
}

fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}
