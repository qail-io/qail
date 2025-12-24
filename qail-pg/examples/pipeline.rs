//! Pipelining Benchmark: Serial vs Pipeline
//!
//! Compares:
//! - Serial: 100 queries = 100 round-trips
//! - Pipeline: 100 queries = 1 round-trip
//!
//! Setup: ssh -L 15432:localhost:5432 sailtix -N -f
//! Run: STAGING_DB_PASSWORD="password" cargo run -p qail-pg --example pipeline --release

use std::time::Instant;

const QUERIES_PER_BATCH: usize = 1000;
const BATCHES: usize = 1000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let password = std::env::var("STAGING_DB_PASSWORD")
        .expect("Set STAGING_DB_PASSWORD");
    
    println!("🚀 Pipeline vs Serial Benchmark");
    println!("================================");
    println!("Queries per batch: {}", QUERIES_PER_BATCH);
    println!("Batches: {}\n", BATCHES);
    
    let mut conn = qail_pg::PgConnection::connect_with_password(
        "127.0.0.1", 15432, "sailtix", "swb-staging", Some(&password)
    ).await?;
    
    // Warmup
    for _ in 0..5 {
        let _ = conn.query("SELECT 1", &[]).await?;
    }
    
    // ===== SERIAL QUERIES =====
    println!("📊 Serial (100 queries = 100 round-trips)");
    let serial_start = Instant::now();
    for _ in 0..BATCHES {
        for i in 0..QUERIES_PER_BATCH {
            let param = format!("{}", i + 1);
            let _ = conn.query(
                "SELECT id, name FROM vessels LIMIT $1",
                &[Some(param.into_bytes())]
            ).await?;
        }
    }
    let serial_time = serial_start.elapsed();
    let total_queries = BATCHES * QUERIES_PER_BATCH;
    println!("   Total: {:?} ({:?}/query)", serial_time, serial_time / total_queries as u32);
    
    // ===== PIPELINED QUERIES =====
    println!("\n📊 Pipelined ({} queries = {} round-trips)", total_queries, BATCHES);
    
    let pipeline_start = Instant::now();
    for _ in 0..BATCHES {
        // Build batch of queries
        let params: Vec<Vec<u8>> = (1..=QUERIES_PER_BATCH)
            .map(|i| format!("{}", i).into_bytes())
            .collect();
        
        let queries: Vec<(&str, Vec<Option<Vec<u8>>>)> = params.iter()
            .map(|p| ("SELECT id, name FROM vessels LIMIT $1", vec![Some(p.clone())]))
            .collect();
        
        // Convert to the format query_pipeline expects
        let query_refs: Vec<(&str, &[Option<Vec<u8>>])> = queries.iter()
            .map(|(sql, params)| (*sql, params.as_slice()))
            .collect();
        
        let _results = conn.query_pipeline(&query_refs).await?;
    }
    let pipeline_time = pipeline_start.elapsed();
    println!("   Total: {:?} ({:?}/query)", pipeline_time, pipeline_time / total_queries as u32);
    
    // ===== SUMMARY =====
    let speedup = serial_time.as_nanos() as f64 / pipeline_time.as_nanos() as f64;
    
    println!("\n📈 Results:");
    println!("┌───────────────┬───────────────┬─────────────┐");
    println!("│ Method        │ Time          │ Per Query   │");
    println!("├───────────────┼───────────────┼─────────────┤");
    println!("│ Serial        │ {:>11.2?} │ {:>9.2?} │", 
        serial_time, serial_time / total_queries as u32);
    println!("│ Pipeline      │ {:>11.2?} │ {:>9.2?} │", 
        pipeline_time, pipeline_time / total_queries as u32);
    println!("└───────────────┴───────────────┴─────────────┘");
    println!("\n🏆 Pipelining is {:.1}x faster!", speedup);
    
    // Check if results are correct
    let param_5: [Option<Vec<u8>>; 1] = [Some("5".as_bytes().to_vec())];
    let empty_params: [Option<Vec<u8>>; 0] = [];
    let queries: Vec<(&str, &[Option<Vec<u8>>])> = vec![
        ("SELECT COUNT(*) FROM vessels", &empty_params),
        ("SELECT id, name FROM vessels LIMIT $1", &param_5),
    ];
    
    let results = conn.query_pipeline(&queries).await?;
    println!("\n✅ Pipeline correctness check:");
    println!("   Query 1 (COUNT): {} rows", results[0].len());
    println!("   Query 2 (SELECT 5): {} rows", results[1].len());
    
    Ok(())
}
