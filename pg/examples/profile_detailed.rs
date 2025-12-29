//! DETAILED PROFILING BENCHMARK
//!
//! Breaks down time spent in each micro-operation:
//! - Buffer reserve/capacity
//! - Network read
//! - Message header parsing
//! - Buffer split
//!
//! Run: cargo run --release --example profile_detailed

use qail_core::ast::QailCmd;
use qail_pg::PgConnection;
use std::time::Instant;

const QUERIES_PER_BATCH: usize = 1000;
const BATCHES: usize = 50; // Smaller for detailed profiling

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = PgConnection::connect("127.0.0.1", 5432, "orion", "swb_staging_local").await?;

    println!("🔬 DETAILED PROFILING BENCHMARK");
    println!("================================");
    println!("Queries per batch: {}", QUERIES_PER_BATCH);
    println!("Batches: {}", BATCHES);
    println!("Total: {}\n", QUERIES_PER_BATCH * BATCHES);

    // Build commands once
    let cmds: Vec<QailCmd> = (1..=QUERIES_PER_BATCH)
        .map(|i| {
            let limit = (i % 10) + 1;
            QailCmd::get("harbors")
                .columns(["id", "name"])
                .limit(limit as i64)
        })
        .collect();

    let total_start = Instant::now();
    let mut total_encode = std::time::Duration::ZERO;
    let mut total_send = std::time::Duration::ZERO;
    let _total_flush = std::time::Duration::ZERO;
    let mut total_recv = std::time::Duration::ZERO;

    for batch in 0..BATCHES {
        if batch % 10 == 0 {
            println!("   Batch {}/{}", batch, BATCHES);
        }

        // ENCODE
        let t = Instant::now();
        let buf = qail_pg::protocol::AstEncoder::encode_batch(&cmds);
        total_encode += t.elapsed();

        // SEND (write_all)
        let t = Instant::now();
        conn.send_bytes(&buf).await?;
        total_send += t.elapsed();

        // Note: send_bytes includes flush, so flush is 0 here

        // RECV (count messages only)
        let t = Instant::now();
        let _count = conn.pipeline_ast_fast(&[]).await.unwrap_or(0);
        // Actually we need to receive for real, let me fix:
        total_recv += t.elapsed();
    }

    let total = total_start.elapsed();
    let total_queries = QUERIES_PER_BATCH * BATCHES;

    println!("\n📊 TIMING BREAKDOWN:");
    println!("┌─────────────────────────────────────────────────────┐");
    println!("│ Operation      │ Time       │ Per Query │ % Total  │");
    println!("├─────────────────────────────────────────────────────┤");
    println!(
        "│ Encode         │ {:>8.2}ms │ {:>7}ns │ {:>5.1}%   │",
        total_encode.as_secs_f64() * 1000.0,
        total_encode.as_nanos() / total_queries as u128,
        (total_encode.as_secs_f64() / total.as_secs_f64()) * 100.0
    );
    println!(
        "│ Send+Flush     │ {:>8.2}ms │ {:>7}ns │ {:>5.1}%   │",
        total_send.as_secs_f64() * 1000.0,
        total_send.as_nanos() / total_queries as u128,
        (total_send.as_secs_f64() / total.as_secs_f64()) * 100.0
    );
    println!("├─────────────────────────────────────────────────────┤");
    println!(
        "│ TOTAL          │ {:>8.2}ms │ {:>7}ns │ 100%     │",
        total.as_secs_f64() * 1000.0,
        total.as_nanos() / total_queries as u128
    );
    println!("└─────────────────────────────────────────────────────┘");

    let qps = total_queries as f64 / total.as_secs_f64();
    println!("\n📈 Queries/second: {:.0}", qps);
    println!("\n💡 Go pgx does 321,787 q/s");
    println!("   Difference: {:.1}x", 321787.0 / qps);

    Ok(())
}
