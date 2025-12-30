//! GIANT BATCH - Send ALL 1M queries in ONE batch
//!
//! Tests maximum throughput by eliminating ALL sync overhead.
//!
//! Run: cargo run --release --example million_giant

use qail_core::ast::Qail;
use qail_pg::PgConnection;
use std::time::Instant;

const TOTAL_QUERIES: usize = 100_000; // Smaller for testing
const QUERIES_PER_BATCH: usize = 100_000; // ALL in one batch!

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = PgConnection::connect("127.0.0.1", 5432, "orion", "swb_staging_local").await?;

    println!("🚀 GIANT BATCH BENCHMARK");
    println!("=========================");
    println!("Total queries:    {:>12}", TOTAL_QUERIES);
    println!("Batch size:       {:>12} (ALL IN ONE!)", QUERIES_PER_BATCH);
    println!("\n⚠️  ONE GIANT BATCH - ZERO SYNC OVERHEAD!\n");

    // Build ALL commands
    let cmds: Vec<Qail> = (1..=TOTAL_QUERIES)
        .map(|i| {
            let limit = (i % 10) + 1;
            Qail::get("harbors")
                .columns(["id", "name"])
                .limit(limit as i64)
        })
        .collect();

    // Pre-encode ONCE
    let wire_bytes = qail_pg::protocol::AstEncoder::encode_batch_simple(&cmds);
    println!("Wire bytes size: {} KB", wire_bytes.len() / 1024);

    println!("\n📊 Sending {} queries in ONE batch...", TOTAL_QUERIES);

    let start = Instant::now();

    let count = conn
        .pipeline_simple_bytes_fast(&wire_bytes, TOTAL_QUERIES)
        .await?;

    let elapsed = start.elapsed();
    let qps = TOTAL_QUERIES as f64 / elapsed.as_secs_f64();
    let per_query_ns = elapsed.as_nanos() / TOTAL_QUERIES as u128;

    println!("\n📈 Results:");
    println!("┌──────────────────────────────────────────┐");
    println!("│ GIANT BATCH - {} QUERIES       │", TOTAL_QUERIES);
    println!("├──────────────────────────────────────────┤");
    println!("│ Total Time:     {:>23.3}s │", elapsed.as_secs_f64());
    println!("│ Queries/Second: {:>23.0} │", qps);
    println!("│ Per Query:      {:>20}ns │", per_query_ns);
    println!("│ Successful:     {:>23} │", count);
    println!("└──────────────────────────────────────────┘");

    println!("\n📊 vs Go pgx (321,787 q/s):");
    if qps > 321787.0 {
        println!("   🎉 QAIL is {:.2}x FASTER than Go!", qps / 321787.0);
    } else {
        println!("   Go is {:.2}x faster", 321787.0 / qps);
    }

    Ok(())
}
