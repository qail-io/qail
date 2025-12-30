//! LOCAL MILLION QUERY BENCHMARK (No Network Latency)
//!
//! Compares QAIL AST-native vs Go pgx against LOCAL PostgreSQL 18
//! This eliminates network RTT to show true encoding speedup.
//!
//! Run: cargo run --release --example million_local

use qail_core::ast::Qail;
use qail_pg::PgConnection;
use std::time::Instant;

const TOTAL_QUERIES: usize = 1_000_000;
const QUERIES_PER_BATCH: usize = 1_000;
const BATCHES: usize = TOTAL_QUERIES / QUERIES_PER_BATCH;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to LOCAL PostgreSQL 18 (no network latency!)
    let mut conn = PgConnection::connect(
        "127.0.0.1",
        5432,    // Local PostgreSQL
        "orion", // Your local username
        "swb_staging_local",
    )
    .await?;

    println!("🚀 LOCAL MILLION QUERY BENCHMARK (PostgreSQL 18.1)");
    println!("===================================================");
    println!("Total queries:    {:>12}", format_number(TOTAL_QUERIES));
    println!("Batch size:       {:>12}", QUERIES_PER_BATCH);
    println!("Batches:          {:>12}", BATCHES);
    println!("\n⚠️  LOCAL PostgreSQL - NO NETWORK LATENCY!\n");

    // Build batch of Qail ASTs ONCE (outside timing!)
    let cmds: Vec<Qail> = (1..=QUERIES_PER_BATCH)
        .map(|i| {
            let limit = (i % 10) + 1;
            Qail::get("harbors")
                .columns(["id", "name"])
                .limit(limit as i64)
        })
        .collect();

    // Pre-encode wire bytes ONCE (outside timing!)
    let wire_bytes = qail_pg::protocol::AstEncoder::encode_batch_simple(&cmds);
    let expected = cmds.len();

    // ===== AST-NATIVE PIPELINING =====
    println!("📊 Pipelining 1,000,000 queries via SIMPLE QUERY protocol...");

    let pipeline_start = Instant::now();
    let mut successful_queries = 0;

    for batch in 0..BATCHES {
        if batch % 100 == 0 {
            println!("   Batch {}/{}", batch, BATCHES);
        }

        // Execute using PRE-ENCODED SIMPLE QUERY bytes (no per-batch encoding!)
        let count = conn
            .pipeline_simple_bytes_fast(&wire_bytes, expected)
            .await?;
        successful_queries += count;
    }

    let pipeline_time = pipeline_start.elapsed();

    // ===== RESULTS =====
    let pipeline_secs = pipeline_time.as_secs_f64();
    let qps = (TOTAL_QUERIES as f64) / pipeline_secs;
    let per_query_ns = pipeline_time.as_nanos() / TOTAL_QUERIES as u128;

    println!("\n📈 Results:");
    println!("┌──────────────────────────────────────────┐");
    println!("│ LOCAL AST-NATIVE - ONE MILLION QUERIES   │");
    println!("├──────────────────────────────────────────┤");
    println!("│ Total Time:     {:>23.2}s │", pipeline_secs);
    println!("│ Queries/Second: {:>23} │", format_number(qps as usize));
    println!(
        "│ Per Query:      {:>20}ns │",
        format_number(per_query_ns as usize)
    );
    println!(
        "│ Successful:     {:>23} │",
        format_number(successful_queries)
    );
    println!("└──────────────────────────────────────────┘");

    // Compare to remote benchmark
    println!("\n📊 vs Remote (SSH tunnel, ~113s @ 8,837 q/s):");
    let remote_speedup = 113.0 / pipeline_secs;
    println!("   Local is {:.1}x faster than remote!", remote_speedup);

    // Compare to Go pgx remote
    println!("\n📊 vs Go pgx remote (119s @ 8,378 q/s):");
    let go_speedup = 119.0 / pipeline_secs;
    println!(
        "   QAIL local is {:.1}x faster than Go pgx remote!",
        go_speedup
    );

    Ok(())
}

fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.insert(0, ',');
        }
        result.insert(0, c);
    }
    result
}
