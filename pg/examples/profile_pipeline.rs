//! PROFILING BENCHMARK - Identify bottlenecks
//!
//! Measures time spent in each phase:
//! 1. Encoding (AST → wire bytes)
//! 2. Sending (network write)
//! 3. Receiving + Parsing (network read + decode)
//!
//! Run: cargo run --release --example profile_pipeline

use qail_core::ast::Qail;
use qail_pg::PgConnection;
use std::time::Instant;

const QUERIES_PER_BATCH: usize = 1000;
const BATCHES: usize = 100; // Smaller for profiling

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = PgConnection::connect("127.0.0.1", 5432, "orion", "swb_staging_local").await?;

    println!("🔬 PIPELINE PROFILING BENCHMARK");
    println!("================================");
    println!("Queries per batch: {}", QUERIES_PER_BATCH);
    println!("Batches: {}", BATCHES);
    println!("Total: {}\n", QUERIES_PER_BATCH * BATCHES);

    let mut total_encode_time = std::time::Duration::ZERO;
    let mut total_send_time = std::time::Duration::ZERO;
    let mut total_recv_time = std::time::Duration::ZERO;

    // Build commands once (don't include in timing)
    let cmds: Vec<Qail> = (1..=QUERIES_PER_BATCH)
        .map(|i| {
            let limit = (i % 10) + 1;
            Qail::get("harbors")
                .columns(["id", "name"])
                .limit(limit as i64)
        })
        .collect();

    for batch in 0..BATCHES {
        if batch % 10 == 0 {
            println!("   Batch {}/{}", batch, BATCHES);
        }

        // Phase 1: Encode
        let encode_start = Instant::now();
        let buf = qail_pg::protocol::AstEncoder::encode_batch(&cmds);
        total_encode_time += encode_start.elapsed();

        // Phase 2: Send (using raw stream access)
        let send_start = Instant::now();
        conn.send_bytes(&buf).await?;
        total_send_time += send_start.elapsed();

        // Phase 3: Receive and parse
        let recv_start = Instant::now();
        let mut queries_completed = 0;
        loop {
            let msg = conn.recv().await?;
            match msg {
                qail_pg::protocol::BackendMessage::ParseComplete
                | qail_pg::protocol::BackendMessage::BindComplete
                | qail_pg::protocol::BackendMessage::RowDescription(_)
                | qail_pg::protocol::BackendMessage::DataRow(_) => {}
                qail_pg::protocol::BackendMessage::CommandComplete(_)
                | qail_pg::protocol::BackendMessage::NoData => {
                    queries_completed += 1;
                }
                qail_pg::protocol::BackendMessage::ReadyForQuery(_) => {
                    if queries_completed == QUERIES_PER_BATCH {
                        break;
                    }
                }
                qail_pg::protocol::BackendMessage::ErrorResponse(e) => {
                    return Err(format!("Error: {}", e.message).into());
                }
                _ => {}
            }
        }
        total_recv_time += recv_start.elapsed();
    }

    let total = total_encode_time + total_send_time + total_recv_time;
    let total_queries = QUERIES_PER_BATCH * BATCHES;

    println!("\n📊 TIMING BREAKDOWN:");
    println!("┌────────────────────────────────────────────────────┐");
    println!("│ Phase          │ Time       │ Per Query │ % Total │");
    println!("├────────────────────────────────────────────────────┤");
    println!(
        "│ Encoding       │ {:>8.2}ms │ {:>7}ns │ {:>5.1}%  │",
        total_encode_time.as_secs_f64() * 1000.0,
        total_encode_time.as_nanos() / total_queries as u128,
        (total_encode_time.as_secs_f64() / total.as_secs_f64()) * 100.0
    );
    println!(
        "│ Sending        │ {:>8.2}ms │ {:>7}ns │ {:>5.1}%  │",
        total_send_time.as_secs_f64() * 1000.0,
        total_send_time.as_nanos() / total_queries as u128,
        (total_send_time.as_secs_f64() / total.as_secs_f64()) * 100.0
    );
    println!(
        "│ Recv+Parse     │ {:>8.2}ms │ {:>7}ns │ {:>5.1}%  │",
        total_recv_time.as_secs_f64() * 1000.0,
        total_recv_time.as_nanos() / total_queries as u128,
        (total_recv_time.as_secs_f64() / total.as_secs_f64()) * 100.0
    );
    println!("├────────────────────────────────────────────────────┤");
    println!(
        "│ TOTAL          │ {:>8.2}ms │ {:>7}ns │ 100.0%  │",
        total.as_secs_f64() * 1000.0,
        total.as_nanos() / total_queries as u128
    );
    println!("└────────────────────────────────────────────────────┘");

    let qps = total_queries as f64 / total.as_secs_f64();
    println!("\n📈 Queries/second: {:.0}", qps);
    println!("\n💡 Go pgx does 321,787 q/s - we need to optimize the slowest phase!");

    Ok(())
}
