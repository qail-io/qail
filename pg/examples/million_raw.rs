//! RAW SQL BENCHMARK - No Qail overhead
//!
//! Tests absolute maximum by writing SQL directly to wire protocol.
//! This bypasses ALL QAIL overhead to find the I/O ceiling.
//!
//! Run: cargo run --release --example million_raw

use bytes::BytesMut;
use qail_pg::PgConnection;
use std::time::Instant;

const TOTAL_QUERIES: usize = 1_000_000;
const QUERIES_PER_BATCH: usize = 1_000;
const BATCHES: usize = TOTAL_QUERIES / QUERIES_PER_BATCH;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = PgConnection::connect("127.0.0.1", 5432, "orion", "swb_staging_local").await?;

    println!("🚀 RAW SQL MILLION QUERY BENCHMARK");
    println!("===================================");
    println!("Total queries:    {:>12}", TOTAL_QUERIES);
    println!("Batch size:       {:>12}", QUERIES_PER_BATCH);
    println!("Batches:          {:>12}", BATCHES);
    println!("\n⚠️  BYPASSING QAIL AST - PURE SQL!\n");

    // Build raw SQL wire bytes directly (NO QAIL AT ALL)
    let mut sql_buf = BytesMut::with_capacity(QUERIES_PER_BATCH * 50);
    for i in 1..=QUERIES_PER_BATCH {
        let limit = (i % 10) + 1;
        sql_buf.extend_from_slice(b"SELECT id,name FROM harbors LIMIT ");
        sql_buf.extend_from_slice(limit.to_string().as_bytes());
        sql_buf.extend_from_slice(b";");
    }

    // Build simple query message
    let sql_len = sql_buf.len();
    let msg_len = (4 + sql_len + 1) as i32;
    let mut wire_bytes = BytesMut::with_capacity(1 + 4 + sql_len + 1);
    wire_bytes.extend_from_slice(&[b'Q']);
    wire_bytes.extend_from_slice(&msg_len.to_be_bytes());
    wire_bytes.extend_from_slice(&sql_buf);
    wire_bytes.extend_from_slice(&[0]);

    println!("Wire bytes size: {} KB", wire_bytes.len() / 1024);
    println!("\n📊 Pipelining 1,000,000 queries via RAW SQL...");

    let start = Instant::now();
    let mut successful_queries = 0;

    for batch in 0..BATCHES {
        if batch % 100 == 0 {
            println!("   Batch {}/{}", batch, BATCHES);
        }

        let count = conn
            .pipeline_simple_bytes_fast(&wire_bytes, QUERIES_PER_BATCH)
            .await?;
        successful_queries += count;
    }

    let elapsed = start.elapsed();
    let qps = TOTAL_QUERIES as f64 / elapsed.as_secs_f64();
    let per_query_ns = elapsed.as_nanos() / TOTAL_QUERIES as u128;

    println!("\n📈 Results:");
    println!("┌──────────────────────────────────────────┐");
    println!("│ RAW SQL - ONE MILLION QUERIES            │");
    println!("├──────────────────────────────────────────┤");
    println!("│ Total Time:     {:>23.2}s │", elapsed.as_secs_f64());
    println!("│ Queries/Second: {:>23.0} │", qps);
    println!("│ Per Query:      {:>20}ns │", per_query_ns);
    println!("│ Successful:     {:>23} │", successful_queries);
    println!("└──────────────────────────────────────────┘");

    println!("\n📊 vs Go pgx (322,703 q/s):");
    if qps > 322703.0 {
        println!("   🎉 QAIL RAW is {:.2}x FASTER than Go!", qps / 322703.0);
    } else {
        println!("   Go is {:.2}x faster", 322703.0 / qps);
    }

    println!("\n📊 vs QAIL AST (99,229 q/s):");
    println!("   Raw SQL is {:.2}x faster than AST", qps / 99229.0);

    Ok(())
}
