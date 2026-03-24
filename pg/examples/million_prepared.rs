//! ZERO-LOOKUP PREPARED STATEMENT BENCHMARK
//!
//! Uses pre-computed PreparedStatement handle to eliminate:
//! - Hash computation per query
//! - HashMap lookup per query
//!
//! Run: cargo run --release --example million_prepared

use qail_pg::PgConnection;
use std::time::Instant;

const TOTAL_QUERIES: usize = 1_000_000;
const QUERIES_PER_BATCH: usize = 1_000;
const BATCHES: usize = TOTAL_QUERIES / QUERIES_PER_BATCH;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = PgConnection::connect("127.0.0.1", 5432, "orion", "example_staging").await?;

    println!("🚀 ZERO-LOOKUP PREPARED STATEMENT BENCHMARK");
    println!("=============================================");
    println!("Total queries:    {:>12}", TOTAL_QUERIES);
    println!("Batch size:       {:>12}", QUERIES_PER_BATCH);
    println!("Batches:          {:>12}", BATCHES);
    println!("\n⚠️  NO HASH, NO LOOKUP PER QUERY!\n");

    // PREPARE ONCE (outside timing!)
    let stmt = conn
        .prepare("SELECT id, name FROM harbors LIMIT $1")
        .await?;
    println!("✅ Statement prepared: {}", stmt.name());

    // Build params batch ONCE (outside timing!)
    let params_batch: Vec<Vec<Option<Vec<u8>>>> = (1..=QUERIES_PER_BATCH)
        .map(|i| {
            let limit = ((i % 10) + 1).to_string();
            vec![Some(limit.into_bytes())]
        })
        .collect();

    println!("\n📊 Pipelining 1,000,000 queries via ZERO-LOOKUP prepared statements...");

    let start = Instant::now();
    let mut successful_queries = 0;

    for batch in 0..BATCHES {
        if batch % 100 == 0 {
            println!("   Batch {}/{}", batch, BATCHES);
        }

        // Execute using ZERO-LOOKUP pipeline
        let count = conn
            .pipeline_execute_prepared_count(&stmt, &params_batch)
            .await?;
        successful_queries += count;
    }

    let elapsed = start.elapsed();
    let qps = TOTAL_QUERIES as f64 / elapsed.as_secs_f64();
    let per_query_ns = elapsed.as_nanos() / TOTAL_QUERIES as u128;

    println!("\n📈 Results:");
    println!("┌──────────────────────────────────────────┐");
    println!("│ ZERO-LOOKUP - ONE MILLION QUERIES        │");
    println!("├──────────────────────────────────────────┤");
    println!("│ Total Time:     {:>23.2}s │", elapsed.as_secs_f64());
    println!("│ Queries/Second: {:>23.0} │", qps);
    println!("│ Per Query:      {:>20}ns │", per_query_ns);
    println!("│ Successful:     {:>23} │", successful_queries);
    println!("└──────────────────────────────────────────┘");

    println!("\n📊 vs QAIL Cached (275,471 q/s):");
    let cached_speedup = qps / 275471.0;
    if cached_speedup > 1.0 {
        println!("   🎉 Zero-lookup is {:.2}x faster!", cached_speedup);
    } else {
        println!("   Cached is {:.2}x faster", 1.0 / cached_speedup);
    }

    println!("\n📊 vs Go pgx (322,703 q/s):");
    if qps > 322703.0 {
        println!("   🎉 QAIL beats Go by {:.2}x!", qps / 322703.0);
    } else if qps > 290000.0 {
        println!("   QAIL is within 10% of Go! ({:.2}x)", qps / 322703.0);
    } else {
        println!("   Go is {:.2}x faster", 322703.0 / qps);
    }

    Ok(())
}
