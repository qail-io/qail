//! CACHED PREPARED STATEMENT BENCHMARK
//!
//! Uses Parse-once, Bind+Execute-many pattern like Go pgx.
//! This should match Go's 322k q/s performance.
//!
//! Run: cargo run --release --example million_cached

use qail_core::ast::QailCmd;
use qail_pg::PgConnection;
use std::time::Instant;

const TOTAL_QUERIES: usize = 1_000_000;
const QUERIES_PER_BATCH: usize = 1_000;
const BATCHES: usize = TOTAL_QUERIES / QUERIES_PER_BATCH;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = PgConnection::connect("127.0.0.1", 5432, "orion", "swb_staging_local").await?;

    println!("🚀 CACHED PREPARED STATEMENT BENCHMARK");
    println!("======================================");
    println!("Total queries:    {:>12}", TOTAL_QUERIES);
    println!("Batch size:       {:>12}", QUERIES_PER_BATCH);
    println!("Batches:          {:>12}", BATCHES);
    println!("\n⚠️  PARSE ONCE, BIND+EXECUTE MANY!\n");

    // Build batch of QailCmd ASTs ONCE (outside timing!)
    // Note: All queries have same structure, just different LIMIT value
    // This means ONE prepared statement, 1000 Bind+Execute per batch
    let cmds: Vec<QailCmd> = (1..=QUERIES_PER_BATCH)
        .map(|i| {
            let limit = (i % 10) + 1;
            QailCmd::get("harbors")
                .columns(["id", "name"])
                .limit(limit as i64)
        })
        .collect();

    println!("📊 Pipelining 1,000,000 queries via CACHED PREPARED STATEMENTS...");

    let start = Instant::now();
    let mut successful_queries = 0;

    for batch in 0..BATCHES {
        if batch % 100 == 0 {
            println!("   Batch {}/{}", batch, BATCHES);
        }

        // Execute using CACHED prepared statement pipeline
        let count = conn.pipeline_ast_cached(&cmds).await?;
        successful_queries += count;
    }

    let elapsed = start.elapsed();
    let qps = TOTAL_QUERIES as f64 / elapsed.as_secs_f64();
    let per_query_ns = elapsed.as_nanos() / TOTAL_QUERIES as u128;

    println!("\n📈 Results:");
    println!("┌──────────────────────────────────────────┐");
    println!("│ CACHED PREPARED - ONE MILLION QUERIES    │");
    println!("├──────────────────────────────────────────┤");
    println!("│ Total Time:     {:>23.2}s │", elapsed.as_secs_f64());
    println!("│ Queries/Second: {:>23.0} │", qps);
    println!("│ Per Query:      {:>20}ns │", per_query_ns);
    println!("│ Successful:     {:>23} │", successful_queries);
    println!("└──────────────────────────────────────────┘");

    println!("\n📊 vs QAIL Simple Query (99,229 q/s):");
    let simple_speedup = qps / 99229.0;
    println!(
        "   Cached is {:.2}x faster than simple query",
        simple_speedup
    );

    println!("\n📊 vs Go pgx (322,703 q/s):");
    if qps > 322703.0 {
        println!("   🎉 QAIL is {:.2}x FASTER than Go!", qps / 322703.0);
    } else if qps > 280000.0 {
        println!("   QAIL is within 15% of Go! ({:.2}x)", qps / 322703.0);
    } else {
        println!("   Go is {:.2}x faster", 322703.0 / qps);
    }

    Ok(())
}
