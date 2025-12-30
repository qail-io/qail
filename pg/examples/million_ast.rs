//! TRUE AST-NATIVE MILLION QUERY BENCHMARK
//!
//! This uses pipeline_ast() which encodes directly: AST → Wire Bytes
//! NO SQL STRINGS! Should be faster than Go pgx.
//!
//! Run: cargo run --release --example million_ast

use qail_core::ast::Qail;
use qail_pg::PgConnection;
use std::env;
use std::time::Instant;

const TOTAL_QUERIES: usize = 1_000_000;
const QUERIES_PER_BATCH: usize = 1_000;
const BATCHES: usize = TOTAL_QUERIES / QUERIES_PER_BATCH;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let password = env::var("STAGING_DB_PASSWORD").expect("Set STAGING_DB_PASSWORD");

    // Connect via SSH tunnel
    let mut conn = PgConnection::connect_with_password(
        "127.0.0.1",
        5444,
        "sailtix",
        "swb-staging",
        Some(&password),
    )
    .await?;

    println!("🚀 TRUE AST-NATIVE MILLION QUERY BENCHMARK");
    println!("==========================================");
    println!(
        "Total queries:    {:>12}",
        format!(
            "{:>12}",
            TOTAL_QUERIES
                .to_string()
                .chars()
                .collect::<Vec<_>>()
                .chunks(3)
                .rev()
                .map(|c| c.iter().collect::<String>())
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join(",")
        )
    );
    println!("Batch size:       {:>12}", QUERIES_PER_BATCH);
    println!("Batches:          {:>12}", BATCHES);
    println!("\n⚠️  Using pipeline_ast() - TRUE AST-NATIVE (no SQL strings!)\n");

    // ===== AST-NATIVE PIPELINING =====
    println!("📊 Pipelining 1,000,000 queries via AST-native encoder...");

    let pipeline_start = Instant::now();
    let mut successful_queries = 0;

    for batch in 0..BATCHES {
        if batch % 100 == 0 {
            println!("   Batch {}/{}", batch, BATCHES);
        }

        // Build batch of Qail ASTs (NO SQL STRINGS!)
        let cmds: Vec<Qail> = (1..=QUERIES_PER_BATCH)
            .map(|i| {
                let limit = (i % 10) + 1;
                Qail::get("harbors")
                    .columns(["id", "name"])
                    .limit(limit as i64)
            })
            .collect();

        // Execute using TRUE AST-NATIVE path!
        let results = conn.pipeline_ast(&cmds).await?;
        successful_queries += results.len();
    }

    let pipeline_time = pipeline_start.elapsed();

    // ===== RESULTS =====
    let pipeline_secs = pipeline_time.as_secs_f64();
    let qps = (TOTAL_QUERIES as f64) / pipeline_secs;
    let per_query_ns = pipeline_time.as_nanos() / TOTAL_QUERIES as u128;

    println!("\n📈 Results:");
    println!("┌──────────────────────────────────────────┐");
    println!("│ AST-NATIVE - ONE MILLION QUERIES         │");
    println!("├──────────────────────────────────────────┤");
    println!("│ Total Time:     {:>23.2}s │", pipeline_secs);
    println!("│ Queries/Second: {:>23.0} │", qps);
    println!("│ Per Query:      {:>20}ns │", per_query_ns);
    println!("│ Successful:     {:>23} │", successful_queries);
    println!("└──────────────────────────────────────────┘");

    // Compare to serial baseline (37ms/query)
    let serial_estimate_secs = (TOTAL_QUERIES as f64) * 0.037;
    let speedup = serial_estimate_secs / pipeline_secs;

    println!("\n🏆 Comparison:");
    println!(
        "   Serial estimate:  {:.0} seconds ({:.1} hours)",
        serial_estimate_secs,
        serial_estimate_secs / 3600.0
    );
    println!("   AST-native:       {:.1} seconds", pipeline_secs);
    println!("   Speedup:          {:.0}x faster!", speedup);

    // Compare to previous SQL-string based benchmark
    println!("\n📊 vs SQL String pipeline (190s):");
    let sql_string_speedup = 190.9 / pipeline_secs;
    println!(
        "   Improvement:      {:.1}x faster with AST-native!",
        sql_string_speedup
    );

    // Compare to Go pgx
    println!("\n📊 vs Go pgx (119s @ 8,378 q/s):");
    let go_speedup = 119.4 / pipeline_secs;
    if go_speedup > 1.0 {
        println!("   QAIL is {:.1}x FASTER than Go pgx! 🎉", go_speedup);
    } else {
        println!("   Go pgx is {:.1}x faster", 1.0 / go_speedup);
    }

    Ok(())
}
