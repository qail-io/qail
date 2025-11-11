//! ENCODING-ONLY BENCHMARK
//!
//! This benchmark isolates the ENCODING performance only - no network.
//! Compares:
//! 1. to_sql() → SQL string → PgEncoder
//! 2. AstEncoder → wire bytes directly (no SQL string)
//!
//! Run: cargo run --release --example encoding_benchmark

use qail_core::ast::Qail;
use std::time::Instant;

const ITERATIONS: usize = 1_000_000;

fn main() {
    println!("🔬 ENCODING-ONLY BENCHMARK (no network)");
    println!("========================================");
    println!("Iterations: {}\n", ITERATIONS);

    // Build a sample command
    let sample_cmd = Qail::get("harbors")
        .columns(["id", "name", "city", "country"])
        .limit(10);

    // ===== BENCHMARK 1: OLD PATH (to_sql + PgEncoder) =====
    println!("📊 Testing OLD path: to_sql() → PgEncoder...");

    let old_start = Instant::now();
    let mut old_bytes_total = 0usize;

    for _ in 0..ITERATIONS {
        use qail_core::transpiler::ToSql;
        use qail_pg::protocol::PgEncoder;

        // Step 1: AST → SQL string (ALLOCATION!)
        let sql = sample_cmd.to_sql();

        // Step 2: SQL string → wire bytes
        let bytes = PgEncoder::encode_query_string(&sql);
        old_bytes_total += bytes.len();
    }

    let old_time = old_start.elapsed();

    // ===== BENCHMARK 2: NEW PATH (AstEncoder) =====
    println!("📊 Testing NEW path: AstEncoder (no SQL string)...");

    let new_start = Instant::now();
    let mut new_bytes_total = 0usize;

    for _ in 0..ITERATIONS {
        use qail_pg::protocol::AstEncoder;

        // Single step: AST → wire bytes (NO SQL STRING!)
        let (bytes, _params) = AstEncoder::encode_cmd(&sample_cmd).unwrap();
        new_bytes_total += bytes.len();
    }

    let new_time = new_start.elapsed();

    // ===== RESULTS =====
    println!("\n📈 Results:");
    println!("┌────────────────────────────────────────────────────┐");
    println!("│ ENCODING PERFORMANCE (no network)                  │");
    println!("├────────────────────────────────────────────────────┤");
    println!("│ OLD (to_sql + PgEncoder):                          │");
    println!(
        "│   Time:         {:>12.2}ms                       │",
        old_time.as_secs_f64() * 1000.0
    );
    println!(
        "│   Per encode:   {:>12}ns                       │",
        old_time.as_nanos() / ITERATIONS as u128
    );
    println!(
        "│   Bytes/iter:   {:>12}                         │",
        old_bytes_total / ITERATIONS
    );
    println!("├────────────────────────────────────────────────────┤");
    println!("│ NEW (AstEncoder):                                  │");
    println!(
        "│   Time:         {:>12.2}ms                       │",
        new_time.as_secs_f64() * 1000.0
    );
    println!(
        "│   Per encode:   {:>12}ns                       │",
        new_time.as_nanos() / ITERATIONS as u128
    );
    println!(
        "│   Bytes/iter:   {:>12}                         │",
        new_bytes_total / ITERATIONS
    );
    println!("└────────────────────────────────────────────────────┘");

    let speedup = old_time.as_secs_f64() / new_time.as_secs_f64();
    println!(
        "\n🏆 Speedup: {:.2}x faster with AST-native encoding!",
        speedup
    );

    if speedup > 1.0 {
        let saved_per_query_ns =
            (old_time.as_nanos() as f64 - new_time.as_nanos() as f64) / ITERATIONS as f64;
        println!("   Saved per query: {:.0}ns", saved_per_query_ns);
        println!(
            "   For 1M queries: {:.2}ms saved",
            saved_per_query_ns * 1_000_000.0 / 1_000_000.0
        );
    }

    // Compare with network RTT
    println!("\n📊 Context: Network RTT dominates");
    println!(
        "   Encoding time:  ~{}ns per query",
        new_time.as_nanos() / ITERATIONS as u128
    );
    println!("   Network RTT:    ~100,000,000ns (100ms to remote DB)");
    println!(
        "   Encoding is {:.4}% of total time",
        (new_time.as_nanos() / ITERATIONS as u128) as f64 / 100_000_000.0 * 100.0
    );
}
