//! Benchmark: Text vs Binary QAIL queries
//!
//! Run server first: DATABASE_URL="..." cargo run -p qail-gateway --example serve
//! Then run: cargo run -p qail-gateway --example gateway_benchmark --release

use qail_core::ast::Qail;
use qail_core::wire::encode_cmd_binary;
use std::time::Instant;

const ITERATIONS: usize = 1000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();

    // Create query
    let cmd = Qail::get("harbors").columns(["id", "name"]).limit(3);
    let text_query = "get harbors fields id,name limit 3";
    let binary_query = encode_cmd_binary(&cmd).map_err(std::io::Error::other)?;

    println!("═══════════════════════════════════════════════════");
    println!("  QAIL Gateway Benchmark: Text vs Binary");
    println!("═══════════════════════════════════════════════════");
    println!("  Iterations: {}", ITERATIONS);
    println!("  Text query:   {} bytes", text_query.len());
    println!("  Binary query: {} bytes", binary_query.len());
    println!("═══════════════════════════════════════════════════");

    // Warmup
    println!("\n⏳ Warming up...");
    for _ in 0..10 {
        client
            .post("http://localhost:8080/qail")
            .body(text_query)
            .send()
            .await?;
    }

    // Benchmark TEXT
    println!("\n📝 Benchmarking TEXT queries...");
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let resp = client
            .post("http://localhost:8080/qail")
            .body(text_query)
            .send()
            .await?;
        let _ = resp.bytes().await?;
    }
    let text_duration = start.elapsed();

    // Benchmark BINARY
    println!("📦 Benchmarking BINARY queries...");
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let resp = client
            .post("http://localhost:8080/qail/binary")
            .header("Content-Type", "application/octet-stream")
            .body(binary_query.clone())
            .send()
            .await?;
        let _ = resp.bytes().await?;
    }
    let binary_duration = start.elapsed();

    // Results
    println!("\n═══════════════════════════════════════════════════");
    println!("  RESULTS");
    println!("═══════════════════════════════════════════════════");
    println!(
        "  TEXT:   {:>8.2}ms total | {:>6.2}µs/query | {:>6.0} qps",
        text_duration.as_secs_f64() * 1000.0,
        text_duration.as_micros() as f64 / ITERATIONS as f64,
        ITERATIONS as f64 / text_duration.as_secs_f64()
    );
    println!(
        "  BINARY: {:>8.2}ms total | {:>6.2}µs/query | {:>6.0} qps",
        binary_duration.as_secs_f64() * 1000.0,
        binary_duration.as_micros() as f64 / ITERATIONS as f64,
        ITERATIONS as f64 / binary_duration.as_secs_f64()
    );
    println!("───────────────────────────────────────────────────");

    let speedup = text_duration.as_secs_f64() / binary_duration.as_secs_f64();
    if speedup > 1.0 {
        println!("  🚀 Binary is {:.1}x FASTER than text", speedup);
    } else {
        println!("  📝 Text is {:.1}x faster than binary", 1.0 / speedup);
    }
    println!("═══════════════════════════════════════════════════");

    Ok(())
}
