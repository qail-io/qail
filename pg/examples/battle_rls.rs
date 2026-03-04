//! Battle test: RLS + Pool Contention Benchmark
//!
//! Simulates realistic multi-tenant load with RLS context switching
//! and concurrent pool contention. Profiles worst-case latency.
//!
//! Run: cargo run --release -p qail-pg --example battle_rls

use qail_core::ast::Qail;
use qail_pg::{PgPool, PoolConfig};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const CONCURRENCY: usize = 20;
const ITERATIONS: usize = 100;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  BATTLE TEST: RLS + Pool Contention 🔐                    ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    // Tiny pool (10 connections for 20 tasks → 2x contention)
    let config = PoolConfig::new("localhost", 5432, "postgres", "postgres")
        .max_connections(10)
        .min_connections(2);

    let pool = PgPool::connect(config).await?;

    println!(
        "1️⃣  Pool ready: 10 max connections for {} tasks (2x contention)",
        CONCURRENCY
    );

    let success = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let total_start = Instant::now();
    let mut handles = Vec::new();

    // Simulate 10 different tenants
    let tenants: Vec<String> = (0..10).map(|i| format!("tenant-{:04}", i)).collect();

    println!(
        "2️⃣  Spawning {} × {} = {} queries across 10 tenants...\n",
        CONCURRENCY,
        ITERATIONS,
        CONCURRENCY * ITERATIONS
    );

    for task_id in 0..CONCURRENCY {
        let pool = pool.clone();
        let success = Arc::clone(&success);
        let errors = Arc::clone(&errors);
        let tenant = tenants[task_id % 10].clone();

        handles.push(tokio::spawn(async move {
            let mut latencies = Vec::with_capacity(ITERATIONS);

            for _ in 0..ITERATIONS {
                let t = Instant::now();

                let ctx = qail_core::rls::RlsContext::operator(&tenant);

                match pool.acquire_with_rls(ctx).await {
                    Ok(mut conn) => {
                        let q = Qail::get("generate_series(1,1)");
                        match conn.pipeline_ast(&[q]).await {
                            Ok(_) => {
                                latencies.push(t.elapsed());
                                success.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_) => {
                                errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            latencies
        }));
    }

    // Collect
    let mut all: Vec<Duration> = Vec::new();
    for h in handles {
        all.extend(h.await?);
    }

    let total = total_start.elapsed();
    all.sort();
    let n = all.len();

    if n == 0 {
        println!("   ❌ No successful queries!");
        std::process::exit(1);
    }

    let avg = all.iter().sum::<Duration>() / n as u32;
    let p50 = all[n / 2];
    let p95 = all[n * 95 / 100];
    let p99 = all[n * 99 / 100];
    let max = all[n - 1];
    let qps = n as f64 / total.as_secs_f64();

    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📊 Results  ({} queries in {:.2?})", n, total);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("   Throughput:  {:.0} qps", qps);
    println!("   Errors:      {}", errors.load(Ordering::Relaxed));
    println!();
    println!("   Latency:");
    println!("     avg: {:>8.2?}", avg);
    println!("     p50: {:>8.2?}", p50);
    println!("     p95: {:>8.2?}", p95);
    println!("     p99: {:>8.2?}", p99);
    println!("     max: {:>8.2?}", max);
    println!();

    let stats = pool.stats().await;
    println!(
        "   Pool: idle={} active={} max={}",
        stats.idle, stats.active, stats.max_size
    );
    println!();

    if p99 > Duration::from_millis(100) {
        println!("   ⚠️  p99 > 100ms — contention may be too high");
    } else {
        println!("   ✅ Latency healthy (p99 < 100ms)");
    }

    Ok(())
}
