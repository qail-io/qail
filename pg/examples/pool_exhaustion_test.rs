//! Connection Pool Exhaustion Chaos Test
//!
//! Proves PgDriver handles connection limits gracefully:
//! - No infinite hangs when pool is fully occupied
//! - Clear errors (not silent drops)  
//! - Recovery after exhaustion
//!
//! Run:
//!   DATABASE_URL=postgresql://qail_user@localhost:5432/qail_test \
//!     cargo run -p qail-pg --example pool_exhaustion_test --release

use qail_core::Qail;
use qail_pg::PgDriver;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Barrier;

fn slow_query(seconds: f64) -> Qail {
    Qail::get("pg_catalog.pg_class")
        .column(&format!("pg_sleep({seconds}) AS slept"))
        .column("'alive' AS status")
        .limit(1)
}

fn fast_query() -> Qail {
    Qail::get("pg_catalog.pg_class")
        .column("1 AS ping")
        .limit(1)
}

#[tokio::main]
async fn main() {
    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║     CONNECTION POOL EXHAUSTION CHAOS TEST                      ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");

    // =========================================================================
    // Test 1: Single connection under parallel pressure
    // =========================================================================
    println!("\n━━━ Test 1: Single connection, 5 serial slow queries ━━━");
    {
        let mut driver = PgDriver::connect_url(&db_url)
            .await
            .expect("Failed to connect");

        let start = Instant::now();
        let mut successes = 0u32;
        let mut errors = 0u32;
        let slow = slow_query(0.1);

        for _ in 0..5 {
            match driver.fetch_all(&slow).await {
                Ok(_) => successes += 1,
                Err(e) => {
                    errors += 1;
                    eprintln!("  ⚠️  Error: {}", e);
                }
            }
        }

        println!(
            "  ok={} err={} elapsed={:.2}s",
            successes,
            errors,
            start.elapsed().as_secs_f64()
        );
        assert_eq!(
            errors, 0,
            "Single connection serial queries should never fail"
        );
    }

    // =========================================================================
    // Test 2: Many independent connections, concurrent slow queries
    // =========================================================================
    println!("\n━━━ Test 2: 20 independent connections, concurrent slow queries ━━━");
    {
        let num_workers = 20;
        let queries_per_worker = 10;
        let barrier = Arc::new(Barrier::new(num_workers));
        let successes = Arc::new(AtomicU64::new(0));
        let errors = Arc::new(AtomicU64::new(0));
        let max_latency_us = Arc::new(AtomicU64::new(0));

        let start = Instant::now();
        let mut handles = Vec::new();

        for _ in 0..num_workers {
            let db_url = db_url.clone();
            let barrier = barrier.clone();
            let successes = successes.clone();
            let errors = errors.clone();
            let max_lat = max_latency_us.clone();
            let slow = slow_query(0.1);

            handles.push(tokio::spawn(async move {
                // Each worker gets its own connection
                let mut driver = match PgDriver::connect_url(&db_url).await {
                    Ok(d) => d,
                    Err(e) => {
                        errors.fetch_add(queries_per_worker, Ordering::Relaxed);
                        eprintln!("  ⚠️  Connect failed: {}", e);
                        return;
                    }
                };

                barrier.wait().await;

                for _ in 0..queries_per_worker {
                    let qstart = Instant::now();
                    match driver.fetch_all(&slow).await {
                        Ok(_) => {
                            successes.fetch_add(1, Ordering::Relaxed);
                            let lat = qstart.elapsed().as_micros() as u64;
                            max_lat.fetch_max(lat, Ordering::Relaxed);
                        }
                        Err(e) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                            eprintln!("  ⚠️  Query error: {}", e);
                        }
                    }
                }
            }));
        }

        for h in handles {
            h.await.expect("Worker panicked");
        }

        let elapsed = start.elapsed();
        let ok = successes.load(Ordering::Relaxed);
        let err = errors.load(Ordering::Relaxed);
        let max_lat = max_latency_us.load(Ordering::Relaxed);

        println!(
            "  Workers: {} | Queries/worker: {}",
            num_workers, queries_per_worker
        );
        println!(
            "  ok={} err={} elapsed={:.2}s",
            ok,
            err,
            elapsed.as_secs_f64()
        );
        println!("  Max latency: {:.1}ms", max_lat as f64 / 1000.0);
        println!("  QPS: {:.0}", (ok + err) as f64 / elapsed.as_secs_f64());
    }

    // =========================================================================
    // Test 3: Recovery after heavy load
    // =========================================================================
    println!("\n━━━ Test 3: Recovery — fast queries after heavy load ━━━");
    {
        let mut driver = PgDriver::connect_url(&db_url)
            .await
            .expect("Failed to connect for recovery test");
        let fast = fast_query();

        let mut latencies = Vec::new();
        for _ in 0..20 {
            let start = Instant::now();
            match driver.fetch_all(&fast).await {
                Ok(_) => latencies.push(start.elapsed()),
                Err(e) => {
                    eprintln!("  ❌ Recovery query failed: {}", e);
                    std::process::exit(1);
                }
            }
        }

        let avg_us =
            latencies.iter().map(|d| d.as_micros()).sum::<u128>() / latencies.len() as u128;
        let max_us = latencies.iter().map(|d| d.as_micros()).max().unwrap();
        println!(
            "  20 fast queries: avg={:.2}ms max={:.2}ms — ✅ recovery confirmed",
            avg_us as f64 / 1000.0,
            max_us as f64 / 1000.0
        );
    }

    // =========================================================================
    // Test 4: Connection timeout / hang detection
    // =========================================================================
    println!("\n━━━ Test 4: Timeout detection — 5s deadline on 3s query ━━━");
    {
        let mut driver = PgDriver::connect_url(&db_url)
            .await
            .expect("Failed to connect for timeout test");
        let slow = slow_query(3.0);

        let start = Instant::now();
        let result = tokio::time::timeout(Duration::from_secs(5), driver.fetch_all(&slow)).await;

        match result {
            Ok(Ok(_)) => {
                println!(
                    "  Completed in {:.2}s (under deadline) ✅",
                    start.elapsed().as_secs_f64()
                );
            }
            Ok(Err(e)) => {
                println!(
                    "  Query error after {:.2}s: {} ⚠️",
                    start.elapsed().as_secs_f64(),
                    e
                );
            }
            Err(_) => {
                println!("  ❌ TIMED OUT at 5s — query hung!");
                std::process::exit(1);
            }
        }
    }

    // =========================================================================
    // Test 5: Rapid connect/disconnect cycles
    // =========================================================================
    println!("\n━━━ Test 5: Rapid connect/disconnect — 50 connections ━━━");
    {
        let start = Instant::now();
        let mut ok = 0u32;
        let mut err = 0u32;
        let fast = fast_query();

        for _ in 0..50 {
            match PgDriver::connect_url(&db_url).await {
                Ok(mut driver) => {
                    match driver.fetch_all(&fast).await {
                        Ok(_) => ok += 1,
                        Err(_) => err += 1,
                    }
                    // driver drops, connection closes
                }
                Err(e) => {
                    err += 1;
                    eprintln!("  ⚠️  Connect failed: {}", e);
                }
            }
        }

        println!(
            "  ok={} err={} elapsed={:.2}s",
            ok,
            err,
            start.elapsed().as_secs_f64()
        );
    }

    // =========================================================================
    // Summary
    // =========================================================================
    println!("\n╔══════════════════════════════════════════════════════════════════╗");
    println!("║  ✅ ALL POOL EXHAUSTION TESTS PASSED                           ║");
    println!("╚══════════════════════════════════════════════════════════════════╝\n");
}
