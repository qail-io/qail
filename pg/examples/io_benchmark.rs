//! qail-pg Real Database Query Benchmark
//!
//! Equivalent to qail-zig's io_bench — measures ACTUAL query throughput
//! against a live PostgreSQL over TCP round-trips.
//!
//! Run: cargo run -p qail-pg --example io_benchmark --release
//!
//! Requires: PostgreSQL on 127.0.0.1:5432, trust auth, database qail_e2e_test

use qail_core::prelude::*;
use qail_pg::driver::PgDriver;
use std::time::Instant;

const READ_ITERS: usize = 1_000_000;
const WRITE_ITERS: usize = 100_000;
const WARMUP: usize = 1_000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!();
    println!("╔═══════════════════════════════════════════════════════╗");
    println!("║  qail-rs — Real Database Query Benchmark             ║");
    println!("╠═══════════════════════════════════════════════════════╣");
    println!(
        "║  Read iters: {:>10}                              ║",
        READ_ITERS
    );
    println!(
        "║  Write iters:{:>10}                              ║",
        WRITE_ITERS
    );
    println!("║  Host:       127.0.0.1:5432                          ║");
    println!("║  Database:   qail_e2e_test                           ║");
    println!("║  Mode:       Full I/O (TCP round-trip, async)        ║");
    println!("╚═══════════════════════════════════════════════════════╝");
    println!();

    // ── Connect ──────────────────────────────────────────────
    let mut driver = PgDriver::connect("127.0.0.1", 5432, "postgres", "qail_e2e_test").await?;
    println!("  Connected\n");

    // ── Setup: seed tables ───────────────────────────────────
    println!("  Setting up benchmark tables...");

    let _ = driver
        .execute_raw("DROP TABLE IF EXISTS bench_orders")
        .await;
    let _ = driver.execute_raw("DROP TABLE IF EXISTS bench_users").await;
    driver
        .execute_raw(
            "CREATE TABLE bench_users (
              id SERIAL PRIMARY KEY,
              name TEXT NOT NULL,
              email TEXT NOT NULL,
              active BOOLEAN DEFAULT true
            )",
        )
        .await?;
    driver
        .execute_raw(
            "CREATE TABLE bench_orders (
              id SERIAL PRIMARY KEY,
              user_id INTEGER REFERENCES bench_users(id),
              product TEXT NOT NULL,
              amount NUMERIC(10,2) NOT NULL,
              status TEXT DEFAULT 'pending'
            )",
        )
        .await?;

    // Seed 100 users
    for i in 1..=100 {
        let sql = format!(
            "INSERT INTO bench_users (name, email, active) VALUES ('User {}', 'user{}@test.com', {})",
            i,
            i,
            if i % 3 != 0 { "true" } else { "false" }
        );
        let _ = driver.execute_raw(&sql).await;
    }

    // Seed 500 orders
    let statuses = ["pending", "completed", "shipped", "cancelled", "refunded"];
    for uid in 1..=100u32 {
        for j in 0..5u32 {
            let sql = format!(
                "INSERT INTO bench_orders (user_id, product, amount, status) VALUES ({}, 'Product {}-{}', {}.99, '{}')",
                uid,
                uid,
                j,
                (j + 1) * 10,
                statuses[j as usize % 5]
            );
            let _ = driver.execute_raw(&sql).await;
        }
    }

    // Indexes
    let _ = driver
        .execute_raw("CREATE INDEX IF NOT EXISTS idx_users_active ON bench_users (active)")
        .await;
    let _ = driver
        .execute_raw("CREATE INDEX IF NOT EXISTS idx_orders_status ON bench_orders (status)")
        .await;
    let _ = driver
        .execute_raw("CREATE INDEX IF NOT EXISTS idx_orders_user ON bench_orders (user_id)")
        .await;

    println!("  Seeded: 100 users, 500 orders (indexed)\n");

    let mut total_ops: u64 = 0;
    println!("  ── Real I/O Query Benchmarks ──\n");

    // ── T1: SELECT LIMIT 1 (simplest) ────────────────────────
    {
        let cmd = Qail::get("bench_users").columns(["id", "name"]).limit(1);

        // Warmup
        for _ in 0..WARMUP {
            let _ = driver.fetch_all(&cmd).await?;
        }

        let start = Instant::now();
        for _ in 0..READ_ITERS {
            let rows = driver.fetch_all(&cmd).await?;
            std::hint::black_box(&rows);
        }
        let elapsed = start.elapsed();
        let us = elapsed.as_micros() as f64 / READ_ITERS as f64;
        let qps = READ_ITERS as f64 / elapsed.as_secs_f64();
        println!(
            "  {:<24} {:>7.1} μs/q  {:>10.0} qps",
            "T1 — SELECT LIMIT 1", us, qps
        );
        total_ops += READ_ITERS as u64;
    }

    // ── T2: SELECT WHERE ─────────────────────────────────────
    {
        let cmd = Qail::get("bench_users")
            .columns(["id", "name", "email"])
            .filter("active", Operator::Eq, Value::Bool(true))
            .limit(10);

        for _ in 0..WARMUP {
            let _ = driver.fetch_all(&cmd).await?;
        }

        let start = Instant::now();
        for _ in 0..READ_ITERS {
            let rows = driver.fetch_all(&cmd).await?;
            std::hint::black_box(&rows);
        }
        let elapsed = start.elapsed();
        let us = elapsed.as_micros() as f64 / READ_ITERS as f64;
        let qps = READ_ITERS as f64 / elapsed.as_secs_f64();
        println!(
            "  {:<24} {:>7.1} μs/q  {:>10.0} qps",
            "T2 — SELECT WHERE", us, qps
        );
        total_ops += READ_ITERS as u64;
    }

    // ── T3: ORDER BY + LIMIT ─────────────────────────────────
    {
        let cmd = Qail::get("bench_users")
            .columns(["id", "name"])
            .order_by("name", SortOrder::Asc)
            .limit(5);

        for _ in 0..WARMUP {
            let _ = driver.fetch_all(&cmd).await?;
        }

        let start = Instant::now();
        for _ in 0..READ_ITERS {
            let rows = driver.fetch_all(&cmd).await?;
            std::hint::black_box(&rows);
        }
        let elapsed = start.elapsed();
        let us = elapsed.as_micros() as f64 / READ_ITERS as f64;
        let qps = READ_ITERS as f64 / elapsed.as_secs_f64();
        println!(
            "  {:<24} {:>7.1} μs/q  {:>10.0} qps",
            "T3 — ORDER BY LIMIT", us, qps
        );
        total_ops += READ_ITERS as u64;
    }

    // ── T4: INSERT + DELETE write cycle ──────────────────────
    {
        // Warmup
        for _ in 0..100 {
            let _ = driver
                .execute_raw("INSERT INTO bench_users (name, email) VALUES ('_tmp', '_tmp@t.com')")
                .await;
            let _ = driver
                .execute_raw("DELETE FROM bench_users WHERE name = '_tmp'")
                .await;
        }

        let start = Instant::now();
        for _ in 0..WRITE_ITERS {
            let _ = driver
                .execute_raw("INSERT INTO bench_users (name, email) VALUES ('_tmp', '_tmp@t.com')")
                .await;
            let _ = driver
                .execute_raw("DELETE FROM bench_users WHERE name = '_tmp'")
                .await;
        }
        let elapsed = start.elapsed();
        let write_ops = WRITE_ITERS * 2;
        let us = elapsed.as_micros() as f64 / WRITE_ITERS as f64;
        let qps = write_ops as f64 / elapsed.as_secs_f64();
        println!(
            "  {:<24} {:>7.1} μs/cyc {:>10.0} ops  (INS+DEL)",
            "T4 — INSERT+DELETE", us, qps
        );
        total_ops += write_ops as u64;
    }

    // ── T5: UPDATE WHERE ─────────────────────────────────────
    {
        // Warmup
        for _ in 0..100 {
            let _ = driver
                .execute_raw("UPDATE bench_users SET active = true WHERE id = 1")
                .await;
        }

        let start = Instant::now();
        for i in 0..WRITE_ITERS {
            if i % 2 == 0 {
                let _ = driver
                    .execute_raw("UPDATE bench_users SET active = true WHERE id = 1")
                    .await;
            } else {
                let _ = driver
                    .execute_raw("UPDATE bench_users SET active = false WHERE id = 1")
                    .await;
            }
        }
        let elapsed = start.elapsed();
        let us = elapsed.as_micros() as f64 / WRITE_ITERS as f64;
        let qps = WRITE_ITERS as f64 / elapsed.as_secs_f64();
        println!(
            "  {:<24} {:>7.1} μs/q  {:>10.0} qps",
            "T5 — UPDATE WHERE", us, qps
        );
        total_ops += WRITE_ITERS as u64;
    }

    // ── T6: Complex JOIN ─────────────────────────────────────
    {
        let cmd = Qail::get("bench_orders")
            .columns([
                "bench_users.name",
                "bench_orders.product",
                "bench_orders.amount",
            ])
            .join(
                JoinKind::Inner,
                "bench_users",
                "bench_orders.user_id",
                "bench_users.id",
            )
            .filter(
                "bench_orders.status",
                Operator::Eq,
                Value::String("completed".into()),
            )
            .order_by("bench_orders.amount", SortOrder::Desc)
            .limit(10);

        for _ in 0..WARMUP {
            let _ = driver.fetch_all(&cmd).await?;
        }

        let start = Instant::now();
        for _ in 0..READ_ITERS {
            let rows = driver.fetch_all(&cmd).await?;
            std::hint::black_box(&rows);
        }
        let elapsed = start.elapsed();
        let us = elapsed.as_micros() as f64 / READ_ITERS as f64;
        let qps = READ_ITERS as f64 / elapsed.as_secs_f64();
        println!(
            "  {:<24} {:>7.1} μs/q  {:>10.0} qps",
            "T6 — JOIN+WHERE+ORDER", us, qps
        );
        total_ops += READ_ITERS as u64;
    }

    // ── Cleanup ──────────────────────────────────────────────
    print!("\n  Cleaning up...");
    let _ = driver.execute_raw("DROP TABLE bench_orders").await;
    let _ = driver.execute_raw("DROP TABLE bench_users").await;
    println!(" done");

    println!("\n────────────────────────────────────────────────────────");
    println!("  Total operations: {}", total_ops);
    println!("────────────────────────────────────────────────────────\n");

    Ok(())
}
