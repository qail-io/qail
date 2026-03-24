//! qail-pg Pipeline + Pool Benchmark
//!
//! Measures 3 modes of operation against live PostgreSQL:
//!   M1: Single query (sequential fetch_all_cached)
//!   M2: Pipeline (batch N queries in 1 round-trip)
//!   M3: Pool + Pipeline (parallel batches across N connections)
//!
//! Run: cargo run -p qail-pg --example pool_pipeline_bench --release

use qail_core::prelude::*;
use qail_pg::driver::{PgConnection, PgDriver, PgPool, PoolConfig};
use std::time::Instant;

const TOTAL_QUERIES: usize = 100_000;
const BATCH_SIZE: usize = 500;
const POOL_SIZE: usize = 10;
const WARMUP: usize = 500;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!();
    println!("╔═══════════════════════════════════════════════════════╗");
    println!("║  qail-rs — Pipeline + Pool Benchmark                 ║");
    println!("╠═══════════════════════════════════════════════════════╣");
    println!(
        "║  Queries:    {:>10}                              ║",
        TOTAL_QUERIES
    );
    println!(
        "║  Batch size: {:>10}                              ║",
        BATCH_SIZE
    );
    println!(
        "║  Pool size:  {:>10}                              ║",
        POOL_SIZE
    );
    println!("║  Host:       127.0.0.1:5432                          ║");
    println!("║  Database:   qail_e2e_test                           ║");
    println!("║  Mode:       Full I/O (TCP round-trip, async)        ║");
    println!("╚═══════════════════════════════════════════════════════╝");
    println!();

    // ── Setup ────────────────────────────────────────────────
    let mut driver = PgDriver::connect("127.0.0.1", 5432, "postgres", "qail_e2e_test").await?;
    println!("  Connected\n");

    println!("  Setting up benchmark tables...");
    driver
        .execute_raw("DROP TABLE IF EXISTS pipe_orders")
        .await
        .ok();
    driver
        .execute_raw("DROP TABLE IF EXISTS pipe_users")
        .await
        .ok();
    driver
        .execute_raw(
            "CREATE TABLE pipe_users (
              id SERIAL PRIMARY KEY,
              name TEXT NOT NULL,
              email TEXT NOT NULL,
              active BOOLEAN DEFAULT true
            )",
        )
        .await?;
    driver
        .execute_raw(
            "CREATE TABLE pipe_orders (
              id SERIAL PRIMARY KEY,
              user_id INTEGER REFERENCES pipe_users(id),
              product TEXT NOT NULL,
              amount NUMERIC(10,2) NOT NULL,
              status TEXT DEFAULT 'pending'
            )",
        )
        .await?;

    // Seed 100 users
    for i in 1..=100 {
        driver
            .execute_raw(&format!(
                "INSERT INTO pipe_users (name, email, active) VALUES ('User {}', 'u{}@t.com', {})",
                i,
                i,
                if i % 3 != 0 { "true" } else { "false" }
            ))
            .await?;
    }

    // Seed 500 orders
    let statuses = ["pending", "completed", "shipped", "cancelled", "refunded"];
    for uid in 1..=100u32 {
        for j in 0..5u32 {
            driver
                .execute_raw(&format!(
                    "INSERT INTO pipe_orders (user_id, product, amount, status) \
                     VALUES ({}, 'P{}-{}', {}.99, '{}')",
                    uid,
                    uid,
                    j,
                    (j + 1) * 10,
                    statuses[j as usize % 5]
                ))
                .await?;
        }
    }

    driver
        .execute_raw("CREATE INDEX IF NOT EXISTS idx_pu_active ON pipe_users (active)")
        .await
        .ok();
    driver
        .execute_raw("CREATE INDEX IF NOT EXISTS idx_po_status ON pipe_orders (status)")
        .await
        .ok();
    driver
        .execute_raw("CREATE INDEX IF NOT EXISTS idx_po_user ON pipe_orders (user_id)")
        .await
        .ok();

    println!("  Seeded: 100 users, 500 orders (indexed)\n");

    // Build the query
    let query = Qail::get("pipe_users")
        .columns(["id", "name", "email"])
        .filter("active", Operator::Eq, Value::Bool(true))
        .limit(10);

    let join_query = Qail::get("pipe_orders")
        .columns([
            "pipe_users.name",
            "pipe_orders.product",
            "pipe_orders.amount",
        ])
        .join(
            JoinKind::Inner,
            "pipe_users",
            "pipe_orders.user_id",
            "pipe_users.id",
        )
        .filter(
            "pipe_orders.status",
            Operator::Eq,
            Value::String("completed".into()),
        )
        .order_by("pipe_orders.amount", SortOrder::Desc)
        .limit(10);

    println!("  ── Mode Comparison ──\n");

    // ═══════════════════════════════════════════════════════════
    // M1: Single query — sequential fetch_all_cached
    // ═══════════════════════════════════════════════════════════
    {
        let mut drv = PgDriver::connect("127.0.0.1", 5432, "postgres", "qail_e2e_test").await?;

        // Warmup
        for _ in 0..WARMUP {
            let _ = drv.fetch_all_cached(&query).await?;
        }

        let start = Instant::now();
        for _ in 0..TOTAL_QUERIES {
            let rows = drv.fetch_all_cached(&query).await?;
            std::hint::black_box(&rows);
        }
        let elapsed = start.elapsed();
        let us = elapsed.as_micros() as f64 / TOTAL_QUERIES as f64;
        let qps = TOTAL_QUERIES as f64 / elapsed.as_secs_f64();
        println!(
            "  {:<32} {:>7.1} μs/q  {:>10.0} qps",
            "M1 — Single (cached)", us, qps
        );
    }

    // ═══════════════════════════════════════════════════════════
    // M1b: Single query — JOIN
    // ═══════════════════════════════════════════════════════════
    {
        let mut drv = PgDriver::connect("127.0.0.1", 5432, "postgres", "qail_e2e_test").await?;

        for _ in 0..WARMUP {
            let _ = drv.fetch_all_cached(&join_query).await?;
        }

        let start = Instant::now();
        for _ in 0..TOTAL_QUERIES {
            let rows = drv.fetch_all_cached(&join_query).await?;
            std::hint::black_box(&rows);
        }
        let elapsed = start.elapsed();
        let us = elapsed.as_micros() as f64 / TOTAL_QUERIES as f64;
        let qps = TOTAL_QUERIES as f64 / elapsed.as_secs_f64();
        println!(
            "  {:<32} {:>7.1} μs/q  {:>10.0} qps",
            "M1b — Single (JOIN, cached)", us, qps
        );
    }

    // ═══════════════════════════════════════════════════════════
    // M2: Pipeline — batch queries in 1 round-trip
    // ═══════════════════════════════════════════════════════════
    {
        let mut conn =
            PgConnection::connect("127.0.0.1", 5432, "postgres", "qail_e2e_test").await?;

        let batch: Vec<Qail> = (0..BATCH_SIZE)
            .map(|i| {
                Qail::get("pipe_users")
                    .columns(["id", "name", "email"])
                    .filter("active", Operator::Eq, Value::Bool(i % 2 == 0))
                    .limit(10)
            })
            .collect();

        let batches = TOTAL_QUERIES / BATCH_SIZE;

        // Warmup
        let _ = conn.pipeline_execute_rows_ast(&batch).await?;

        let start = Instant::now();
        for _ in 0..batches {
            let _ = conn.pipeline_execute_rows_ast(&batch).await?;
        }
        let elapsed = start.elapsed();
        let total = batches * BATCH_SIZE;
        let us = elapsed.as_micros() as f64 / total as f64;
        let qps = total as f64 / elapsed.as_secs_f64();
        println!(
            "  {:<32} {:>7.1} μs/q  {:>10.0} qps",
            "M2 — Pipeline (1 conn)", us, qps
        );
    }

    // ═══════════════════════════════════════════════════════════
    // M2b: Pipeline — JOIN batch
    // ═══════════════════════════════════════════════════════════
    {
        let mut conn =
            PgConnection::connect("127.0.0.1", 5432, "postgres", "qail_e2e_test").await?;

        let batch: Vec<Qail> = (0..BATCH_SIZE)
            .map(|_| {
                Qail::get("pipe_orders")
                    .columns([
                        "pipe_users.name",
                        "pipe_orders.product",
                        "pipe_orders.amount",
                    ])
                    .join(
                        JoinKind::Inner,
                        "pipe_users",
                        "pipe_orders.user_id",
                        "pipe_users.id",
                    )
                    .filter(
                        "pipe_orders.status",
                        Operator::Eq,
                        Value::String("completed".into()),
                    )
                    .order_by("pipe_orders.amount", SortOrder::Desc)
                    .limit(10)
            })
            .collect();

        let batches = TOTAL_QUERIES / BATCH_SIZE;

        let _ = conn.pipeline_execute_rows_ast(&batch).await?;

        let start = Instant::now();
        for _ in 0..batches {
            let _ = conn.pipeline_execute_rows_ast(&batch).await?;
        }
        let elapsed = start.elapsed();
        let total = batches * BATCH_SIZE;
        let us = elapsed.as_micros() as f64 / total as f64;
        let qps = total as f64 / elapsed.as_secs_f64();
        println!(
            "  {:<32} {:>7.1} μs/q  {:>10.0} qps",
            "M2b — Pipeline (JOIN)", us, qps
        );
    }

    // ═══════════════════════════════════════════════════════════
    // M3: Pool + Pipeline — parallel batches across N connections
    // ═══════════════════════════════════════════════════════════
    {
        let config = PoolConfig::new("127.0.0.1", 5432, "postgres", "qail_e2e_test")
            .max_connections(POOL_SIZE)
            .min_connections(POOL_SIZE);
        let pool = PgPool::connect(config).await?;

        let batch: Vec<Qail> = (0..BATCH_SIZE)
            .map(|i| {
                Qail::get("pipe_users")
                    .columns(["id", "name", "email"])
                    .filter("active", Operator::Eq, Value::Bool(i % 2 == 0))
                    .limit(10)
            })
            .collect();

        let batches_per_conn = TOTAL_QUERIES / BATCH_SIZE / POOL_SIZE;

        let start = Instant::now();
        let mut handles = Vec::new();
        for _ in 0..POOL_SIZE {
            let pool_clone = pool.clone();
            let batch_clone = batch.clone();
            handles.push(tokio::spawn(async move {
                let mut conn = pool_clone.acquire_system().await.unwrap();
                for _ in 0..batches_per_conn {
                    let _ = conn.pipeline_execute_rows_ast(&batch_clone).await;
                }
            }));
        }
        for h in handles {
            h.await?;
        }
        let elapsed = start.elapsed();
        let total = batches_per_conn * BATCH_SIZE * POOL_SIZE;
        let us = elapsed.as_micros() as f64 / total as f64;
        let qps = total as f64 / elapsed.as_secs_f64();
        println!(
            "  {:<32} {:>7.1} μs/q  {:>10.0} qps",
            format!("M3 — Pool+Pipe ({} conn)", POOL_SIZE),
            us,
            qps
        );
    }

    // ═══════════════════════════════════════════════════════════
    // M3b: Pool + Pipeline — JOIN
    // ═══════════════════════════════════════════════════════════
    {
        let config = PoolConfig::new("127.0.0.1", 5432, "postgres", "qail_e2e_test")
            .max_connections(POOL_SIZE)
            .min_connections(POOL_SIZE);
        let pool = PgPool::connect(config).await?;

        let batch: Vec<Qail> = (0..BATCH_SIZE)
            .map(|_| {
                Qail::get("pipe_orders")
                    .columns([
                        "pipe_users.name",
                        "pipe_orders.product",
                        "pipe_orders.amount",
                    ])
                    .join(
                        JoinKind::Inner,
                        "pipe_users",
                        "pipe_orders.user_id",
                        "pipe_users.id",
                    )
                    .filter(
                        "pipe_orders.status",
                        Operator::Eq,
                        Value::String("completed".into()),
                    )
                    .order_by("pipe_orders.amount", SortOrder::Desc)
                    .limit(10)
            })
            .collect();

        let batches_per_conn = TOTAL_QUERIES / BATCH_SIZE / POOL_SIZE;

        let start = Instant::now();
        let mut handles = Vec::new();
        for _ in 0..POOL_SIZE {
            let pool_clone = pool.clone();
            let batch_clone = batch.clone();
            handles.push(tokio::spawn(async move {
                let mut conn = pool_clone.acquire_system().await.unwrap();
                for _ in 0..batches_per_conn {
                    let _ = conn.pipeline_execute_rows_ast(&batch_clone).await;
                }
            }));
        }
        for h in handles {
            h.await?;
        }
        let elapsed = start.elapsed();
        let total = batches_per_conn * BATCH_SIZE * POOL_SIZE;
        let us = elapsed.as_micros() as f64 / total as f64;
        let qps = total as f64 / elapsed.as_secs_f64();
        println!(
            "  {:<32} {:>7.1} μs/q  {:>10.0} qps",
            format!("M3b — Pool+Pipe JOIN ({} conn)", POOL_SIZE),
            us,
            qps
        );
    }

    // ── Cleanup ──────────────────────────────────────────────
    print!("\n  Cleaning up...");
    driver
        .execute_raw("DROP TABLE IF EXISTS pipe_orders")
        .await
        .ok();
    driver
        .execute_raw("DROP TABLE IF EXISTS pipe_users")
        .await
        .ok();
    println!(" done\n");

    Ok(())
}
