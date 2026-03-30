//! qail-pg Real Database Query Benchmark
//!
//! Equivalent to qail-zig's io_bench — measures ACTUAL query throughput
//! against a live PostgreSQL over TCP round-trips.
//!
//! Run: cargo run -p qail-pg --example io_benchmark --release
//!
//! Requires: PostgreSQL on 127.0.0.1:5432, trust auth, database qail_e2e_test

use qail_core::ast::{Action, Constraint, Expr, IndexDef, Qail, Value};
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

    let drop_orders = Qail {
        action: Action::Drop,
        table: "bench_orders".to_string(),
        ..Default::default()
    };
    let drop_users = Qail {
        action: Action::Drop,
        table: "bench_users".to_string(),
        ..Default::default()
    };
    let create_users = Qail {
        action: Action::Make,
        table: "bench_users".to_string(),
        columns: vec![
            Expr::Def {
                name: "id".to_string(),
                data_type: "serial".to_string(),
                constraints: vec![Constraint::PrimaryKey],
            },
            Expr::Def {
                name: "name".to_string(),
                data_type: "text".to_string(),
                constraints: vec![],
            },
            Expr::Def {
                name: "email".to_string(),
                data_type: "text".to_string(),
                constraints: vec![],
            },
            Expr::Def {
                name: "active".to_string(),
                data_type: "boolean".to_string(),
                constraints: vec![Constraint::Default("true".to_string())],
            },
        ],
        ..Default::default()
    };
    let create_orders = Qail {
        action: Action::Make,
        table: "bench_orders".to_string(),
        columns: vec![
            Expr::Def {
                name: "id".to_string(),
                data_type: "serial".to_string(),
                constraints: vec![Constraint::PrimaryKey],
            },
            Expr::Def {
                name: "user_id".to_string(),
                data_type: "integer".to_string(),
                constraints: vec![
                    Constraint::Nullable,
                    Constraint::References("bench_users(id)".to_string()),
                ],
            },
            Expr::Def {
                name: "product".to_string(),
                data_type: "text".to_string(),
                constraints: vec![],
            },
            Expr::Def {
                name: "amount".to_string(),
                data_type: "numeric(10,2)".to_string(),
                constraints: vec![],
            },
            Expr::Def {
                name: "status".to_string(),
                data_type: "text".to_string(),
                constraints: vec![Constraint::Default("'pending'".to_string())],
            },
        ],
        ..Default::default()
    };

    let _ = driver.execute(&drop_orders).await;
    let _ = driver.execute(&drop_users).await;
    driver.execute(&create_users).await?;
    driver.execute(&create_orders).await?;

    // Seed 100 users
    for i in 1..=100 {
        let insert_user = Qail::add("bench_users")
            .columns(["name", "email", "active"])
            .values([
                Value::String(format!("User {}", i)),
                Value::String(format!("user{}@test.com", i)),
                Value::Bool(i % 3 != 0),
            ]);
        let _ = driver.execute(&insert_user).await;
    }

    // Seed 500 orders
    let statuses = ["pending", "completed", "shipped", "cancelled", "refunded"];
    for uid in 1..=100u32 {
        for j in 0..5u32 {
            let insert_order = Qail::add("bench_orders")
                .columns(["user_id", "product", "amount", "status"])
                .values([
                    Value::Int(uid as i64),
                    Value::String(format!("Product {}-{}", uid, j)),
                    Value::Float(((j + 1) * 10) as f64 + 0.99),
                    Value::String(statuses[j as usize % 5].to_string()),
                ]);
            let _ = driver.execute(&insert_order).await;
        }
    }

    // Indexes
    let idx_users_active = Qail {
        action: Action::Index,
        index_def: Some(IndexDef {
            name: "idx_users_active".to_string(),
            table: "bench_users".to_string(),
            columns: vec!["active".to_string()],
            unique: false,
            index_type: None,
            where_clause: None,
        }),
        ..Default::default()
    };
    let idx_orders_status = Qail {
        action: Action::Index,
        index_def: Some(IndexDef {
            name: "idx_orders_status".to_string(),
            table: "bench_orders".to_string(),
            columns: vec!["status".to_string()],
            unique: false,
            index_type: None,
            where_clause: None,
        }),
        ..Default::default()
    };
    let idx_orders_user = Qail {
        action: Action::Index,
        index_def: Some(IndexDef {
            name: "idx_orders_user".to_string(),
            table: "bench_orders".to_string(),
            columns: vec!["user_id".to_string()],
            unique: false,
            index_type: None,
            where_clause: None,
        }),
        ..Default::default()
    };
    let _ = driver.execute(&idx_users_active).await;
    let _ = driver.execute(&idx_orders_status).await;
    let _ = driver.execute(&idx_orders_user).await;

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
        let insert_tmp = Qail::add("bench_users")
            .columns(["name", "email"])
            .values(["_tmp", "_tmp@t.com"]);
        let delete_tmp = Qail::del("bench_users").filter("name", Operator::Eq, "_tmp");

        // Warmup
        for _ in 0..100 {
            let _ = driver.execute(&insert_tmp).await;
            let _ = driver.execute(&delete_tmp).await;
        }

        let start = Instant::now();
        for _ in 0..WRITE_ITERS {
            let _ = driver.execute(&insert_tmp).await;
            let _ = driver.execute(&delete_tmp).await;
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
        let set_true =
            Qail::set("bench_users")
                .set_value("active", true)
                .filter("id", Operator::Eq, 1);
        let set_false =
            Qail::set("bench_users")
                .set_value("active", false)
                .filter("id", Operator::Eq, 1);

        // Warmup
        for _ in 0..100 {
            let _ = driver.execute(&set_true).await;
        }

        let start = Instant::now();
        for i in 0..WRITE_ITERS {
            if i % 2 == 0 {
                let _ = driver.execute(&set_true).await;
            } else {
                let _ = driver.execute(&set_false).await;
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
    let _ = driver.execute(&drop_orders).await;
    let _ = driver.execute(&drop_users).await;
    println!(" done");

    println!("\n────────────────────────────────────────────────────────");
    println!("  Total operations: {}", total_ops);
    println!("────────────────────────────────────────────────────────\n");

    Ok(())
}
