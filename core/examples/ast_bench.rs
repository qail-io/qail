//! AST Build + Transpile Benchmark — Rust side
//!
//! Measures pure CPU time for building AST nodes and transpiling to SQL.
//! Split into two phases:
//!   1) Build: construct the AST
//!   2) Transpile: convert pre-built AST → SQL string
//!
//! No I/O, no database. Run with:
//!
//! ```bash
//! cargo run --release --example ast_bench -p qail-core
//! ```

use qail_core::prelude::*;
use std::hint::black_box;
use std::time::Instant;

const ITERATIONS: usize = 1_000_000;
const WARMUP: usize = 10_000;

fn main() {
    println!();
    println!("╔═══════════════════════════════════════════════════════╗");
    println!("║  qail-core (Rust) — AST Build + Transpile Benchmark  ║");
    println!("╠═══════════════════════════════════════════════════════╣");
    println!("║  Iterations: {:>10} per tier                    ║", ITERATIONS);
    println!("║  Warmup:     {:>10}                              ║", WARMUP);
    println!("║  Mode:       --release (optimized)                   ║");
    println!("╚═══════════════════════════════════════════════════════╝");
    println!();

    let mut total_bytes: usize = 0;

    // ─────────────────────────────────────────────────────
    // Phase A: Combined (Build + Transpile) — old behavior
    // ─────────────────────────────────────────────────────
    println!("  ── Phase A: Build AST + Transpile (combined) ──");
    println!();

    total_bytes += bench("T1 — SELECT *", || {
        let cmd = Qail::get("users");
        let sql = cmd.to_sql();
        sql.len()
    });

    total_bytes += bench("T2 — SELECT WHERE", || {
        let cmd = Qail::get("orders")
            .columns(["id", "total", "status"])
            .filter("status", Operator::Eq, "active")
            .filter("total", Operator::Gt, Value::Int(100));
        let sql = cmd.to_sql();
        sql.len()
    });

    total_bytes += bench("T3 — ORDER/LIMIT", || {
        let cmd = Qail::get("products")
            .columns(["id", "name", "price", "stock"])
            .filter("price", Operator::Gte, Value::Float(9.99))
            .filter("stock", Operator::Gt, Value::Int(0))
            .order_by("price", SortOrder::Asc)
            .limit(25)
            .offset(50);
        let sql = cmd.to_sql();
        sql.len()
    });

    total_bytes += bench("T4 — INSERT 5 cols", || {
        let cmd = Qail::add("events")
            .set_value("name", "click")
            .set_value("user_id", "u-123")
            .set_value("timestamp", "2026-01-01")
            .set_value("payload", "{}")
            .set_value("version", Value::Int(1));
        let sql = cmd.to_sql();
        sql.len()
    });

    total_bytes += bench("T5 — UPDATE WHERE", || {
        let cmd = Qail::set("users")
            .set_value("email", "new@mail.com")
            .set_value("updated_at", "2026-01-01T00:00:00Z")
            .filter("id", Operator::Eq, "abc-123");
        let sql = cmd.to_sql();
        sql.len()
    });

    total_bytes += bench("T6 — JOIN/GROUP/HAVING", || {
        let cmd = Qail::get("orders")
            .column("users.name")
            .column_expr(count().build())
            .column_expr(sum("orders.total").build())
            .left_join("users", "orders.user_id", "users.id")
            .filter("orders.status", Operator::Eq, "completed")
            .group_by(["users.name"])
            .having_cond(Condition {
                left: Expr::Named("count".into()),
                op: Operator::Gte,
                value: Value::Int(5),
                is_array_unnest: false,
            })
            .order_by("users.name", SortOrder::Asc)
            .limit(100);
        let sql = cmd.to_sql();
        sql.len()
    });

    // ─────────────────────────────────────────────────────
    // Phase B: Transpile-only (pre-built AST)
    // ─────────────────────────────────────────────────────
    println!();
    println!("  ── Phase B: Transpile Only (pre-built AST) ──");
    println!();

    let t1 = Qail::get("users");
    total_bytes += bench("T1 — SELECT *", || {
        let sql = t1.to_sql();
        sql.len()
    });

    let t2 = Qail::get("orders")
        .columns(["id", "total", "status"])
        .filter("status", Operator::Eq, "active")
        .filter("total", Operator::Gt, Value::Int(100));
    total_bytes += bench("T2 — SELECT WHERE", || {
        let sql = t2.to_sql();
        sql.len()
    });

    let t3 = Qail::get("products")
        .columns(["id", "name", "price", "stock"])
        .filter("price", Operator::Gte, Value::Float(9.99))
        .filter("stock", Operator::Gt, Value::Int(0))
        .order_by("price", SortOrder::Asc)
        .limit(25)
        .offset(50);
    total_bytes += bench("T3 — ORDER/LIMIT", || {
        let sql = t3.to_sql();
        sql.len()
    });

    let t4 = Qail::add("events")
        .set_value("name", "click")
        .set_value("user_id", "u-123")
        .set_value("timestamp", "2026-01-01")
        .set_value("payload", "{}")
        .set_value("version", Value::Int(1));
    total_bytes += bench("T4 — INSERT 5 cols", || {
        let sql = t4.to_sql();
        sql.len()
    });

    let t5 = Qail::set("users")
        .set_value("email", "new@mail.com")
        .set_value("updated_at", "2026-01-01T00:00:00Z")
        .filter("id", Operator::Eq, "abc-123");
    total_bytes += bench("T5 — UPDATE WHERE", || {
        let sql = t5.to_sql();
        sql.len()
    });

    let t6 = Qail::get("orders")
        .column("users.name")
        .column_expr(count().build())
        .column_expr(sum("orders.total").build())
        .left_join("users", "orders.user_id", "users.id")
        .filter("orders.status", Operator::Eq, "completed")
        .group_by(["users.name"])
        .having_cond(Condition {
            left: Expr::Named("count".into()),
            op: Operator::Gte,
            value: Value::Int(5),
            is_array_unnest: false,
        })
        .order_by("users.name", SortOrder::Asc)
        .limit(100);
    total_bytes += bench("T6 — JOIN/GROUP/HAVING", || {
        let sql = t6.to_sql();
        sql.len()
    });

    // ─────────────────────────────────────────────────────
    // Sample SQL
    // ─────────────────────────────────────────────────────
    println!();
    println!("────────────────────────────────────────────────────────");
    println!("  Total SQL bytes produced: {} (consumed to prevent DCE)", total_bytes);
    println!();
    println!("  Sample SQL outputs:");
    println!("    T1: {}", t1.to_sql());
    println!("    T6: {}", t6.to_sql());
    println!();
}

fn bench<F: Fn() -> usize>(label: &str, f: F) -> usize {
    // Warmup
    let mut sink: usize = 0;
    for _ in 0..WARMUP {
        sink += black_box(f());
    }

    // Timed run
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        sink += black_box(f());
    }
    let elapsed = start.elapsed();

    let ns_per = elapsed.as_nanos() as f64 / ITERATIONS as f64;
    let ops_per_sec = ITERATIONS as f64 / elapsed.as_secs_f64();

    println!(
        "  {:<22} {:>7.0} ns/op  {:>12.0} ops/s",
        label, ns_per, ops_per_sec,
    );

    sink
}
