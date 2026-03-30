//! BATTLE TEST #9: The JSON Stack Smash 🧱
//!
//! Purpose: Crash the driver with deep recursion.
//! Fail Condition: Process aborts (Stack Overflow) or Panic.
//!
//! Run: cargo run --release -p qail-pg --example battle_stack

use qail_core::ast::{Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};
use qail_core::transpiler::ToSql;
use qail_pg::PgDriver;

fn build_recursive_subquery(depth: usize) -> Qail {
    let mut inner = Qail::get("vessels").columns(["id"]).limit(1);

    for _ in 0..depth {
        let mut outer = Qail::get("vessels").columns(["id"]);
        outer.cages.push(Cage {
            kind: CageKind::Filter,
            conditions: vec![Condition {
                left: Expr::Named("id".to_string()),
                op: Operator::In,
                value: Value::Subquery(Box::new(inner)),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        });
        inner = outer.limit(1);
    }

    inner
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  BATTLE TEST #9: The Stack Smash (Recursive JSON) 🧱      ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    let mut driver =
        PgDriver::connect_with_password("localhost", 5432, "postgres", "postgres", "postgres")
            .await?;

    // 1. Construct a recursive subquery chain 2,000 levels deep.
    let depth = 2_000;
    println!("1️⃣  Building recursive subquery {} levels deep...", depth);

    let deep_cmd = build_recursive_subquery(depth);
    let sql = deep_cmd.to_sql();
    println!("   Generated SQL length: {} bytes", sql.len());

    println!("2️⃣  Executing deep query...");

    match driver.fetch_all_uncached(&deep_cmd).await {
        Ok(rows) => {
            println!(
                "   ✅ Deep query executed without driver crash (rows={})",
                rows.len()
            );
        }
        Err(err) => {
            // PostgreSQL may reject very deep queries; that's acceptable here.
            println!("   ⚠️  Deep query rejected by server: {}", err);
        }
    }

    // 3. Connection health check after deep query attempt.
    let health = driver
        .fetch_all(&Qail::get("vessels").columns(["id"]).limit(1))
        .await?;
    println!(
        "\n   ✅ PASS: No Stack Overflow / Panic. Connection healthy (rows={}).",
        health.len()
    );

    Ok(())
}
