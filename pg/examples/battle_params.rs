//! BATTLE TEST #8: The Parameter Limit 🔢
//!
//! Purpose: Ensure the driver catches the i16 protocol limit (32,767 params).
//! Fail Condition:
//! 1. Panic (Integer overflow during packet building).
//! 2. Protocol Error from server (Driver sent bad packet).
//!
//! Run: cargo run --release -p qail-pg --example battle_params

use qail_pg::{PgPool, PoolConfig};

#[tokio::main]
#[allow(deprecated)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  BATTLE TEST #8: The 32k Parameter Limit 🔢               ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    let config = PoolConfig::new("localhost", 5432, "postgres", "postgres").password("postgres");
    let pool = PgPool::connect(config).await?;
    let mut conn = pool.acquire_system().await?;

    // 1. Create a query with 40,000 parameters
    // VALUES ($1), ($2), ... ($40000)
    let param_count = 40_000;
    println!("1️⃣  Building query with {} parameters...", param_count);

    let mut sql = String::from("VALUES ");
    let mut params: Vec<Option<Vec<u8>>> = Vec::with_capacity(param_count);

    for i in 1..=param_count {
        if i > 1 {
            sql.push(',');
        }
        sql.push_str(&format!("(${})", i));
        params.push(Some(b"1".to_vec()));
    }

    println!("2️⃣  Executing (Expecting Client-Side Error)...");

    // Use `query_pipeline` to access extended protocol logic
    let result = conn.query_raw_with_params(&sql, &params).await;

    match result {
        Ok(_) => {
            println!("   ❌ FAIL: The query somehow succeeded? (Impossible)");
            std::process::exit(1);
        }
        Err(e) => {
            let msg = e.to_string();
            println!("   Result: {:?}", msg);

            if msg.to_lowercase().contains("too many parameter") || msg.contains("limit") {
                println!("   ✅ PASS: Driver blocked the request safely.");
            } else if msg.contains("Postgres error")
                || msg.contains("syntax")
                || msg.contains("protocol")
            {
                println!("   ❌ FAIL: You sent the packet and Postgres complained.");
                println!("   (You wasted bandwidth sending a doomed packet).");
                std::process::exit(1);
            } else if msg.contains("overflow") {
                // If panic caught as error? Unlikely.
                println!("   ⚠️  WARN: You panicked or overflowed?");
                std::process::exit(1);
            } else {
                println!("   ❌ FAIL: Unknown error state: {}", msg);
                std::process::exit(1);
            }
        }
    }

    Ok(())
}
