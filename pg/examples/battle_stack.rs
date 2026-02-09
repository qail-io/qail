//! BATTLE TEST #9: The JSON Stack Smash 🧱
//! 
//! Purpose: Crash the driver with deep recursion.
//! Fail Condition: Process aborts (Stack Overflow) or Panic.
//!
//! Run: cargo run --release -p qail-pg --example battle_stack

use qail_pg::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  BATTLE TEST #9: The Stack Smash (Recursive JSON) 🧱      ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    let mut driver = PgDriver::connect_with_password(
        "localhost", 5432, "postgres", "postgres", "postgres"
    ).await?;

    // 1. Construct a JSONB From Hell
    // {"a": {"a": {"a": ... }}} nested 5,000 times
    let depth = 5000;
    println!("1️⃣  Asking Postgres for JSON nested {} levels deep...", depth);
    
    // We use standard SQL generation to avoid sending 5000 params
    // '{"a":' repeated 5000 times + '1' + '}' repeated 5000 times
    let sql = format!(
        "SELECT (repeat('{{\"a\":', {}) || '1' || repeat('}}', {}))::jsonb::text", 
        depth, depth
    );

    println!("2️⃣  Parsing response...");
    
    // QAIL receives this as a String (via get_string or get_json).
    // Since we return the raw string without recursive parsing, this should pass.
    let rows = driver.fetch_raw(&sql).await?;
    
    if rows.is_empty() {
        println!("   ❌ FAIL: No rows returned.");
        return Err("No rows".into());
    }

    let json_str = rows[0].get_string(0);
    
    match json_str {
        Some(s) => {
            if s.len() > depth {
                println!("   ✅ PASS: Driver survived the stack depth!");
                println!("   (Length received: {} chars)", s.len());
            } else {
                println!("   ⚠️  Received short string?");
            }
        },
        None => {
            println!("   ❌ FAIL: Could not parse response as string.");
            return Err("Parse failed".into());
        }
    }

    println!("\n   ✅ PASS: No Stack Overflow, No Panic.");

    Ok(())
}
