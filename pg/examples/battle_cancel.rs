//! BATTLE TEST #4: The Cancellation Race 🏁
//!
//! Purpose: Ensure we can kill a query without killing the connection.
//!
//! Fail Condition:
//! 1. The query keeps running for the full 5s (Cancel failed).
//! 2. The connection is dead after cancel (Recovery failed).
//!
//! Run: cargo run --release -p qail-pg --example battle_cancel

use qail_core::ast::Qail;
use qail_pg::{PgPool, PoolConfig};
use std::time::{Duration, Instant};

#[tokio::main]
#[allow(deprecated)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  BATTLE TEST #4: Query Cancellation Race 🏁               ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    // Use PoolConfig instead of string
    let config = PoolConfig::new("localhost", 5432, "postgres", "postgres").password("postgres");

    let pool = PgPool::connect(config).await?;
    let mut conn = pool.acquire_system().await?;

    // Get backend PID via pipeline_ast
    // SELECT * FROM pg_backend_pid()
    let pid_q = Qail::get("pg_backend_pid()");
    // pipeline_ast returns Vec<Vec<Vec<Option<Vec<u8>>>>> (Queries -> Rows -> Columns -> Bytes)
    let results = conn.pipeline_ast(&[pid_q]).await?;

    // Parse PID i32 (binary or text)
    // results[0][0][0]
    let pid_bytes = results[0][0][0].as_ref().expect("PID missing");

    // Try parse as text first (Postgres default for simple?), but extended protocol might be binary.
    // pg_backend_pid returns int4.
    let pid: i32 = if pid_bytes.len() == 4 {
        i32::from_be_bytes(pid_bytes.as_slice().try_into()?)
    } else {
        String::from_utf8_lossy(pid_bytes).parse()?
    };

    println!("1️⃣  Acquired Connection (PID: {})", pid);

    // Get cancel token BEFORE moving connection to background task
    let cancel_token = conn.cancel_token()?;

    // Spawn the slow query in background
    println!("2️⃣  Starting 5-second sleep query...");
    let start = Instant::now();

    let query_task = tokio::spawn(async move {
        // SELECT * FROM pg_sleep(5)
        let sleep_q = Qail::get("pg_sleep(5)");
        // This should return an Error::QueryCancelled
        let result = conn.pipeline_ast(&[sleep_q]).await;
        (conn, result)
    });

    // Wait 1s to ensure query is running on server
    tokio::time::sleep(Duration::from_millis(1000)).await;

    println!("3️⃣  Sending CANCEL signal...");
    cancel_token.cancel_query().await?;
    println!("   ✓ Cancel packet sent");

    // Await the task
    let (mut conn, result) = query_task.await?;
    let elapsed = start.elapsed();

    println!("   ⏱️  Duration: {:.2?}", elapsed);

    match result {
        Err(e) => {
            let err_str = e.to_string();
            // Postgres error: "canceling statement due to user request" (57014)
            if err_str.contains("canceling statement") || err_str.contains("57014") {
                println!("   ✓ Query was successfully killed");
            } else {
                println!("   ❌ FAIL: Got wrong error: {:?}", e);
                return Err("Test failed: Wrong error".into());
            }
        }
        Ok(_) => {
            println!("   ❌ FAIL: Query finished successfully (Cancel ignored).");
            return Err("Test failed: Query completed".into());
        }
    }

    if elapsed.as_secs_f32() > 4.5 {
        println!("   ❌ FAIL: Query took too long (> 4.5s). Cancel didn't work immediately.");
        return Err("Test failed: Too slow".into());
    }

    // 4. THE SURVIVAL CHECK
    println!("4️⃣  Checking connection health...");
    // SELECT * FROM generate_series(1,1) -> returns 1 row
    let check_q = Qail::get("generate_series(1,1)");
    match conn.pipeline_ast(&[check_q]).await {
        Ok(_) => println!("   ✅ PASS: Connection is ready for next query!"),
        Err(e) => {
            println!("   ❌ FAIL: Connection died after cancel. Error: {:?}", e);
            return Err("Test failed: Connection dead".into());
        }
    }

    Ok(())
}
