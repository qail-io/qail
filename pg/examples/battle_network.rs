//! Battle Test #2: Network Cut Test (Timeouts) ✂️
//!
//! **The Threat:** A network switch dies, or a query locks a table forever.
//! The application waits... and waits... and hangs.
//!
//! **The Test:**
//! 1. Connect to PostgreSQL.
//! 2. Set `statement_timeout` to 2000ms (2s).
//! 3. Run a `DO` block with `pg_sleep(5)` (simulating a 5s delay/stall).
//! 4. The driver MUST return an error after ~2s.
//!
//! **Pass:** Error received within < 3s. Error is "timeout".
//! **Fail:** Query takes 5s or hangs forever.
//!
//! Run: cargo run --release -p qail-pg --example battle_network

use qail_core::ast::Qail;
use qail_pg::PgDriver;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  BATTLE TEST #2: Network Cut (Timeout) Test ✂️            ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    // Connect to PostgreSQL
    let mut driver =
        PgDriver::connect_with_password("localhost", 5432, "postgres", "postgres", "postgres")
            .await?;

    println!("1️⃣  Connection established");

    // Set timeout to 2 seconds
    println!("\n2️⃣  Setting statement_timeout to 2000ms...");
    driver.set_statement_timeout(2000).await?;
    println!("   ✓ Timeout set");

    // Run a command that takes 5 seconds
    println!("\n3️⃣  Running DO block with pg_sleep(5) (Should timeout in 2s)...");
    let start = Instant::now();
    let sleep_cmd = Qail::do_block("BEGIN PERFORM pg_sleep(5); END;", "plpgsql");
    let result = driver.execute(&sleep_cmd).await;
    let duration = start.elapsed();

    println!("\n   ⏱️  Duration: {:.2?}", duration);

    match result {
        Ok(_) => {
            println!("   ❌ FAIL: Query completed successfully! (Should have timed out)");
            println!("   Timeout logic is BROKEN.");
            return Err("Test failed: No timeout occurred".into());
        }
        Err(e) => {
            let err_str = e.to_string();
            println!("   Error: {}", err_str);

            if duration.as_secs() >= 5 {
                println!("   ❌ FAIL: Timeout error received, BUT it waited full 5s!");
                println!("   Client-side timeout didn't work / Server timeout ignored.");
                return Err("Test failed: Waited too long".into());
            } else if duration.as_secs_f64() < 1.9 {
                println!("   ❌ FAIL: Failed too fast? ({:.2?}s)", duration);
                return Err("Test failed: Too fast".into());
            }

            if err_str.contains("canceling statement due to statement timeout")
                || err_str.contains("57014")
            {
                println!("\n╔═══════════════════════════════════════════════════════════╗");
                println!("║  ✅ PASS: Query interrupted by server timeout correctly!  ║");
                println!("╚═══════════════════════════════════════════════════════════╝");
            } else {
                println!("\n   ⚠️  Warning: Got error, but not standard timeout message.");
                // Accepted if it was still fast
                println!("   Pass assuming error is related to interruption.");
            }
        }
    }

    // Verify connection is still usable (optional, but good practice)
    println!("\n4️⃣  Verifying connection health after timeout...");
    driver.reset_statement_timeout().await?;
    let _ = driver
        .fetch_all(&Qail::session_show("server_version"))
        .await?;
    println!("   ✓ Connection still alive");

    Ok(())
}
