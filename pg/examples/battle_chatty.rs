//! BATTLE TEST #5: The Chatty Server (Async Notices)
//!
//! Purpose: Ensure the driver ignores/logs 'NoticeResponse' messages
//! without crashing the parser.
//!
//! Run: cargo run --release -p qail-pg --example battle_chatty

use qail_core::ast::Qail;
use qail_pg::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  BATTLE TEST #5: The Chatty Server (Notices) 🗣️           ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    let mut driver =
        PgDriver::connect_with_password("localhost", 5432, "postgres", "postgres", "postgres")
            .await?;

    println!("1️⃣  Executing noisy DO block...");
    let noisy = Qail::do_block(
        "BEGIN RAISE NOTICE 'Hear me roar!'; RAISE WARNING 'I am warning you!'; END;",
        "plpgsql",
    );
    let result = driver.execute(&noisy).await;

    match result {
        Ok(_) => {
            println!("   ✅ PASS: Driver handled NOTICE/WARNING packets cleanly.");
        }
        Err(e) => {
            println!("   ❌ FAIL: Driver choked on the Notice message.");
            println!("   Error: {:?}", e);
            println!("   (You probably tried to parse 'N' packet as a DataRow)");
        }
    }

    let _ = driver
        .fetch_all(&Qail::session_show("server_version"))
        .await?;

    Ok(())
}
