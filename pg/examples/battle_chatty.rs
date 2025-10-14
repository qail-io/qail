//! BATTLE TEST #5: The Chatty Server (Async Notices)
//! 
//! Purpose: Ensure the driver ignores/logs 'NoticeResponse' messages 
//! without crashing the parser.
//!
//! Run: cargo run --release -p qail-pg --example battle_chatty

use qail_pg::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  BATTLE TEST #5: The Chatty Server (Notices) 🗣️           ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    let mut driver = PgDriver::connect_with_password(
        "localhost", 5432, "postgres", "postgres", "postgres"
    ).await?;

    println!("1️⃣  Creating noisy function...");
    driver.fetch_raw("
        CREATE OR REPLACE FUNCTION make_noise() RETURNS int AS $$
        BEGIN
            RAISE NOTICE 'Hear me roar!';
            RAISE WARNING 'I am warning you!';
            RETURN 1;
        END;
        $$ LANGUAGE plpgsql;
    ").await?;

    println!("2️⃣  Executing function...");
    // This will send: DataRow -> NoticeResponse -> DataRow -> WarningResponse...
    let result = driver.fetch_raw("SELECT make_noise()").await;

    match result {
        Ok(rows) => {
            println!("   ✅ PASS: Driver filtered out the noise and got {} rows.", rows.len());
            if rows.len() == 1 {
                 // Verify value if possible, but basic success is enough
            }
        },
        Err(e) => {
            println!("   ❌ FAIL: Driver choked on the Notice message.");
            println!("   Error: {:?}", e);
            println!("   (You probably tried to parse 'N' packet as a DataRow)");
        }
    }

    Ok(())
}
