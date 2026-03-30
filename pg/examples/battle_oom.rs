//! Battle Test #3: OOM Bomb Test (Memory Safety) 💣
//!
//! **The Threat:** A malicious or buggy server sends a packet header
//! claiming to have a payload of 2GB (or more).
//! If the driver blindly trusts this length and tries to allocate a buffer,
//! it could crash the application with Out Of Memory (OOM).
//! or allow distinct DoS attacks.
//!
//! **The Test:**
//! 1. Start a malicious mock PostgreSQL server.
//! 2. Connect with `PgDriver`.
//! 3. Server sends a valid Startup sequence.
//! 4. Server sends a `DataRow` header with length = 2,000,000,000 (2GB).
//! 5. Driver MUST return an error "Message too large" immediately.
//!
//! **Pass:** Driver returns error gracefully. process does not crash.
//! **Fail:** Driver panics, OOMs, or hangs trying to read 2GB.
//!
//! Run: cargo run --release -p qail-pg --example battle_oom

use qail_core::ast::Qail;
use qail_pg::PgDriver;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  BATTLE TEST #3: OOM Bomb Test 💣                         ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();

    // Spawn malicious server
    tokio::spawn(async move {
        if let Ok((mut socket, _)) = listener.accept().await {
            println!("😈 Mock Server: Victim connected.");

            // 1. Handshake (Skip SSL, just accept startup)
            // Read startup message (length + protocol)
            let mut buf = [0u8; 1024];
            let _ = socket.try_read(&mut buf);

            // 2. Send Auth OK
            // 'R' + len(8) + 0 (AuthOk)
            socket
                .write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0])
                .await
                .unwrap();

            // 3. Send ReadyForQuery
            // 'Z' + len(5) + 'I'
            socket.write_all(&[b'Z', 0, 0, 0, 5, b'I']).await.unwrap();

            // 4. Wait for query
            tokio::time::sleep(Duration::from_millis(100)).await;

            // 5. SEND OOM BOMB 💣
            println!("😈 Mock Server: Sending 2GB payload header...");
            // 'D' (DataRow) + Length=2,000,000,000
            let huge_len: u32 = 2_000_000_000;
            let mut msg = Vec::new();
            msg.push(b'D'); // Type
            msg.extend_from_slice(&huge_len.to_be_bytes());

            socket.write_all(&msg).await.unwrap();

            println!("😈 Mock Server: Bomb sent! Stalling...");
            // We don't verify what happens, we just keep connection open
            // If driver tries to read 2GB, it will hang here waiting for data
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });

    println!("1️⃣  Connecting to malicious server at port {}...", port);

    // Connect
    let mut driver =
        PgDriver::connect_with_password("127.0.0.1", port, "victim", "db", "pass").await?;

    println!("   ✓ Connected");

    // Execute query (triggers the bomb response)
    println!("\n2️⃣  Running query to trigger bomb...");
    println!("   Expecting immediate error due to size check...");

    let bomb_query = Qail::get("pg_catalog.pg_type").columns(["oid"]).limit(1);
    let result = tokio::time::timeout(Duration::from_secs(2), driver.fetch_all(&bomb_query)).await;

    match result {
        Ok(res) => match res {
            Ok(_) => {
                println!("   ❌ FAIL: Driver accepted the header?! (Should verify full body)");
                return Err("Test failed: Driver didn't error".into());
            }
            Err(e) => {
                println!("   Error: {}", e);
                if e.to_string().contains("too large") {
                    println!("   ✅ PASS: Driver rejected huge message!");
                    return Ok(());
                } else if e.to_string().contains("closed") {
                    println!("   ❌ FAIL: Connection closed unexpectedly");
                    return Err("Test failed: Connection closed".into());
                } else {
                    println!("   ⚠️  Got unexpected error: {}", e);
                    return Err("Test failed: Unexpected error".into());
                }
            }
        },
        Err(_) => {
            println!("   ❌ FAIL: Driver HANGED trying to allocate/read 2GB!");
            println!("   It is waiting for data to fill the huge buffer.");
            println!("   Safety check MISSING.");
            return Err("Test failed: Driver hung (timeout)".into());
        }
    }
}
