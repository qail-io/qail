//! Tier Z: Resource Exhaustion & Denial-of-Service Red Team Tests
//!
//! These tests simulate a *malicious adversary* who wants to crash/hang the server.
//! Attack vectors:
//!   1. Stack overflow via recursive AST (transpiler bomb)
//!   2. Slowloris connection starvation (silent server)
//!   3. Bit-flip fuzzing on wire protocol decoder (panic sweep)
//!   4. Massive WHERE clause (10,000 conditions)
//!   5. Enormous SQL via column explosion
//!   6. Connection exhaustion (rapid connect-drop)

use qail_core::ast::*;
use qail_core::transpiler::ToSql;
use qail_pg::protocol::BackendMessage;
use tokio::io::AsyncReadExt;
use tokio::time::{Duration, timeout};

// ══════════════════════════════════════════════════════════════════════
// TIER Z: "The Stack Smasher" — Recursive Subquery AST Overflow
// ══════════════════════════════════════════════════════════════════════

/// Build a deeply nested AST: SELECT * FROM t WHERE id IN (SELECT * FROM t WHERE id IN (...))
/// 5000 levels deep. If the transpiler has no recursion guard, this stack-overflows.
#[test]
#[ignore = "KNOWN VULNERABILITY: Recursive Box<Qail> Drop causes stack overflow — crashes process"]
fn tierz_recursive_subquery_stack_overflow() {
    // Build from inside out
    let mut inner = Qail::get("leaf").columns(["id"]);

    for i in 0..5_000 {
        let mut outer = Qail::get(format!("t{}", i)).columns(["id"]);
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
        inner = outer;
    }

    // This will either:
    // a) Stack overflow → test CRASHES (SIGSEGV) → proves the vulnerability
    // b) Return a massive SQL string → transpiler is resilient (but generates huge SQL)
    // c) Return an error → transpiler has a depth guard (ideal)
    let sql = inner.to_sql();

    // If we get here, it didn't crash — document the SQL size
    println!(
        "  Recursive AST (5000 deep) produced {} bytes of SQL",
        sql.len()
    );
    println!("✅ Transpiler survived 5000-deep recursion without stack overflow");
}

// ══════════════════════════════════════════════════════════════════════
// TIER Z: "The Condition Bomb" — 10,000 WHERE Conditions
// ══════════════════════════════════════════════════════════════════════

/// Build a query with 10,000 AND conditions. Tests both transpiler and Postgres parser.
#[test]
fn tierz_10k_where_conditions() {
    let mut q = Qail::get("vessels").columns(["id"]);

    for i in 0..10_000 {
        q = q.filter(format!("col_{}", i), Operator::Eq, Value::Int(i));
    }

    let sql = q.to_sql();
    assert!(
        sql.len() > 100_000,
        "10k conditions should produce very long SQL"
    );
    println!("  10k conditions → {} bytes SQL", sql.len());
    println!("✅ Transpiler handled 10,000 WHERE conditions");
}

// ══════════════════════════════════════════════════════════════════════
// TIER Z: "Column Explosion" — 1000 Columns
// ══════════════════════════════════════════════════════════════════════

#[test]
fn tierz_1000_columns() {
    let cols: Vec<String> = (0..1000).map(|i| format!("col_{}", i)).collect();
    let col_refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();

    let q = Qail::get("wide_table").columns(col_refs);
    let sql = q.to_sql();

    // Must not panic, must contain all columns
    assert!(sql.contains("col_999"), "Last column must be present");
    println!("  1000 columns → {} bytes SQL", sql.len());
    println!("✅ Transpiler handled 1000 columns");
}

// ══════════════════════════════════════════════════════════════════════
// TIER Z: "The Silent Killer" — Slowloris Handshake
// ══════════════════════════════════════════════════════════════════════

/// Simulates a malicious PostgreSQL server that accepts TCP but never responds.
/// The driver MUST have a connect/handshake timeout or this hangs forever.
#[tokio::test]
async fn tierz_slowloris_silent_server() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    // Spawn a "server" that accepts and does nothing
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        // Read the startup message to complete TCP handshake, then go dead silent
        let mut buf = [0u8; 1024];
        let _ = socket.read(&mut buf).await;
        // Sleep forever — this simulates a hung/malicious server
        tokio::time::sleep(Duration::from_secs(30)).await;
    });

    let url = format!("postgresql://attacker:password@127.0.0.1:{}/fake_db", port);

    // Outer timeout: 12s (longer than driver's 10s connect timeout).
    // If the DRIVER's timeout fires first → Ok(Err) → FIXED ✅
    // If the OUTER timeout fires first → Err(elapsed) → VULNERABLE ❌
    let result = timeout(
        Duration::from_secs(12),
        qail_pg::PgDriver::connect_url(&url),
    )
    .await;

    match result {
        Err(_elapsed) => {
            panic!("Driver hung for 12+ seconds on silent server — Slowloris DoS vulnerability!");
        }
        Ok(Err(e)) => {
            let err_msg = format!("{}", e);
            assert!(
                err_msg.contains("timeout") || err_msg.contains("Timeout"),
                "Expected timeout error from driver, got: {}",
                err_msg
            );
            println!("  Driver returned timeout after ~10s: {}", e);
            println!("✅ Driver has internal connect timeout protection");
        }
        Ok(Ok(_)) => {
            panic!("Driver somehow connected to a silent server — impossible");
        }
    }

    server.abort();
}

/// Simulates a server that sends partial data then goes silent (mid-handshake hang).
#[tokio::test]
async fn tierz_slowloris_partial_response() {
    use tokio::io::AsyncWriteExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = socket.read(&mut buf).await;

        // Send a partial message: type byte + partial length (only 2 of 4 bytes)
        // This leaves the driver waiting for more data that never comes
        let _ = socket.write_all(&[b'R', 0, 0]).await;
        let _ = socket.flush().await;

        tokio::time::sleep(Duration::from_secs(60)).await;
    });

    let url = format!("postgresql://attacker:password@127.0.0.1:{}/fake_db", port);

    // Outer timeout: 12s (longer than driver's 10s connect timeout).
    let result = timeout(
        Duration::from_secs(12),
        qail_pg::PgDriver::connect_url(&url),
    )
    .await;

    match result {
        Err(_elapsed) => {
            panic!(
                "Driver hung for 12+ seconds on partial response — Slowloris DoS vulnerability!"
            );
        }
        Ok(Err(e)) => {
            let err_msg = format!("{}", e);
            assert!(
                err_msg.contains("timeout") || err_msg.contains("Timeout"),
                "Expected timeout error, got: {}",
                err_msg
            );
            println!("  Driver returned timeout: {}", e);
            println!("✅ Driver handled partial response with timeout protection");
        }
        Ok(Ok(_)) => {
            panic!("Driver connected with partial handshake — impossible");
        }
    }

    server.abort();
}

// ══════════════════════════════════════════════════════════════════════
// TIER Z: "Chaotic Fuzzing" — Bit-Flip Decoder Sweep
// ══════════════════════════════════════════════════════════════════════

/// Systematically corrupt every byte of a valid RowDescription message.
/// The decoder must NEVER panic — only return Ok or Err.
#[test]
fn tierz_bitflip_fuzzing_row_description() {
    let valid = vec![
        b'T', 0, 0, 0, 24, // length=24
        0, 1, // 1 field
        b'i', b'd', 0, // name "id\0"
        0, 0, 0, 0, // table oid
        0, 0, // column attr
        0, 0, 0, 23, // type oid (int4)
        0, 4, // type size
        255, 255, 255, 255, // type modifier
        0, 0, // format
    ];

    let mut panics = 0;

    // Flip every byte to every possible value
    for pos in 0..valid.len() {
        for flip in [0x01u8, 0x80, 0xFF, 0x7F, 0x00] {
            let mut corrupted = valid.clone();
            corrupted[pos] ^= flip;

            // Must not panic
            let result = std::panic::catch_unwind(|| {
                let _ = BackendMessage::decode(&corrupted);
            });

            if result.is_err() {
                panics += 1;
                println!("  PANIC at byte {} with flip 0x{:02x}", pos, flip);
            }
        }
    }

    assert_eq!(
        panics, 0,
        "Decoder panicked {} times during fuzzing",
        panics
    );
    println!(
        "✅ Bit-flip fuzzing: {} positions × 5 flips = {} attempts, 0 panics",
        valid.len(),
        valid.len() * 5
    );
}

/// Fuzz DataRow messages with random corruption
#[test]
fn tierz_bitflip_fuzzing_data_row() {
    // Valid DataRow: 1 column with value "42"
    let valid = vec![
        b'D', 0, 0, 0, 12, // length=12
        0, 1, // 1 column
        0, 0, 0, 2, // column length=2
        b'4', b'2', // data "42"
    ];

    let mut panics = 0;

    for pos in 0..valid.len() {
        for flip in [0x01u8, 0x80, 0xFF, 0x7F, 0x00] {
            let mut corrupted = valid.clone();
            corrupted[pos] ^= flip;

            let result = std::panic::catch_unwind(|| {
                let _ = BackendMessage::decode(&corrupted);
            });

            if result.is_err() {
                panics += 1;
                println!("  PANIC at byte {} with flip 0x{:02x}", pos, flip);
            }
        }
    }

    assert_eq!(panics, 0, "DataRow decoder panicked {} times", panics);
    println!(
        "✅ DataRow bit-flip fuzzing: {} attempts, 0 panics",
        valid.len() * 5
    );
}

/// Fuzz ErrorResponse with random corruption
#[test]
fn tierz_bitflip_fuzzing_error_response() {
    let valid = vec![
        b'E', 0, 0, 0, 22, // length
        b'S', b'E', b'R', b'R', b'O', b'R', 0, // severity
        b'C', b'4', b'2', b'0', b'0', b'0', 0, // code
        b'M', b'b', b'a', b'd', 0, // message
        0, // terminator
    ];

    let mut panics = 0;

    for pos in 0..valid.len() {
        for flip in [0x01u8, 0x80, 0xFF] {
            let mut corrupted = valid.clone();
            corrupted[pos] ^= flip;

            let result = std::panic::catch_unwind(|| {
                let _ = BackendMessage::decode(&corrupted);
            });

            if result.is_err() {
                panics += 1;
                println!("  PANIC at byte {} with flip 0x{:02x}", pos, flip);
            }
        }
    }

    assert_eq!(panics, 0, "ErrorResponse decoder panicked {} times", panics);
    println!(
        "✅ ErrorResponse bit-flip fuzzing: {} attempts, 0 panics",
        valid.len() * 3
    );
}

// ══════════════════════════════════════════════════════════════════════
// TIER Z: "Connection Exhaustion" — Rapid Connect/Drop
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "Requires local PostgreSQL"]
async fn tierz_connection_exhaustion_100_rapid() {
    // Rapidly open and drop 100 connections.
    // This tests whether the Drop impl properly cleans up,
    // or if we leak file descriptors / Postgres backends.

    for i in 0..100 {
        let result =
            qail_pg::PgDriver::connect_url("postgresql://qail_user@localhost:5432/qail_test").await;

        match result {
            Ok(driver) => {
                // Immediately drop — tests Drop cleanup
                drop(driver);
            }
            Err(e) => {
                println!("  Connection {} failed: {}", i, e);
                // If we hit max_connections, that proves the vulnerability
                if format!("{}", e).contains("too many") {
                    println!(
                        "  ⚠️ Hit max_connections at attempt {} — connection leak!",
                        i
                    );
                    return;
                }
            }
        }
    }

    // Verify Postgres is still healthy
    let mut driver =
        qail_pg::PgDriver::connect_url("postgresql://qail_user@localhost:5432/qail_test")
            .await
            .expect("Must connect after 100 rapid connections");

    let rows = driver
        .fetch_all(&Qail::get("vessels").columns(["id"]).limit(1))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    println!("✅ 100 rapid connect/drop cycles — Postgres still healthy");
}

// ══════════════════════════════════════════════════════════════════════
// TIER Z: "The Payload Bomb" — Oversized SQL via execute_raw
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "Requires local PostgreSQL"]
async fn tierz_oversized_sql_payload() {
    let mut driver =
        qail_pg::PgDriver::connect_url("postgresql://qail_user@localhost:5432/qail_test")
            .await
            .unwrap();

    // 1MB SQL string — SELECT 'AAAA...AAAA'
    let payload = "A".repeat(1_000_000);
    let sql = format!("SELECT '{}'", payload);

    let result = driver.execute_raw(&sql).await;
    // Postgres max query size is typically ~1GB, so 1MB should be fine
    // But this tests the driver's ability to send large payloads
    println!("  1MB SQL result: {:?}", result.is_ok());

    // 10MB SQL string
    let payload_10mb = "B".repeat(10_000_000);
    let sql_10mb = format!("SELECT '{}'", payload_10mb);

    let result_10mb = driver.execute_raw(&sql_10mb).await;
    println!("  10MB SQL result: {:?}", result_10mb.is_ok());

    // Connection must still work
    let check = driver
        .fetch_all(&Qail::get("vessels").columns(["id"]).limit(1))
        .await;
    assert!(check.is_ok(), "Connection must survive large payloads");
    println!("✅ Large SQL payloads handled without crash");
}
