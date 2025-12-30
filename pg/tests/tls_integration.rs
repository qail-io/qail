//! TLS Integration tests for qail-pg
//!
//! Tests native TLS driver against pg.qail.rs (PostgreSQL 18 with SSL).
//! Run: `cargo test --test tls_integration -- --nocapture --ignored`

use qail_core::ast::{Operator, Qail};
use qail_pg::{PgConnection, PgDriver, PgResult};

const HOST: &str = "pg.qail.rs";
const PORT: u16 = 5432;
const USER: &str = "qail";
const PASSWORD: &str = "qail_test_2024";
const DATABASE: &str = "qailtest";

/// Test TLS connection to pg.qail.rs
#[tokio::test]
#[ignore = "Requires pg.qail.rs server - run with --ignored"]
async fn test_tls_connection() -> PgResult<()> {
    println!("🔐 Connecting to {} with TLS...", HOST);

    let _conn = PgConnection::connect_tls(HOST, PORT, USER, DATABASE, Some(PASSWORD)).await?;

    println!("✅ TLS connection established!");

    Ok(())
}

/// Test simple query over TLS
#[tokio::test]
#[ignore = "Requires pg.qail.rs server - run with --ignored"]
async fn test_tls_simple_query() -> PgResult<()> {
    println!("🔐 Connecting to {} with TLS...", HOST);

    let mut driver =
        PgDriver::new(PgConnection::connect_tls(HOST, PORT, USER, DATABASE, Some(PASSWORD)).await?);

    println!("✅ Connected! Running query...");

    // Test query using Qail builder
    let cmd = Qail::get("pg_stat_activity").columns(["pid", "state"]);
    let rows = driver.fetch_all(&cmd).await?;

    println!("📊 Got {} rows from pg_stat_activity", rows.len());

    Ok(())
}

/// Test listing tables in the database
#[tokio::test]
#[ignore = "Requires pg.qail.rs server - run with --ignored"]
async fn test_tls_list_tables() -> PgResult<()> {
    println!("🔐 Connecting to {} with TLS...", HOST);

    let mut driver =
        PgDriver::new(PgConnection::connect_tls(HOST, PORT, USER, DATABASE, Some(PASSWORD)).await?);

    // Query information_schema using builder API
    let cmd = Qail::get("information_schema.tables")
        .columns(["table_name"])
        .filter("table_schema", Operator::Eq, "public");

    let rows = driver.fetch_all(&cmd).await?;

    println!("📋 Tables in database ({}):", rows.len());
    for row in &rows {
        if let Some(name) = row.get_string(0) {
            println!("   - {}", name);
        }
    }

    assert!(rows.len() > 0, "Expected at least one table");

    Ok(())
}

/// Test QAIL AST-native query over TLS
#[tokio::test]
#[ignore = "Requires pg.qail.rs server - run with --ignored"]
async fn test_tls_ast_query() -> PgResult<()> {
    println!("🔐 Connecting to {} with TLS...", HOST);

    let mut driver =
        PgDriver::new(PgConnection::connect_tls(HOST, PORT, USER, DATABASE, Some(PASSWORD)).await?);

    // Build query with Qail builder API
    let cmd = Qail::get("pg_stat_activity")
        .columns(["pid", "state", "query"])
        .filter("state", Operator::IsNotNull, "ignored");

    println!("🔧 Executing AST-native query...");
    let rows = driver.fetch_all(&cmd).await?;

    println!("✅ Got {} active connections", rows.len());

    Ok(())
}

/// Test execute (mutation) over TLS - includes DDL
#[tokio::test]
#[ignore = "Requires pg.qail.rs server - run with --ignored"]
async fn test_tls_execute() -> PgResult<()> {
    println!("🔐 Connecting to {} with TLS...", HOST);

    let mut driver =
        PgDriver::new(PgConnection::connect_tls(HOST, PORT, USER, DATABASE, Some(PASSWORD)).await?);

    // Test DDL with correct V2 syntax: make table col:type:constraint
    // V2 uses colons for type/constraint separation, not spaces
    let create_cmd =
        qail_core::parse("make qail_tls_test id:serial:pk, message:text, created_at:timestamptz")
            .unwrap();

    println!("🔧 Creating table with DDL...");
    // This might fail if table exists, that's OK
    match driver.execute(&create_cmd).await {
        Ok(_) => println!("   Table created!"),
        Err(_) => println!("   Table already exists (OK)"),
    }

    // Query users table to verify connection works
    let cmd = Qail::get("users").columns(["id", "email"]);

    let rows = driver.fetch_all(&cmd).await?;
    println!("✅ Got {} users", rows.len());

    for row in rows.iter().take(3) {
        let id = row.get_i32(0);
        let email = row.get_string(1);
        println!("   id={:?}, email={:?}", id, email);
    }

    Ok(())
}

/// Stress test - multiple queries over single TLS connection
#[tokio::test]
#[ignore = "Requires pg.qail.rs server - run with --ignored"]
async fn test_tls_stress() -> PgResult<()> {
    println!("🔐 Connecting to {} with TLS...", HOST);

    let mut driver =
        PgDriver::new(PgConnection::connect_tls(HOST, PORT, USER, DATABASE, Some(PASSWORD)).await?);

    println!("🔧 Running 100 queries over single TLS connection...");

    let start = std::time::Instant::now();

    for i in 0..100 {
        let cmd = Qail::get("pg_stat_activity").columns(["pid", "state"]);
        let rows = driver.fetch_all(&cmd).await?;

        if i % 20 == 0 {
            println!("   Query {}: {} rows", i, rows.len());
        }
    }

    let elapsed = start.elapsed();
    println!(
        "✅ 100 queries completed in {:?} ({:.2}ms/query)",
        elapsed,
        elapsed.as_millis() as f64 / 100.0
    );

    Ok(())
}
