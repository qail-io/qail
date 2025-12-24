//! Integration tests for qail-pg
//!
//! Requires PostgreSQL running on localhost:5432 with SCRAM-SHA-256 authentication.
//! Run: `podman run -d --name qail-test-pg -e POSTGRES_USER=qail -e POSTGRES_PASSWORD=qail -e POSTGRES_DB=qail_test -p 5432:5432 postgres:17`
//! Then: `cargo test --test integration -- --nocapture`

use qail_core::ast::QailCmd;
use qail_pg::{PgDriver, PgResult};

/// Test connecting to PostgreSQL and running a simple query.
#[tokio::test]
async fn test_simple_query() -> PgResult<()> {
    // Connect via SCRAM-SHA-256 auth
    let mut driver = PgDriver::connect_with_password(
        "127.0.0.1", 5432, "qail", "qail_test", "qail"
    ).await?;
    
    // Build a QAIL command
    let cmd = QailCmd::get("users")
        .select_all();
    
    // Execute and fetch rows
    let rows = driver.fetch_all(&cmd).await?;
    
    println!("Fetched {} rows:", rows.len());
    for row in &rows {
        let id = row.get_i32(0);
        let name = row.get_string(1);
        let email = row.get_string(2);
        let active = row.get_bool(3);
        println!("  id={:?}, name={:?}, email={:?}, active={:?}", id, name, email, active);
    }
    
    assert!(rows.len() >= 2, "Expected at least 2 users");
    
    Ok(())
}

/// Test with QailCmd filter
#[tokio::test]
async fn test_filtered_query() -> PgResult<()> {
    use qail_core::ast::Operator;
    
    let mut driver = PgDriver::connect_with_password(
        "127.0.0.1", 5432, "qail", "qail_test", "qail"
    ).await?;
    
    // Query with filter
    let cmd = QailCmd::get("users")
        .columns(["id", "name"])
        .filter("active", Operator::Eq, true);
    
    let rows = driver.fetch_all(&cmd).await?;
    
    println!("Active users: {}", rows.len());
    
    Ok(())
}

/// Test extended query protocol with binary parameters.
/// This is the "skip the string layer" approach - parameters are binary bytes.
#[tokio::test]
async fn test_extended_query() -> PgResult<()> {
    use qail_pg::PgConnection;
    
    let mut conn = PgConnection::connect_with_password(
        "127.0.0.1", 5432, "qail", "qail_test", Some("qail")
    ).await?;
    
    // Extended query with $1 placeholder - parameter sent as binary bytes
    // The value "Alice" never becomes SQL text - it's sent as raw bytes
    let rows = conn.extended_query(
        "SELECT id, name, email FROM users WHERE name = $1",
        &[Some(b"Alice".to_vec())]  // Binary bytes, not SQL string!
    ).await?;
    
    println!("\n=== Extended Query Protocol Test ===");
    println!("Query: SELECT ... WHERE name = $1");
    println!("Param $1 sent as binary bytes: {:?}", b"Alice");
    println!("Rows returned: {}", rows.len());
    
    for row in &rows {
        let empty = vec![];
        let name = String::from_utf8_lossy(row[1].as_ref().unwrap_or(&empty));
        println!("  Found: {}", name);
    }
    
    assert_eq!(rows.len(), 1, "Expected 1 row for Alice");
    
    Ok(())
}
