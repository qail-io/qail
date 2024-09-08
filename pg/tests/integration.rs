//! Integration tests for qail-pg
//!
//! Requires PostgreSQL running on localhost:5432 with SCRAM-SHA-256 authentication.
//! Run: `podman run -d --name qail-test-pg -e POSTGRES_USER=qail -e POSTGRES_PASSWORD=qail -e POSTGRES_DB=qail_test -p 5432:5432 postgres:17`
//! Then: `cargo test --test integration -- --nocapture`

use qail_core::ast::Qail;
use qail_pg::{PgDriver, PgResult};

/// Test connecting to PostgreSQL and running a simple query.
#[tokio::test]
#[ignore = "Requires PostgreSQL server - run manually"]
async fn test_simple_query() -> PgResult<()> {
    // Connect via SCRAM-SHA-256 auth
    let mut driver =
        PgDriver::connect_with_password("127.0.0.1", 5432, "qail", "qail_test", "qail").await?;

    // Build a QAIL command
    let cmd = Qail::get("users").select_all();

    // Execute and fetch rows
    let rows = driver.fetch_all(&cmd).await?;

    println!("Fetched {} rows:", rows.len());
    for row in &rows {
        let id = row.get_i32(0);
        let name = row.get_string(1);
        let email = row.get_string(2);
        let active = row.get_bool(3);
        println!(
            "  id={:?}, name={:?}, email={:?}, active={:?}",
            id, name, email, active
        );
    }

    assert!(rows.len() >= 2, "Expected at least 2 users");

    Ok(())
}

/// Test with Qail filter
#[tokio::test]
#[ignore = "Requires PostgreSQL server - run manually"]
async fn test_filtered_query() -> PgResult<()> {
    use qail_core::ast::Operator;

    let mut driver =
        PgDriver::connect_with_password("127.0.0.1", 5432, "qail", "qail_test", "qail").await?;

    // Query with filter
    let cmd = Qail::get("users")
        .columns(["id", "name"])
        .filter("active", Operator::Eq, true);

    let rows = driver.fetch_all(&cmd).await?;

    println!("Active users: {}", rows.len());

    Ok(())
}
