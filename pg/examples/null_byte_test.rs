//! NULL byte rejection test
use qail_core::ast::Qail;
use qail_core::rls::RlsContext;
use qail_pg::driver::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut driver = PgDriver::connect("localhost", 5432, "orion", "postgres").await?;

    // Test: Embedded NULL byte in RLS tenant value (generates SQL internally)
    println!("Testing NULL byte rejection...");
    let evil_tenant = "hello\x00world";
    println!("  tenant bytes: {:?}", evil_tenant.as_bytes());

    match driver
        .set_rls_context(RlsContext::tenant(evil_tenant))
        .await
    {
        Ok(_) => println!("  ❌ NULL byte ACCEPTED (should reject!)"),
        Err(e) => println!("  ✓ NULL byte REJECTED: {}", e),
    }

    // Test: Normal SQL should still work
    println!("\nTesting normal query...");
    match driver
        .fetch_all(&Qail::session_show("server_version"))
        .await
    {
        Ok(rows) => println!("  ✓ Normal query works (rows={})", rows.len()),
        Err(e) => println!("  ❌ Normal query failed: {}", e),
    }

    Ok(())
}
