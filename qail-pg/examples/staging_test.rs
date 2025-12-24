//! Test QAIL against real staging database
//!
//! Setup: SSH tunnel to staging DB
//!   ssh -L 15432:localhost:5432 sailtix -N -f
//!
//! Run: cargo run -p qail-pg --example staging_test

use qail_core::ast::QailCmd;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use qail_pg::PgConnection;
    
    println!("🔌 Connecting to staging database...");
    println!("   Host: localhost:15432 (via SSH tunnel)");
    println!("   Database: swb-staging");
    
    // Connect via SSH tunnel
    // Note: Using swb-staging password from env
    let password = std::env::var("STAGING_DB_PASSWORD")
        .unwrap_or_else(|_| {
            eprintln!("⚠️  Set STAGING_DB_PASSWORD env var");
            std::process::exit(1);
        });
    
    let mut conn = PgConnection::connect_with_password(
        "127.0.0.1", 15432, "sailtix", "swb-staging", Some(&password)
    ).await?;
    
    println!("✅ Connected via SCRAM-SHA-256!\n");
    
    // Test 1: Simple query using Extended Query Protocol
    println!("📊 Test 1: Simple query (Extended Query Protocol)");
    let rows = conn.query("SELECT COUNT(*) FROM vessels", &[]).await?;
    
    if let Some(row) = rows.first() {
        if let Some(Some(count)) = row.first() {
            let count_str = String::from_utf8_lossy(count);
            println!("   Vessel count: {}\n", count_str);
        }
    }
    
    // Test 2: Parameterized query - binary params!
    println!("📊 Test 2: Parameterized query (Binary params)");
    let rows = conn.query(
        "SELECT id, name FROM harbors WHERE name ILIKE $1 LIMIT 5",
        &[Some(b"%port%".to_vec())]  // Binary bytes, not SQL string!
    ).await?;
    
    println!("   Found {} harbors matching '%port%':", rows.len());
    for row in &rows {
        let name = row.get(1).and_then(|v| v.as_ref().map(|b| String::from_utf8_lossy(b).to_string()));
        println!("   - {:?}", name);
    }
    
    // Test 3: Using QAIL AST
    println!("\n📊 Test 3: QAIL AST → SQL");
    let cmd = QailCmd::get("vessels")
        .columns(["id", "name"])
        .limit(5);
    
    use qail_core::transpiler::ToSqlParameterized;
    let result = cmd.to_sql_parameterized();
    println!("   SQL: {}", result.sql);
    
    let rows = conn.query(&result.sql, &[]).await?;
    println!("   Fetched {} vessels", rows.len());
    
    println!("\n✅ All tests passed against staging DB!");
    
    Ok(())
}
