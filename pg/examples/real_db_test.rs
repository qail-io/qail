/// Real DB Execution Test - Runs against local PostgreSQL
/// Run with: cargo run --example real_db_test

use qail_core::Qail;
use qail_core::transpiler::ToSql;
use qail_pg::{PgPool, PoolConfig};

#[tokio::main]
#[allow(deprecated)]
async fn main() {
    println!("=== QAIL Real Database Execution Test ===\n");
    
    // Connect to local DB using PoolConfig
    let config = PoolConfig::new("localhost", 5432, "qail_user", "qail_test");
    println!("Connecting to: localhost:5432/qail_test\n");
    
    let pool = match PgPool::connect(config).await {
        Ok(p) => {
            println!("✓ Connected to database\n");
            p
        }
        Err(e) => {
            eprintln!("✗ Failed to connect: {}", e);
            return;
        }
    };
    
    // Test 1: Simple query using QAIL
    println!("1. Testing Qail::get('users').limit(3)...");
    
    let query = Qail::get("users")
        .columns(["id", "email", "first_name"])
        .limit(3);
    
    let sql = query.to_sql();
    println!("   SQL: {}", sql);
    
    // Acquire connection and execute
    let mut conn = match pool.acquire_system().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("   ✗ Failed to acquire connection: {}", e);
            return;
        }
    };
    
    match conn.fetch_all_uncached(&query).await {
        Ok(rows) => {
            println!("   ✓ Fetched {} rows\n", rows.len());
        }
        Err(e) => {
            eprintln!("   ✗ Query failed: {}\n", e);
        }
    }
    
    // Test 2: Query with WHERE clause
    println!("\n2. Testing Qail::get('operators').eq('is_active', true).limit(2)...");
    
    let query2 = Qail::get("operators")
        .columns(["id", "brand_name", "slug"])
        .eq("is_active", true)
        .limit(2);
    
    let sql2 = query2.to_sql();
    println!("   SQL: {}", sql2);
    
    match conn.fetch_all_uncached(&query2).await {
        Ok(rows) => {
            println!("   ✓ Fetched {} active operators\n", rows.len());
        }
        Err(e) => {
            eprintln!("   ✗ Query failed: {}\n", e);
        }
    }
    
    println!("\n✅ Real database test complete!");
}
