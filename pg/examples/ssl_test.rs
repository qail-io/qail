//! SSL/TLS Connection Test
//!
//! Tests TLS connection to PostgreSQL.
//!
//! Run: cargo run --release --example ssl_test

use qail_pg::PgConnection;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔐 QAIL TLS CONNECTION TEST");
    println!("===========================\n");

    // Test 1: Basic TLS connection (server cert only)
    print!("Testing TLS connection... ");
    match PgConnection::connect_tls("127.0.0.1", 5432, "orion", "swb_staging_local", None).await {
        Ok(_conn) => {
            println!("✅ TLS connection successful!");
        }
        Err(e) => {
            println!("❌ Failed: {:?}", e);
        }
    }

    // Test 2: mTLS connection (client cert) - requires pg_hba.conf setup
    // Uncomment when client cert auth is configured in PostgreSQL
    /*
    use qail_pg::TlsConfig;

    print!("Testing mTLS connection... ");
    let config = TlsConfig {
        client_cert_pem: std::fs::read("/tmp/pg_ssl_test/client.crt")?,
        client_key_pem: std::fs::read("/tmp/pg_ssl_test/client.key")?,
        ca_cert_pem: Some(std::fs::read("/tmp/pg_ssl_test/server.crt")?),
    };

    match PgConnection::connect_mtls(
        "127.0.0.1",
        5432,
        "orion",
        "swb_staging_local",
        config,
    ).await {
        Ok(_conn) => {
            println!("✅ mTLS connection successful!");
        }
        Err(e) => {
            println!("❌ Failed: {:?}", e);
        }
    }
    */

    println!("\n===========================");
    println!("✅ TLS TEST COMPLETE");

    Ok(())
}
