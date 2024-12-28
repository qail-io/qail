//! Example: Run the QAIL Gateway
//!
//! ```bash
//! DATABASE_URL=postgres://localhost/mydb cargo run -p qail-gateway --example serve
//! ```

use qail_gateway::Gateway;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing for logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("qail_gateway=info".parse()?)
                .add_directive("tower_http=info".parse()?),
        )
        .init();
    
    // Get configuration from environment
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://localhost/qail".to_string());
    
    let bind_address = std::env::var("BIND_ADDRESS")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    
    let schema_path = std::env::var("SCHEMA_PATH").ok();
    let policy_path = std::env::var("POLICY_PATH").ok();
    
    tracing::info!("Starting QAIL Gateway...");
    tracing::info!("  Database: {}", database_url);
    if let Some(ref path) = schema_path {
        tracing::info!("  Schema: {}", path);
    }
    if let Some(ref path) = policy_path {
        tracing::info!("  Policies: {}", path);
    }
    
    let mut builder = Gateway::builder()
        .database(&database_url)
        .bind(&bind_address);
    
    if let Some(ref path) = schema_path {
        builder = builder.schema(path);
    }
    if let Some(ref path) = policy_path {
        builder = builder.policy(path);
    }
    
    let gateway = builder.build_and_init().await?;
    
    gateway.serve().await?;
    
    Ok(())
}
