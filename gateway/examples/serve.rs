//! Example: Run the QAIL Gateway
//!
//! ```bash
//! cargo run -p qail-gateway --example serve
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
    
    // Get database URL from environment or use default
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://localhost/qail".to_string());
    
    // Get bind address from environment or use default
    let bind_address = std::env::var("BIND_ADDRESS")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    
    tracing::info!("Starting QAIL Gateway...");
    
    let gateway = Gateway::builder()
        .database(&database_url)
        .bind(&bind_address)
        .build_and_init()
        .await?;
    
    gateway.serve().await?;
    
    Ok(())
}
