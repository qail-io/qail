//! Example: Run the QAIL Gateway
//!
//! ```bash
//! DATABASE_URL=postgres://localhost/mydb cargo run -p qail-gateway --example serve
//! ```
//!
//! Config resolution (highest priority wins):
//!   1. Environment variables (DATABASE_URL, BIND_ADDRESS, SCHEMA_PATH, POLICY_PATH)
//!   2. `qail-gateway.toml` in the current working directory
//!   3. Built-in defaults

use qail_gateway::{Gateway, GatewayConfig};

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

    // ── Load config from TOML file (if present) ────────────────────
    let config = load_config()?;

    tracing::info!("Starting QAIL Gateway...");
    tracing::info!("  Database: {}", config.database_url);
    if let Some(ref path) = config.schema_path {
        tracing::info!("  Schema: {}", path);
    }
    if let Some(ref path) = config.policy_path {
        tracing::info!("  Policies: {}", path);
    }
    if !config.allowed_tables.is_empty() {
        tracing::info!(
            "  Allowlist mode: {} table(s) allowed: {:?}",
            config.allowed_tables.len(),
            config.allowed_tables
        );
    } else if !config.blocked_tables.is_empty() {
        tracing::info!(
            "  Blocked tables: {:?}",
            config.blocked_tables
        );
    }

    let mut gateway = Gateway::new(config);
    gateway.init().await?;
    gateway.serve().await?;

    Ok(())
}

/// Load GatewayConfig from `qail-gateway.toml` in CWD, then apply env overrides.
fn load_config() -> anyhow::Result<GatewayConfig> {
    const CONFIG_FILES: &[&str] = &["qail-gateway.toml", "gateway.toml"];

    let mut config = GatewayConfig::default();

    // Try loading from TOML config file in CWD
    for filename in CONFIG_FILES {
        let path = std::path::Path::new(filename);
        if path.exists() {
            let content = std::fs::read_to_string(path)?;
            config = toml::from_str(&content)?;
            tracing::info!("  Config: loaded from {}", filename);

            // Set config_root to the directory containing the config file,
            // if not explicitly configured. This enables manifest writing.
            if config.config_root.is_none() {
                if let Some(parent) = path.canonicalize().ok().and_then(|p| {
                    p.parent().map(|d| d.to_string_lossy().to_string())
                }) {
                    config.config_root = Some(parent);
                }
            }

            break;
        }
    }

    // Environment variables override TOML settings (highest priority)
    if let Ok(url) = std::env::var("DATABASE_URL") {
        config.database_url = url;
    }
    if let Ok(addr) = std::env::var("BIND_ADDRESS") {
        config.bind_address = addr;
    }
    if let Ok(path) = std::env::var("SCHEMA_PATH") {
        config.schema_path = Some(path);
    }
    if let Ok(path) = std::env::var("POLICY_PATH") {
        config.policy_path = Some(path);
    }
    if let Ok(rate) = std::env::var("RATE_LIMIT_RATE") {
        if let Ok(r) = rate.parse::<f64>() {
            config.rate_limit_rate = r;
            config.rate_limit_burst = std::env::var("RATE_LIMIT_BURST")
                .ok()
                .and_then(|b| b.parse().ok())
                .unwrap_or((r * 2.0) as u32);
        }
    }

    Ok(config)
}
