//! Utility functions for qail-cli

use anyhow::Result;

/// Parse a PostgreSQL URL into (host, port, user, password, database).
pub fn parse_pg_url(url: &str) -> Result<(String, u16, String, Option<String>, String)> {
    let parsed = url::Url::parse(url)
        .map_err(|e| anyhow::anyhow!("Invalid URL: {}", e))?;
    
    let host = parsed.host_str()
        .ok_or_else(|| anyhow::anyhow!("Missing host in URL"))?
        .to_string();
    
    let port = parsed.port().unwrap_or(5432);
    
    let user = if parsed.username().is_empty() {
        "postgres".to_string()
    } else {
        parsed.username().to_string()
    };
    
    let password = parsed.password().map(|s| s.to_string());
    
    let database = parsed.path().trim_start_matches('/').to_string();
    if database.is_empty() {
        return Err(anyhow::anyhow!("Missing database in URL"));
    }
    
    Ok((host, port, user, password, database))
}
