//! Utility functions for qail-cli

use anyhow::Result;

/// Parse a PostgreSQL URL into (host, port, user, password, database).
///
/// Handles: `postgres://user:pass@host:port/database`
pub fn parse_pg_url(url: &str) -> Result<(String, u16, String, Option<String>, String)> {
    // Strip scheme
    let rest = url
        .strip_prefix("postgres://")
        .or_else(|| url.strip_prefix("postgresql://"))
        .ok_or_else(|| anyhow::anyhow!("URL must start with postgres:// or postgresql://"))?;

    // Split at '/' for database
    let (authority, database) = rest
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("Missing database in URL"))?;
    let database = database
        .split('?')
        .next()
        .unwrap_or(database)
        .to_string();
    if database.is_empty() {
        return Err(anyhow::anyhow!("Missing database in URL"));
    }

    // Split authority into userinfo and host
    let (user, password, hostport) = if let Some((userinfo, hp)) = authority.split_once('@') {
        if let Some((u, p)) = userinfo.split_once(':') {
            (u.to_string(), Some(p.to_string()), hp)
        } else {
            (userinfo.to_string(), None, hp)
        }
    } else {
        ("postgres".to_string(), None, authority)
    };

    // Split host:port
    let (host, port) = if let Some((h, p)) = hostport.rsplit_once(':') {
        (h.to_string(), p.parse::<u16>().unwrap_or(5432))
    } else {
        (hostport.to_string(), 5432u16)
    };

    if host.is_empty() {
        return Err(anyhow::anyhow!("Missing host in URL"));
    }

    Ok((host, port, user, password, database))
}

/// Parse a generic URL into (scheme, host, port, path).
/// Used by exec.rs for SSH tunnel URL rewriting.
pub fn parse_url_parts(url: &str) -> Result<(String, String, u16, String)> {
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| anyhow::anyhow!("Invalid URL: missing ://"))?;
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));

    // Strip userinfo
    let hostport = if let Some((_userinfo, hp)) = authority.split_once('@') {
        hp
    } else {
        authority
    };

    let (host, port) = if let Some((h, p)) = hostport.rsplit_once(':') {
        (h.to_string(), p.parse::<u16>().unwrap_or(5432))
    } else {
        (hostport.to_string(), 5432u16)
    };

    Ok((scheme.to_string(), host, port, format!("/{}", path)))
}

/// Rewrite a URL to point at a different host:port (for SSH tunneling).
pub fn rewrite_url_host(url: &str, new_host: &str, new_port: u16) -> Result<String> {
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| anyhow::anyhow!("Invalid URL: missing ://"))?;
    let (authority, path_and_rest) = rest.split_once('/').unwrap_or((rest, ""));

    // Preserve userinfo if present
    let userinfo = authority.split_once('@').map(|(u, _)| u);

    let mut result = format!("{}://", scheme);
    if let Some(ui) = userinfo {
        result.push_str(ui);
        result.push('@');
    }
    result.push_str(&format!("{}:{}/{}", new_host, new_port, path_and_rest));

    Ok(result)
}

