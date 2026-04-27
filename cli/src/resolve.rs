//! Centralized resolution for database URL and SSH tunnel config.
//!
//! Priority chain: CLI flag → `qail.toml` → error.
//!
//! # Example
//!
//! ```ignore
//! let url = resolve_db_url(cli_url.as_deref())?;
//! let ssh = resolve_ssh(cli_ssh.as_deref());
//! ```

use anyhow::Result;
use qail_core::config::QailConfig;

/// Resolve database URL from CLI `--url` flag or `qail.toml`.
///
/// Priority: `--url` > `DATABASE_URL` env > `qail.toml [postgres].url` > error
pub fn resolve_db_url(cli_url: Option<&str>) -> Result<String> {
    // 1. Explicit CLI flag wins
    if let Some(url) = cli_url {
        return Ok(url.to_string());
    }

    // 2. Try DATABASE_URL env var directly
    if let Ok(url) = std::env::var("DATABASE_URL") {
        return Ok(url);
    }

    // 3. Try qail.toml
    match QailConfig::load() {
        Ok(config) => {
            let url = config.postgres.url;
            // Don't return the default placeholder URL
            if url == "postgres://postgres@localhost:5432/postgres" {
                anyhow::bail!(
                    "No database URL configured.\n\n\
                     Set one of:\n  \
                     • --url postgres://user:pass@host/db\n  \
                     • DATABASE_URL env var\n  \
                     • [postgres].url in qail.toml"
                );
            }
            Ok(url)
        }
        Err(_) => {
            anyhow::bail!(
                "No database URL found.\n\n\
                 Set one of:\n  \
                 • --url postgres://user:pass@host/db\n  \
                 • DATABASE_URL env var\n  \
                 • [postgres].url in qail.toml"
            );
        }
    }
}

/// Resolve SSH tunnel host from CLI `--ssh` flag or `qail.toml`.
///
/// Priority: `--ssh` > `qail.toml [postgres].ssh` > None
pub fn resolve_ssh(cli_ssh: Option<&str>) -> Option<String> {
    // 1. Explicit CLI flag wins
    if let Some(ssh) = cli_ssh {
        return Some(ssh.to_string());
    }

    // 2. Try qail.toml
    QailConfig::load().ok().and_then(|c| c.postgres.ssh)
}
