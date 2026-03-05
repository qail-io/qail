use std::collections::HashSet;
use std::path::Path;

use crate::error::GatewayError;

pub(super) fn load_rpc_allow_list(path: &Path) -> Result<HashSet<String>, GatewayError> {
    let content = std::fs::read_to_string(path).map_err(|e| {
        GatewayError::Config(format!(
            "Failed to read RPC allow-list '{}': {}",
            path.display(),
            e
        ))
    })?;

    let mut entries = HashSet::new();
    for line in content.lines() {
        let trimmed = line.split('#').next().unwrap_or("").trim();
        if trimmed.is_empty() {
            continue;
        }
        entries.insert(trimmed.to_ascii_lowercase());
    }

    Ok(entries)
}
