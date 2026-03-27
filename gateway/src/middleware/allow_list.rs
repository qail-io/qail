// ============================================================================
// Query Allow-List
// ============================================================================

/// Query allow-list: only pre-approved query patterns are executed.
///
/// When enabled, any query not in the allow-list is rejected.
/// This prevents arbitrary query injection and limits the attack surface.
#[derive(Debug, Default)]
pub struct QueryAllowList {
    enabled: bool,
    allowed: std::collections::HashSet<String>,
}

impl QueryAllowList {
    /// Create a new, disabled allow-list.
    pub fn new() -> Self {
        Self {
            enabled: false,
            allowed: std::collections::HashSet::new(),
        }
    }

    /// Enable the allow-list (queries not in the list will be rejected)
    pub fn enable(&mut self) {
        self.enabled = true;
    }

    /// Returns whether allow-list enforcement is active.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Add a query pattern to the allow-list
    pub fn allow(&mut self, pattern: &str) {
        self.enabled = true;
        self.allowed.insert(pattern.to_string());
    }

    /// Load allow-list from a file (one pattern per line)
    pub fn load_from_file(&mut self, path: &str) -> Result<(), std::io::Error> {
        let content = std::fs::read_to_string(path)?;
        // SECURITY: fail closed once an allow-list file is configured.
        // Empty/comment-only files should deny all queries instead of allowing all.
        self.enabled = true;
        let mut loaded = 0usize;
        for line in content.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() && !trimmed.starts_with('#') {
                self.allow(trimmed);
                loaded = loaded.saturating_add(1);
            }
        }
        if loaded == 0 {
            tracing::warn!(
                path = %path,
                "Allow-list file loaded with zero active patterns; all queries will be denied"
            );
        }
        Ok(())
    }

    /// Check if a query pattern is allowed
    pub fn is_allowed(&self, pattern: &str) -> bool {
        if !self.enabled {
            return true; // Allow-list disabled: all queries pass
        }
        self.allowed.contains(pattern)
    }

    /// Number of patterns in the allow-list.
    pub fn len(&self) -> usize {
        self.allowed.len()
    }

    /// Returns `true` if the allow-list has no patterns.
    pub fn is_empty(&self) -> bool {
        self.allowed.is_empty()
    }
}
