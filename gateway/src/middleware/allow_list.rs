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
    /// Entries that parse as QAIL, keyed by the AST digest.
    commands: std::collections::HashMap<String, Vec<qail_core::ast::Qail>>,
}

fn command_digest(cmd: &qail_core::ast::Qail) -> String {
    let mut digest = crate::cache::CacheKeyDigest::new("qail:allow-list:cmd:v1");
    digest.qail(cmd);
    digest.finish_hex()
}

impl QueryAllowList {
    /// Create a new, disabled allow-list.
    pub fn new() -> Self {
        Self {
            enabled: false,
            allowed: std::collections::HashSet::new(),
            commands: std::collections::HashMap::new(),
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

    /// Add a query pattern to the allow-list.
    ///
    /// Returns `false` when the pattern is not QAIL text: it is kept for
    /// [`Self::is_allowed`] but admits no command in [`Self::allows_command`].
    pub fn allow(&mut self, pattern: &str) -> bool {
        self.enabled = true;
        self.allowed.insert(pattern.to_string());
        match qail_core::parser::parse(pattern) {
            Ok(cmd) => {
                let entries = self.commands.entry(command_digest(&cmd)).or_default();
                if !entries.contains(&cmd) {
                    entries.push(cmd);
                }
                true
            }
            Err(_) => false,
        }
    }

    /// Load allow-list from a file (one pattern per line)
    pub fn load_from_file(&mut self, path: &str) -> Result<(), std::io::Error> {
        let content = std::fs::read_to_string(path)?;
        // SECURITY: fail closed once an allow-list file is configured.
        // Empty/comment-only files should deny all queries instead of allowing all.
        self.enabled = true;
        let mut loaded = 0usize;
        let mut not_qail = Vec::new();
        for (index, line) in content.lines().enumerate() {
            let trimmed = line.trim();
            if !trimmed.is_empty() && !trimmed.starts_with('#') {
                if !self.allow(trimmed) {
                    not_qail.push(index + 1);
                }
                loaded = loaded.saturating_add(1);
            }
        }
        if loaded == 0 {
            tracing::warn!(
                path = %path,
                "Allow-list file loaded with zero active patterns; all queries will be denied"
            );
        }
        if !not_qail.is_empty() {
            tracing::warn!(
                path = %path,
                lines = ?not_qail,
                "Allow-list lines that do not parse as QAIL admit no query (SQL lines are not matched)"
            );
        }
        Ok(())
    }

    /// Check if a query pattern is allowed (exact string membership).
    pub fn is_allowed(&self, pattern: &str) -> bool {
        if !self.enabled {
            return true; // Allow-list disabled: all queries pass
        }
        self.allowed.contains(pattern)
    }

    /// Check whether some entry parses to exactly `cmd`.
    ///
    /// Matching is on the AST, not on a rendering of it: QAIL `Display` and
    /// `to_sql()` both drop clauses, so a string match would admit a command
    /// carrying more than the listed one.
    pub fn allows_command(&self, cmd: &qail_core::ast::Qail) -> bool {
        if !self.enabled {
            return true; // Allow-list disabled: all queries pass
        }
        self.commands
            .get(&command_digest(cmd))
            .is_some_and(|entries| entries.contains(cmd))
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
