//! Branch Context for Data Virtualization
//!
//! Provides branch identity for row-level branching ("GitHub for Databases").
//! Each request can target a specific branch via the `X-Branch-ID` header.
//!
//! When on main (no branch), queries hit tables directly.
//! When on a branch, reads merge main + overlay, writes go to overlay.
//!
//! # Example
//!
//! ```
//! use qail_core::branch::BranchContext;
//!
//! // Main branch — no overlay
//! let ctx = BranchContext::main();
//! assert!(ctx.is_main());
//!
//! // Feature branch — reads merge, writes go to overlay
//! let ctx = BranchContext::branch("feature-auth");
//! assert_eq!(ctx.branch_name(), Some("feature-auth"));
//! ```

/// Branch context for data virtualization.
///
/// Determines which branch a request targets. When a branch is active,
/// the gateway applies Copy-on-Write semantics:
/// - **Reads**: main rows UNION branch overlay (overlay wins on PK conflict)
/// - **Writes**: inserted/updated rows go to `_qail_branch_rows` overlay
/// - **Deletes**: a tombstone marker is added to the overlay
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchContext {
    /// Branch name (None = main/default branch)
    branch_id: Option<String>,
}

impl BranchContext {
    /// Maximum branch name length.
    pub const MAX_NAME_LEN: usize = 64;

    /// Create a context targeting the main branch (no overlay).
    pub fn main() -> Self {
        Self { branch_id: None }
    }

    /// Create a context targeting a named branch.
    ///
    /// # Panics
    /// Panics if the branch name is invalid. Use `try_branch` for fallible creation.
    pub fn branch(name: &str) -> Self {
        assert!(
            Self::is_valid_name(name),
            "Invalid branch name: '{}'. Must be 1-{} chars, alphanumeric/hyphen/underscore/dot only.",
            name,
            Self::MAX_NAME_LEN
        );
        Self {
            branch_id: Some(name.to_string()),
        }
    }

    /// Try to create a branch context, returning None if the name is invalid.
    pub fn try_branch(name: &str) -> Option<Self> {
        if Self::is_valid_name(name) {
            Some(Self {
                branch_id: Some(name.to_string()),
            })
        } else {
            None
        }
    }

    /// Parse an optional branch header value into a [`BranchContext`].
    ///
    /// Rules:
    /// - `None`, empty string, and `main` (case-insensitive) map to main.
    /// - Any other value must pass [`Self::is_valid_name`].
    pub fn parse_header(value: Option<&str>) -> Result<Self, String> {
        match value {
            None => Ok(Self::main()),
            Some(name) if name.is_empty() || name.eq_ignore_ascii_case("main") => Ok(Self::main()),
            Some(name) if Self::is_valid_name(name) => Ok(Self {
                branch_id: Some(name.to_string()),
            }),
            Some(name) => Err(format!(
                "Invalid branch name '{}'. Use 1-{} ASCII alphanumeric/._- characters",
                name,
                Self::MAX_NAME_LEN
            )),
        }
    }

    /// Create from an optional branch name (None = main).
    /// Invalid branch names are treated as main for backward compatibility.
    pub fn from_header(value: Option<&str>) -> Self {
        Self::parse_header(value).unwrap_or_else(|_| Self::main())
    }

    /// Validate a branch name.
    ///
    /// Rules:
    /// - 1 to 64 characters
    /// - Only alphanumeric, hyphens (`-`), underscores (`_`), and dots (`.`)
    /// - Must not start with `.` or `-`
    pub fn is_valid_name(name: &str) -> bool {
        !name.is_empty()
            && name.len() <= Self::MAX_NAME_LEN
            && !name.starts_with('.')
            && !name.starts_with('-')
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
    }

    /// Returns true if this is the main branch.
    pub fn is_main(&self) -> bool {
        self.branch_id.is_none()
    }

    /// Returns true if this is a named branch (not main).
    pub fn has_branch(&self) -> bool {
        self.branch_id.is_some()
    }

    /// Get the branch name, if any.
    pub fn branch_name(&self) -> Option<&str> {
        self.branch_id.as_deref()
    }
}

impl std::fmt::Display for BranchContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.branch_id {
            Some(name) => write!(f, "BranchContext({})", name),
            None => write!(f, "BranchContext(main)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_main_branch() {
        let ctx = BranchContext::main();
        assert!(ctx.is_main());
        assert!(!ctx.has_branch());
        assert_eq!(ctx.branch_name(), None);
    }

    #[test]
    fn test_named_branch() {
        let ctx = BranchContext::branch("feature-auth");
        assert!(!ctx.is_main());
        assert!(ctx.has_branch());
        assert_eq!(ctx.branch_name(), Some("feature-auth"));
    }

    #[test]
    fn test_from_header() {
        assert!(BranchContext::from_header(None).is_main());
        assert!(BranchContext::from_header(Some("")).is_main());
        assert!(BranchContext::from_header(Some("main")).is_main());
        assert!(BranchContext::from_header(Some("MAIN")).is_main());
        assert_eq!(
            BranchContext::from_header(Some("feat-1")).branch_name(),
            Some("feat-1")
        );
    }

    #[test]
    fn test_parse_header_strict_rejects_invalid() {
        assert!(BranchContext::parse_header(Some("feat-1")).is_ok());
        assert!(BranchContext::parse_header(Some("main")).is_ok());
        assert!(BranchContext::parse_header(Some("MAIN")).is_ok());
        assert!(BranchContext::parse_header(None).is_ok());
        assert!(BranchContext::parse_header(Some("bad name")).is_err());
        assert!(BranchContext::parse_header(Some("🚀")).is_err());
    }

    #[test]
    fn test_display() {
        assert_eq!(BranchContext::main().to_string(), "BranchContext(main)");
        assert_eq!(
            BranchContext::branch("dev").to_string(),
            "BranchContext(dev)"
        );
    }

    #[test]
    fn test_equality() {
        assert_eq!(BranchContext::main(), BranchContext::main());
        assert_eq!(BranchContext::branch("a"), BranchContext::branch("a"));
        assert_ne!(BranchContext::main(), BranchContext::branch("a"));
    }

    // ================================================================
    // Branch name validation tests
    // ================================================================

    #[test]
    fn test_valid_branch_names() {
        assert!(BranchContext::is_valid_name("feature-auth"));
        assert!(BranchContext::is_valid_name("dev"));
        assert!(BranchContext::is_valid_name("release.1.0"));
        assert!(BranchContext::is_valid_name("my_branch_2"));
        assert!(BranchContext::is_valid_name("a")); // single char
    }

    #[test]
    fn test_invalid_branch_names() {
        assert!(!BranchContext::is_valid_name("")); // empty
        assert!(!BranchContext::is_valid_name(".hidden")); // starts with dot
        assert!(!BranchContext::is_valid_name("-flag")); // starts with hyphen
        assert!(!BranchContext::is_valid_name("has space")); // space
        assert!(!BranchContext::is_valid_name("has;semicolon")); // SQL injection char
        assert!(!BranchContext::is_valid_name("it's bad")); // single quote
        assert!(!BranchContext::is_valid_name("a/b")); // slash (path traversal)
        assert!(!BranchContext::is_valid_name(&"x".repeat(65))); // too long
    }

    #[test]
    fn test_try_branch() {
        assert!(BranchContext::try_branch("valid-name").is_some());
        assert!(BranchContext::try_branch("has;injection").is_none());
        assert!(BranchContext::try_branch("").is_none());
    }

    #[test]
    fn test_from_header_rejects_invalid() {
        // Invalid branch names silently fall back to main
        assert!(BranchContext::from_header(Some("has;semicolon")).is_main());
        assert!(BranchContext::from_header(Some(".hidden")).is_main());
        assert!(BranchContext::from_header(Some("' OR 1=1 --")).is_main());
        // Valid names still work
        assert_eq!(
            BranchContext::from_header(Some("feat-1")).branch_name(),
            Some("feat-1")
        );
    }

    #[test]
    #[should_panic(expected = "Invalid branch name")]
    fn test_branch_panics_on_invalid() {
        let _ = BranchContext::branch("'; DROP TABLE users; --");
    }
}
