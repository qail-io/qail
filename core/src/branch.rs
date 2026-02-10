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
    /// Create a context targeting the main branch (no overlay).
    pub fn main() -> Self {
        Self { branch_id: None }
    }

    /// Create a context targeting a named branch.
    pub fn branch(name: &str) -> Self {
        Self {
            branch_id: Some(name.to_string()),
        }
    }

    /// Create from an optional branch name (None = main).
    pub fn from_header(value: Option<&str>) -> Self {
        match value {
            Some(name) if !name.is_empty() && name != "main" => Self::branch(name),
            _ => Self::main(),
        }
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
        assert_eq!(
            BranchContext::from_header(Some("feat-1")).branch_name(),
            Some("feat-1")
        );
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
        assert_eq!(
            BranchContext::branch("a"),
            BranchContext::branch("a")
        );
        assert_ne!(BranchContext::main(), BranchContext::branch("a"));
    }
}
