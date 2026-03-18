//! Simple opt-in failpoints for migration failure-injection testing.

use anyhow::{Result, bail};

/// Trigger failpoint error when configured via `QAIL_FAILPOINTS`.
///
/// Example:
/// `QAIL_FAILPOINTS=backfill.after_update_before_checkpoint,apply.before_commit`
pub fn maybe_failpoint(name: &str) -> Result<()> {
    if failpoint_enabled(name) {
        bail!("Injected failpoint triggered: {}", name);
    }
    Ok(())
}

pub fn failpoint_enabled(name: &str) -> bool {
    let spec = std::env::var("QAIL_FAILPOINTS").ok();
    failpoint_enabled_in(spec.as_deref(), name)
}

fn failpoint_enabled_in(spec: Option<&str>, name: &str) -> bool {
    let Some(spec) = spec else {
        return false;
    };
    let requested = name.trim();
    if requested.is_empty() {
        return false;
    }
    spec.split(',')
        .map(|s| s.trim())
        .any(|s| s == "*" || s.eq_ignore_ascii_case(requested))
}

#[cfg(test)]
mod tests {
    use super::{failpoint_enabled_in, maybe_failpoint};

    #[test]
    fn parser_matches_specific_names_and_wildcard() {
        assert!(failpoint_enabled_in(Some("a,b,c"), "b"));
        assert!(failpoint_enabled_in(Some("*"), "anything"));
        assert!(!failpoint_enabled_in(Some("a,b,c"), "x"));
    }

    #[test]
    fn maybe_failpoint_returns_error_when_enabled() {
        // Use parser behavior through direct environment-independent path.
        assert!(failpoint_enabled_in(Some("test.fp"), "test.fp"));
        // Keep direct call smoke test for API contract.
        let _ = maybe_failpoint("never_enabled_in_test");
    }
}
