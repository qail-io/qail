//! Migration policy loading from qail.toml.
//!
//! The policy is resolved by walking from the current directory to the
//! filesystem root, taking the nearest `qail.toml` that declares
//! `[migrations.policy]`. This mirrors `nearest_qail_toml` in the LSP and the
//! config lookup in `schema_tools`, so the answer no longer depends on which
//! directory `qail` happened to be invoked from.
//!
//! Two rules keep the resolution honest:
//!
//! - Every run prints which file the policy came from. A policy that silently
//!   fails to load is indistinguishable from one that loaded and permitted the
//!   operation, and that is the failure this module exists to prevent.
//! - When no file declares a policy, destructive operations are refused rather
//!   than falling back to the permissive default. `require-flag` is a fine
//!   default for a project that never drops anything; it is not a safe silent
//!   answer to "may I drop this column?".

use crate::colors::*;
use anyhow::{Context, Result, anyhow, bail};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EnforcementMode {
    Deny,
    #[default]
    RequireFlag,
    Allow,
}

impl EnforcementMode {
    fn parse(raw: &str, field: &str, source: &Path) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "deny" => Ok(Self::Deny),
            "require-flag" | "require_flag" | "requireflag" => Ok(Self::RequireFlag),
            "allow" => Ok(Self::Allow),
            other => bail!(
                "Invalid migrations.policy.{} value '{}' in {}. Allowed: deny, require-flag, allow",
                field,
                other,
                source.display()
            ),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Deny => "deny",
            Self::RequireFlag => "require-flag",
            Self::Allow => "allow",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReceiptValidationMode {
    Warn,
    #[default]
    Error,
}

impl ReceiptValidationMode {
    fn parse(raw: &str, source: &Path) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "warn" => Ok(Self::Warn),
            "error" => Ok(Self::Error),
            other => bail!(
                "Invalid migrations.policy.receipt_validation value '{}' in {}. Allowed: warn, error",
                other,
                source.display()
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MigrationPolicy {
    pub destructive: EnforcementMode,
    pub lock_risk: EnforcementMode,
    pub lock_risk_max_score: u8,
    pub require_shadow_receipt: bool,
    pub allow_no_shadow_receipt: bool,
    pub receipt_validation: ReceiptValidationMode,
    /// The `qail.toml` this policy came from.
    ///
    /// When `declared` is false this is the nearest `qail.toml` found (which
    /// declared no policy), or `None` when no file was found at all.
    pub source: Option<PathBuf>,
    /// Whether a `[migrations.policy]` table was actually declared.
    ///
    /// When false, every field above is a default that nobody chose, and
    /// destructive operations are refused — see
    /// [`ensure_destructive_policy_declared`].
    pub declared: bool,
}

impl Default for MigrationPolicy {
    fn default() -> Self {
        Self {
            destructive: EnforcementMode::RequireFlag,
            lock_risk: EnforcementMode::RequireFlag,
            lock_risk_max_score: 90,
            require_shadow_receipt: true,
            allow_no_shadow_receipt: true,
            receipt_validation: ReceiptValidationMode::Error,
            source: None,
            declared: false,
        }
    }
}

/// The outcome of walking the directory tree for a policy.
#[derive(Debug, Clone)]
pub struct PolicyResolution {
    pub policy: MigrationPolicy,
    /// Other `qail.toml` files on the walked path that declare a
    /// `[migrations.policy]` disagreeing with the one that won. A genuine
    /// ambiguity for the developer to resolve, not one to silently pick a
    /// winner for.
    pub conflicts: Vec<PathBuf>,
}

/// Load the migration policy for the current directory and report its origin.
pub fn load_migration_policy() -> Result<MigrationPolicy> {
    let cwd = std::env::current_dir()
        .context("Failed to determine the current directory for qail.toml lookup")?;
    let resolution = resolve_migration_policy(&cwd)?;
    report_policy_source(&resolution);
    Ok(resolution.policy)
}

/// Resolve the policy by walking from `start` to the filesystem root.
///
/// The nearest `qail.toml` that declares `[migrations.policy]` wins. Files
/// closer to `start` that declare no policy do not mask an ancestor that does.
pub fn resolve_migration_policy(start: &Path) -> Result<PolicyResolution> {
    let configs = crate::project::ancestor_configs(start);
    let nearest_file = configs.first().cloned();

    let mut declared: Vec<MigrationPolicy> = Vec::new();
    for candidate in &configs {
        if let Some(policy) = parse_policy_file(candidate)? {
            declared.push(policy);
        }
    }

    // `ancestor_configs` yields nearest-first, so the first declaration wins.
    let Some(policy) = declared.first().cloned() else {
        return Ok(PolicyResolution {
            policy: MigrationPolicy {
                source: nearest_file,
                declared: false,
                ..Default::default()
            },
            conflicts: Vec::new(),
        });
    };

    let conflicts = declared
        .iter()
        .skip(1)
        .filter(|other| policy_values_differ(&policy, other))
        .filter_map(|other| other.source.clone())
        .collect();

    Ok(PolicyResolution { policy, conflicts })
}

/// Refuse destructive work when no `[migrations.policy]` was declared.
///
/// A `deny` policy that is never read is worse than no policy at all: it reads
/// as a control that is protecting you. Rather than fall back to `require-flag`
/// — which `--allow-destructive` satisfies — a missing declaration blocks the
/// operation and says where it looked.
pub fn ensure_destructive_policy_declared(policy: &MigrationPolicy, detail: &str) -> Result<()> {
    if policy.declared {
        return Ok(());
    }

    let looked = match &policy.source {
        Some(path) => format!("{} declares no [migrations.policy]", path.display()),
        None => "no qail.toml was found from the current directory upward".to_string(),
    };

    bail!(
        "Migration blocked: destructive operations detected ({}), but {}.\n\
         Declare the policy explicitly in that file before applying:\n\
         \n\
         \x20   [migrations.policy]\n\
         \x20   destructive = \"deny\"  # or \"require-flag\" / \"allow\"\n\
         \n\
         --allow-destructive does not substitute for a declared policy.",
        detail,
        looked
    );
}

/// Print which file the policy came from, and warn about ambiguity.
///
/// One line per run. It converts a silent misconfiguration into an obvious one
/// for almost nothing.
fn report_policy_source(resolution: &PolicyResolution) {
    let policy = &resolution.policy;

    match (&policy.source, policy.declared) {
        (Some(path), true) => println!(
            "{} policy: destructive={} lock_risk={} receipt_validation={} (from {})",
            "→".cyan(),
            policy.destructive.as_str(),
            policy.lock_risk.as_str(),
            match policy.receipt_validation {
                ReceiptValidationMode::Warn => "warn",
                ReceiptValidationMode::Error => "error",
            },
            path.display()
        ),
        (Some(path), false) => println!(
            "{} policy: no [migrations.policy] in {} — destructive operations are blocked until one is declared",
            "!".yellow(),
            path.display()
        ),
        (None, _) => println!(
            "{} policy: no qail.toml found — destructive operations are blocked until one declares [migrations.policy]",
            "!".yellow()
        ),
    }

    if resolution.conflicts.is_empty() {
        return;
    }

    eprintln!(
        "{} Multiple qail.toml files declare a conflicting [migrations.policy]:",
        "!".yellow()
    );
    if let Some(winner) = &policy.source {
        eprintln!("    using:    {}", winner.display());
    }
    for path in &resolution.conflicts {
        eprintln!("    ignoring: {}", path.display());
    }
    eprintln!("    Resolve the ambiguity by keeping one declaration.");
}

/// Parse `[migrations.policy]` out of a single `qail.toml`.
///
/// Returns `None` when the file parses but declares no policy table.
fn parse_policy_file(path: &Path) -> Result<Option<MigrationPolicy>> {
    let content = fs::read_to_string(path).with_context(|| {
        format!(
            "Failed to read {} for migration policy",
            path.display()
        )
    })?;

    let config: toml::Value = toml::from_str(&content)
        .map_err(|e| anyhow!("Failed to parse {}: {}", path.display(), e))?;

    let Some(policy_tbl) = config
        .get("migrations")
        .and_then(|v| v.get("policy"))
        .and_then(|v| v.as_table())
    else {
        return Ok(None);
    };

    let mut policy = MigrationPolicy {
        source: Some(path.to_path_buf()),
        declared: true,
        ..Default::default()
    };

    if let Some(raw) = policy_tbl.get("destructive") {
        let s = raw.as_str().ok_or_else(|| {
            anyhow!(
                "migrations.policy.destructive must be a string (deny|require-flag|allow) in {}",
                path.display()
            )
        })?;
        policy.destructive = EnforcementMode::parse(s, "destructive", path)?;
    }

    if let Some(raw) = policy_tbl.get("lock_risk") {
        let s = raw.as_str().ok_or_else(|| {
            anyhow!(
                "migrations.policy.lock_risk must be a string (deny|require-flag|allow) in {}",
                path.display()
            )
        })?;
        policy.lock_risk = EnforcementMode::parse(s, "lock_risk", path)?;
    }

    if let Some(raw) = policy_tbl
        .get("lock_risk_max_score")
        .or_else(|| policy_tbl.get("max_lock_risk_score"))
    {
        let score = raw.as_integer().ok_or_else(|| {
            anyhow!(
                "migrations.policy.lock_risk_max_score must be an integer in {}",
                path.display()
            )
        })?;
        if !(0..=100).contains(&score) {
            bail!(
                "migrations.policy.lock_risk_max_score must be between 0 and 100 (got {}) in {}",
                score,
                path.display()
            );
        }
        policy.lock_risk_max_score = u8::try_from(score).unwrap_or(100);
    }

    if let Some(raw) = policy_tbl.get("require_shadow_receipt") {
        policy.require_shadow_receipt = raw.as_bool().ok_or_else(|| {
            anyhow!(
                "migrations.policy.require_shadow_receipt must be true/false in {}",
                path.display()
            )
        })?;
    }

    if let Some(raw) = policy_tbl
        .get("allow_no_shadow_receipt")
        .or_else(|| policy_tbl.get("allow_shadow_receipt_bypass"))
    {
        policy.allow_no_shadow_receipt = raw.as_bool().ok_or_else(|| {
            anyhow!(
                "migrations.policy.allow_no_shadow_receipt must be true/false in {}",
                path.display()
            )
        })?;
    }

    if let Some(raw) = policy_tbl.get("receipt_validation") {
        let s = raw.as_str().ok_or_else(|| {
            anyhow!(
                "migrations.policy.receipt_validation must be string in {}",
                path.display()
            )
        })?;
        policy.receipt_validation = ReceiptValidationMode::parse(s, path)?;
    }

    Ok(Some(policy))
}

/// Compare the declared values, ignoring provenance.
fn policy_values_differ(a: &MigrationPolicy, b: &MigrationPolicy) -> bool {
    a.destructive != b.destructive
        || a.lock_risk != b.lock_risk
        || a.lock_risk_max_score != b.lock_risk_max_score
        || a.require_shadow_receipt != b.require_shadow_receipt
        || a.allow_no_shadow_receipt != b.allow_no_shadow_receipt
        || a.receipt_validation != b.receipt_validation
}

#[cfg(test)]
mod tests {
    use super::{
        EnforcementMode, MigrationPolicy, ReceiptValidationMode, ensure_destructive_policy_declared,
        resolve_migration_policy,
    };
    use crate::project::TempTree;
    use std::path::PathBuf;

    #[test]
    fn default_policy_is_conservative() {
        let p = MigrationPolicy::default();
        assert_eq!(p.destructive, EnforcementMode::RequireFlag);
        assert_eq!(p.lock_risk, EnforcementMode::RequireFlag);
        assert_eq!(p.lock_risk_max_score, 90);
        assert!(p.require_shadow_receipt);
        assert!(p.allow_no_shadow_receipt);
        assert_eq!(p.receipt_validation, ReceiptValidationMode::Error);
        assert!(!p.declared, "a default nobody chose is not a declaration");
    }

    #[test]
    fn policy_resolves_the_same_from_any_subdirectory() {
        let tree = TempTree::new();
        tree.write(
            "qail.toml",
            "[project]\nname = \"t\"\n\n[migrations.policy]\ndestructive = \"deny\"\n",
        );
        let nested = tree.dir("gateway/deep/nested");

        for start in [tree.path(""), tree.path("gateway"), nested] {
            let resolved = resolve_migration_policy(&start).expect("resolve");
            assert!(resolved.policy.declared, "from {}", start.display());
            assert_eq!(
                resolved.policy.destructive,
                EnforcementMode::Deny,
                "policy must not depend on the invocation directory (from {})",
                start.display()
            );
            assert_eq!(resolved.policy.source, Some(tree.path("qail.toml")));
        }
    }

    #[test]
    fn nearer_file_without_a_policy_does_not_mask_an_ancestor() {
        let tree = TempTree::new();
        tree.write(
            "qail.toml",
            "[project]\nname = \"root\"\n\n[migrations.policy]\ndestructive = \"deny\"\n",
        );
        // The exact shape that made a `deny` inert: a nearer file that resolves
        // the project but declares no policy.
        tree.write("gateway/qail.toml", "[project]\nname = \"gateway\"\n");

        let resolved = resolve_migration_policy(&tree.path("gateway")).expect("resolve");

        assert!(resolved.policy.declared);
        assert_eq!(resolved.policy.destructive, EnforcementMode::Deny);
        assert_eq!(resolved.policy.source, Some(tree.path("qail.toml")));
    }

    #[test]
    fn a_file_without_a_policy_table_is_not_a_declaration() {
        let tree = TempTree::new();
        tree.write("qail.toml", "[project]\nname = \"t\"\nmode = \"postgres\"\n");
        if !tree.ancestors_are_clean() {
            return; // a stray qail.toml above temp_dir would join the walk
        }

        let resolved = resolve_migration_policy(&tree.path("")).expect("resolve");

        assert!(
            !resolved.policy.declared,
            "[project] without [migrations.policy] must not read as a policy"
        );
        assert_eq!(
            resolved.policy.source,
            Some(tree.path("qail.toml")),
            "the file that was read is still reported, so the message can name it"
        );
    }

    #[test]
    fn undeclared_policy_blocks_destructive_operations() {
        let policy = MigrationPolicy::default();

        let err = ensure_destructive_policy_declared(&policy, "DROP COLUMN users.email")
            .expect_err("an undeclared policy must fail closed");
        let msg = err.to_string();

        assert!(msg.contains("Migration blocked"), "{msg}");
        assert!(msg.contains("DROP COLUMN users.email"), "{msg}");
        assert!(
            msg.contains("[migrations.policy]"),
            "the error must say what to add: {msg}"
        );
    }

    #[test]
    fn undeclared_policy_names_the_file_it_read() {
        let policy = MigrationPolicy {
            source: Some(PathBuf::from("/repo/qail.toml")),
            ..Default::default()
        };

        let err = ensure_destructive_policy_declared(&policy, "DROP TABLE orders")
            .expect_err("must fail closed");

        assert!(
            err.to_string().contains("/repo/qail.toml"),
            "the error must name the file it looked in: {err}"
        );
    }

    #[test]
    fn declared_policy_passes_the_guard() {
        let tree = TempTree::new();
        tree.write(
            "qail.toml",
            "[project]\nname = \"t\"\n\n[migrations.policy]\ndestructive = \"require-flag\"\n",
        );

        let resolved = resolve_migration_policy(&tree.path("")).expect("resolve");

        assert!(resolved.policy.declared);
        ensure_destructive_policy_declared(&resolved.policy, "DROP COLUMN t.c")
            .expect("a declared policy governs on its own terms");
    }

    #[test]
    fn conflicting_ancestor_policies_are_reported() {
        let tree = TempTree::new();
        tree.write(
            "qail.toml",
            "[project]\nname = \"root\"\n\n[migrations.policy]\ndestructive = \"deny\"\n",
        );
        tree.write(
            "gateway/qail.toml",
            "[project]\nname = \"gw\"\n\n[migrations.policy]\ndestructive = \"allow\"\n",
        );

        let resolved = resolve_migration_policy(&tree.path("gateway")).expect("resolve");

        assert_eq!(resolved.policy.destructive, EnforcementMode::Allow);
        assert!(
            resolved.conflicts.contains(&tree.path("qail.toml")),
            "the ignored declaration must be surfaced, not silently dropped"
        );
    }

    #[test]
    fn agreeing_policies_are_not_reported_as_conflicts() {
        let tree = TempTree::new();
        let same = "[migrations.policy]\ndestructive = \"deny\"\n";
        tree.write("qail.toml", same);
        tree.write("gateway/qail.toml", same);

        let resolved = resolve_migration_policy(&tree.path("gateway")).expect("resolve");

        assert!(resolved.conflicts.is_empty());
    }

    #[test]
    fn an_invalid_value_is_an_error_not_a_default() {
        let tree = TempTree::new();
        tree.write(
            "qail.toml",
            "[migrations.policy]\ndestructive = \"sort-of\"\n",
        );

        let err = resolve_migration_policy(&tree.path("")).expect_err("must reject");

        assert!(err.to_string().contains("sort-of"), "{err}");
        assert!(
            err.to_string().contains("qail.toml"),
            "the error must name the offending file: {err}"
        );
    }
}
