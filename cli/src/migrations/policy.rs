//! Migration policy loading from qail.toml.

use anyhow::{Result, anyhow, bail};
use std::fs;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EnforcementMode {
    Deny,
    #[default]
    RequireFlag,
    Allow,
}

impl EnforcementMode {
    fn parse(raw: &str, field: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "deny" => Ok(Self::Deny),
            "require-flag" | "require_flag" | "requireflag" => Ok(Self::RequireFlag),
            "allow" => Ok(Self::Allow),
            other => bail!(
                "Invalid migrations.policy.{} value '{}'. Allowed: deny, require-flag, allow",
                field,
                other
            ),
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
    fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "warn" => Ok(Self::Warn),
            "error" => Ok(Self::Error),
            other => bail!(
                "Invalid migrations.policy.receipt_validation value '{}'. Allowed: warn, error",
                other
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
        }
    }
}

pub fn load_migration_policy() -> Result<MigrationPolicy> {
    let mut policy = MigrationPolicy::default();
    let content = match fs::read_to_string("qail.toml") {
        Ok(v) => v,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(policy),
        Err(err) => {
            return Err(anyhow!(
                "Failed to read qail.toml for migration policy: {}",
                err
            ));
        }
    };

    let config: toml::Value =
        toml::from_str(&content).map_err(|e| anyhow!("Failed to parse qail.toml: {}", e))?;

    let Some(policy_tbl) = config
        .get("migrations")
        .and_then(|v| v.get("policy"))
        .and_then(|v| v.as_table())
    else {
        return Ok(policy);
    };

    if let Some(raw) = policy_tbl.get("destructive") {
        let s = raw.as_str().ok_or_else(|| {
            anyhow!("migrations.policy.destructive must be a string (deny|require-flag|allow)")
        })?;
        policy.destructive = EnforcementMode::parse(s, "destructive")?;
    }

    if let Some(raw) = policy_tbl.get("lock_risk") {
        let s = raw.as_str().ok_or_else(|| {
            anyhow!("migrations.policy.lock_risk must be a string (deny|require-flag|allow)")
        })?;
        policy.lock_risk = EnforcementMode::parse(s, "lock_risk")?;
    }

    if let Some(raw) = policy_tbl
        .get("lock_risk_max_score")
        .or_else(|| policy_tbl.get("max_lock_risk_score"))
    {
        let score = raw
            .as_integer()
            .ok_or_else(|| anyhow!("migrations.policy.lock_risk_max_score must be an integer"))?;
        if !(0..=100).contains(&score) {
            bail!(
                "migrations.policy.lock_risk_max_score must be between 0 and 100 (got {})",
                score
            );
        }
        policy.lock_risk_max_score = u8::try_from(score).unwrap_or(100);
    }

    if let Some(raw) = policy_tbl.get("require_shadow_receipt") {
        policy.require_shadow_receipt = raw.as_bool().ok_or_else(|| {
            anyhow!("migrations.policy.require_shadow_receipt must be true/false")
        })?;
    }

    if let Some(raw) = policy_tbl
        .get("allow_no_shadow_receipt")
        .or_else(|| policy_tbl.get("allow_shadow_receipt_bypass"))
    {
        policy.allow_no_shadow_receipt = raw.as_bool().ok_or_else(|| {
            anyhow!("migrations.policy.allow_no_shadow_receipt must be true/false")
        })?;
    }

    if let Some(raw) = policy_tbl.get("receipt_validation") {
        let s = raw
            .as_str()
            .ok_or_else(|| anyhow!("migrations.policy.receipt_validation must be string"))?;
        policy.receipt_validation = ReceiptValidationMode::parse(s)?;
    }

    Ok(policy)
}

#[cfg(test)]
mod tests {
    use super::{EnforcementMode, MigrationPolicy, ReceiptValidationMode};

    #[test]
    fn default_policy_is_conservative() {
        let p = MigrationPolicy::default();
        assert_eq!(p.destructive, EnforcementMode::RequireFlag);
        assert_eq!(p.lock_risk, EnforcementMode::RequireFlag);
        assert_eq!(p.lock_risk_max_score, 90);
        assert!(p.require_shadow_receipt);
        assert!(p.allow_no_shadow_receipt);
        assert_eq!(p.receipt_validation, ReceiptValidationMode::Error);
    }
}
