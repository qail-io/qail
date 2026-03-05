//! Row-level security policy engine
//!
//! Parses and evaluates security policies defined in policies.yaml.
//! Injects filters into QAIL queries based on user context.

use qail_core::ast::Action;
use serde::{Deserialize, Serialize};

mod engine;

/// Policy configuration loaded from YAML
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyConfig {
    /// List of security policy definitions.
    pub policies: Vec<PolicyDef>,
}

/// A security policy definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyDef {
    /// Human-readable policy name.
    pub name: String,
    /// Table this policy applies to (`"*"` for all tables).
    pub table: String,
    /// Filter template with `$user_id`, `$tenant_id`, etc. placeholders.
    #[serde(default)]
    pub filter: Option<String>,
    /// If set, the policy only applies when the user has this role.
    #[serde(default)]
    pub role: Option<String>,
    /// Operations this policy governs (empty = all).
    #[serde(default)]
    pub operations: Vec<OperationType>,
    /// Column-level permissions: only these columns are visible (whitelist).
    /// If empty, all columns are allowed.
    #[serde(default)]
    pub allowed_columns: Vec<String>,
    /// Column-level permissions: these columns are hidden (blacklist).
    /// Applied after allowed_columns.
    #[serde(default)]
    pub denied_columns: Vec<String>,
}

/// Operations a policy can allow
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OperationType {
    /// SELECT / GET.
    Read,
    /// INSERT / ADD.
    Create,
    /// UPDATE / SET.
    Update,
    /// DELETE / DEL.
    Delete,
}

impl OperationType {
    /// Map a Qail AST [`Action`] to the corresponding operation type.
    pub fn from_action(action: Action) -> Option<Self> {
        match action {
            Action::Get => Some(OperationType::Read),
            Action::Add => Some(OperationType::Create),
            Action::Set => Some(OperationType::Update),
            Action::Del => Some(OperationType::Delete),
            _ => None,
        }
    }
}

/// Policy engine that evaluates access control and injects filters
#[derive(Debug, Default)]
pub struct PolicyEngine {
    policies: Vec<PolicyDef>,
}

#[cfg(test)]
mod tests;
