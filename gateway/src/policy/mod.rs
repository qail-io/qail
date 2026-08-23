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
    /// Authorization for manual WebSocket `subscribe` channels.
    ///
    /// Absent or empty → every syntactically valid channel is allowed
    /// (tenant scoping still applies). Once any entry exists, a subscribe
    /// must match at least one entry or it is denied before LISTEN.
    #[serde(default)]
    pub channel_policies: Vec<ChannelPolicyDef>,
}

/// Authorization rule for a manual subscribe channel fragment.
///
/// `pattern` is matched against the client-supplied fragment after expanding
/// `$user_id`, `$tenant_id`, `$role`, and other JWT claims; `*` matches any
/// run of fragment characters. Example — a buyer or seller may only listen
/// to their own chat threads:
///
/// ```yaml
/// channel_policies:
///   - name: own_chat
///     pattern: "chat_$user_id_*"
///   - name: seller_sold
///     pattern: "sold_$user_id"
///     role: seller
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelPolicyDef {
    /// Human-readable policy name.
    pub name: String,
    /// Fragment pattern with placeholders and `*` wildcards.
    pub pattern: String,
    /// If set, the policy only applies when the user has this role.
    #[serde(default)]
    pub role: Option<String>,
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
            Action::Get
            | Action::Cnt
            | Action::Export
            | Action::With
            | Action::Search
            | Action::Scroll => Some(OperationType::Read),
            Action::Add => Some(OperationType::Create),
            Action::Set | Action::Put | Action::Over | Action::Upsert | Action::Merge => {
                Some(OperationType::Update)
            }
            Action::Del => Some(OperationType::Delete),
            _ => None,
        }
    }

    /// Return every operation capability required to execute an action.
    pub fn required_for_action(action: Action) -> Option<&'static [Self]> {
        match action {
            Action::Get
            | Action::Cnt
            | Action::Export
            | Action::With
            | Action::Search
            | Action::Scroll => Some(&[OperationType::Read]),
            Action::Add => Some(&[OperationType::Create]),
            Action::Set | Action::Put | Action::Over => Some(&[OperationType::Update]),
            Action::Upsert => Some(&[OperationType::Create, OperationType::Update]),
            Action::Merge => Some(&[
                OperationType::Create,
                OperationType::Update,
                OperationType::Delete,
            ]),
            Action::Del => Some(&[OperationType::Delete]),
            _ => None,
        }
    }
}

/// Policy engine that evaluates access control and injects filters
#[derive(Debug, Default)]
pub struct PolicyEngine {
    policies: Vec<PolicyDef>,
    channel_policies: Vec<ChannelPolicyDef>,
}

#[cfg(test)]
mod tests;
