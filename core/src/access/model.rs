use std::collections::BTreeSet;

use crate::ast::Action;
use crate::rls::SuperAdminToken;

use super::ident::normalize_column_name;

/// High-level data operation governed by access policy.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum AccessOperation {
    /// Read rows or vector points.
    Read,
    /// Create rows or vector points.
    Create,
    /// Update existing rows or vector points.
    Update,
    /// Delete rows or vector points.
    Delete,
}

impl AccessOperation {
    /// Conservative operation mapping for non-MERGE commands.
    pub fn required_for_action(action: Action) -> Option<&'static [Self]> {
        match action {
            Action::Get
            | Action::Cnt
            | Action::Export
            | Action::With
            | Action::Search
            | Action::Scroll => Some(&[Self::Read]),
            Action::Add => Some(&[Self::Create]),
            Action::Set | Action::Put | Action::Over => Some(&[Self::Update]),
            Action::Upsert => Some(&[Self::Create, Self::Update]),
            Action::Del => Some(&[Self::Delete]),
            _ => None,
        }
    }
}

/// The subject being checked against an access policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessContext {
    /// Authenticated user or service principal ID.
    pub subject_id: Option<String>,
    /// Tenant carried with the subject, if any.
    pub tenant_id: Option<String>,
    /// Subject roles.
    pub roles: BTreeSet<String>,
    /// Subject scopes or permissions.
    pub scopes: BTreeSet<String>,
    bypass: bool,
}

impl AccessContext {
    /// Anonymous context with no roles, scopes, tenant, or bypass.
    pub fn anonymous() -> Self {
        Self {
            subject_id: None,
            tenant_id: None,
            roles: BTreeSet::new(),
            scopes: BTreeSet::new(),
            bypass: false,
        }
    }

    /// Authenticated context for a subject ID.
    pub fn subject(subject_id: impl Into<String>) -> Self {
        Self {
            subject_id: Some(subject_id.into()),
            ..Self::anonymous()
        }
    }

    /// Super-admin context. The token cannot be fabricated outside `qail-core`.
    pub fn super_admin(_token: SuperAdminToken) -> Self {
        Self {
            bypass: true,
            ..Self::anonymous()
        }
    }

    /// Attach a tenant ID.
    pub fn with_tenant(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }

    /// Attach one role.
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.roles.insert(role.into());
        self
    }

    /// Attach many roles.
    pub fn with_roles<I, S>(mut self, roles: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.roles.extend(roles.into_iter().map(Into::into));
        self
    }

    /// Attach one scope.
    pub fn with_scope(mut self, scope: impl Into<String>) -> Self {
        self.scopes.insert(scope.into());
        self
    }

    /// Attach many scopes.
    pub fn with_scopes<I, S>(mut self, scopes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.scopes.extend(scopes.into_iter().map(Into::into));
        self
    }

    /// Returns true when this context bypasses vertical checks.
    pub fn bypasses_access(&self) -> bool {
        self.bypass
    }

    pub(super) fn has_any_role(&self, required: &BTreeSet<String>) -> bool {
        required.is_empty() || required.iter().any(|role| self.roles.contains(role))
    }

    pub(super) fn has_all_scopes(&self, required: &BTreeSet<String>) -> bool {
        required.is_subset(&self.scopes)
    }
}

impl Default for AccessContext {
    fn default() -> Self {
        Self::anonymous()
    }
}

/// Default decision when no table policy matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AccessDecision {
    /// Allow when no table policy matches.
    Allow,
    /// Deny when no table policy matches.
    Deny,
}

/// Column access rule for reads, writes, or RETURNING clauses.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnRule {
    /// Any column is allowed.
    #[default]
    Any,
    /// No columns are allowed.
    DenyAll,
    /// Only the listed columns are allowed.
    Only(BTreeSet<String>),
    /// Any column except the listed columns is allowed.
    Except(BTreeSet<String>),
}

impl ColumnRule {
    /// Create an allow-list rule.
    pub fn only<I, S>(columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::Only(columns.into_iter().map(normalize_column_name).collect())
    }

    /// Create a deny-list rule.
    pub fn except<I, S>(columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::Except(columns.into_iter().map(normalize_column_name).collect())
    }

    /// Returns true if this rule constrains column access.
    pub fn is_restrictive(&self) -> bool {
        !matches!(self, Self::Any)
    }

    /// Returns true if `column` is allowed by this rule.
    pub fn allows(&self, column: &str) -> bool {
        let normalized = normalize_column_name(column);
        match self {
            Self::Any => true,
            Self::DenyAll => false,
            Self::Only(columns) => columns.contains(&normalized),
            Self::Except(columns) => !columns.contains(&normalized),
        }
    }
}

/// Access rule for one table.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TableAccessPolicy {
    /// Allowed operations.
    #[serde(default)]
    pub operations: BTreeSet<AccessOperation>,
    /// Explicitly denied operations.
    #[serde(default)]
    pub denied_operations: BTreeSet<AccessOperation>,
    /// Read projection rule.
    #[serde(default)]
    pub read_columns: ColumnRule,
    /// Write payload rule.
    #[serde(default)]
    pub write_columns: ColumnRule,
    /// RETURNING projection rule. Enforced together with `read_columns`.
    #[serde(default)]
    pub returning_columns: ColumnRule,
    /// At least one of these roles is required. Empty means no role gate.
    #[serde(default)]
    pub require_any_role: BTreeSet<String>,
    /// All listed scopes are required. Empty means no scope gate.
    #[serde(default)]
    pub require_scopes: BTreeSet<String>,
}

impl TableAccessPolicy {
    /// Empty table policy: no operations allowed until added.
    pub fn new() -> Self {
        Self::default()
    }

    /// Allow the listed operations.
    pub fn allow_operations<I>(mut self, operations: I) -> Self
    where
        I: IntoIterator<Item = AccessOperation>,
    {
        self.operations.extend(operations);
        self
    }

    /// Deny the listed operations even if otherwise allowed.
    pub fn deny_operations<I>(mut self, operations: I) -> Self
    where
        I: IntoIterator<Item = AccessOperation>,
    {
        self.denied_operations.extend(operations);
        self
    }

    /// Restrict read projection columns.
    pub fn read_columns(mut self, rule: ColumnRule) -> Self {
        self.read_columns = rule;
        self
    }

    /// Restrict write payload columns.
    pub fn write_columns(mut self, rule: ColumnRule) -> Self {
        self.write_columns = rule;
        self
    }

    /// Restrict RETURNING columns.
    pub fn returning_columns(mut self, rule: ColumnRule) -> Self {
        self.returning_columns = rule;
        self
    }

    /// Require at least one role.
    pub fn require_any_role<I, S>(mut self, roles: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.require_any_role
            .extend(roles.into_iter().map(Into::into));
        self
    }

    /// Require all scopes.
    pub fn require_scopes<I, S>(mut self, scopes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.require_scopes
            .extend(scopes.into_iter().map(Into::into));
        self
    }

    pub(super) fn allows_operation(&self, operation: AccessOperation) -> bool {
        self.operations.contains(&operation) && !self.denied_operations.contains(&operation)
    }
}

impl Default for TableAccessPolicy {
    fn default() -> Self {
        Self {
            operations: BTreeSet::new(),
            denied_operations: BTreeSet::new(),
            read_columns: ColumnRule::Any,
            write_columns: ColumnRule::Any,
            returning_columns: ColumnRule::Any,
            require_any_role: BTreeSet::new(),
            require_scopes: BTreeSet::new(),
        }
    }
}
