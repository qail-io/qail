use std::collections::BTreeSet;

use crate::ast::Action;

use super::model::AccessOperation;

/// Access policy file loading failure.
#[derive(Debug)]
pub enum AccessPolicyLoadError {
    /// Filesystem read failure.
    Read(std::io::Error),
    /// TOML parse failure.
    Toml(toml::de::Error),
    /// JSON parse failure.
    Json(serde_json::Error),
    /// File extension is not supported.
    UnsupportedExtension(String),
}

impl std::fmt::Display for AccessPolicyLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read(err) => write!(f, "failed to read access policy: {err}"),
            Self::Toml(err) => write!(f, "failed to parse TOML access policy: {err}"),
            Self::Json(err) => write!(f, "failed to parse JSON access policy: {err}"),
            Self::UnsupportedExtension(extension) if extension.is_empty() => {
                write!(f, "access policy file must use .toml or .json extension")
            }
            Self::UnsupportedExtension(extension) => {
                write!(
                    f,
                    "unsupported access policy extension '.{extension}' (expected .toml or .json)"
                )
            }
        }
    }
}

impl std::error::Error for AccessPolicyLoadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Read(err) => Some(err),
            Self::Toml(err) => Some(err),
            Self::Json(err) => Some(err),
            Self::UnsupportedExtension(_) => None,
        }
    }
}

/// Access check failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessError {
    /// Table being checked.
    pub table: String,
    /// Operation being checked, if known.
    pub operation: Option<AccessOperation>,
    /// Specific failure reason.
    pub kind: AccessErrorKind,
}

impl AccessError {
    pub(super) fn new(
        table: String,
        operation: Option<AccessOperation>,
        kind: AccessErrorKind,
    ) -> Self {
        Self {
            table,
            operation,
            kind,
        }
    }
}

impl std::fmt::Display for AccessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            AccessErrorKind::NoPolicy => {
                write!(f, "no access policy allows table '{}'", self.table)
            }
            AccessErrorKind::UnsupportedAction(action) => {
                write!(f, "action {action:?} is not supported by access policy")
            }
            AccessErrorKind::OperationDenied => write!(
                f,
                "operation {:?} is denied on table '{}'",
                self.operation, self.table
            ),
            AccessErrorKind::MissingRole { required } => write!(
                f,
                "table '{}' requires one of roles {:?}",
                self.table, required
            ),
            AccessErrorKind::MissingScope { required } => {
                write!(f, "table '{}' requires scopes {:?}", self.table, required)
            }
            AccessErrorKind::ColumnDenied { column } => write!(
                f,
                "column '{}' is denied for operation {:?} on table '{}'",
                column, self.operation, self.table
            ),
            AccessErrorKind::WildcardProjectionDenied => write!(
                f,
                "wildcard projection is denied by column policy on table '{}'",
                self.table
            ),
            AccessErrorKind::UnsupportedColumnExpression { context } => write!(
                f,
                "{} contains an expression that cannot be checked by column policy on table '{}'",
                context, self.table
            ),
            AccessErrorKind::ExplicitWriteColumnsRequired => write!(
                f,
                "operation {:?} on table '{}' requires explicit write columns",
                self.operation, self.table
            ),
            AccessErrorKind::JoinedTableColumnPolicyUnsupported => write!(
                f,
                "joined table '{}' has column policy that cannot be enforced in a flat join",
                self.table
            ),
            AccessErrorKind::SourceTableColumnPolicyUnsupported => write!(
                f,
                "source table '{}' has column policy that cannot be enforced without an explicit source query",
                self.table
            ),
            AccessErrorKind::AuxiliaryTableColumnPolicyUnsupported => write!(
                f,
                "auxiliary table '{}' has column policy that cannot be enforced in UPDATE FROM or DELETE USING",
                self.table
            ),
            AccessErrorKind::CteMutationUnsupported => {
                write!(f, "CTE relation '{}' cannot be mutated", self.table)
            }
            AccessErrorKind::EmptyTable => write!(f, "command has no target table"),
        }
    }
}

impl std::error::Error for AccessError {}

/// Specific access denial reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessErrorKind {
    /// No matching table policy exists and default decision is deny.
    NoPolicy,
    /// Command action is not a runtime data action covered by this policy.
    UnsupportedAction(Action),
    /// The table policy does not allow this operation.
    OperationDenied,
    /// Required role gate failed.
    MissingRole {
        /// Accepted roles.
        required: BTreeSet<String>,
    },
    /// Required scope gate failed.
    MissingScope {
        /// Required scopes.
        required: BTreeSet<String>,
    },
    /// Column is not allowed by the relevant column rule.
    ColumnDenied {
        /// Normalized column name.
        column: String,
    },
    /// `*` or `table.*` cannot be checked against a restrictive column rule.
    WildcardProjectionDenied,
    /// A projection expression cannot be mapped to a concrete column.
    UnsupportedColumnExpression {
        /// Human-readable context.
        context: &'static str,
    },
    /// A write used positional or implicit payloads under a restrictive write rule.
    ExplicitWriteColumnsRequired,
    /// Joined table column policies cannot be enforced by this checker.
    JoinedTableColumnPolicyUnsupported,
    /// Source table column policies cannot be enforced without an explicit source projection.
    SourceTableColumnPolicyUnsupported,
    /// UPDATE FROM / DELETE USING table column policies cannot be enforced by this checker.
    AuxiliaryTableColumnPolicyUnsupported,
    /// CTE aliases are read-only derived relations.
    CteMutationUnsupported,
    /// Command did not carry a target table.
    EmptyTable,
}
