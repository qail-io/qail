//! Error types for QAIL.

/// Error types for QAIL operations.
#[derive(Debug)]
pub enum QailError {
    /// Failed to parse the QAIL query string.
    Parse {
        /// Byte offset of the error.
        position: usize,
        /// Human-readable error message.
        message: String,
    },

    /// Invalid action (must be get, set, del, or add).
    InvalidAction(String),

    /// Required syntax symbol is missing.
    MissingSymbol {
        /// The missing symbol.
        symbol: &'static str,
        /// Description of the expected symbol.
        description: &'static str,
    },

    /// Invalid operator in expression.
    InvalidOperator(String),

    /// Invalid value in expression.
    InvalidValue(String),

    /// Database-layer error.
    Database(String),

    /// Connection-layer error.
    Connection(String),

    /// Execution-layer error.
    Execution(String),

    /// Validation error.
    Validation(String),

    /// Configuration error.
    Config(String),

    /// I/O error.
    Io(std::io::Error),
}

impl QailError {
    /// Create a parse error at the given position.
    pub fn parse(position: usize, message: impl Into<String>) -> Self {
        Self::Parse {
            position,
            message: message.into(),
        }
    }

    /// Create a missing symbol error.
    pub fn missing(symbol: &'static str, description: &'static str) -> Self {
        Self::MissingSymbol {
            symbol,
            description,
        }
    }
}

impl std::fmt::Display for QailError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parse { position, message } => {
                write!(f, "Parse error at position {position}: {message}")
            }
            Self::InvalidAction(action) => {
                write!(
                    f,
                    "Invalid action: '{action}'. Expected: get, set, del, or add"
                )
            }
            Self::MissingSymbol {
                symbol,
                description,
            } => {
                write!(f, "Missing required symbol: {symbol} ({description})")
            }
            Self::InvalidOperator(op) => write!(f, "Invalid operator: '{op}'"),
            Self::InvalidValue(value) => write!(f, "Invalid value: {value}"),
            Self::Database(msg) => write!(f, "Database error: {msg}"),
            Self::Connection(msg) => write!(f, "Connection error: {msg}"),
            Self::Execution(msg) => write!(f, "Execution error: {msg}"),
            Self::Validation(msg) => write!(f, "Validation error: {msg}"),
            Self::Config(msg) => write!(f, "Configuration error: {msg}"),
            Self::Io(err) => write!(f, "IO error: {err}"),
        }
    }
}

impl std::error::Error for QailError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for QailError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

/// Result type alias for QAIL operations.
pub type QailResult<T> = Result<T, QailError>;

/// Error type for query-builder operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QailBuildError {
    /// RLS insertion cannot safely align positional values without columns.
    RlsInsertRequiresExplicitColumns {
        /// Target table being scoped.
        table: String,
        /// Tenant column that would be injected.
        tenant_column: String,
    },

    /// RLS-protected updates cannot rewrite the tenant column.
    RlsTenantColumnMutationDenied {
        /// Target table being scoped.
        table: String,
        /// Tenant column that was assigned.
        tenant_column: String,
    },

    /// RLS-protected MERGE query sources need a tenant projection for safe row classification.
    RlsMergeSourceTenantProjectionRequired {
        /// Target table being scoped.
        table: String,
        /// Tenant column that must be projected by the source query.
        tenant_column: String,
    },

    /// `with_rls` was called before the process declared its scope registries.
    ///
    /// Call `qail_core::rls::init_scope_registries(&schema)` (AST injection
    /// from schema metadata) or `declare_policy_only_isolation(reason)` (DB
    /// policies only) at application startup.
    RlsRegistryUninitialized {
        /// Table the query targeted.
        table: String,
    },

    /// A scope registry could not be read (poisoned lock) while scoping.
    ///
    /// Fail-closed: "cannot read the registry" must never be interpreted as
    /// "table is unregistered", or a late poison would disable every
    /// tenant/owner predicate in the process at once.
    RlsRegistryUnavailable {
        /// Table whose registration was being looked up.
        table: String,
        /// Underlying registry error.
        reason: String,
    },

    /// A registered RLS table was scoped with a context lacking the scope it requires.
    ///
    /// Fail-closed: a tenant-registered table needs a tenant (or global)
    /// context; an owner-registered table needs an authenticated user.
    RlsScopeMissing {
        /// Target table being scoped.
        table: String,
        /// The scope that is required (`"tenant"` or `"user"`).
        scope: &'static str,
        /// The column the scope would have been injected on.
        column: String,
    },

    /// A joined RLS relation cannot be scoped through this join kind.
    ///
    /// RIGHT/FULL joins reintroduce rows the ON predicate rejects, so a scope
    /// predicate on the joined side does not isolate.
    RlsJoinKindUnsupported {
        /// Joined relation that required scoping.
        table: String,
        /// The join kind in use.
        join_kind: String,
    },

    /// Owner-scoped MERGE is not supported by AST injection.
    RlsOwnerMergeUnsupported {
        /// Target table being scoped.
        table: String,
        /// Owner column registered for the table.
        owner_column: String,
    },

    /// Runtime relation registry lock failed.
    RelationRegistryLock(String),

    /// Relation metadata has more than one possible join edge.
    AmbiguousRelation {
        /// Source table.
        from_table: String,
        /// Related table.
        to_table: String,
        /// Number of registered foreign-key candidates.
        foreign_key_count: usize,
    },

    /// No schema relation could be found for an implicit join.
    RelationNotFound {
        /// Current table.
        from_table: String,
        /// Requested related table.
        to_table: String,
    },
}

impl std::fmt::Display for QailBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RlsInsertRequiresExplicitColumns {
                table,
                tenant_column,
            } => write!(
                f,
                "with_rls requires explicit columns for positional INSERT payloads on table '{table}' (tenant column '{tenant_column}')"
            ),
            Self::RlsTenantColumnMutationDenied {
                table,
                tenant_column,
            } => write!(
                f,
                "with_rls rejects tenant column mutation on table '{table}' (tenant column '{tenant_column}')"
            ),
            Self::RlsRegistryUninitialized { table } => write!(
                f,
                "with_rls on table '{table}' before scope registries were declared — call qail_core::rls::init_scope_registries(&schema) or declare_policy_only_isolation(reason) at startup"
            ),
            Self::RlsRegistryUnavailable { table, reason } => write!(
                f,
                "with_rls on table '{table}' cannot read the scope registry ({reason}) — refusing to run unscoped"
            ),
            Self::RlsScopeMissing {
                table,
                scope,
                column,
            } => write!(
                f,
                "with_rls on table '{table}' requires a {scope} scope (column '{column}') but the context carries none — refusing to run unscoped"
            ),
            Self::RlsJoinKindUnsupported { table, join_kind } => write!(
                f,
                "with_rls cannot isolate RLS table '{table}' joined via {join_kind}; use INNER/LEFT/LATERAL or a CTE"
            ),
            Self::RlsOwnerMergeUnsupported {
                table,
                owner_column,
            } => write!(
                f,
                "with_rls cannot owner-scope MERGE on table '{table}' (owner column '{owner_column}'); use with_rls_policy and a DB policy"
            ),
            Self::RlsMergeSourceTenantProjectionRequired {
                table,
                tenant_column,
            } => write!(
                f,
                "with_rls requires MERGE query sources for table '{table}' to project tenant column '{tenant_column}'"
            ),
            Self::RelationRegistryLock(msg) => write!(f, "Relation registry lock error: {msg}"),
            Self::AmbiguousRelation {
                from_table,
                to_table,
                foreign_key_count,
            } => write!(
                f,
                "Ambiguous relation between '{from_table}' and '{to_table}': {foreign_key_count} foreign keys registered. Use an explicit join condition."
            ),
            Self::RelationNotFound {
                from_table,
                to_table,
            } => write!(
                f,
                "No relation found between '{from_table}' and '{to_table}'. Define a ref: in schema.qail or use load_schema_relations() first."
            ),
        }
    }
}

impl std::error::Error for QailBuildError {}

/// Result type alias for query-builder operations.
pub type QailBuildResult<T> = Result<T, QailBuildError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = QailError::parse(5, "unexpected character");
        assert_eq!(
            err.to_string(),
            "Parse error at position 5: unexpected character"
        );
    }
}
