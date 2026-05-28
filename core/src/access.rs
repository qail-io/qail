//! Native vertical access policy checks.
//!
//! Row-level security decides which rows a subject can see. This module covers
//! the vertical layer: which operations and columns a subject may use before a
//! driver sends the AST to a backend.

use std::collections::{BTreeMap, BTreeSet};

use crate::ast::{
    Action, CageKind, Condition, ConflictAction, Expr, MergeAction, MergeSource, Qail, Value,
};
use crate::rls::SuperAdminToken;

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

/// The subject being checked against an [`AccessPolicy`].
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

    fn has_any_role(&self, required: &BTreeSet<String>) -> bool {
        required.is_empty() || required.iter().any(|role| self.roles.contains(role))
    }

    fn has_all_scopes(&self, required: &BTreeSet<String>) -> bool {
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

    fn allows_operation(&self, operation: AccessOperation) -> bool {
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

/// Complete access policy set.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AccessPolicy {
    /// Default decision when no exact or wildcard table policy matches.
    pub default_decision: AccessDecision,
    /// Table policies by table name. `"*"` is a wildcard fallback.
    #[serde(default)]
    pub tables: BTreeMap<String, TableAccessPolicy>,
}

impl AccessPolicy {
    /// Deny-by-default policy set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Allow-by-default policy set for trusted/internal use.
    pub fn allow_by_default() -> Self {
        Self {
            default_decision: AccessDecision::Allow,
            tables: BTreeMap::new(),
        }
    }

    /// Add or replace a table policy.
    pub fn with_table(mut self, table: impl Into<String>, policy: TableAccessPolicy) -> Self {
        self.tables
            .insert(normalize_table_ref(&table.into()), policy);
        self
    }

    /// Mutably access a table policy, creating an empty policy if needed.
    pub fn table_mut(&mut self, table: impl Into<String>) -> &mut TableAccessPolicy {
        self.tables
            .entry(normalize_table_ref(&table.into()))
            .or_default()
    }

    /// Check whether a command is allowed for the supplied context.
    pub fn check_command(&self, ctx: &AccessContext, cmd: &Qail) -> Result<(), AccessError> {
        self.check_command_inner(ctx, cmd)
    }

    fn check_command_inner(&self, ctx: &AccessContext, cmd: &Qail) -> Result<(), AccessError> {
        if ctx.bypasses_access() {
            return Ok(());
        }

        for cte in &cmd.ctes {
            self.check_command_inner(ctx, &cte.base_query)?;
            if let Some(recursive_query) = &cte.recursive_query {
                self.check_command_inner(ctx, recursive_query)?;
            }
        }
        for (_, set_query) in &cmd.set_ops {
            self.check_command_inner(ctx, set_query)?;
        }
        if let Some(source_query) = &cmd.source_query {
            self.check_command_inner(ctx, source_query)?;
        }
        if let Some(merge) = &cmd.merge
            && let MergeSource::Query { query, .. } = &merge.source
        {
            self.check_command_inner(ctx, query)?;
        }

        self.check_embedded_queries(ctx, cmd)?;

        let table = normalize_table_ref(&cmd.table);
        if table.is_empty() {
            return Err(AccessError::new(
                String::new(),
                None,
                AccessErrorKind::EmptyTable,
            ));
        }

        let cte_names: BTreeSet<String> = cmd
            .ctes
            .iter()
            .map(|cte| normalize_table_ref(&cte.name))
            .collect();
        self.check_join_read_access(ctx, cmd, &cte_names)?;

        let required_ops = required_operations_for_command(cmd).ok_or_else(|| {
            AccessError::new(
                table.clone(),
                None,
                AccessErrorKind::UnsupportedAction(cmd.action),
            )
        })?;

        for operation in &required_ops {
            self.check_table_operation(ctx, &table, *operation)?;
        }

        if required_ops.contains(&AccessOperation::Read) && projection_restricted_action(cmd.action)
        {
            self.check_read_columns(&table, AccessOperation::Read, &cmd.columns)?;
        }

        if required_ops.contains(&AccessOperation::Create) {
            let columns = create_columns(cmd)?;
            self.check_write_columns(&table, AccessOperation::Create, &columns)?;
        }

        if required_ops.contains(&AccessOperation::Update) {
            let columns = update_columns(cmd)?;
            self.check_write_columns(&table, AccessOperation::Update, &columns)?;
        }

        if let Some(returning) = &cmd.returning {
            self.check_returning_columns(&table, returning)?;
        }

        Ok(())
    }

    fn check_table_operation(
        &self,
        ctx: &AccessContext,
        table: &str,
        operation: AccessOperation,
    ) -> Result<(), AccessError> {
        let Some(policy) = self.table_policy(table) else {
            return match self.default_decision {
                AccessDecision::Allow => Ok(()),
                AccessDecision::Deny => Err(AccessError::new(
                    table.to_string(),
                    Some(operation),
                    AccessErrorKind::NoPolicy,
                )),
            };
        };

        if !ctx.has_any_role(&policy.require_any_role) {
            return Err(AccessError::new(
                table.to_string(),
                Some(operation),
                AccessErrorKind::MissingRole {
                    required: policy.require_any_role.clone(),
                },
            ));
        }

        if !ctx.has_all_scopes(&policy.require_scopes) {
            return Err(AccessError::new(
                table.to_string(),
                Some(operation),
                AccessErrorKind::MissingScope {
                    required: policy.require_scopes.clone(),
                },
            ));
        }

        if !policy.allows_operation(operation) {
            return Err(AccessError::new(
                table.to_string(),
                Some(operation),
                AccessErrorKind::OperationDenied,
            ));
        }

        Ok(())
    }

    fn check_join_read_access(
        &self,
        ctx: &AccessContext,
        cmd: &Qail,
        cte_names: &BTreeSet<String>,
    ) -> Result<(), AccessError> {
        for join in &cmd.joins {
            let table = normalize_table_ref(&join.table);
            if table.is_empty() || cte_names.contains(&table) {
                continue;
            }
            self.check_table_operation(ctx, &table, AccessOperation::Read)?;
            if self
                .table_policy(&table)
                .is_some_and(|policy| policy.read_columns.is_restrictive())
            {
                return Err(AccessError::new(
                    table,
                    Some(AccessOperation::Read),
                    AccessErrorKind::JoinedTableColumnPolicyUnsupported,
                ));
            }
        }
        Ok(())
    }

    fn check_read_columns(
        &self,
        table: &str,
        operation: AccessOperation,
        columns: &[Expr],
    ) -> Result<(), AccessError> {
        let rule = self
            .table_policy(table)
            .map(|policy| &policy.read_columns)
            .unwrap_or(&ColumnRule::Any);
        check_projection_rule(table, operation, rule, columns, "read projection")
    }

    fn check_write_columns(
        &self,
        table: &str,
        operation: AccessOperation,
        columns: &[String],
    ) -> Result<(), AccessError> {
        let rule = self
            .table_policy(table)
            .map(|policy| &policy.write_columns)
            .unwrap_or(&ColumnRule::Any);
        if !rule.is_restrictive() {
            return Ok(());
        }
        if columns.is_empty() {
            return Err(AccessError::new(
                table.to_string(),
                Some(operation),
                AccessErrorKind::ExplicitWriteColumnsRequired,
            ));
        }
        for column in columns {
            if !rule.allows(column) {
                return Err(AccessError::new(
                    table.to_string(),
                    Some(operation),
                    AccessErrorKind::ColumnDenied {
                        column: normalize_column_name(column),
                    },
                ));
            }
        }
        Ok(())
    }

    fn check_returning_columns(&self, table: &str, columns: &[Expr]) -> Result<(), AccessError> {
        let read_rule = self
            .table_policy(table)
            .map(|policy| &policy.read_columns)
            .unwrap_or(&ColumnRule::Any);
        let returning_rule = self
            .table_policy(table)
            .map(|policy| &policy.returning_columns)
            .unwrap_or(&ColumnRule::Any);
        check_projection_rule(
            table,
            AccessOperation::Read,
            read_rule,
            columns,
            "returning projection",
        )?;
        check_projection_rule(
            table,
            AccessOperation::Read,
            returning_rule,
            columns,
            "returning projection",
        )
    }

    fn table_policy(&self, table: &str) -> Option<&TableAccessPolicy> {
        self.tables.get(table).or_else(|| self.tables.get("*"))
    }

    fn check_embedded_queries(&self, ctx: &AccessContext, cmd: &Qail) -> Result<(), AccessError> {
        for expr in &cmd.columns {
            self.check_expr(ctx, expr)?;
        }
        if let Some(returning) = &cmd.returning {
            for expr in returning {
                self.check_expr(ctx, expr)?;
            }
        }
        for cage in &cmd.cages {
            for condition in &cage.conditions {
                self.check_condition(ctx, condition)?;
            }
        }
        for condition in &cmd.having {
            self.check_condition(ctx, condition)?;
        }
        for join in &cmd.joins {
            if let Some(conditions) = &join.on {
                for condition in conditions {
                    self.check_condition(ctx, condition)?;
                }
            }
        }
        if let Some(on_conflict) = &cmd.on_conflict
            && let ConflictAction::DoUpdate { assignments } = &on_conflict.action
        {
            for (_, expr) in assignments {
                self.check_expr(ctx, expr)?;
            }
        }
        if let Some(merge) = &cmd.merge {
            for condition in &merge.on {
                self.check_condition(ctx, condition)?;
            }
            for clause in &merge.clauses {
                for condition in &clause.condition {
                    self.check_condition(ctx, condition)?;
                }
                match &clause.action {
                    MergeAction::Update { assignments } => {
                        for (_, expr) in assignments {
                            self.check_expr(ctx, expr)?;
                        }
                    }
                    MergeAction::Insert { values, .. } => {
                        for expr in values {
                            self.check_expr(ctx, expr)?;
                        }
                    }
                    MergeAction::Delete | MergeAction::DoNothing => {}
                }
            }
        }
        Ok(())
    }

    fn check_condition(
        &self,
        ctx: &AccessContext,
        condition: &Condition,
    ) -> Result<(), AccessError> {
        self.check_expr(ctx, &condition.left)?;
        self.check_value(ctx, &condition.value)
    }

    fn check_expr(&self, ctx: &AccessContext, expr: &Expr) -> Result<(), AccessError> {
        match expr {
            Expr::Cast { expr, .. }
            | Expr::Mod { col: expr, .. }
            | Expr::FieldAccess { expr, .. }
            | Expr::Collate { expr, .. } => self.check_expr(ctx, expr),
            Expr::Subscript { expr, index, .. } => {
                self.check_expr(ctx, expr)?;
                self.check_expr(ctx, index)
            }
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    self.check_expr(ctx, arg)?;
                }
                Ok(())
            }
            Expr::SpecialFunction { args, .. } => {
                for (_, arg) in args {
                    self.check_expr(ctx, arg)?;
                }
                Ok(())
            }
            Expr::Binary { left, right, .. } => {
                self.check_expr(ctx, left)?;
                self.check_expr(ctx, right)
            }
            Expr::Literal(value) => self.check_value(ctx, value),
            Expr::ArrayConstructor { elements, .. } | Expr::RowConstructor { elements, .. } => {
                for element in elements {
                    self.check_expr(ctx, element)?;
                }
                Ok(())
            }
            Expr::Case {
                when_clauses,
                else_value,
                ..
            } => {
                for (condition, value) in when_clauses {
                    self.check_condition(ctx, condition)?;
                    self.check_expr(ctx, value)?;
                }
                if let Some(value) = else_value {
                    self.check_expr(ctx, value)?;
                }
                Ok(())
            }
            Expr::Window { params, order, .. } => {
                for param in params {
                    self.check_expr(ctx, param)?;
                }
                for cage in order {
                    for condition in &cage.conditions {
                        self.check_condition(ctx, condition)?;
                    }
                }
                Ok(())
            }
            Expr::Aggregate { filter, .. } => {
                if let Some(conditions) = filter {
                    for condition in conditions {
                        self.check_condition(ctx, condition)?;
                    }
                }
                Ok(())
            }
            Expr::Subquery { query, .. } | Expr::Exists { query, .. } => {
                self.check_command_inner(ctx, query)
            }
            Expr::Star
            | Expr::Named(_)
            | Expr::Aliased { .. }
            | Expr::Def { .. }
            | Expr::JsonAccess { .. } => Ok(()),
        }
    }

    fn check_value(&self, ctx: &AccessContext, value: &Value) -> Result<(), AccessError> {
        match value {
            Value::Subquery(query) => self.check_command_inner(ctx, query),
            Value::Expr(expr) => self.check_expr(ctx, expr),
            Value::Array(values) => {
                for value in values {
                    self.check_value(ctx, value)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

impl Default for AccessPolicy {
    fn default() -> Self {
        Self {
            default_decision: AccessDecision::Deny,
            tables: BTreeMap::new(),
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
    fn new(table: String, operation: Option<AccessOperation>, kind: AccessErrorKind) -> Self {
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
    /// Command did not carry a target table.
    EmptyTable,
}

/// Required operations for a full command.
pub fn required_operations_for_command(cmd: &Qail) -> Option<BTreeSet<AccessOperation>> {
    let mut operations = BTreeSet::new();
    match cmd.action {
        Action::Add => {
            operations.insert(AccessOperation::Create);
            if matches!(
                cmd.on_conflict.as_ref().map(|conflict| &conflict.action),
                Some(ConflictAction::DoUpdate { .. })
            ) {
                operations.insert(AccessOperation::Update);
            }
        }
        Action::Merge => {
            if let Some(merge) = &cmd.merge {
                for clause in &merge.clauses {
                    match &clause.action {
                        MergeAction::Update { .. } => {
                            operations.insert(AccessOperation::Update);
                        }
                        MergeAction::Insert { .. } => {
                            operations.insert(AccessOperation::Create);
                        }
                        MergeAction::Delete => {
                            operations.insert(AccessOperation::Delete);
                        }
                        MergeAction::DoNothing => {}
                    }
                }
                if operations.is_empty() {
                    operations.extend([
                        AccessOperation::Create,
                        AccessOperation::Update,
                        AccessOperation::Delete,
                    ]);
                }
            } else {
                operations.extend([
                    AccessOperation::Create,
                    AccessOperation::Update,
                    AccessOperation::Delete,
                ]);
            }
        }
        action => {
            operations.extend(AccessOperation::required_for_action(action)?);
        }
    }
    Some(operations)
}

fn projection_restricted_action(action: Action) -> bool {
    matches!(
        action,
        Action::Get | Action::Export | Action::With | Action::Search | Action::Scroll
    )
}

fn check_projection_rule(
    table: &str,
    operation: AccessOperation,
    rule: &ColumnRule,
    columns: &[Expr],
    context: &'static str,
) -> Result<(), AccessError> {
    if !rule.is_restrictive() {
        return Ok(());
    }
    if columns.is_empty() {
        return Err(AccessError::new(
            table.to_string(),
            Some(operation),
            AccessErrorKind::WildcardProjectionDenied,
        ));
    }
    for expr in columns {
        if expr_projects_all_columns(expr) {
            return Err(AccessError::new(
                table.to_string(),
                Some(operation),
                AccessErrorKind::WildcardProjectionDenied,
            ));
        }
        let Some(column) = projection_column_name(expr) else {
            return Err(AccessError::new(
                table.to_string(),
                Some(operation),
                AccessErrorKind::UnsupportedColumnExpression { context },
            ));
        };
        if !rule.allows(&column) {
            return Err(AccessError::new(
                table.to_string(),
                Some(operation),
                AccessErrorKind::ColumnDenied { column },
            ));
        }
    }
    Ok(())
}

fn create_columns(cmd: &Qail) -> Result<Vec<String>, AccessError> {
    let mut columns = match cmd.action {
        Action::Merge => merge_insert_columns(cmd)?,
        _ => {
            let mut columns = Vec::new();
            if !cmd.columns.is_empty() {
                columns.extend(write_columns_from_exprs(&cmd.columns, "create columns")?);
            }
            let payload_columns = payload_columns(cmd)?;
            if columns.is_empty() || !payload_columns.is_empty() {
                columns.extend(payload_columns);
            }
            columns
        }
    };
    columns.sort();
    columns.dedup();
    Ok(columns)
}

fn update_columns(cmd: &Qail) -> Result<Vec<String>, AccessError> {
    let mut columns = match cmd.action {
        Action::Merge => merge_update_columns(cmd),
        Action::Add | Action::Upsert => conflict_update_columns(cmd),
        _ => payload_columns(cmd)?,
    };
    columns.sort();
    columns.dedup();
    Ok(columns)
}

fn write_columns_from_exprs(
    exprs: &[Expr],
    context: &'static str,
) -> Result<Vec<String>, AccessError> {
    let mut columns = Vec::new();
    for expr in exprs {
        let Some(column) = projection_column_name(expr) else {
            return Err(AccessError::new(
                String::new(),
                None,
                AccessErrorKind::UnsupportedColumnExpression { context },
            ));
        };
        columns.push(column);
    }
    Ok(columns)
}

fn payload_columns(cmd: &Qail) -> Result<Vec<String>, AccessError> {
    let mut columns = Vec::new();
    for cage in &cmd.cages {
        if !matches!(cage.kind, CageKind::Payload) {
            continue;
        }
        for condition in &cage.conditions {
            match &condition.left {
                Expr::Named(name) if name.trim_start().starts_with('$') => return Ok(Vec::new()),
                Expr::Named(name) => columns.push(normalize_column_name(name)),
                _ => {
                    return Err(AccessError::new(
                        String::new(),
                        None,
                        AccessErrorKind::UnsupportedColumnExpression {
                            context: "write payload",
                        },
                    ));
                }
            }
        }
    }
    Ok(columns)
}

fn conflict_update_columns(cmd: &Qail) -> Vec<String> {
    match cmd.on_conflict.as_ref().map(|conflict| &conflict.action) {
        Some(ConflictAction::DoUpdate { assignments }) => assignments
            .iter()
            .map(|(column, _)| normalize_column_name(column))
            .collect(),
        _ => Vec::new(),
    }
}

fn merge_insert_columns(cmd: &Qail) -> Result<Vec<String>, AccessError> {
    let mut columns = Vec::new();
    let Some(merge) = &cmd.merge else {
        return Ok(columns);
    };
    for clause in &merge.clauses {
        if let MergeAction::Insert {
            columns: insert_columns,
            ..
        } = &clause.action
        {
            if insert_columns.is_empty() {
                return Ok(Vec::new());
            }
            columns.extend(
                insert_columns
                    .iter()
                    .map(|column| normalize_column_name(column.as_str())),
            );
        }
    }
    Ok(columns)
}

fn merge_update_columns(cmd: &Qail) -> Vec<String> {
    let mut columns = Vec::new();
    if let Some(merge) = &cmd.merge {
        for clause in &merge.clauses {
            if let MergeAction::Update { assignments } = &clause.action {
                columns.extend(
                    assignments
                        .iter()
                        .map(|(column, _)| normalize_column_name(column)),
                );
            }
        }
    }
    columns
}

fn expr_projects_all_columns(expr: &Expr) -> bool {
    match expr {
        Expr::Star => true,
        Expr::Named(name) => {
            let trimmed = name.trim();
            trimmed == "*" || trimmed.ends_with(".*")
        }
        _ => false,
    }
}

fn projection_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Named(name) => simple_column_name(name),
        Expr::Aliased { name, .. } => simple_column_name(name),
        Expr::JsonAccess { column, .. } => simple_column_name(column),
        Expr::Aggregate { col, .. } if col != "*" => simple_column_name(col),
        _ => None,
    }
}

fn simple_column_name(name: &str) -> Option<String> {
    let trimmed = name.trim();
    if trimmed.is_empty()
        || trimmed == "*"
        || trimmed.ends_with(".*")
        || trimmed.contains('(')
        || trimmed.contains(')')
        || trimmed.split_whitespace().count() != 1
    {
        return None;
    }
    Some(normalize_column_name(trimmed))
}

fn normalize_column_name(name: impl Into<String>) -> String {
    let name = name.into();
    name.rsplit('.')
        .next()
        .unwrap_or(&name)
        .trim_matches('"')
        .to_ascii_lowercase()
}

fn normalize_table_ref(table_ref: &str) -> String {
    table_ref
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .trim_matches('"')
        .to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Condition, Operator};

    fn read_policy(table: &str) -> AccessPolicy {
        AccessPolicy::new().with_table(
            table,
            TableAccessPolicy::new().allow_operations([AccessOperation::Read]),
        )
    }

    #[test]
    fn deny_by_default_without_matching_table_policy() {
        let policy = AccessPolicy::new();
        let err = policy
            .check_command(&AccessContext::anonymous(), &Qail::get("orders"))
            .expect_err("missing table policy should fail closed");

        assert_eq!(err.kind, AccessErrorKind::NoPolicy);
        assert_eq!(err.operation, Some(AccessOperation::Read));
    }

    #[test]
    fn role_and_scope_gates_are_enforced() {
        let policy = AccessPolicy::new().with_table(
            "orders",
            TableAccessPolicy::new()
                .allow_operations([AccessOperation::Read])
                .require_any_role(["operator", "admin"])
                .require_scopes(["orders:read"]),
        );

        let missing_role = AccessContext::subject("user-1").with_scope("orders:read");
        assert!(matches!(
            policy
                .check_command(&missing_role, &Qail::get("orders"))
                .expect_err("role gate should fail")
                .kind,
            AccessErrorKind::MissingRole { .. }
        ));

        let allowed = AccessContext::subject("user-1")
            .with_role("operator")
            .with_scope("orders:read");
        policy
            .check_command(&allowed, &Qail::get("orders"))
            .expect("matching role and scope should pass");
    }

    #[test]
    fn read_column_allowlist_rejects_wildcard_and_denied_columns() {
        let policy = AccessPolicy::new().with_table(
            "users",
            TableAccessPolicy::new()
                .allow_operations([AccessOperation::Read])
                .read_columns(ColumnRule::only(["id", "email"])),
        );

        let wildcard = Qail::get("users");
        assert_eq!(
            policy
                .check_command(&AccessContext::anonymous(), &wildcard)
                .expect_err("implicit SELECT * should fail")
                .kind,
            AccessErrorKind::WildcardProjectionDenied
        );

        let denied = Qail::get("users").columns(["id", "password_hash"]);
        assert_eq!(
            policy
                .check_command(&AccessContext::anonymous(), &denied)
                .expect_err("password_hash should be denied")
                .kind,
            AccessErrorKind::ColumnDenied {
                column: "password_hash".to_string()
            }
        );

        policy
            .check_command(
                &AccessContext::anonymous(),
                &Qail::get("users").columns(["id", "email"]),
            )
            .expect("allowed projection should pass");
    }

    #[test]
    fn write_column_allowlist_checks_update_insert_upsert_and_merge() {
        let policy = AccessPolicy::new().with_table(
            "orders",
            TableAccessPolicy::new()
                .allow_operations([
                    AccessOperation::Create,
                    AccessOperation::Update,
                    AccessOperation::Delete,
                ])
                .write_columns(ColumnRule::only(["status", "total"])),
        );

        let update = Qail::set("orders").set_value("admin_note", "nope");
        assert_eq!(
            policy
                .check_command(&AccessContext::anonymous(), &update)
                .expect_err("update denied column should fail")
                .kind,
            AccessErrorKind::ColumnDenied {
                column: "admin_note".to_string()
            }
        );

        let insert = Qail::add("orders")
            .columns(["status"])
            .values(["paid"])
            .on_conflict_update(
                &["id"],
                &[("total", Expr::Named("EXCLUDED.total".to_string()))],
            );
        policy
            .check_command(&AccessContext::anonymous(), &insert)
            .expect("insert and conflict update columns should pass");

        let mixed_insert = Qail::add("orders")
            .columns(["status"])
            .set_value("admin_note", "hidden");
        assert_eq!(
            policy
                .check_command(&AccessContext::anonymous(), &mixed_insert)
                .expect_err("named payload columns must still be checked when columns are set")
                .kind,
            AccessErrorKind::ColumnDenied {
                column: "admin_note".to_string()
            }
        );

        let merge = Qail::merge_into("orders")
            .using_table_as("incoming_orders", "src")
            .merge_on_condition(Condition {
                left: Expr::Named("orders.id".to_string()),
                op: Operator::Eq,
                value: Value::Column("src.id".to_string()),
                is_array_unnest: false,
            })
            .when_matched_update(&[("private_note", Expr::Named("src.note".to_string()))])
            .when_not_matched_insert(
                &["status", "total"],
                &[
                    Expr::Named("src.status".to_string()),
                    Expr::Named("src.total".to_string()),
                ],
            );
        assert_eq!(
            policy
                .check_command(&AccessContext::anonymous(), &merge)
                .expect_err("merge update denied column should fail")
                .kind,
            AccessErrorKind::ColumnDenied {
                column: "private_note".to_string()
            }
        );
    }

    #[test]
    fn returning_uses_read_column_policy_even_on_writes() {
        let policy = AccessPolicy::new().with_table(
            "users",
            TableAccessPolicy::new()
                .allow_operations([AccessOperation::Update])
                .write_columns(ColumnRule::only(["email"]))
                .read_columns(ColumnRule::only(["id", "email"])),
        );

        let cmd = Qail::set("users")
            .set_value("email", "a@example.com")
            .returning(["password_hash"]);
        assert_eq!(
            policy
                .check_command(&AccessContext::anonymous(), &cmd)
                .expect_err("RETURNING denied read column should fail")
                .kind,
            AccessErrorKind::ColumnDenied {
                column: "password_hash".to_string()
            }
        );
    }

    #[test]
    fn subqueries_are_checked_recursively() {
        let policy = read_policy("orders");
        let mut cmd = Qail::get("users").columns_expr([Expr::Subquery {
            query: Box::new(Qail::get("orders").columns(["id"])),
            alias: None,
        }]);

        let err = policy
            .check_command(&AccessContext::anonymous(), &cmd)
            .expect_err("outer table still needs a policy");
        assert_eq!(err.table, "users");

        cmd.table = "orders".to_string();
        policy
            .check_command(&AccessContext::anonymous(), &cmd)
            .expect("outer and subquery table policies should pass");
    }

    #[test]
    fn super_admin_token_bypasses_policy_checks() {
        let token = SuperAdminToken::for_system_process("access-check-test");
        let ctx = AccessContext::super_admin(token);
        AccessPolicy::new()
            .check_command(&ctx, &Qail::get("missing"))
            .expect("super admin context should bypass access policy");
    }

    #[test]
    fn merge_query_source_is_checked_as_read() {
        let policy = AccessPolicy::new().with_table(
            "orders",
            TableAccessPolicy::new().allow_operations([AccessOperation::Update]),
        );

        let cmd = Qail::merge_into("orders")
            .using_query_as(Qail::get("source_orders").columns(["id"]), "src")
            .merge_on_condition(Condition {
                left: Expr::Named("orders.id".to_string()),
                op: Operator::Eq,
                value: Value::Column("src.id".to_string()),
                is_array_unnest: false,
            })
            .when_matched_update(&[("status", Expr::Named("src.status".to_string()))]);

        let err = policy
            .check_command(&AccessContext::anonymous(), &cmd)
            .expect_err("merge source query table should require read policy");
        assert_eq!(err.table, "source_orders");
        assert_eq!(err.operation, Some(AccessOperation::Read));
    }
}
