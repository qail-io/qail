//! Native vertical access policy checks.
//!
//! Row-level security decides which rows a subject can see. This module covers
//! the vertical layer: which operations and columns a subject may use before a
//! driver sends the AST to a backend.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::ast::{
    Action, CageKind, Condition, ConflictAction, Expr, MergeAction, MergeSource, Qail, Value,
};

mod columns;
mod error;
mod ident;
mod model;

use columns::{
    check_named_read_column, check_projection_rule, create_columns, expr_projects_all_columns,
    projection_restricted_action, update_columns,
};
use ident::{normalize_column_name, normalize_table_ref, target_refs_for_command};

pub use error::{AccessError, AccessErrorKind, AccessPolicyLoadError};
pub use model::{AccessContext, AccessDecision, AccessOperation, ColumnRule, TableAccessPolicy};

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

    /// Parse an access policy from TOML.
    pub fn from_toml_str(input: &str) -> Result<Self, AccessPolicyLoadError> {
        toml::from_str::<Self>(input)
            .map(Self::normalize_table_keys)
            .map_err(AccessPolicyLoadError::Toml)
    }

    /// Parse an access policy from JSON.
    pub fn from_json_str(input: &str) -> Result<Self, AccessPolicyLoadError> {
        serde_json::from_str::<Self>(input)
            .map(Self::normalize_table_keys)
            .map_err(AccessPolicyLoadError::Json)
    }

    /// Load an access policy from a `.toml` or `.json` file.
    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self, AccessPolicyLoadError> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path).map_err(AccessPolicyLoadError::Read)?;
        match path
            .extension()
            .and_then(|extension| extension.to_str())
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("toml") => Self::from_toml_str(&raw),
            Some("json") => Self::from_json_str(&raw),
            other => Err(AccessPolicyLoadError::UnsupportedExtension(
                other.unwrap_or_default().to_string(),
            )),
        }
    }

    /// Mutably access a table policy, creating an empty policy if needed.
    pub fn table_mut(&mut self, table: impl Into<String>) -> &mut TableAccessPolicy {
        self.tables
            .entry(normalize_table_ref(&table.into()))
            .or_default()
    }

    fn normalize_table_keys(mut self) -> Self {
        self.tables = self
            .tables
            .into_iter()
            .map(|(table, policy)| (normalize_table_ref(&table), policy))
            .collect();
        self
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
        if let Some(merge) = &cmd.merge {
            match &merge.source {
                MergeSource::Query { query, .. } => self.check_command_inner(ctx, query)?,
                MergeSource::Table { name, .. } => self.check_merge_table_source(ctx, name)?,
            }
        }

        let table = normalize_table_ref(&cmd.table);
        if table.is_empty() {
            return Err(AccessError::new(
                String::new(),
                None,
                AccessErrorKind::EmptyTable,
            ));
        }

        self.check_embedded_queries(ctx, cmd)?;
        self.check_condition_read_columns(&table, cmd)?;

        let cte_names: BTreeSet<String> = cmd
            .ctes
            .iter()
            .map(|cte| normalize_table_ref(&cte.name))
            .collect();
        self.check_join_read_access(ctx, cmd, &cte_names)?;
        self.check_auxiliary_read_access(ctx, cmd, &cte_names)?;

        let required_ops = required_operations_for_command(cmd).ok_or_else(|| {
            AccessError::new(
                table.clone(),
                None,
                AccessErrorKind::UnsupportedAction(cmd.action),
            )
        })?;
        if cte_names.contains(&table) {
            if required_ops.iter().all(|op| *op == AccessOperation::Read) {
                return Ok(());
            }
            return Err(AccessError::new(
                table,
                None,
                AccessErrorKind::CteMutationUnsupported,
            ));
        }

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

    fn check_merge_table_source(
        &self,
        ctx: &AccessContext,
        source_table: &str,
    ) -> Result<(), AccessError> {
        let table = normalize_table_ref(source_table);
        if table.is_empty() {
            return Err(AccessError::new(
                String::new(),
                Some(AccessOperation::Read),
                AccessErrorKind::EmptyTable,
            ));
        }

        self.check_table_operation(ctx, &table, AccessOperation::Read)?;
        if self
            .table_policy(&table)
            .is_some_and(|policy| policy.read_columns.is_restrictive())
        {
            return Err(AccessError::new(
                table,
                Some(AccessOperation::Read),
                AccessErrorKind::SourceTableColumnPolicyUnsupported,
            ));
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

    fn check_auxiliary_read_access(
        &self,
        ctx: &AccessContext,
        cmd: &Qail,
        cte_names: &BTreeSet<String>,
    ) -> Result<(), AccessError> {
        for table_ref in cmd.from_tables.iter().chain(&cmd.using_tables) {
            let table = normalize_table_ref(table_ref);
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
                    AccessErrorKind::AuxiliaryTableColumnPolicyUnsupported,
                ));
            }
        }
        Ok(())
    }

    fn check_condition_read_columns(&self, table: &str, cmd: &Qail) -> Result<(), AccessError> {
        let rule = self
            .table_policy(table)
            .map(|policy| &policy.read_columns)
            .unwrap_or(&ColumnRule::Any);
        if !rule.is_restrictive() {
            return Ok(());
        }

        let target_refs = target_refs_for_command(cmd, table);
        self.check_distinct_on_columns(table, rule, &target_refs, cmd)?;
        self.check_grouping_set_columns(table, rule, &target_refs, cmd)?;
        for cage in &cmd.cages {
            if matches!(cage.kind, CageKind::Payload) {
                continue;
            }
            for condition in &cage.conditions {
                self.check_condition_column_refs(
                    table,
                    rule,
                    &target_refs,
                    condition,
                    "condition",
                )?;
            }
        }
        for condition in &cmd.having {
            self.check_condition_column_refs(
                table,
                rule,
                &target_refs,
                condition,
                "having condition",
            )?;
        }
        for join in &cmd.joins {
            if let Some(conditions) = &join.on {
                for condition in conditions {
                    self.check_condition_column_refs(
                        table,
                        rule,
                        &target_refs,
                        condition,
                        "join condition",
                    )?;
                }
            }
        }
        if let Some(merge) = &cmd.merge {
            for condition in &merge.on {
                self.check_condition_column_refs(
                    table,
                    rule,
                    &target_refs,
                    condition,
                    "merge condition",
                )?;
            }
            for clause in &merge.clauses {
                for condition in &clause.condition {
                    self.check_condition_column_refs(
                        table,
                        rule,
                        &target_refs,
                        condition,
                        "merge condition",
                    )?;
                }
            }
        }
        Ok(())
    }

    fn check_distinct_on_columns(
        &self,
        table: &str,
        rule: &ColumnRule,
        target_refs: &BTreeSet<String>,
        cmd: &Qail,
    ) -> Result<(), AccessError> {
        for expr in &cmd.distinct_on {
            if expr_projects_all_columns(expr) {
                return Err(AccessError::new(
                    table.to_string(),
                    Some(AccessOperation::Read),
                    AccessErrorKind::WildcardProjectionDenied,
                ));
            }
            self.check_expr_column_refs(table, rule, target_refs, expr, "distinct on")?;
        }
        Ok(())
    }

    fn check_grouping_set_columns(
        &self,
        table: &str,
        rule: &ColumnRule,
        target_refs: &BTreeSet<String>,
        cmd: &Qail,
    ) -> Result<(), AccessError> {
        if let crate::ast::GroupByMode::GroupingSets(sets) = &cmd.group_by_mode {
            for group in sets {
                for column in group {
                    check_named_read_column(table, rule, target_refs, column, "grouping sets")?;
                }
            }
        }
        Ok(())
    }

    fn check_condition_column_refs(
        &self,
        table: &str,
        rule: &ColumnRule,
        target_refs: &BTreeSet<String>,
        condition: &Condition,
        context: &'static str,
    ) -> Result<(), AccessError> {
        self.check_expr_column_refs(table, rule, target_refs, &condition.left, context)?;
        self.check_value_column_refs(table, rule, target_refs, &condition.value, context)
    }

    fn check_expr_column_refs(
        &self,
        table: &str,
        rule: &ColumnRule,
        target_refs: &BTreeSet<String>,
        expr: &Expr,
        context: &'static str,
    ) -> Result<(), AccessError> {
        match expr {
            Expr::Named(name)
            | Expr::Aliased { name, .. }
            | Expr::JsonAccess { column: name, .. } => {
                check_named_read_column(table, rule, target_refs, name, context)
            }
            Expr::Aggregate { col, filter, .. } => {
                if col != "*" {
                    check_named_read_column(table, rule, target_refs, col, context)?;
                }
                if let Some(conditions) = filter {
                    for condition in conditions {
                        self.check_condition_column_refs(
                            table,
                            rule,
                            target_refs,
                            condition,
                            context,
                        )?;
                    }
                }
                Ok(())
            }
            Expr::Cast { expr, .. }
            | Expr::Mod { col: expr, .. }
            | Expr::FieldAccess { expr, .. }
            | Expr::Collate { expr, .. } => {
                self.check_expr_column_refs(table, rule, target_refs, expr, context)
            }
            Expr::Subscript { expr, index, .. } => {
                self.check_expr_column_refs(table, rule, target_refs, expr, context)?;
                self.check_expr_column_refs(table, rule, target_refs, index, context)
            }
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    self.check_expr_column_refs(table, rule, target_refs, arg, context)?;
                }
                Ok(())
            }
            Expr::SpecialFunction { args, .. } => {
                for (_, arg) in args {
                    self.check_expr_column_refs(table, rule, target_refs, arg, context)?;
                }
                Ok(())
            }
            Expr::Binary { left, right, .. } => {
                self.check_expr_column_refs(table, rule, target_refs, left, context)?;
                self.check_expr_column_refs(table, rule, target_refs, right, context)
            }
            Expr::Literal(value) => {
                self.check_value_column_refs(table, rule, target_refs, value, context)
            }
            Expr::ArrayConstructor { elements, .. } | Expr::RowConstructor { elements, .. } => {
                for element in elements {
                    self.check_expr_column_refs(table, rule, target_refs, element, context)?;
                }
                Ok(())
            }
            Expr::Case {
                when_clauses,
                else_value,
                ..
            } => {
                for (condition, value) in when_clauses {
                    self.check_condition_column_refs(table, rule, target_refs, condition, context)?;
                    self.check_expr_column_refs(table, rule, target_refs, value, context)?;
                }
                if let Some(value) = else_value {
                    self.check_expr_column_refs(table, rule, target_refs, value, context)?;
                }
                Ok(())
            }
            Expr::Window { params, order, .. } => {
                for param in params {
                    self.check_expr_column_refs(table, rule, target_refs, param, context)?;
                }
                for cage in order {
                    for condition in &cage.conditions {
                        self.check_condition_column_refs(
                            table,
                            rule,
                            target_refs,
                            condition,
                            context,
                        )?;
                    }
                }
                Ok(())
            }
            Expr::Subquery { .. } | Expr::Exists { .. } | Expr::Star | Expr::Def { .. } => Ok(()),
        }
    }

    fn check_value_column_refs(
        &self,
        table: &str,
        rule: &ColumnRule,
        target_refs: &BTreeSet<String>,
        value: &Value,
        context: &'static str,
    ) -> Result<(), AccessError> {
        match value {
            Value::Column(name) => check_named_read_column(table, rule, target_refs, name, context),
            Value::Expr(expr) => {
                self.check_expr_column_refs(table, rule, target_refs, expr, context)
            }
            Value::Array(values) => {
                for value in values {
                    self.check_value_column_refs(table, rule, target_refs, value, context)?;
                }
                Ok(())
            }
            Value::Subquery(_) => Ok(()),
            _ => Ok(()),
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Condition, Operator};
    use crate::rls::SuperAdminToken;

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

        let denied_filter =
            Qail::get("users")
                .columns(["id"])
                .filter("password_hash", Operator::Eq, "secret");
        assert_eq!(
            policy
                .check_command(&AccessContext::anonymous(), &denied_filter)
                .expect_err("filtering by a denied column should fail")
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
        let policy = AccessPolicy::new()
            .with_table(
                "orders",
                TableAccessPolicy::new()
                    .allow_operations([
                        AccessOperation::Create,
                        AccessOperation::Update,
                        AccessOperation::Delete,
                    ])
                    .write_columns(ColumnRule::only(["status", "total"])),
            )
            .with_table(
                "incoming_orders",
                TableAccessPolicy::new().allow_operations([AccessOperation::Read]),
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
    fn read_column_policy_does_not_block_write_only_payloads() {
        let policy = AccessPolicy::new().with_table(
            "orders",
            TableAccessPolicy::new()
                .allow_operations([AccessOperation::Update])
                .read_columns(ColumnRule::only(["id"]))
                .write_columns(ColumnRule::only(["status"])),
        );

        let allowed = Qail::set("orders")
            .set_value("status", "paid")
            .filter("id", Operator::Eq, 1);
        policy
            .check_command(&AccessContext::anonymous(), &allowed)
            .expect("write-only payload column should not require read access");

        let denied_filter =
            Qail::set("orders")
                .set_value("status", "paid")
                .filter("status", Operator::Eq, "draft");
        assert_eq!(
            policy
                .check_command(&AccessContext::anonymous(), &denied_filter)
                .expect_err("filter column should still require read access")
                .kind,
            AccessErrorKind::ColumnDenied {
                column: "status".to_string()
            }
        );
    }

    #[test]
    fn update_from_and_delete_using_require_read_access_on_auxiliary_tables() {
        let policy = AccessPolicy::new().with_table(
            "orders",
            TableAccessPolicy::new()
                .allow_operations([AccessOperation::Update, AccessOperation::Delete]),
        );

        let update = Qail::set("orders")
            .set_value("status", "paid")
            .update_from(["accounts"])
            .filter(
                "orders.account_id",
                Operator::Eq,
                Value::Column("accounts.id".into()),
            );
        let err = policy
            .check_command(&AccessContext::anonymous(), &update)
            .expect_err("UPDATE FROM source table should require read policy");
        assert_eq!(err.table, "accounts");
        assert_eq!(err.operation, Some(AccessOperation::Read));

        let delete = Qail::del("orders").delete_using(["accounts"]).filter(
            "orders.account_id",
            Operator::Eq,
            Value::Column("accounts.id".into()),
        );
        let err = policy
            .check_command(&AccessContext::anonymous(), &delete)
            .expect_err("DELETE USING source table should require read policy");
        assert_eq!(err.table, "accounts");
        assert_eq!(err.operation, Some(AccessOperation::Read));
    }

    #[test]
    fn auxiliary_tables_with_restrictive_read_columns_fail_closed() {
        let policy = AccessPolicy::new()
            .with_table(
                "orders",
                TableAccessPolicy::new().allow_operations([AccessOperation::Update]),
            )
            .with_table(
                "accounts",
                TableAccessPolicy::new()
                    .allow_operations([AccessOperation::Read])
                    .read_columns(ColumnRule::only(["id"])),
            );

        let cmd = Qail::set("orders")
            .set_value("status", "paid")
            .update_from(["accounts"])
            .filter(
                "orders.account_id",
                Operator::Eq,
                Value::Column("accounts.id".into()),
            );

        assert_eq!(
            policy
                .check_command(&AccessContext::anonymous(), &cmd)
                .expect_err("restrictive auxiliary source columns cannot be enforced precisely")
                .kind,
            AccessErrorKind::AuxiliaryTableColumnPolicyUnsupported
        );
    }

    #[test]
    fn read_column_policy_checks_distinct_on_and_grouping_sets() {
        let policy = AccessPolicy::new().with_table(
            "orders",
            TableAccessPolicy::new()
                .allow_operations([AccessOperation::Read])
                .read_columns(ColumnRule::only(["id", "status"])),
        );

        let distinct = Qail::get("orders")
            .columns(["id"])
            .distinct_on(["private_note"]);
        assert_eq!(
            policy
                .check_command(&AccessContext::anonymous(), &distinct)
                .expect_err("DISTINCT ON denied column should fail")
                .kind,
            AccessErrorKind::ColumnDenied {
                column: "private_note".to_string()
            }
        );

        let mut grouping = Qail::get("orders").columns(["id"]);
        grouping.group_by_mode =
            crate::ast::GroupByMode::GroupingSets(vec![vec!["private_note".to_string()]]);
        assert_eq!(
            policy
                .check_command(&AccessContext::anonymous(), &grouping)
                .expect_err("GROUPING SETS denied column should fail")
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
    fn cte_alias_reads_do_not_require_separate_table_policy() {
        let policy = AccessPolicy::new().with_table(
            "orders",
            TableAccessPolicy::new()
                .allow_operations([AccessOperation::Read])
                .read_columns(ColumnRule::only(["id", "status"])),
        );
        let cmd = Qail::get("recent_orders")
            .with(
                "recent_orders",
                Qail::get("orders").columns(["id", "status"]),
            )
            .columns(["id"]);

        policy
            .check_command(&AccessContext::anonymous(), &cmd)
            .expect("CTE alias should be treated as a checked derived relation");
    }

    #[test]
    fn cte_body_still_enforces_base_table_policy() {
        let policy = AccessPolicy::new().with_table(
            "orders",
            TableAccessPolicy::new()
                .allow_operations([AccessOperation::Read])
                .read_columns(ColumnRule::only(["id"])),
        );
        let cmd = Qail::get("recent_orders")
            .with(
                "recent_orders",
                Qail::get("orders").columns(["id", "private_note"]),
            )
            .columns(["id"]);

        assert_eq!(
            policy
                .check_command(&AccessContext::anonymous(), &cmd)
                .expect_err("CTE body denied columns must still fail")
                .kind,
            AccessErrorKind::ColumnDenied {
                column: "private_note".to_string()
            }
        );
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

    #[test]
    fn merge_table_source_is_checked_as_read() {
        let policy = AccessPolicy::new().with_table(
            "orders",
            TableAccessPolicy::new().allow_operations([AccessOperation::Update]),
        );

        let cmd = Qail::merge_into("orders")
            .using_table_as("source_orders", "src")
            .merge_on_condition(Condition {
                left: Expr::Named("orders.id".to_string()),
                op: Operator::Eq,
                value: Value::Column("src.id".to_string()),
                is_array_unnest: false,
            })
            .when_matched_update(&[("status", Expr::Named("src.status".to_string()))]);

        let err = policy
            .check_command(&AccessContext::anonymous(), &cmd)
            .expect_err("merge source table should require read policy");
        assert_eq!(err.table, "source_orders");
        assert_eq!(err.operation, Some(AccessOperation::Read));
    }

    #[test]
    fn merge_table_source_with_restrictive_columns_requires_query_source() {
        let policy = AccessPolicy::new()
            .with_table(
                "orders",
                TableAccessPolicy::new().allow_operations([AccessOperation::Update]),
            )
            .with_table(
                "source_orders",
                TableAccessPolicy::new()
                    .allow_operations([AccessOperation::Read])
                    .read_columns(ColumnRule::only(["id"])),
            );

        let cmd = Qail::merge_into("orders")
            .using_table_as("source_orders", "src")
            .merge_on_condition(Condition {
                left: Expr::Named("orders.id".to_string()),
                op: Operator::Eq,
                value: Value::Column("src.id".to_string()),
                is_array_unnest: false,
            })
            .when_matched_update(&[("status", Expr::Named("src.status".to_string()))]);

        assert_eq!(
            policy
                .check_command(&AccessContext::anonymous(), &cmd)
                .expect_err("restrictive source table columns need an explicit query source")
                .kind,
            AccessErrorKind::SourceTableColumnPolicyUnsupported
        );
    }

    #[test]
    fn access_policy_loads_from_toml_and_json() {
        let toml_policy = r#"
default_decision = "deny"

[tables.Orders]
operations = ["read"]
read_columns = { only = ["id", "status"] }
require_any_role = ["operator"]
require_scopes = ["orders:read"]
"#;
        let policy = AccessPolicy::from_toml_str(toml_policy).unwrap();
        policy
            .check_command(
                &AccessContext::subject("user-1")
                    .with_role("operator")
                    .with_scope("orders:read"),
                &Qail::get("orders").columns(["id", "status"]),
            )
            .expect("TOML policy should allow declared columns");
        assert!(policy.tables.contains_key("orders"));

        let json_policy = r#"{
            "default_decision": "deny",
            "tables": {
                "orders": {
                    "operations": ["read"],
                    "read_columns": {"only": ["id"]}
                }
            }
        }"#;
        let policy = AccessPolicy::from_json_str(json_policy).unwrap();
        policy
            .check_command(
                &AccessContext::anonymous(),
                &Qail::get("orders").columns(["id"]),
            )
            .expect("JSON policy should allow declared column");
    }

    #[test]
    fn access_policy_rejects_unsupported_file_extensions() {
        let path = std::env::temp_dir().join(format!(
            "qail-access-policy-{}-{}.yaml",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, "default_decision: deny").unwrap();
        let err = AccessPolicy::load_from_path(&path).unwrap_err();
        let _ = std::fs::remove_file(&path);

        assert!(matches!(
            err,
            AccessPolicyLoadError::UnsupportedExtension(extension) if extension == "yaml"
        ));
    }
}
