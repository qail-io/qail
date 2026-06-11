//! Native vertical access policy checks.
//!
//! Row-level security decides which rows a subject can see. This module covers
//! the vertical layer: which operations and columns a subject may use before a
//! driver sends the AST to a backend.

use std::collections::{BTreeMap, BTreeSet};

use crate::ast::{
    CageKind, Condition, ConflictAction, Expr, MergeAction, MergeSource, Qail, Value,
};

mod columns;
mod config;
mod error;
mod ident;
mod model;
mod operations;

use columns::{
    check_named_read_column, check_projection_rule, check_qualified_read_column, create_columns,
    expr_projects_all_columns, projection_restricted_action, update_columns,
};
use ident::{normalize_column_name, normalize_table_ref, target_refs_for_command};

pub use error::{AccessError, AccessErrorKind, AccessPolicyLoadError};
pub use model::{AccessContext, AccessDecision, AccessOperation, ColumnRule, TableAccessPolicy};
pub use operations::required_operations_for_command;

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
                for condition in &cage.conditions {
                    self.check_value_column_refs(
                        table,
                        rule,
                        &target_refs,
                        &condition.value,
                        "write payload value",
                    )?;
                }
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
        if let Some(on_conflict) = &cmd.on_conflict
            && let ConflictAction::DoUpdate { assignments } = &on_conflict.action
        {
            for (_, expr) in assignments {
                self.check_expr_column_refs(
                    table,
                    rule,
                    &target_refs,
                    expr,
                    "conflict update value",
                )?;
            }
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
                match &clause.action {
                    MergeAction::Update { assignments } => {
                        for (_, expr) in assignments {
                            self.check_expr_column_refs(
                                table,
                                rule,
                                &target_refs,
                                expr,
                                "merge update value",
                            )?;
                        }
                    }
                    MergeAction::Insert { values, .. } => {
                        for expr in values {
                            self.check_expr_column_refs(
                                table,
                                rule,
                                &target_refs,
                                expr,
                                "merge insert value",
                            )?;
                        }
                    }
                    MergeAction::Delete | MergeAction::DoNothing => {}
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
            Expr::Window {
                params,
                partition,
                order,
                ..
            } => {
                for param in params {
                    self.check_expr_column_refs(table, rule, target_refs, param, context)?;
                }
                for column in partition {
                    check_named_read_column(table, rule, target_refs, column, context)?;
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
            Expr::Subquery { query, .. } | Expr::Exists { query, .. } => {
                self.check_outer_command_column_refs(table, rule, target_refs, query)
            }
            Expr::Star | Expr::Def { .. } => Ok(()),
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
            Value::Function(_) => Err(AccessError::new(
                table.to_string(),
                Some(AccessOperation::Read),
                AccessErrorKind::UnsupportedColumnExpression { context },
            )),
            Value::Subquery(query) => {
                self.check_outer_command_column_refs(table, rule, target_refs, query)
            }
            _ => Ok(()),
        }
    }

    fn check_outer_command_column_refs(
        &self,
        table: &str,
        rule: &ColumnRule,
        target_refs: &BTreeSet<String>,
        cmd: &Qail,
    ) -> Result<(), AccessError> {
        for expr in &cmd.columns {
            self.check_outer_expr_column_refs(table, rule, target_refs, expr)?;
        }
        if let Some(returning) = &cmd.returning {
            for expr in returning {
                self.check_outer_expr_column_refs(table, rule, target_refs, expr)?;
            }
        }
        for cage in &cmd.cages {
            for condition in &cage.conditions {
                self.check_outer_condition_column_refs(table, rule, target_refs, condition)?;
            }
        }
        for condition in &cmd.having {
            self.check_outer_condition_column_refs(table, rule, target_refs, condition)?;
        }
        for join in &cmd.joins {
            if let Some(conditions) = &join.on {
                for condition in conditions {
                    self.check_outer_condition_column_refs(table, rule, target_refs, condition)?;
                }
            }
        }
        if let Some(on_conflict) = &cmd.on_conflict
            && let ConflictAction::DoUpdate { assignments } = &on_conflict.action
        {
            for (_, expr) in assignments {
                self.check_outer_expr_column_refs(table, rule, target_refs, expr)?;
            }
        }
        if let Some(merge) = &cmd.merge {
            if let MergeSource::Query { query, .. } = &merge.source {
                self.check_outer_command_column_refs(table, rule, target_refs, query)?;
            }
            for condition in &merge.on {
                self.check_outer_condition_column_refs(table, rule, target_refs, condition)?;
            }
            for clause in &merge.clauses {
                for condition in &clause.condition {
                    self.check_outer_condition_column_refs(table, rule, target_refs, condition)?;
                }
                match &clause.action {
                    MergeAction::Update { assignments } => {
                        for (_, expr) in assignments {
                            self.check_outer_expr_column_refs(table, rule, target_refs, expr)?;
                        }
                    }
                    MergeAction::Insert { values, .. } => {
                        for expr in values {
                            self.check_outer_expr_column_refs(table, rule, target_refs, expr)?;
                        }
                    }
                    MergeAction::Delete | MergeAction::DoNothing => {}
                }
            }
        }
        for cte in &cmd.ctes {
            self.check_outer_command_column_refs(table, rule, target_refs, &cte.base_query)?;
            if let Some(recursive_query) = &cte.recursive_query {
                self.check_outer_command_column_refs(table, rule, target_refs, recursive_query)?;
            }
        }
        for (_, set_query) in &cmd.set_ops {
            self.check_outer_command_column_refs(table, rule, target_refs, set_query)?;
        }
        if let Some(source_query) = &cmd.source_query {
            self.check_outer_command_column_refs(table, rule, target_refs, source_query)?;
        }
        Ok(())
    }

    fn check_outer_condition_column_refs(
        &self,
        table: &str,
        rule: &ColumnRule,
        target_refs: &BTreeSet<String>,
        condition: &Condition,
    ) -> Result<(), AccessError> {
        self.check_outer_expr_column_refs(table, rule, target_refs, &condition.left)?;
        self.check_outer_value_column_refs(table, rule, target_refs, &condition.value)
    }

    fn check_outer_expr_column_refs(
        &self,
        table: &str,
        rule: &ColumnRule,
        target_refs: &BTreeSet<String>,
        expr: &Expr,
    ) -> Result<(), AccessError> {
        match expr {
            Expr::Named(name)
            | Expr::Aliased { name, .. }
            | Expr::JsonAccess { column: name, .. } => {
                check_qualified_read_column(table, rule, target_refs, name)
            }
            Expr::Aggregate { col, filter, .. } => {
                if col != "*" {
                    check_qualified_read_column(table, rule, target_refs, col)?;
                }
                if let Some(conditions) = filter {
                    for condition in conditions {
                        self.check_outer_condition_column_refs(
                            table,
                            rule,
                            target_refs,
                            condition,
                        )?;
                    }
                }
                Ok(())
            }
            Expr::Cast { expr, .. }
            | Expr::Mod { col: expr, .. }
            | Expr::FieldAccess { expr, .. }
            | Expr::Collate { expr, .. } => {
                self.check_outer_expr_column_refs(table, rule, target_refs, expr)
            }
            Expr::Subscript { expr, index, .. } => {
                self.check_outer_expr_column_refs(table, rule, target_refs, expr)?;
                self.check_outer_expr_column_refs(table, rule, target_refs, index)
            }
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    self.check_outer_expr_column_refs(table, rule, target_refs, arg)?;
                }
                Ok(())
            }
            Expr::SpecialFunction { args, .. } => {
                for (_, arg) in args {
                    self.check_outer_expr_column_refs(table, rule, target_refs, arg)?;
                }
                Ok(())
            }
            Expr::Binary { left, right, .. } => {
                self.check_outer_expr_column_refs(table, rule, target_refs, left)?;
                self.check_outer_expr_column_refs(table, rule, target_refs, right)
            }
            Expr::Literal(value) => {
                self.check_outer_value_column_refs(table, rule, target_refs, value)
            }
            Expr::ArrayConstructor { elements, .. } | Expr::RowConstructor { elements, .. } => {
                for element in elements {
                    self.check_outer_expr_column_refs(table, rule, target_refs, element)?;
                }
                Ok(())
            }
            Expr::Case {
                when_clauses,
                else_value,
                ..
            } => {
                for (condition, value) in when_clauses {
                    self.check_outer_condition_column_refs(table, rule, target_refs, condition)?;
                    self.check_outer_expr_column_refs(table, rule, target_refs, value)?;
                }
                if let Some(value) = else_value {
                    self.check_outer_expr_column_refs(table, rule, target_refs, value)?;
                }
                Ok(())
            }
            Expr::Window {
                params,
                partition,
                order,
                ..
            } => {
                for param in params {
                    self.check_outer_expr_column_refs(table, rule, target_refs, param)?;
                }
                for column in partition {
                    check_qualified_read_column(table, rule, target_refs, column)?;
                }
                for cage in order {
                    for condition in &cage.conditions {
                        self.check_outer_condition_column_refs(
                            table,
                            rule,
                            target_refs,
                            condition,
                        )?;
                    }
                }
                Ok(())
            }
            Expr::Subquery { query, .. } | Expr::Exists { query, .. } => {
                self.check_outer_command_column_refs(table, rule, target_refs, query)
            }
            Expr::Star | Expr::Def { .. } => Ok(()),
        }
    }

    fn check_outer_value_column_refs(
        &self,
        table: &str,
        rule: &ColumnRule,
        target_refs: &BTreeSet<String>,
        value: &Value,
    ) -> Result<(), AccessError> {
        match value {
            Value::Column(name) => check_qualified_read_column(table, rule, target_refs, name),
            Value::Expr(expr) => self.check_outer_expr_column_refs(table, rule, target_refs, expr),
            Value::Array(values) => {
                for value in values {
                    self.check_outer_value_column_refs(table, rule, target_refs, value)?;
                }
                Ok(())
            }
            Value::Subquery(query) => {
                self.check_outer_command_column_refs(table, rule, target_refs, query)
            }
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

#[cfg(test)]
mod tests;
