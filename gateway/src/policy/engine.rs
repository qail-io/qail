use std::fs;

use qail_core::ast::{
    Action, Cage, CageKind, Condition, ConflictAction, Expr, Join, JoinKind, LogicalOp,
    MergeAction, MergeSource, Operator, Qail, Value,
};

use crate::auth::AuthContext;
use crate::error::GatewayError;

use super::{OperationType, PolicyConfig, PolicyDef, PolicyEngine};

impl PolicyEngine {
    /// Create an empty policy engine.
    pub fn new() -> Self {
        Self::default()
    }

    /// Load policies from a YAML configuration file.
    pub fn load_from_file(&mut self, path: &str) -> Result<(), GatewayError> {
        let content = fs::read_to_string(path)
            .map_err(|e| GatewayError::Config(format!("Failed to read policy file: {}", e)))?;

        let config: PolicyConfig = serde_yaml::from_str(&content)
            .map_err(|e| GatewayError::Config(format!("Failed to parse policy file: {}", e)))?;

        self.policies = config.policies;
        tracing::info!("Loaded {} policies from {}", self.policies.len(), path);

        for policy in &self.policies {
            tracing::debug!(
                "Policy '{}': table={}, filter={:?}, role={:?}",
                policy.name,
                policy.table,
                policy.filter,
                policy.role
            );
        }

        Ok(())
    }

    /// Register an additional policy definition.
    pub fn add_policy(&mut self, policy: PolicyDef) {
        self.policies.push(policy);
    }

    /// Evaluate all matching policies for a given auth context and command,
    /// injecting filters and column restrictions into the AST.
    ///
    /// # Arguments
    ///
    /// * `auth` — Authenticated user context (role, operator, agent).
    /// * `cmd` — Mutable Qail AST command to inject policy filters into.
    pub fn apply_policies(&self, auth: &AuthContext, cmd: &mut Qail) -> Result<(), GatewayError> {
        self.apply_policies_inner(auth, cmd)
    }

    fn table_ref_name_and_qualifier(table_ref: &str) -> (String, String) {
        let parts: Vec<&str> = table_ref.split_whitespace().collect();
        match parts.as_slice() {
            [] => (String::new(), String::new()),
            [table] => (
                table.trim_matches('"').to_string(),
                table.trim_matches('"').to_string(),
            ),
            [table, alias] => (
                table.trim_matches('"').to_string(),
                alias.trim_matches('"').to_string(),
            ),
            [table, as_kw, alias, ..] if as_kw.eq_ignore_ascii_case("as") => (
                table.trim_matches('"').to_string(),
                alias.trim_matches('"').to_string(),
            ),
            [table, alias, ..] => (
                table.trim_matches('"').to_string(),
                alias.trim_matches('"').to_string(),
            ),
        }
    }

    fn qualify_condition_left(condition: &mut Condition, qualifier: &str) {
        if let Expr::Named(name) = &condition.left
            && !name.contains('.')
        {
            condition.left = Expr::Named(format!("{}.{}", qualifier, name));
        }
    }

    fn projection_restricted_action(action: Action) -> bool {
        matches!(action, Action::Get | Action::Export | Action::With)
    }

    fn applicable_policies<'a>(
        &'a self,
        auth: &AuthContext,
        table: &str,
        op: OperationType,
    ) -> Result<Vec<&'a PolicyDef>, GatewayError> {
        let mut matched_policy_names: Vec<String> = Vec::new();
        let mut applicable_policies: Vec<&PolicyDef> = Vec::new();

        for policy in &self.policies {
            if policy.table != "*" && policy.table != table {
                continue;
            }

            if let Some(ref required_role) = policy.role
                && &auth.role != required_role
            {
                continue;
            }

            matched_policy_names.push(policy.name.clone());

            let op_allowed = policy.operations.is_empty() || policy.operations.contains(&op);

            if op_allowed {
                applicable_policies.push(policy);
            }
        }

        if !matched_policy_names.is_empty() && applicable_policies.is_empty() {
            return Err(GatewayError::AccessDenied(format!(
                "Operation {:?} not allowed on table '{}' by matching policies {:?}",
                op, table, matched_policy_names
            )));
        }

        if applicable_policies.is_empty() {
            return Err(GatewayError::AccessDenied(format!(
                "No policy allows {:?} on table '{}'",
                op, table
            )));
        }

        Ok(applicable_policies)
    }

    fn inject_join_filter(
        cmd: &mut Qail,
        join: &mut Join,
        filter: Condition,
    ) -> Result<(), GatewayError> {
        match join.kind {
            JoinKind::Inner | JoinKind::Left | JoinKind::Lateral => {
                join.on_true = false;
                join.on.get_or_insert_with(Vec::new).push(filter);
                Ok(())
            }
            JoinKind::Cross => {
                cmd.cages.push(Cage {
                    kind: CageKind::Filter,
                    conditions: vec![filter],
                    logical_op: LogicalOp::And,
                });
                Ok(())
            }
            JoinKind::Right | JoinKind::Full => Err(GatewayError::AccessDenied(format!(
                "Policy filters cannot be safely enforced on joined table '{}' through {:?} joins",
                join.table, join.kind
            ))),
        }
    }

    fn apply_join_policies(
        &self,
        auth: &AuthContext,
        cmd: &mut Qail,
        cte_names: &[String],
    ) -> Result<(), GatewayError> {
        if self.policies.is_empty() || cmd.joins.is_empty() {
            return Ok(());
        }

        let Some(op) = OperationType::from_action(cmd.action) else {
            return Ok(());
        };
        let projection_restricted_action = Self::projection_restricted_action(cmd.action);

        let mut rewritten_joins = Vec::with_capacity(cmd.joins.len());
        for mut join in std::mem::take(&mut cmd.joins) {
            let (join_table, qualifier) = Self::table_ref_name_and_qualifier(&join.table);
            if join_table.is_empty() || cte_names.iter().any(|name| name == &join_table) {
                rewritten_joins.push(join);
                continue;
            }

            let applicable_policies = self.applicable_policies(auth, &join_table, op)?;

            for policy in &applicable_policies {
                if projection_restricted_action
                    && (!policy.allowed_columns.is_empty() || !policy.denied_columns.is_empty())
                {
                    return Err(GatewayError::AccessDenied(format!(
                        "Joined table '{}' has column policies that cannot be enforced in a flat join",
                        join_table
                    )));
                }
            }

            let mut filters_to_inject = Vec::new();
            let mut has_unrestricted_policy = false;
            for policy in &applicable_policies {
                if let Some(ref filter_template) = policy.filter {
                    let filter_str = self.expand_filter(filter_template, auth);
                    let mut condition = self.parse_filter_to_condition(&filter_str)?;
                    Self::qualify_condition_left(&mut condition, &qualifier);
                    filters_to_inject.push(condition);
                } else {
                    has_unrestricted_policy = true;
                    break;
                }
            }

            if !has_unrestricted_policy {
                match filters_to_inject.len() {
                    0 => {}
                    1 => {
                        let Some(filter) = filters_to_inject.pop() else {
                            return Err(GatewayError::Internal(anyhow::anyhow!(
                                "missing join policy filter after length check"
                            )));
                        };
                        Self::inject_join_filter(cmd, &mut join, filter)?;
                    }
                    _ => {
                        return Err(GatewayError::AccessDenied(format!(
                            "Joined table '{}' has multiple filtered policies that cannot be represented safely in a flat join",
                            join_table
                        )));
                    }
                }
            }

            rewritten_joins.push(join);
        }
        cmd.joins = rewritten_joins;
        Ok(())
    }

    fn payload_is_positional(cage: &Cage) -> bool {
        cage.conditions.iter().all(|cond| {
            matches!(
                &cond.left,
                Expr::Named(name)
                    if name.starts_with('$') && name[1..].chars().all(|c| c.is_ascii_digit())
            )
        })
    }

    fn expr_named_eq(expr: &Expr, name: &str) -> bool {
        matches!(expr, Expr::Named(existing) if existing.trim_matches('"').eq_ignore_ascii_case(name.trim_matches('"')))
    }

    fn payload_condition(column: String, value: Value) -> Condition {
        Condition {
            left: Expr::Named(column),
            op: Operator::Eq,
            value,
            is_array_unnest: false,
        }
    }

    fn normalized_policy_column(name: &str) -> String {
        name.rsplit('.')
            .next()
            .unwrap_or(name)
            .trim_matches('"')
            .to_ascii_lowercase()
    }

    fn projects_all_columns(expr: &Expr) -> bool {
        match expr {
            Expr::Star => true,
            Expr::Named(name) => {
                let trimmed = name.trim();
                trimmed == "*" || trimmed.ends_with(".*")
            }
            _ => false,
        }
    }

    fn write_target_column_name(expr: &Expr, context: &str) -> Result<String, GatewayError> {
        match expr {
            Expr::Named(name)
                if !Self::projects_all_columns(expr)
                    && !name.trim().is_empty()
                    && Self::is_safe_policy_column_name(name) =>
            {
                Ok(Self::normalized_policy_column(name))
            }
            other => Err(GatewayError::AccessDenied(format!(
                "Policy column restrictions cannot be enforced on {} target expression {:?}",
                context, other
            ))),
        }
    }

    fn payload_column_from_condition(
        cmd: &Qail,
        condition: &Condition,
    ) -> Result<String, GatewayError> {
        match &condition.left {
            Expr::Named(name)
                if name.starts_with('$') && name[1..].chars().all(|c| c.is_ascii_digit()) =>
            {
                let index: usize = name[1..].parse().map_err(|_| {
                    GatewayError::AccessDenied(format!(
                        "Policy column restrictions cannot map positional payload '{}'",
                        name
                    ))
                })?;
                if index == 0 || cmd.columns.is_empty() {
                    return Err(GatewayError::AccessDenied(
                        "Policy column restrictions require explicit columns for positional mutation payloads"
                            .to_string(),
                    ));
                }
                let expr = cmd.columns.get(index - 1).ok_or_else(|| {
                    GatewayError::AccessDenied(format!(
                        "Policy column restrictions cannot map positional payload '{}' to a target column",
                        name
                    ))
                })?;
                Self::write_target_column_name(expr, "positional mutation payload")
            }
            Expr::Named(name) if Self::is_safe_policy_column_name(name) => {
                Ok(Self::normalized_policy_column(name))
            }
            other => Err(GatewayError::AccessDenied(format!(
                "Policy column restrictions cannot be enforced on mutation payload expression {:?}",
                other
            ))),
        }
    }

    fn write_payload_columns(cmd: &Qail) -> Result<Vec<String>, GatewayError> {
        if cmd.source_query.is_some() {
            if cmd.columns.is_empty() {
                return Err(GatewayError::AccessDenied(
                    "Policy column restrictions require explicit target columns for INSERT ... SELECT"
                        .to_string(),
                ));
            }

            return cmd
                .columns
                .iter()
                .map(|expr| Self::write_target_column_name(expr, "INSERT ... SELECT"))
                .collect();
        }

        let mut columns = Vec::new();
        for cage in &cmd.cages {
            if !matches!(cage.kind, CageKind::Payload) {
                continue;
            }
            for condition in &cage.conditions {
                columns.push(Self::payload_column_from_condition(cmd, condition)?);
            }
        }
        Ok(columns)
    }

    fn conflict_update_columns(cmd: &Qail) -> Result<Vec<String>, GatewayError> {
        let Some(on_conflict) = cmd.on_conflict.as_ref() else {
            return Ok(Vec::new());
        };
        let ConflictAction::DoUpdate { assignments } = &on_conflict.action else {
            return Ok(Vec::new());
        };

        assignments
            .iter()
            .map(|(column, _)| {
                if Self::is_safe_policy_column_name(column) {
                    Ok(Self::normalized_policy_column(column))
                } else {
                    Err(GatewayError::AccessDenied(format!(
                        "Policy column restrictions rejected unsupported conflict update column '{}'",
                        column
                    )))
                }
            })
            .collect()
    }

    fn merge_required_operations(cmd: &Qail) -> Result<Vec<OperationType>, GatewayError> {
        let Some(merge) = cmd.merge.as_ref() else {
            return Err(GatewayError::AccessDenied(
                "MERGE action is missing merge specification".to_string(),
            ));
        };

        let mut operations = Vec::new();
        for clause in &merge.clauses {
            let operation = match &clause.action {
                MergeAction::Insert { .. } => Some(OperationType::Create),
                MergeAction::Update { .. } => Some(OperationType::Update),
                MergeAction::Delete => Some(OperationType::Delete),
                MergeAction::DoNothing => None,
            };
            if let Some(operation) = operation
                && !operations.contains(&operation)
            {
                operations.push(operation);
            }
        }

        if operations.is_empty() {
            return Err(GatewayError::AccessDenied(
                "MERGE requires at least one mutating action".to_string(),
            ));
        }

        Ok(operations)
    }

    fn required_operations_for_command(cmd: &Qail) -> Result<Vec<OperationType>, GatewayError> {
        if cmd.action == Action::Merge {
            return Self::merge_required_operations(cmd);
        }

        OperationType::required_for_action(cmd.action)
            .map(|operations| operations.to_vec())
            .ok_or_else(|| {
                GatewayError::AccessDenied(format!(
                    "Action {:?} is not permitted by policy engine",
                    cmd.action
                ))
            })
    }

    fn merge_write_columns(
        cmd: &Qail,
        operation: OperationType,
    ) -> Result<Vec<String>, GatewayError> {
        let Some(merge) = cmd.merge.as_ref() else {
            return Ok(Vec::new());
        };

        let mut columns = Vec::new();
        for clause in &merge.clauses {
            match (&clause.action, operation) {
                (
                    MergeAction::Insert {
                        columns: insert_columns,
                        ..
                    },
                    OperationType::Create,
                ) => {
                    if insert_columns.is_empty() {
                        return Err(GatewayError::AccessDenied(
                            "Policy column restrictions require explicit columns for MERGE INSERT"
                                .to_string(),
                        ));
                    }
                    for column in insert_columns {
                        if Self::is_safe_policy_column_name(column) {
                            columns.push(Self::normalized_policy_column(column));
                        } else {
                            return Err(GatewayError::AccessDenied(format!(
                                "Policy column restrictions rejected unsupported MERGE insert column '{}'",
                                column
                            )));
                        }
                    }
                }
                (MergeAction::Update { assignments }, OperationType::Update) => {
                    for (column, _) in assignments {
                        if Self::is_safe_policy_column_name(column) {
                            columns.push(Self::normalized_policy_column(column));
                        } else {
                            return Err(GatewayError::AccessDenied(format!(
                                "Policy column restrictions rejected unsupported MERGE update column '{}'",
                                column
                            )));
                        }
                    }
                }
                _ => {}
            }
        }

        columns.sort();
        columns.dedup();
        Ok(columns)
    }

    fn enforce_merge_policy_filters(
        &self,
        auth: &AuthContext,
        base_table: &str,
        required_ops: &[OperationType],
    ) -> Result<(), GatewayError> {
        for operation in required_ops {
            let policies = self.applicable_policies(auth, base_table, *operation)?;
            let (filters, unrestricted) =
                self.policy_filters_for(auth, &policies, base_table, false)?;
            if !unrestricted && !filters.is_empty() {
                return Err(GatewayError::AccessDenied(format!(
                    "Filtered {:?} policies cannot be safely enforced on MERGE for table '{}'",
                    operation, base_table
                )));
            }
        }

        Ok(())
    }

    fn enforce_merge_source_table_policies(
        &self,
        auth: &AuthContext,
        cmd: &mut Qail,
        cte_names: &[String],
    ) -> Result<(), GatewayError> {
        if cmd.action != Action::Merge {
            return Ok(());
        }

        let Some(merge) = cmd.merge.as_ref() else {
            return Ok(());
        };
        let (source_table, source_qualifier) = match &merge.source {
            MergeSource::Table { name, alias } => {
                let table = name.trim_matches('"').to_string();
                if cte_names.iter().any(|cte_name| cte_name == &table) {
                    return Ok(());
                }
                let qualifier = alias
                    .as_deref()
                    .unwrap_or(name)
                    .trim_matches('"')
                    .to_string();
                (table, qualifier)
            }
            MergeSource::Query { .. } => return Ok(()),
        };

        let policies = self.applicable_policies(auth, &source_table, OperationType::Read)?;
        if policies
            .iter()
            .any(|policy| Self::policy_restricts_columns(policy))
        {
            return Err(GatewayError::AccessDenied(format!(
                "MERGE source table '{}' has column policies that cannot be enforced safely; use a policy-checked query source",
                source_table
            )));
        }

        let (mut source_filters, has_unrestricted_policy) =
            self.policy_filters_for(auth, &policies, &source_qualifier, true)?;
        if has_unrestricted_policy || source_filters.is_empty() {
            return Ok(());
        }
        if source_filters.len() > 1 {
            return Err(GatewayError::AccessDenied(format!(
                "MERGE source table '{}' has multiple filtered read policies that cannot be represented safely",
                source_table
            )));
        }

        if let Some(merge) = cmd.merge.as_mut() {
            merge.on.append(&mut source_filters);
        }
        Ok(())
    }

    fn enforce_merge_column_policies(
        cmd: &Qail,
        policies: &[&PolicyDef],
        operation: OperationType,
    ) -> Result<(), GatewayError> {
        if !policies
            .iter()
            .any(|policy| Self::policy_restricts_columns(policy))
        {
            return Ok(());
        }

        let columns = Self::merge_write_columns(cmd, operation)?;
        Self::enforce_write_columns_for_policies(policies, &columns, operation)
    }

    fn policy_restricts_columns(policy: &PolicyDef) -> bool {
        !policy.allowed_columns.is_empty() || !policy.denied_columns.is_empty()
    }

    fn enforce_write_columns_for_policies(
        policies: &[&PolicyDef],
        columns: &[String],
        operation: OperationType,
    ) -> Result<(), GatewayError> {
        for policy in policies {
            if !Self::policy_restricts_columns(policy) {
                continue;
            }

            let allowed: std::collections::HashSet<String> = policy
                .allowed_columns
                .iter()
                .map(|column| Self::normalized_policy_column(column))
                .collect();
            let denied: std::collections::HashSet<String> = policy
                .denied_columns
                .iter()
                .map(|column| Self::normalized_policy_column(column))
                .collect();

            for column in columns {
                if !allowed.is_empty() && !allowed.contains(column) {
                    return Err(GatewayError::AccessDenied(format!(
                        "Policy '{}' does not allow {:?} on column '{}'",
                        policy.name, operation, column
                    )));
                }
                if denied.contains(column) {
                    return Err(GatewayError::AccessDenied(format!(
                        "Policy '{}' denies {:?} on column '{}'",
                        policy.name, operation, column
                    )));
                }
            }
        }

        Ok(())
    }

    fn enforce_write_column_policies(
        cmd: &Qail,
        policies: &[&PolicyDef],
        operation: OperationType,
    ) -> Result<(), GatewayError> {
        if !policies
            .iter()
            .any(|policy| Self::policy_restricts_columns(policy))
        {
            return Ok(());
        }

        let columns = Self::write_payload_columns(cmd)?;
        Self::enforce_write_columns_for_policies(policies, &columns, operation)
    }

    fn enforce_conflict_update_column_policies(
        cmd: &Qail,
        policies: &[&PolicyDef],
    ) -> Result<(), GatewayError> {
        if !policies
            .iter()
            .any(|policy| Self::policy_restricts_columns(policy))
        {
            return Ok(());
        }

        let columns = Self::conflict_update_columns(cmd)?;
        Self::enforce_write_columns_for_policies(policies, &columns, OperationType::Update)
    }

    fn create_policy_column(condition: &Condition) -> Result<String, GatewayError> {
        match &condition.left {
            Expr::Named(name) if !name.contains('.') => Ok(name.trim_matches('"').to_string()),
            Expr::Named(name) => Ok(name
                .rsplit('.')
                .next()
                .unwrap_or(name)
                .trim_matches('"')
                .to_string()),
            other => Err(GatewayError::AccessDenied(format!(
                "Create policy filter left expression {:?} cannot be enforced for INSERT payloads",
                other
            ))),
        }
    }

    fn ensure_explicit_insert_select_target_columns(
        cmd: &Qail,
        column: &str,
    ) -> Result<(), GatewayError> {
        if cmd.columns.is_empty() {
            return Err(GatewayError::AccessDenied(format!(
                "Create policy filter on '{}' requires explicit target columns for INSERT ... SELECT",
                column
            )));
        }

        if cmd.columns.iter().any(|expr| {
            !matches!(expr, Expr::Named(name) if !Self::projects_all_columns(expr) && !name.trim().is_empty() && !name.contains('.'))
        }) {
            return Err(GatewayError::AccessDenied(format!(
                "Create policy filter on '{}' requires simple named target columns for INSERT ... SELECT",
                column
            )));
        }

        Ok(())
    }

    fn ensure_source_projection_can_be_rewritten(
        source_query: &Qail,
        expected_len: usize,
        column: &str,
    ) -> Result<(), GatewayError> {
        if source_query.columns.is_empty()
            || source_query.columns.iter().any(Self::projects_all_columns)
        {
            return Err(GatewayError::AccessDenied(format!(
                "Create policy filter on '{}' requires an explicit non-star source projection for INSERT ... SELECT",
                column
            )));
        }

        if source_query.columns.len() != expected_len {
            return Err(GatewayError::AccessDenied(format!(
                "Create policy filter on '{}' cannot be enforced for INSERT ... SELECT target/source column count mismatch: target has {}, source has {}",
                column,
                expected_len,
                source_query.columns.len()
            )));
        }

        for (_, set_query) in &source_query.set_ops {
            Self::ensure_source_projection_can_be_rewritten(set_query, expected_len, column)?;
        }

        Ok(())
    }

    fn rewrite_source_projection_create_policy(
        source_query: &mut Qail,
        column_index: usize,
        append_column: bool,
        value: &Value,
    ) {
        if append_column {
            source_query.columns.push(Expr::Literal(value.clone()));
        } else {
            source_query.columns[column_index] = Expr::Literal(value.clone());
        }

        for (_, set_query) in &mut source_query.set_ops {
            Self::rewrite_source_projection_create_policy(
                set_query,
                column_index,
                append_column,
                value,
            );
        }
    }

    fn apply_create_policy_constraint_to_source_query(
        cmd: &mut Qail,
        column: &str,
        value: &Value,
    ) -> Result<(), GatewayError> {
        Self::ensure_explicit_insert_select_target_columns(cmd, column)?;

        let target_column_count = cmd
            .columns
            .iter()
            .filter(|expr| Self::expr_named_eq(expr, column))
            .count();
        if target_column_count > 1 {
            return Err(GatewayError::AccessDenied(format!(
                "Create policy filter on '{}' cannot be enforced for duplicate target columns",
                column
            )));
        }

        let append_column = target_column_count == 0;
        let column_index = cmd
            .columns
            .iter()
            .position(|expr| Self::expr_named_eq(expr, column))
            .unwrap_or(cmd.columns.len());
        let expected_source_len = cmd.columns.len();

        let Some(source_query) = cmd.source_query.as_deref() else {
            return Ok(());
        };
        Self::ensure_source_projection_can_be_rewritten(source_query, expected_source_len, column)?;

        if append_column {
            cmd.columns.push(Expr::Named(column.to_string()));
        }

        if let Some(source_query) = cmd.source_query.as_deref_mut() {
            Self::rewrite_source_projection_create_policy(
                source_query,
                column_index,
                append_column,
                value,
            );
        }

        Ok(())
    }

    fn apply_create_policy_constraint(
        cmd: &mut Qail,
        condition: &Condition,
    ) -> Result<(), GatewayError> {
        if condition.op != Operator::Eq {
            return Err(GatewayError::AccessDenied(format!(
                "Create policy filter on '{}' cannot be enforced for INSERT payloads; use equality filters",
                condition.left
            )));
        }

        let column = Self::create_policy_column(condition)?;

        if cmd.source_query.is_some() {
            return Self::apply_create_policy_constraint_to_source_query(
                cmd,
                &column,
                &condition.value,
            );
        }

        let payload_idx = cmd
            .cages
            .iter()
            .position(|cage| matches!(cage.kind, CageKind::Payload));

        let Some(idx) = payload_idx else {
            cmd.cages.push(Cage {
                kind: CageKind::Payload,
                conditions: vec![Self::payload_condition(column, condition.value.clone())],
                logical_op: LogicalOp::And,
            });
            return Ok(());
        };

        if Self::payload_is_positional(&cmd.cages[idx]) {
            if cmd.columns.is_empty() {
                return Err(GatewayError::AccessDenied(format!(
                    "Create policy filter on '{}' requires explicit columns for positional INSERT payloads",
                    column
                )));
            }

            if let Some(col_idx) = cmd
                .columns
                .iter()
                .position(|expr| Self::expr_named_eq(expr, &column))
            {
                let placeholder = format!("${}", col_idx + 1);
                let cage = &mut cmd.cages[idx];
                if let Some(cond) = cage
                    .conditions
                    .iter_mut()
                    .find(|cond| Self::expr_named_eq(&cond.left, &placeholder))
                {
                    *cond = Self::payload_condition(placeholder, condition.value.clone());
                } else {
                    cage.conditions.push(Self::payload_condition(
                        placeholder,
                        condition.value.clone(),
                    ));
                }
                return Ok(());
            }

            cmd.columns.push(Expr::Named(column));
            let col_idx = cmd.columns.len() - 1;
            cmd.cages[idx].conditions.push(Self::payload_condition(
                format!("${}", col_idx + 1),
                condition.value.clone(),
            ));
            return Ok(());
        }

        let cage = &mut cmd.cages[idx];
        cage.conditions
            .retain(|cond| !Self::expr_named_eq(&cond.left, &column));
        cage.conditions
            .push(Self::payload_condition(column, condition.value.clone()));
        Ok(())
    }

    fn on_conflict_do_update(cmd: &Qail) -> bool {
        cmd.action == Action::Add
            && cmd.on_conflict.as_ref().is_some_and(|on_conflict| {
                matches!(
                    on_conflict.action,
                    qail_core::ast::ConflictAction::DoUpdate { .. }
                )
            })
    }

    fn policy_filters_for(
        &self,
        auth: &AuthContext,
        policies: &[&PolicyDef],
        qualifier: &str,
        qualify: bool,
    ) -> Result<(Vec<Condition>, bool), GatewayError> {
        let mut filters = Vec::new();
        let mut has_unrestricted_policy = false;

        for policy in policies {
            if let Some(ref filter_template) = policy.filter {
                let filter_str = self.expand_filter(filter_template, auth);
                let mut condition = self.parse_filter_to_condition(&filter_str)?;
                if qualify {
                    Self::qualify_condition_left(&mut condition, qualifier);
                }
                filters.push(condition);
            } else {
                has_unrestricted_policy = true;
                break;
            }
        }

        Ok((filters, has_unrestricted_policy))
    }

    pub(crate) fn filter_cages_for_operation(
        &self,
        auth: &AuthContext,
        table: &str,
        op: OperationType,
    ) -> Result<Vec<Cage>, GatewayError> {
        if self.policies.is_empty() {
            return Ok(Vec::new());
        }

        let applicable_policies = self.applicable_policies(auth, table, op)?;
        let (filters, has_unrestricted_policy) =
            self.policy_filters_for(auth, &applicable_policies, table, false)?;
        if has_unrestricted_policy || filters.is_empty() {
            return Ok(Vec::new());
        }

        Ok(vec![Cage {
            kind: CageKind::Filter,
            conditions: filters,
            logical_op: LogicalOp::Or,
        }])
    }

    fn apply_value_subquery_policies(
        &self,
        auth: &AuthContext,
        value: &mut Value,
    ) -> Result<(), GatewayError> {
        match value {
            Value::Array(values) => {
                for value in values {
                    self.apply_value_subquery_policies(auth, value)?;
                }
            }
            Value::Subquery(query) => self.apply_policies_inner(auth, query)?,
            Value::Expr(expr) => self.apply_expr_subquery_policies(auth, expr)?,
            _ => {}
        }

        Ok(())
    }

    fn apply_condition_subquery_policies(
        &self,
        auth: &AuthContext,
        condition: &mut Condition,
    ) -> Result<(), GatewayError> {
        self.apply_expr_subquery_policies(auth, &mut condition.left)?;
        self.apply_value_subquery_policies(auth, &mut condition.value)
    }

    fn apply_expr_subquery_policies(
        &self,
        auth: &AuthContext,
        expr: &mut Expr,
    ) -> Result<(), GatewayError> {
        match expr {
            Expr::Aggregate {
                filter: Some(filter),
                ..
            } => {
                for condition in filter {
                    self.apply_condition_subquery_policies(auth, condition)?;
                }
            }
            Expr::Cast { expr, .. } | Expr::Mod { col: expr, .. } | Expr::Collate { expr, .. } => {
                self.apply_expr_subquery_policies(auth, expr)?;
            }
            Expr::Window { params, order, .. } => {
                for expr in params {
                    self.apply_expr_subquery_policies(auth, expr)?;
                }
                for cage in order {
                    for condition in &mut cage.conditions {
                        self.apply_condition_subquery_policies(auth, condition)?;
                    }
                }
            }
            Expr::Case {
                when_clauses,
                else_value,
                ..
            } => {
                for (condition, then_expr) in when_clauses {
                    self.apply_condition_subquery_policies(auth, condition)?;
                    self.apply_expr_subquery_policies(auth, then_expr)?;
                }
                if let Some(expr) = else_value {
                    self.apply_expr_subquery_policies(auth, expr)?;
                }
            }
            Expr::FunctionCall { args, .. } => {
                for expr in args {
                    self.apply_expr_subquery_policies(auth, expr)?;
                }
            }
            Expr::SpecialFunction { args, .. } => {
                for (_, expr) in args {
                    self.apply_expr_subquery_policies(auth, expr)?;
                }
            }
            Expr::Binary { left, right, .. } => {
                self.apply_expr_subquery_policies(auth, left)?;
                self.apply_expr_subquery_policies(auth, right)?;
            }
            Expr::Literal(value) => self.apply_value_subquery_policies(auth, value)?,
            Expr::ArrayConstructor { elements, .. } | Expr::RowConstructor { elements, .. } => {
                for expr in elements {
                    self.apply_expr_subquery_policies(auth, expr)?;
                }
            }
            Expr::Subscript { expr, index, .. } => {
                self.apply_expr_subquery_policies(auth, expr)?;
                self.apply_expr_subquery_policies(auth, index)?;
            }
            Expr::FieldAccess { expr, .. } => self.apply_expr_subquery_policies(auth, expr)?,
            Expr::Subquery { query, .. } | Expr::Exists { query, .. } => {
                self.apply_policies_inner(auth, query)?;
            }
            Expr::Star
            | Expr::Named(_)
            | Expr::Aliased { .. }
            | Expr::Aggregate { filter: None, .. }
            | Expr::Def { .. }
            | Expr::JsonAccess { .. } => {}
        }

        Ok(())
    }

    fn apply_embedded_subquery_policies(
        &self,
        auth: &AuthContext,
        cmd: &mut Qail,
    ) -> Result<(), GatewayError> {
        for expr in &mut cmd.columns {
            self.apply_expr_subquery_policies(auth, expr)?;
        }
        for expr in &mut cmd.distinct_on {
            self.apply_expr_subquery_policies(auth, expr)?;
        }
        if let Some(returning) = &mut cmd.returning {
            for expr in returning {
                self.apply_expr_subquery_policies(auth, expr)?;
            }
        }
        for cage in &mut cmd.cages {
            for condition in &mut cage.conditions {
                self.apply_condition_subquery_policies(auth, condition)?;
            }
        }
        for condition in &mut cmd.having {
            self.apply_condition_subquery_policies(auth, condition)?;
        }
        for join in &mut cmd.joins {
            if let Some(conditions) = &mut join.on {
                for condition in conditions {
                    self.apply_condition_subquery_policies(auth, condition)?;
                }
            }
        }
        if let Some(on_conflict) = &mut cmd.on_conflict
            && let qail_core::ast::ConflictAction::DoUpdate { assignments } =
                &mut on_conflict.action
        {
            for (_, expr) in assignments {
                self.apply_expr_subquery_policies(auth, expr)?;
            }
        }
        if let Some(merge) = &mut cmd.merge {
            if let MergeSource::Query { query, .. } = &mut merge.source {
                self.apply_policies_inner(auth, query)?;
            }
            for condition in &mut merge.on {
                self.apply_condition_subquery_policies(auth, condition)?;
            }
            for clause in &mut merge.clauses {
                for condition in &mut clause.condition {
                    self.apply_condition_subquery_policies(auth, condition)?;
                }
                match &mut clause.action {
                    MergeAction::Update { assignments } => {
                        for (_, expr) in assignments {
                            self.apply_expr_subquery_policies(auth, expr)?;
                        }
                    }
                    MergeAction::Insert { values, .. } => {
                        for expr in values {
                            self.apply_expr_subquery_policies(auth, expr)?;
                        }
                    }
                    MergeAction::Delete | MergeAction::DoNothing => {}
                }
            }
        }

        Ok(())
    }

    fn apply_policies_inner(&self, auth: &AuthContext, cmd: &mut Qail) -> Result<(), GatewayError> {
        for cte in &mut cmd.ctes {
            self.apply_policies_inner(auth, &mut cte.base_query)?;
            if let Some(ref mut recursive_query) = cte.recursive_query {
                self.apply_policies_inner(auth, recursive_query)?;
            }
        }
        for (_, set_query) in &mut cmd.set_ops {
            self.apply_policies_inner(auth, set_query)?;
        }
        if let Some(ref mut source_query) = cmd.source_query {
            self.apply_policies_inner(auth, source_query)?;
        }

        self.apply_embedded_subquery_policies(auth, cmd)?;

        let cte_names: Vec<String> = cmd.ctes.iter().map(|cte| cte.name.clone()).collect();
        self.apply_join_policies(auth, cmd, &cte_names)?;

        if self.policies.is_empty() {
            return Ok(());
        }
        self.enforce_merge_source_table_policies(auth, cmd, &cte_names)?;

        if command_reads_cte_alias(cmd) {
            return Ok(());
        }

        let required_ops = Self::required_operations_for_command(cmd)?;
        let Some(op) = required_ops.last().copied() else {
            return Err(GatewayError::AccessDenied(format!(
                "Action {:?} has no policy operations",
                cmd.action
            )));
        };
        let (base_table, base_qualifier) = Self::table_ref_name_and_qualifier(&cmd.table);
        for required_op in &required_ops {
            self.applicable_policies(auth, &base_table, *required_op)?;
        }
        let applicable_policies = self.applicable_policies(auth, &base_table, op)?;
        let (filters_to_inject, has_unrestricted_policy) = self.policy_filters_for(
            auth,
            &applicable_policies,
            &base_qualifier,
            !cmd.joins.is_empty(),
        )?;

        if cmd.action == Action::Merge {
            self.enforce_merge_policy_filters(auth, &base_table, &required_ops)?;
            for required_op in &required_ops {
                let policies = self.applicable_policies(auth, &base_table, *required_op)?;
                Self::enforce_merge_column_policies(cmd, &policies, *required_op)?;
            }
        } else if cmd.action == Action::Upsert {
            let create_policies =
                self.applicable_policies(auth, &base_table, OperationType::Create)?;
            Self::enforce_write_column_policies(cmd, &create_policies, OperationType::Create)?;
            let update_policies =
                self.applicable_policies(auth, &base_table, OperationType::Update)?;
            Self::enforce_write_column_policies(cmd, &update_policies, OperationType::Update)?;
        } else if matches!(
            cmd.action,
            Action::Add | Action::Set | Action::Put | Action::Over
        ) {
            Self::enforce_write_column_policies(cmd, &applicable_policies, op)?;
        }

        if cmd.action == Action::Add && !has_unrestricted_policy {
            if filters_to_inject.len() > 1 {
                return Err(GatewayError::AccessDenied(format!(
                    "Multiple filtered create policies cannot be safely enforced on INSERT for table '{}'",
                    base_table
                )));
            }
            for condition in &filters_to_inject {
                Self::apply_create_policy_constraint(cmd, condition)?;
            }
        }

        if cmd.action != Action::Merge && !has_unrestricted_policy && !filters_to_inject.is_empty()
        {
            cmd.cages.push(Cage {
                kind: CageKind::Filter,
                conditions: filters_to_inject,
                logical_op: LogicalOp::Or, // Combine multiple policies with OR (Permissive)
            });
            tracing::debug!(
                "Applied {} policy filters with OR logic",
                applicable_policies.len()
            );
        }

        if Self::on_conflict_do_update(cmd) {
            let update_policies =
                self.applicable_policies(auth, &base_table, OperationType::Update)?;
            Self::enforce_conflict_update_column_policies(cmd, &update_policies)?;
            let (update_filters, update_unrestricted) = self.policy_filters_for(
                auth,
                &update_policies,
                &base_qualifier,
                !cmd.joins.is_empty(),
            )?;
            if !update_unrestricted && !update_filters.is_empty() {
                cmd.cages.push(Cage {
                    kind: CageKind::Filter,
                    conditions: update_filters,
                    logical_op: LogicalOp::Or,
                });
            }
        }

        let projection_restricted_action = Self::projection_restricted_action(cmd.action);

        // Apply column-level permissions for projection-bearing reads.
        for policy in &applicable_policies {
            if !projection_restricted_action {
                continue;
            }

            // Whitelist: restrict to allowed columns only
            if !policy.allowed_columns.is_empty() {
                self.apply_column_whitelist(cmd, &policy.allowed_columns)?;
                tracing::debug!(
                    "Policy '{}' restricts columns to: {:?}",
                    policy.name,
                    policy.allowed_columns
                );
            }

            // Blacklist: strip denied columns
            if !policy.denied_columns.is_empty() {
                self.apply_column_blacklist(cmd, &policy.denied_columns)?;
                tracing::debug!(
                    "Policy '{}' denies columns: {:?}",
                    policy.name,
                    policy.denied_columns
                );
            }
        }

        Ok(())
    }

    /// Expand filter template with auth context values.
    ///
    /// SECURITY: All string values are SQL-escaped (single quotes doubled)
    /// before interpolation to prevent injection via crafted JWT claims.
    pub(super) fn expand_filter(&self, template: &str, auth: &AuthContext) -> String {
        let mut replacements = vec![
            (
                "$user_id".to_string(),
                Self::quote_policy_string(&auth.user_id),
            ),
            ("$role".to_string(), Self::quote_policy_string(&auth.role)),
        ];

        // SECURITY (H1): only canonical auth.tenant_id may expand $tenant_id.
        // Extra JWT claims must not spoof tenant scope when tenant_id is missing.
        if let Some(ref tid) = auth.tenant_id {
            replacements.push(("$tenant_id".to_string(), Self::quote_policy_string(tid)));
        }

        for (key, value) in &auth.claims {
            if matches!(key.as_str(), "user_id" | "role" | "tenant_id") {
                continue;
            }
            let placeholder = format!("${}", key);
            replacements.push((placeholder, Self::policy_claim_replacement(value)));
        }

        Self::expand_policy_placeholders(template, &replacements)
    }

    fn quote_policy_string(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    fn policy_claim_replacement(value: &serde_json::Value) -> String {
        match value {
            serde_json::Value::String(s) => Self::quote_policy_string(s),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            _ => Self::quote_policy_string(&value.to_string()),
        }
    }

    fn expand_policy_placeholders(template: &str, replacements: &[(String, String)]) -> String {
        let mut result = String::with_capacity(template.len());
        let mut idx = 0;
        while idx < template.len() {
            if template.as_bytes()[idx] == b'$' {
                let token_len = Self::policy_placeholder_token_len(&template[idx..]);
                let token = &template[idx..idx + token_len];
                if let Some((_, replacement)) = replacements
                    .iter()
                    .find(|(placeholder, _)| placeholder == token)
                {
                    result.push_str(replacement);
                } else {
                    result.push_str(token);
                }
                idx += token_len;
                continue;
            }

            let Some(ch) = template[idx..].chars().next() else {
                break;
            };
            result.push(ch);
            idx += ch.len_utf8();
        }

        result
    }

    fn policy_placeholder_token_len(input: &str) -> usize {
        let mut len = 1;
        for ch in input[1..].chars() {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' || ch == '.' {
                len += ch.len_utf8();
            } else {
                break;
            }
        }
        len
    }

    fn parse_policy_quoted_string(value: &str) -> Option<String> {
        if !(value.starts_with('\'') && value.ends_with('\'')) || value.len() < 2 {
            return None;
        }

        let inner = &value[1..value.len() - 1];
        let mut parsed = String::with_capacity(inner.len());
        let mut chars = inner.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == '\'' && chars.peek() == Some(&'\'') {
                parsed.push('\'');
                chars.next();
            } else {
                parsed.push(ch);
            }
        }
        Some(parsed)
    }

    fn split_policy_filter(filter_expr: &str) -> Option<(&str, Operator, &str)> {
        let mut in_quote = false;
        let mut iter = filter_expr.char_indices().peekable();

        while let Some((idx, ch)) = iter.next() {
            if ch == '\'' {
                if in_quote && iter.peek().is_some_and(|(_, next)| *next == '\'') {
                    iter.next();
                } else {
                    in_quote = !in_quote;
                }
                continue;
            }

            if in_quote {
                continue;
            }

            if filter_expr[idx..].starts_with(" != ") {
                return Some((
                    filter_expr[..idx].trim(),
                    Operator::Ne,
                    filter_expr[idx + 4..].trim(),
                ));
            }
            if filter_expr[idx..].starts_with(" = ") {
                return Some((
                    filter_expr[..idx].trim(),
                    Operator::Eq,
                    filter_expr[idx + 3..].trim(),
                ));
            }
        }

        None
    }

    /// Parse a filter string into an AST Condition.
    pub(super) fn parse_filter_to_condition(
        &self,
        filter_expr: &str,
    ) -> Result<Condition, GatewayError> {
        let Some((column, op, value_str)) = Self::split_policy_filter(filter_expr) else {
            return Err(GatewayError::Config(format!(
                "Unsupported filter expression: {}. Use 'column = value' format.",
                filter_expr
            )));
        };

        if column.is_empty() || value_str.is_empty() {
            return Err(GatewayError::Config(format!(
                "Invalid filter expression: {}",
                filter_expr
            )));
        }

        let value = if let Some(parsed) = Self::parse_policy_quoted_string(value_str) {
            Value::String(parsed)
        } else if value_str == "true" {
            Value::Bool(true)
        } else if value_str == "false" {
            Value::Bool(false)
        } else if let Ok(n) = value_str.parse::<i64>() {
            Value::Int(n)
        } else {
            Value::String(value_str.to_string())
        };

        Ok(Condition {
            left: Expr::Named(column.to_string()),
            op,
            value,
            is_array_unnest: false,
        })
    }

    /// Inject a filter expression into the query (legacy wrapper)
    #[cfg(test)]
    pub(super) fn inject_filter(
        &self,
        cmd: &mut Qail,
        filter_expr: &str,
    ) -> Result<(), GatewayError> {
        let condition = self.parse_filter_to_condition(filter_expr)?;

        // Inject as a filter cage
        cmd.cages.push(Cage {
            kind: CageKind::Filter,
            conditions: vec![condition],
            logical_op: LogicalOp::And,
        });

        Ok(())
    }

    fn is_star_projection(columns: &[Expr]) -> bool {
        columns.is_empty() || (columns.len() == 1 && Self::projects_all_columns(&columns[0]))
    }

    fn projection_column_name(expr: &Expr) -> Option<&str> {
        match expr {
            Expr::Named(name) => Some(name.as_str()),
            Expr::Aliased { name, .. } => Some(name.as_str()),
            _ => None,
        }
    }

    fn is_safe_policy_column_name(name: &str) -> bool {
        !name.is_empty()
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    }

    /// Apply column whitelist: replace SELECT columns with only the allowed set.
    /// Fails closed when projection expressions prevent deterministic enforcement.
    fn apply_column_whitelist(
        &self,
        cmd: &mut Qail,
        allowed: &[String],
    ) -> Result<(), GatewayError> {
        if Self::is_star_projection(&cmd.columns) {
            // SELECT * → restrict to allowed columns
            cmd.columns = allowed.iter().map(|c| Expr::Named(c.clone())).collect();
            return Ok(());
        }

        let allowed: std::collections::HashSet<String> = allowed
            .iter()
            .map(|column| Self::normalized_policy_column(column))
            .collect();
        let mut filtered = Vec::with_capacity(cmd.columns.len());
        for expr in &cmd.columns {
            let name = Self::projection_column_name(expr).ok_or_else(|| {
                GatewayError::AccessDenied(
                    "Policy column whitelist cannot be enforced on expression projections"
                        .to_string(),
                )
            })?;
            if !Self::is_safe_policy_column_name(name) {
                return Err(GatewayError::AccessDenied(format!(
                    "Policy column whitelist rejected unsupported projection '{}'",
                    name
                )));
            }
            let normalized_name = Self::normalized_policy_column(name);
            if allowed.contains(&normalized_name) {
                filtered.push(expr.clone());
            }
        }

        if filtered.is_empty() {
            return Err(GatewayError::AccessDenied(
                "No selected columns are allowed by policy".to_string(),
            ));
        }

        cmd.columns = filtered;
        Ok(())
    }

    /// Apply column blacklist: remove denied columns from SELECT.
    /// Fails closed for wildcard/expression projections.
    fn apply_column_blacklist(
        &self,
        cmd: &mut Qail,
        denied: &[String],
    ) -> Result<(), GatewayError> {
        if Self::is_star_projection(&cmd.columns) {
            return Err(GatewayError::AccessDenied(
                "Policy denied_columns cannot be enforced on wildcard projection; select explicit columns"
                    .to_string(),
            ));
        }

        let denied: std::collections::HashSet<String> = denied
            .iter()
            .map(|column| Self::normalized_policy_column(column))
            .collect();
        let mut filtered = Vec::with_capacity(cmd.columns.len());
        for expr in &cmd.columns {
            let name = Self::projection_column_name(expr).ok_or_else(|| {
                GatewayError::AccessDenied(
                    "Policy denied_columns cannot be enforced on expression projections"
                        .to_string(),
                )
            })?;
            if !Self::is_safe_policy_column_name(name) {
                return Err(GatewayError::AccessDenied(format!(
                    "Policy denied_columns rejected unsupported projection '{}'",
                    name
                )));
            }
            let normalized_name = Self::normalized_policy_column(name);
            if !denied.contains(&normalized_name) {
                filtered.push(expr.clone());
            }
        }

        if filtered.is_empty() {
            return Err(GatewayError::AccessDenied(
                "All selected columns are denied by policy".to_string(),
            ));
        }

        cmd.columns = filtered;
        Ok(())
    }

    /// Check if any policy denies access (before filter injection).
    ///
    /// # Arguments
    ///
    /// * `auth` — Authenticated user context.
    /// * `table` — Target table name.
    /// * `action` — The CRUD action being performed.
    pub fn check_access(
        &self,
        auth: &AuthContext,
        table: &str,
        action: Action,
    ) -> Result<(), GatewayError> {
        if self.policies.is_empty() {
            return Ok(());
        }
        let required_ops = OperationType::required_for_action(action).ok_or_else(|| {
            GatewayError::AccessDenied(format!(
                "Action {:?} is not permitted by policy engine",
                action
            ))
        })?;

        for op in required_ops {
            self.applicable_policies(auth, table, *op)?;
        }

        Ok(())
    }
}

fn command_reads_cte_alias(cmd: &Qail) -> bool {
    matches!(
        cmd.action,
        Action::Get | Action::Cnt | Action::Export | Action::With
    ) && cmd.ctes.iter().any(|cte| cte.name == cmd.table)
}
