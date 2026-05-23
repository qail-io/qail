//! Tenant Boundary Invariant Enforcer
//!
//! Runtime verification that RLS is working correctly. After every query,
//! scans returned rows for a configurable tenant column (default `tenant_id`)
//! mismatches against the authenticated tenant context.
//!
//! This catches RLS bypass bugs in code we haven't written yet.
//!
//! # Design
//!
//! - **Fail-closed**: Violations abort the response (500). Leaked rows
//!   never reach the client.
//! - **Type-safe**: Returns `TenantVerified` token that response builders
//!   require. If you skip the check, your code won't compile.
//! - **Projection-safe**: Tenant-scoped row responses must include the tenant
//!   column. Missing projections are treated as violations.
//! - **Performance**: O(n) scan per response, no allocations beyond the counter.

use qail_core::ast::{
    Action, Cage, CageKind, Condition, Expr, Join, JoinKind, LogicalOp, Operator, Qail, Value,
};
use serde_json::Value as JsonValue;
use std::sync::atomic::{AtomicU64, Ordering};

/// Zero-cost proof that tenant boundary was verified.
///
/// Cannot be constructed outside this module. Response builders
/// require this as a parameter — if you skip `verify_tenant_boundary`,
/// your code won't compile.
#[derive(Debug)]
#[must_use]
pub struct TenantVerified(());

impl TenantVerified {
    /// Create a `TenantVerified` for unauthenticated/system requests
    /// where no tenant scoping applies (e.g., no `tenant_id` in context).
    pub fn unscoped() -> Self {
        Self(())
    }
}

/// Tenant boundary violation — one or more rows had wrong tenant column value.
#[derive(Debug)]
pub struct TenantViolation {
    /// Number of rows with mismatched tenant column
    pub violation_count: u64,
    /// Table where the violation was detected.
    pub table: String,
    /// Endpoint / call site that triggered the check.
    pub endpoint: String,
}

impl std::fmt::Display for TenantViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TENANT_BOUNDARY_VIOLATION: {} rows in table={} endpoint={}",
            self.violation_count, self.table, self.endpoint
        )
    }
}

#[derive(Debug, Clone)]
pub struct TenantProjectionError {
    pub column: String,
    pub reason: Option<String>,
}

impl std::fmt::Display for TenantProjectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(reason) = &self.reason {
            return f.write_str(reason);
        }
        write!(
            f,
            "Projection aliases cannot use reserved tenant guard column '{}'",
            self.column
        )
    }
}

#[derive(Debug, Clone)]
pub struct TenantGuardPlan {
    pub column: String,
    pub verify_rows: bool,
    pub strip_output_column: bool,
}

impl TenantGuardPlan {
    fn row_guard(column: String, strip_output_column: bool) -> Self {
        Self {
            column,
            verify_rows: true,
            strip_output_column,
        }
    }

    fn filter_guard(column: String) -> Self {
        Self {
            column,
            verify_rows: false,
            strip_output_column: false,
        }
    }

    fn merge(self, other: Self) -> Self {
        Self {
            column: self.column,
            verify_rows: self.verify_rows || other.verify_rows,
            strip_output_column: self.strip_output_column || other.strip_output_column,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TenantGuardMode {
    ResponseRows,
    InsertSource,
}

fn merge_tenant_guard_plan(plan: &mut Option<TenantGuardPlan>, next: TenantGuardPlan) {
    *plan = Some(match plan.take() {
        Some(existing) => existing.merge(next),
        None => next,
    });
}

fn normalize_identifier_part(part: &str) -> String {
    part.trim_matches('"').to_ascii_lowercase()
}

fn same_identifier(left: &str, right: &str) -> bool {
    normalize_identifier_part(left) == normalize_identifier_part(right)
}

fn projected_name_matches_tenant(name: &str, tenant_column: &str) -> bool {
    normalize_identifier_part(name)
        .rsplit('.')
        .next()
        .is_some_and(|last| last == normalize_identifier_part(tenant_column))
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

fn projection_qualifier<'a>(name: &'a str, tenant_column: &str) -> Option<&'a str> {
    let trimmed = name.trim().trim_matches('"');
    let (qualifier, column) = trimmed.rsplit_once('.')?;
    same_identifier(column, tenant_column).then_some(qualifier.trim_matches('"'))
}

fn table_ref_qualifiers(cmd: &Qail) -> Vec<String> {
    let (table_name, qualifier) = table_ref_name_and_qualifier(&cmd.table);
    let mut qualifiers = Vec::with_capacity(3);

    if !qualifier.is_empty() {
        qualifiers.push(normalize_identifier_part(&qualifier));
    }
    if !table_name.is_empty() {
        qualifiers.push(normalize_identifier_part(&table_name));
        if let Some(last) = table_name.rsplit('.').next() {
            qualifiers.push(normalize_identifier_part(last));
        }
    }

    qualifiers.sort();
    qualifiers.dedup();
    qualifiers
}

fn projects_base_tenant_column(cmd: &Qail, expr: &Expr, tenant_column: &str) -> bool {
    let Expr::Named(name) = expr else {
        return false;
    };

    let trimmed = name.trim().trim_matches('"');
    if same_identifier(trimmed, tenant_column) {
        return cmd.joins.is_empty();
    }

    let Some(qualifier) = projection_qualifier(trimmed, tenant_column) else {
        return false;
    };
    let normalized = normalize_identifier_part(qualifier);
    table_ref_qualifiers(cmd)
        .iter()
        .any(|allowed| allowed == &normalized)
}

fn has_joined_wildcard_projection(cmd: &Qail) -> bool {
    !cmd.joins.is_empty()
        && (cmd.columns.is_empty() || cmd.columns.iter().any(projects_all_columns))
}

fn action_returns_rows_for_guard(action: Action) -> bool {
    matches!(action, Action::Get | Action::With)
}

fn action_needs_tenant_filter_guard(action: Action) -> bool {
    matches!(
        action,
        Action::Cnt | Action::Export | Action::Set | Action::Del | Action::Over
    )
}

fn action_needs_tenant_payload_guard(action: Action) -> bool {
    matches!(action, Action::Add | Action::Put | Action::Upsert)
}

fn tenant_projection_error(
    column: impl Into<String>,
    reason: impl Into<String>,
) -> TenantProjectionError {
    TenantProjectionError {
        column: column.into(),
        reason: Some(reason.into()),
    }
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

fn tenant_filter_condition(column: String, tenant_id: &str) -> Condition {
    Condition {
        left: Expr::Named(column),
        op: Operator::Eq,
        value: Value::String(tenant_id.to_string()),
        is_array_unnest: false,
    }
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
    matches!(expr, Expr::Named(existing) if same_identifier(existing, name))
}

fn make_positional_payload_condition(index: usize, tenant_id: &str) -> Condition {
    tenant_filter_condition(format!("${}", index + 1), tenant_id)
}

fn inject_tenant_payload(
    cmd: &mut Qail,
    tenant_column: &str,
    tenant_id: &str,
) -> Result<(), TenantProjectionError> {
    let payload_idx = cmd
        .cages
        .iter()
        .position(|cage| matches!(cage.kind, CageKind::Payload));

    let Some(idx) = payload_idx else {
        cmd.cages.push(Cage {
            kind: CageKind::Payload,
            conditions: vec![tenant_filter_condition(
                tenant_column.to_string(),
                tenant_id,
            )],
            logical_op: LogicalOp::And,
        });
        return Ok(());
    };

    if payload_is_positional(&cmd.cages[idx]) {
        if cmd.columns.is_empty() {
            return Err(tenant_projection_error(
                tenant_column,
                format!(
                    "Tenant-scoped {:?} requires explicit columns for positional payloads",
                    cmd.action
                ),
            ));
        }

        if let Some(col_idx) = cmd
            .columns
            .iter()
            .position(|expr| expr_named_eq(expr, tenant_column))
        {
            let placeholder = format!("${}", col_idx + 1);
            let cage = &mut cmd.cages[idx];
            if let Some(cond) = cage
                .conditions
                .iter_mut()
                .find(|cond| expr_named_eq(&cond.left, &placeholder))
            {
                *cond = make_positional_payload_condition(col_idx, tenant_id);
            } else {
                cage.conditions
                    .push(make_positional_payload_condition(col_idx, tenant_id));
            }
            return Ok(());
        }

        cmd.columns.push(Expr::Named(tenant_column.to_string()));
        let col_idx = cmd.columns.len() - 1;
        cmd.cages[idx]
            .conditions
            .push(make_positional_payload_condition(col_idx, tenant_id));
        return Ok(());
    }

    let cage = &mut cmd.cages[idx];
    cage.conditions
        .retain(|cond| !expr_named_eq(&cond.left, tenant_column));
    cage.conditions.push(tenant_filter_condition(
        tenant_column.to_string(),
        tenant_id,
    ));
    Ok(())
}

fn tenant_literal_projection(tenant_id: &str) -> Expr {
    Expr::Literal(Value::String(tenant_id.to_string()))
}

fn ensure_explicit_insert_select_columns(
    cmd: &Qail,
    tenant_column: &str,
) -> Result<(), TenantProjectionError> {
    if cmd.columns.is_empty() {
        return Err(tenant_projection_error(
            tenant_column,
            format!(
                "Tenant-scoped {:?} with source query requires explicit target columns",
                cmd.action
            ),
        ));
    }

    if cmd
        .columns
        .iter()
        .any(|expr| !matches!(expr, Expr::Named(name) if !projects_all_columns(expr) && !name.trim().is_empty()))
    {
        return Err(tenant_projection_error(
            tenant_column,
            format!(
                "Tenant-scoped {:?} with source query requires simple named target columns",
                cmd.action
            ),
        ));
    }

    Ok(())
}

fn ensure_source_projection_can_be_rewritten(
    source_query: &Qail,
    expected_len: usize,
    tenant_column: &str,
) -> Result<(), TenantProjectionError> {
    if source_query.columns.is_empty() || source_query.columns.iter().any(projects_all_columns) {
        return Err(tenant_projection_error(
            tenant_column,
            "Tenant-scoped INSERT ... SELECT requires an explicit source projection",
        ));
    }

    if source_query.columns.len() != expected_len {
        return Err(tenant_projection_error(
            tenant_column,
            format!(
                "Tenant-scoped INSERT ... SELECT target/source column count mismatch: target has {}, source has {}",
                expected_len,
                source_query.columns.len()
            ),
        ));
    }

    for (_, set_query) in &source_query.set_ops {
        ensure_source_projection_can_be_rewritten(set_query, expected_len, tenant_column)?;
    }

    Ok(())
}

fn rewrite_source_projection_tenant_column(
    source_query: &mut Qail,
    tenant_index: usize,
    append_tenant_column: bool,
    tenant_id: &str,
) {
    if append_tenant_column {
        source_query
            .columns
            .push(tenant_literal_projection(tenant_id));
    } else {
        source_query.columns[tenant_index] = tenant_literal_projection(tenant_id);
    }

    for (_, set_query) in &mut source_query.set_ops {
        rewrite_source_projection_tenant_column(
            set_query,
            tenant_index,
            append_tenant_column,
            tenant_id,
        );
    }
}

fn inject_tenant_payload_from_source_query(
    cmd: &mut Qail,
    tenant_column: &str,
    tenant_id: &str,
) -> Result<(), TenantProjectionError> {
    ensure_explicit_insert_select_columns(cmd, tenant_column)?;

    let tenant_column_count = cmd
        .columns
        .iter()
        .filter(|expr| expr_named_eq(expr, tenant_column))
        .count();
    if tenant_column_count > 1 {
        return Err(tenant_projection_error(
            tenant_column,
            format!(
                "Tenant-scoped {:?} cannot use duplicate tenant target columns",
                cmd.action
            ),
        ));
    }

    let append_tenant_column = tenant_column_count == 0;
    let tenant_index = cmd
        .columns
        .iter()
        .position(|expr| expr_named_eq(expr, tenant_column))
        .unwrap_or(cmd.columns.len());
    let expected_source_len = cmd.columns.len();

    let Some(source_query) = cmd.source_query.as_deref() else {
        return Ok(());
    };
    ensure_source_projection_can_be_rewritten(source_query, expected_source_len, tenant_column)?;

    if append_tenant_column {
        cmd.columns.push(Expr::Named(tenant_column.to_string()));
    }

    if let Some(source_query) = cmd.source_query.as_deref_mut() {
        rewrite_source_projection_tenant_column(
            source_query,
            tenant_index,
            append_tenant_column,
            tenant_id,
        );
    }

    Ok(())
}

fn inject_join_tenant_filter(
    cmd: &mut Qail,
    join: &mut Join,
    qualifier: &str,
    tenant_column: &str,
    tenant_id: &str,
) -> Result<(), TenantProjectionError> {
    let condition = tenant_filter_condition(format!("{}.{}", qualifier, tenant_column), tenant_id);
    match join.kind {
        JoinKind::Inner | JoinKind::Left | JoinKind::Lateral => {
            join.on_true = false;
            join.on.get_or_insert_with(Vec::new).push(condition);
            Ok(())
        }
        JoinKind::Cross => {
            inject_filter_condition(cmd, condition);
            Ok(())
        }
        JoinKind::Right | JoinKind::Full => Err(tenant_projection_error(
            tenant_column,
            format!(
                "Tenant guard cannot safely prove joined table '{}' through {:?} joins",
                join.table, join.kind
            ),
        )),
    }
}

pub fn expression_projects_tenant_column(expr: &Expr, tenant_column: &str) -> bool {
    match expr {
        Expr::Named(name) => projected_name_matches_tenant(name, tenant_column),
        Expr::Aliased { alias, .. } => projected_name_matches_tenant(alias, tenant_column),
        Expr::Window { name, .. } => projected_name_matches_tenant(name, tenant_column),
        Expr::Aggregate {
            alias: Some(alias), ..
        }
        | Expr::Cast {
            alias: Some(alias), ..
        }
        | Expr::Case {
            alias: Some(alias), ..
        }
        | Expr::JsonAccess {
            alias: Some(alias), ..
        }
        | Expr::FunctionCall {
            alias: Some(alias), ..
        }
        | Expr::SpecialFunction {
            alias: Some(alias), ..
        }
        | Expr::Binary {
            alias: Some(alias), ..
        }
        | Expr::ArrayConstructor {
            alias: Some(alias), ..
        }
        | Expr::RowConstructor {
            alias: Some(alias), ..
        }
        | Expr::Subscript {
            alias: Some(alias), ..
        }
        | Expr::Collate {
            alias: Some(alias), ..
        }
        | Expr::FieldAccess {
            alias: Some(alias), ..
        }
        | Expr::Subquery {
            alias: Some(alias), ..
        }
        | Expr::Exists {
            alias: Some(alias), ..
        } => projected_name_matches_tenant(alias, tenant_column),
        _ => false,
    }
}

/// Returns true when the result row will expose the real tenant column rather
/// than a user-controlled alias with the same output name.
pub fn has_verifiable_tenant_projection(cmd: &Qail, tenant_column: &str) -> bool {
    !action_returns_rows_for_guard(cmd.action)
        || (cmd.joins.is_empty()
            && (cmd.columns.is_empty() || cmd.columns.iter().any(projects_all_columns)))
        || cmd
            .columns
            .iter()
            .any(|expr| projects_base_tenant_column(cmd, expr, tenant_column))
}

/// Return the configured tenant guard column for a table when row-level
/// verification applies.
pub fn tenant_guard_column_for_table(
    state: &crate::GatewayState,
    table_name: &str,
) -> Option<String> {
    if state
        .config
        .tenant_guard_exempt_tables
        .iter()
        .any(|table| table == table_name)
    {
        return None;
    }

    let table = state.schema.table(table_name)?;
    table
        .columns
        .iter()
        .any(|column| column.name == state.config.tenant_column)
        .then(|| state.config.tenant_column.clone())
}

pub fn prepare_tenant_guarded_query(
    state: &crate::GatewayState,
    auth: &crate::auth::AuthContext,
    cmd: &mut Qail,
) -> Result<Option<TenantGuardPlan>, TenantProjectionError> {
    prepare_tenant_guarded_query_inner(state, auth, cmd, TenantGuardMode::ResponseRows)
}

fn prepare_tenant_guarded_query_inner(
    state: &crate::GatewayState,
    auth: &crate::auth::AuthContext,
    cmd: &mut Qail,
    mode: TenantGuardMode,
) -> Result<Option<TenantGuardPlan>, TenantProjectionError> {
    let Some(tenant_id) = auth.tenant_id.as_deref() else {
        return Ok(None);
    };

    let mut cte_plans: Vec<(String, TenantGuardPlan)> = Vec::new();
    for cte in &mut cmd.ctes {
        let base_plan = prepare_tenant_guarded_query_inner(state, auth, &mut cte.base_query, mode)?;
        let recursive_plan = if let Some(ref mut recursive_query) = cte.recursive_query {
            prepare_tenant_guarded_query_inner(state, auth, recursive_query, mode)?
        } else {
            None
        };

        let cte_plan = match (base_plan, recursive_plan) {
            (Some(left), Some(right)) => Some(left.merge(right)),
            (Some(plan), None) | (None, Some(plan)) => Some(plan),
            (None, None) => None,
        };

        if let Some(plan) = cte_plan {
            if plan.verify_rows
                && !cte.columns.is_empty()
                && !cte
                    .columns
                    .iter()
                    .any(|col| same_identifier(col, &plan.column))
            {
                cte.columns.push(plan.column.clone());
            }
            cte_plans.push((cte.name.clone(), plan));
        }
    }

    let mut plan = None;

    if let Some(ref mut source_query) = cmd.source_query {
        let source_plan = prepare_tenant_guarded_query_inner(
            state,
            auth,
            source_query,
            TenantGuardMode::InsertSource,
        )?;
        if let Some(source_plan) = source_plan {
            merge_tenant_guard_plan(&mut plan, source_plan);
        }
    }

    let tenant_guard_column = auth.tenant_id.as_ref().and_then(|_| {
        tenant_guard_column_for_table(state, &table_ref_name_and_qualifier(&cmd.table).0)
    });

    if let Some(ref tenant_column) = tenant_guard_column {
        if mode == TenantGuardMode::InsertSource && action_returns_rows_for_guard(cmd.action) {
            inject_tenant_filter(cmd, tenant_column, tenant_id);
            merge_tenant_guard_plan(
                &mut plan,
                TenantGuardPlan::filter_guard(tenant_column.clone()),
            );
        } else if action_returns_rows_for_guard(cmd.action) {
            let strip_output_column = ensure_tenant_column_projected(cmd, tenant_column)?;
            merge_tenant_guard_plan(
                &mut plan,
                TenantGuardPlan::row_guard(tenant_column.clone(), strip_output_column),
            );
        } else if action_needs_tenant_filter_guard(cmd.action) {
            inject_tenant_filter(cmd, tenant_column, tenant_id);
            merge_tenant_guard_plan(
                &mut plan,
                TenantGuardPlan::filter_guard(tenant_column.clone()),
            );
        } else if action_needs_tenant_payload_guard(cmd.action) {
            if cmd.source_query.is_some() {
                inject_tenant_payload_from_source_query(cmd, tenant_column, tenant_id)?;
            } else {
                inject_tenant_payload(cmd, tenant_column, tenant_id)?;
            }
            merge_tenant_guard_plan(
                &mut plan,
                TenantGuardPlan::filter_guard(tenant_column.clone()),
            );
        }
    }

    if plan.is_none()
        && let Some((_, cte_plan)) = cte_plans.iter().find(|(name, _)| name == &cmd.table)
    {
        if mode == TenantGuardMode::InsertSource
            && action_returns_rows_for_guard(cmd.action)
            && cte_plan.verify_rows
        {
            inject_tenant_filter(cmd, &cte_plan.column, tenant_id);
            plan = Some(TenantGuardPlan::filter_guard(cte_plan.column.clone()));
        } else if mode == TenantGuardMode::InsertSource && action_returns_rows_for_guard(cmd.action)
        {
            plan = Some(cte_plan.clone());
        } else if action_returns_rows_for_guard(cmd.action) {
            let strip_output_column = ensure_tenant_column_projected(cmd, &cte_plan.column)?
                || cte_plan.strip_output_column;
            plan = Some(TenantGuardPlan::row_guard(
                cte_plan.column.clone(),
                strip_output_column,
            ));
        } else if action_needs_tenant_filter_guard(cmd.action) {
            inject_tenant_filter(cmd, &cte_plan.column, tenant_id);
            plan = Some(TenantGuardPlan::filter_guard(cte_plan.column.clone()));
        } else if action_needs_tenant_payload_guard(cmd.action) {
            inject_tenant_payload(cmd, &cte_plan.column, tenant_id)?;
            plan = Some(TenantGuardPlan::filter_guard(cte_plan.column.clone()));
        }
    }

    let cte_names: Vec<String> = cte_plans.iter().map(|(name, _)| name.clone()).collect();
    let mut rewritten_joins = Vec::with_capacity(cmd.joins.len());
    for mut join in std::mem::take(&mut cmd.joins) {
        let (join_table, qualifier) = table_ref_name_and_qualifier(&join.table);
        if !join_table.is_empty()
            && !cte_names
                .iter()
                .any(|name| same_identifier(name, &join_table))
            && let Some(join_tenant_column) = tenant_guard_column_for_table(state, &join_table)
        {
            inject_join_tenant_filter(cmd, &mut join, &qualifier, &join_tenant_column, tenant_id)?;
            let join_plan = TenantGuardPlan::filter_guard(join_tenant_column);
            plan = Some(match plan {
                Some(existing) => existing.merge(join_plan),
                None => join_plan,
            });
        }
        rewritten_joins.push(join);
    }
    cmd.joins = rewritten_joins;

    for (_, set_query) in &mut cmd.set_ops {
        let set_plan = prepare_tenant_guarded_query_inner(state, auth, set_query, mode)?;
        if let Some(set_plan) = set_plan {
            merge_tenant_guard_plan(&mut plan, set_plan);
        }
    }

    Ok(plan)
}

pub fn ensure_verifiable_tenant_projection(
    cmd: &Qail,
    tenant_column: &str,
) -> Result<(), TenantProjectionError> {
    if !has_verifiable_tenant_projection(cmd, tenant_column) {
        return Err(TenantProjectionError {
            column: tenant_column.to_string(),
            reason: None,
        });
    }

    for cte in &cmd.ctes {
        ensure_verifiable_tenant_projection(&cte.base_query, tenant_column)?;
        if let Some(ref recursive_query) = cte.recursive_query {
            ensure_verifiable_tenant_projection(recursive_query, tenant_column)?;
        }
    }
    for (_, set_query) in &cmd.set_ops {
        ensure_verifiable_tenant_projection(set_query, tenant_column)?;
    }
    if let Some(ref source_query) = cmd.source_query {
        ensure_verifiable_tenant_projection(source_query, tenant_column)?;
    }

    Ok(())
}

/// Ensure an explicitly projected SELECT still carries the tenant column needed
/// by `verify_tenant_boundary`.
///
/// Empty projections and `*` already include the column. For joined queries we
/// project the base table column to avoid ambiguity.
pub fn ensure_tenant_column_projected(
    cmd: &mut Qail,
    tenant_column: &str,
) -> Result<bool, TenantProjectionError> {
    if !action_returns_rows_for_guard(cmd.action) {
        return Ok(false);
    }

    if cmd.columns.iter().any(|expr| {
        expression_projects_tenant_column(expr, tenant_column)
            && !projects_base_tenant_column(cmd, expr, tenant_column)
    }) {
        return Err(TenantProjectionError {
            column: tenant_column.to_string(),
            reason: None,
        });
    }

    if has_joined_wildcard_projection(cmd) {
        return Err(tenant_projection_error(
            tenant_column,
            format!(
                "Tenant-scoped joined {:?} requires explicit base-table projections",
                cmd.action
            ),
        ));
    }

    if has_verifiable_tenant_projection(cmd, tenant_column) {
        return Ok(false);
    }

    let column = if cmd.joins.is_empty() {
        tenant_column.to_string()
    } else {
        format!(
            "{}.{}",
            table_ref_name_and_qualifier(&cmd.table).1,
            tenant_column
        )
    };
    cmd.columns.push(Expr::Named(column));
    Ok(true)
}

fn inject_filter_condition(cmd: &mut Qail, condition: Condition) {
    if let Some(cage) = cmd
        .cages
        .iter_mut()
        .find(|cage| matches!(cage.kind, CageKind::Filter) && cage.logical_op == LogicalOp::And)
    {
        cage.conditions.push(condition);
    } else {
        cmd.cages.push(Cage {
            kind: CageKind::Filter,
            conditions: vec![condition],
            logical_op: LogicalOp::And,
        });
    }
}

fn inject_tenant_filter(cmd: &mut Qail, tenant_column: &str, tenant_id: &str) {
    let filter_column = if cmd.joins.is_empty() {
        tenant_column.to_string()
    } else {
        format!(
            "{}.{}",
            table_ref_name_and_qualifier(&cmd.table).1,
            tenant_column
        )
    };
    inject_filter_condition(cmd, tenant_filter_condition(filter_column, tenant_id));
}

pub fn strip_tenant_column_from_json_rows(rows: &mut [JsonValue], tenant_column: &str) {
    for row in rows {
        if let JsonValue::Object(obj) = row {
            obj.remove(tenant_column);
        }
    }
}

pub fn tenant_column_index(row: &qail_pg::PgRow, tenant_column: &str) -> Option<usize> {
    let info = row.column_info.as_ref()?;
    info.name_to_index.get(tenant_column).copied().or_else(|| {
        info.name_to_index
            .iter()
            .find_map(|(name, idx)| same_identifier(name, tenant_column).then_some(*idx))
    })
}

/// Global counters for tenant boundary violations.
///
/// These are designed for external monitoring (Prometheus, health endpoint).
pub struct TenantGuardMetrics {
    /// Total rows checked across all requests
    pub rows_checked: AtomicU64,
    /// Total requests where at least one violation was found
    pub violation_requests: AtomicU64,
    /// Total individual row violations (rows with wrong tenant column value)
    pub violation_rows: AtomicU64,
}

impl TenantGuardMetrics {
    /// Create a new, zeroed metrics instance.
    pub const fn new() -> Self {
        Self {
            rows_checked: AtomicU64::new(0),
            violation_requests: AtomicU64::new(0),
            violation_rows: AtomicU64::new(0),
        }
    }
}

impl Default for TenantGuardMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Global singleton for metrics collection.
pub static TENANT_GUARD: TenantGuardMetrics = TenantGuardMetrics::new();

/// Verify that all rows in a query response belong to the expected tenant.
///
/// Returns `Ok(TenantVerified)` if all rows pass, or `Err(TenantViolation)`
/// if any row has a mismatched tenant column value. On violation, the caller
/// MUST abort the response — never send leaked rows to the client.
///
/// # Arguments
///
/// * `rows` - The JSON rows from the query response
/// * `expected_tenant_id` - The authenticated tenant's ID
/// * `tenant_column` - Column name to check (e.g. "tenant_id")
/// * `table` - Table name for logging context
/// * `endpoint` - Endpoint name for logging context
pub fn verify_tenant_boundary(
    rows: &[JsonValue],
    expected_tenant_id: &str,
    tenant_column: &str,
    table: &str,
    endpoint: &str,
) -> Result<TenantVerified, TenantViolation> {
    if rows.is_empty() || expected_tenant_id.is_empty() {
        return Ok(TenantVerified(()));
    }

    let mut violations = 0u64;
    let mut checked = 0u64;

    for (i, row) in rows.iter().enumerate() {
        let Some(obj) = row.as_object() else {
            checked += 1;
            violations += 1;
            tracing::error!(
                table = table,
                endpoint = endpoint,
                row = i,
                column = tenant_column,
                expected = expected_tenant_id,
                "TENANT_BOUNDARY_VIOLATION — row is not an object"
            );
            continue;
        };

        let Some(op_val) = obj.get(tenant_column) else {
            checked += 1;
            violations += 1;
            tracing::error!(
                table = table,
                endpoint = endpoint,
                row = i,
                column = tenant_column,
                expected = expected_tenant_id,
                "TENANT_BOUNDARY_VIOLATION — tenant column missing from projection"
            );
            continue;
        };

        checked += 1;

        let row_tenant_id = match op_val {
            JsonValue::String(s) => s.as_str(),
            JsonValue::Number(n) => {
                let n_str = n.to_string();
                if n_str != expected_tenant_id {
                    violations += 1;
                    tracing::error!(
                        table = table,
                        endpoint = endpoint,
                        row = i,
                        column = tenant_column,
                        expected = expected_tenant_id,
                        actual = %n_str,
                        "TENANT_BOUNDARY_VIOLATION — RLS MAY BE COMPROMISED"
                    );
                }
                continue;
            }
            JsonValue::Null => continue, // NULL tenant column — skip (system rows)
            other => {
                violations += 1;
                tracing::error!(
                    table = table,
                    endpoint = endpoint,
                    row = i,
                    column = tenant_column,
                    expected = expected_tenant_id,
                    actual = %other,
                    "TENANT_BOUNDARY_VIOLATION — tenant column has unsupported type"
                );
                continue;
            }
        };

        if row_tenant_id != expected_tenant_id {
            violations += 1;
            tracing::error!(
                table = table,
                endpoint = endpoint,
                row = i,
                column = tenant_column,
                expected = expected_tenant_id,
                actual = row_tenant_id,
                "TENANT_BOUNDARY_VIOLATION — RLS MAY BE COMPROMISED"
            );
        }
    }

    // Update global counters
    TENANT_GUARD
        .rows_checked
        .fetch_add(checked, Ordering::Relaxed);
    if violations > 0 {
        TENANT_GUARD
            .violation_requests
            .fetch_add(1, Ordering::Relaxed);
        TENANT_GUARD
            .violation_rows
            .fetch_add(violations, Ordering::Relaxed);
        Err(TenantViolation {
            violation_count: violations,
            table: table.to_string(),
            endpoint: endpoint.to_string(),
        })
    } else {
        Ok(TenantVerified(()))
    }
}

/// Get current tenant guard metrics as a JSON-serializable snapshot.
pub fn metrics_snapshot() -> TenantGuardSnapshot {
    TenantGuardSnapshot {
        rows_checked: TENANT_GUARD.rows_checked.load(Ordering::Relaxed),
        violation_requests: TENANT_GUARD.violation_requests.load(Ordering::Relaxed),
        violation_rows: TENANT_GUARD.violation_rows.load(Ordering::Relaxed),
    }
}

/// Serializable snapshot of tenant guard metrics.
#[derive(Debug, serde::Serialize)]
pub struct TenantGuardSnapshot {
    /// Total rows checked across all requests.
    pub rows_checked: u64,
    /// Total requests where at least one violation was found.
    pub violation_requests: u64,
    /// Total individual row violations.
    pub violation_rows: u64,
}

#[cfg(test)]
mod tests;
