use super::*;
use qail_core::ast::{Condition, Expr, MergeAction, MergeSource, Value};

fn for_each_value_subquery(value: &Value, visit: &mut impl FnMut(&qail_core::ast::Qail)) {
    match value {
        Value::Array(values) => {
            for value in values {
                for_each_value_subquery(value, visit);
            }
        }
        Value::Subquery(query) => visit(query),
        Value::Expr(expr) => for_each_expr_subquery(expr, visit),
        _ => {}
    }
}

fn for_each_condition_subquery(
    condition: &Condition,
    visit: &mut impl FnMut(&qail_core::ast::Qail),
) {
    for_each_expr_subquery(&condition.left, visit);
    for_each_value_subquery(&condition.value, visit);
}

fn for_each_expr_subquery(expr: &Expr, visit: &mut impl FnMut(&qail_core::ast::Qail)) {
    match expr {
        Expr::Aggregate {
            filter: Some(filter),
            ..
        } => {
            for condition in filter {
                for_each_condition_subquery(condition, visit);
            }
        }
        Expr::Cast { expr, .. } | Expr::Mod { col: expr, .. } | Expr::Collate { expr, .. } => {
            for_each_expr_subquery(expr, visit);
        }
        Expr::Window { params, order, .. } => {
            for expr in params {
                for_each_expr_subquery(expr, visit);
            }
            for cage in order {
                for condition in &cage.conditions {
                    for_each_condition_subquery(condition, visit);
                }
            }
        }
        Expr::Case {
            when_clauses,
            else_value,
            ..
        } => {
            for (condition, then_expr) in when_clauses {
                for_each_condition_subquery(condition, visit);
                for_each_expr_subquery(then_expr, visit);
            }
            if let Some(expr) = else_value {
                for_each_expr_subquery(expr, visit);
            }
        }
        Expr::FunctionCall { args, .. } => {
            for expr in args {
                for_each_expr_subquery(expr, visit);
            }
        }
        Expr::SpecialFunction { args, .. } => {
            for (_, expr) in args {
                for_each_expr_subquery(expr, visit);
            }
        }
        Expr::Binary { left, right, .. } => {
            for_each_expr_subquery(left, visit);
            for_each_expr_subquery(right, visit);
        }
        Expr::Literal(value) => for_each_value_subquery(value, visit),
        Expr::ArrayConstructor { elements, .. } | Expr::RowConstructor { elements, .. } => {
            for expr in elements {
                for_each_expr_subquery(expr, visit);
            }
        }
        Expr::Subscript { expr, index, .. } => {
            for_each_expr_subquery(expr, visit);
            for_each_expr_subquery(index, visit);
        }
        Expr::FieldAccess { expr, .. } => for_each_expr_subquery(expr, visit),
        Expr::Subquery { query, .. } | Expr::Exists { query, .. } => visit(query),
        Expr::Star
        | Expr::Named(_)
        | Expr::Aliased { .. }
        | Expr::Aggregate { filter: None, .. }
        | Expr::Def { .. }
        | Expr::JsonAccess { .. } => {}
    }
}

fn expression_subquery_complexity(expr: &Expr) -> (usize, usize, usize) {
    let mut complexity = (0, 0, 0);
    for_each_expr_subquery(expr, &mut |query| {
        let child = query_complexity(query);
        complexity.0 += 1 + child.0;
        complexity.1 += child.1;
        complexity.2 += child.2;
    });
    complexity
}

fn condition_subquery_complexity(condition: &Condition) -> (usize, usize, usize) {
    let mut complexity = expression_subquery_complexity(&condition.left);
    for_each_value_subquery(&condition.value, &mut |query| {
        let child = query_complexity(query);
        complexity.0 += 1 + child.0;
        complexity.1 += child.1;
        complexity.2 += child.2;
    });
    complexity
}

fn add_complexity(total: &mut (usize, usize, usize), child: (usize, usize, usize)) {
    total.0 += child.0;
    total.1 += child.1;
    total.2 += child.2;
}

fn validate_expr_subqueries<F>(expr: &Expr, validate: &mut F) -> Result<(), ApiError>
where
    F: FnMut(&qail_core::ast::Qail) -> Result<(), ApiError>,
{
    let mut result = Ok(());
    for_each_expr_subquery(expr, &mut |query| {
        if result.is_ok() {
            result = validate(query);
        }
    });
    result
}

fn validate_condition_subqueries<F>(condition: &Condition, validate: &mut F) -> Result<(), ApiError>
where
    F: FnMut(&qail_core::ast::Qail) -> Result<(), ApiError>,
{
    validate_expr_subqueries(&condition.left, validate)?;

    let mut result = Ok(());
    for_each_value_subquery(&condition.value, &mut |query| {
        if result.is_ok() {
            result = validate(query);
        }
    });
    result
}

fn validate_embedded_subqueries<F>(
    cmd: &qail_core::ast::Qail,
    validate: &mut F,
) -> Result<(), ApiError>
where
    F: FnMut(&qail_core::ast::Qail) -> Result<(), ApiError>,
{
    for expr in &cmd.columns {
        validate_expr_subqueries(expr, validate)?;
    }
    for expr in &cmd.distinct_on {
        validate_expr_subqueries(expr, validate)?;
    }
    if let Some(returning) = &cmd.returning {
        for expr in returning {
            validate_expr_subqueries(expr, validate)?;
        }
    }
    for cage in &cmd.cages {
        for condition in &cage.conditions {
            validate_condition_subqueries(condition, validate)?;
        }
    }
    for condition in &cmd.having {
        validate_condition_subqueries(condition, validate)?;
    }
    for join in &cmd.joins {
        if let Some(conditions) = &join.on {
            for condition in conditions {
                validate_condition_subqueries(condition, validate)?;
            }
        }
    }
    if let Some(on_conflict) = &cmd.on_conflict
        && let qail_core::ast::ConflictAction::DoUpdate { assignments } = &on_conflict.action
    {
        for (_, expr) in assignments {
            validate_expr_subqueries(expr, validate)?;
        }
    }
    if let Some(merge) = &cmd.merge {
        if let MergeSource::Query { query, .. } = &merge.source {
            validate(query)?;
        }
        for condition in &merge.on {
            validate_condition_subqueries(condition, validate)?;
        }
        for clause in &merge.clauses {
            for condition in &clause.condition {
                validate_condition_subqueries(condition, validate)?;
            }
            match &clause.action {
                MergeAction::Update { assignments } => {
                    for (_, expr) in assignments {
                        validate_expr_subqueries(expr, validate)?;
                    }
                }
                MergeAction::Insert { values, .. } => {
                    for expr in values {
                        validate_expr_subqueries(expr, validate)?;
                    }
                }
                MergeAction::Delete | MergeAction::DoNothing => {}
            }
        }
    }

    Ok(())
}

fn value_is_read_only(value: &Value) -> bool {
    match value {
        Value::Array(values) => values.iter().all(value_is_read_only),
        Value::Subquery(query) => qail_command_is_read_only(query),
        Value::Expr(expr) => expr_is_read_only(expr),
        _ => true,
    }
}

fn condition_is_read_only(condition: &Condition) -> bool {
    expr_is_read_only(&condition.left) && value_is_read_only(&condition.value)
}

fn expr_is_read_only(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate {
            filter: Some(filter),
            ..
        } => filter.iter().all(condition_is_read_only),
        Expr::Cast { expr, .. } | Expr::Mod { col: expr, .. } | Expr::Collate { expr, .. } => {
            expr_is_read_only(expr)
        }
        Expr::Window { params, order, .. } => {
            params.iter().all(expr_is_read_only)
                && order
                    .iter()
                    .flat_map(|cage| &cage.conditions)
                    .all(condition_is_read_only)
        }
        Expr::Case {
            when_clauses,
            else_value,
            ..
        } => {
            when_clauses.iter().all(|(condition, then_expr)| {
                condition_is_read_only(condition) && expr_is_read_only(then_expr)
            }) && else_value.as_deref().is_none_or(expr_is_read_only)
        }
        Expr::FunctionCall { args, .. } => args.iter().all(expr_is_read_only),
        Expr::SpecialFunction { args, .. } => args.iter().all(|(_, expr)| expr_is_read_only(expr)),
        Expr::Binary { left, right, .. } => expr_is_read_only(left) && expr_is_read_only(right),
        Expr::Literal(value) => value_is_read_only(value),
        Expr::ArrayConstructor { elements, .. } | Expr::RowConstructor { elements, .. } => {
            elements.iter().all(expr_is_read_only)
        }
        Expr::Subscript { expr, index, .. } => expr_is_read_only(expr) && expr_is_read_only(index),
        Expr::FieldAccess { expr, .. } => expr_is_read_only(expr),
        Expr::Subquery { query, .. } | Expr::Exists { query, .. } => {
            qail_command_is_read_only(query)
        }
        Expr::Star
        | Expr::Named(_)
        | Expr::Aliased { .. }
        | Expr::Def { .. }
        | Expr::JsonAccess { .. }
        | Expr::Aggregate { filter: None, .. } => true,
    }
}

pub(crate) fn qail_command_is_read_only(cmd: &qail_core::ast::Qail) -> bool {
    let action_is_read_only = matches!(
        cmd.action,
        Action::Get | Action::Cnt | Action::JsonTable | Action::With | Action::Export
    );
    action_is_read_only
        && cmd.columns.iter().all(expr_is_read_only)
        && cmd.distinct_on.iter().all(expr_is_read_only)
        && cmd
            .returning
            .as_ref()
            .is_none_or(|returning| returning.iter().all(expr_is_read_only))
        && cmd
            .cages
            .iter()
            .flat_map(|cage| &cage.conditions)
            .all(condition_is_read_only)
        && cmd.having.iter().all(condition_is_read_only)
        && cmd
            .joins
            .iter()
            .filter_map(|join| join.on.as_ref())
            .flatten()
            .all(condition_is_read_only)
        && cmd
            .on_conflict
            .as_ref()
            .is_none_or(|on_conflict| match &on_conflict.action {
                qail_core::ast::ConflictAction::DoNothing => true,
                qail_core::ast::ConflictAction::DoUpdate { assignments } => {
                    assignments.iter().all(|(_, expr)| expr_is_read_only(expr))
                }
            })
        && cmd.merge.as_ref().is_none_or(|merge| {
            let source_is_read_only = match &merge.source {
                MergeSource::Table { .. } => true,
                MergeSource::Query { query, .. } => qail_command_is_read_only(query),
            };
            source_is_read_only
                && merge.on.iter().all(condition_is_read_only)
                && merge.clauses.iter().all(|clause| {
                    clause.condition.iter().all(condition_is_read_only)
                        && match &clause.action {
                            MergeAction::Update { assignments } => {
                                assignments.iter().all(|(_, expr)| expr_is_read_only(expr))
                            }
                            MergeAction::Insert { values, .. } => {
                                values.iter().all(expr_is_read_only)
                            }
                            MergeAction::Delete | MergeAction::DoNothing => true,
                        }
                })
        })
        && cmd.ctes.iter().all(|cte| {
            qail_command_is_read_only(&cte.base_query)
                && cte
                    .recursive_query
                    .as_deref()
                    .is_none_or(qail_command_is_read_only)
        })
        && cmd
            .source_query
            .as_deref()
            .is_none_or(qail_command_is_read_only)
        && cmd
            .set_ops
            .iter()
            .all(|(_, set_query)| qail_command_is_read_only(set_query))
}

pub(crate) fn reject_dangerous_action(cmd: &qail_core::ast::Qail) -> Result<(), ApiError> {
    if !public_query_action_allowed(cmd.action) {
        return Err(ApiError::with_code(
            "ACTION_DENIED",
            format!(
                "Action {:?} is not allowed on public query endpoints",
                cmd.action
            ),
        ));
    }

    for cte in &cmd.ctes {
        reject_dangerous_action(&cte.base_query)?;
        if let Some(ref recursive_query) = cte.recursive_query {
            reject_dangerous_action(recursive_query)?;
        }
    }
    for (_, set_query) in &cmd.set_ops {
        reject_dangerous_action(set_query)?;
    }
    if let Some(ref source_query) = cmd.source_query {
        reject_dangerous_action(source_query)?;
    }
    validate_embedded_subqueries(cmd, &mut |query| reject_dangerous_action(query))?;

    Ok(())
}

pub(crate) fn reject_non_read_action(
    cmd: &qail_core::ast::Qail,
    surface: &str,
) -> Result<(), ApiError> {
    if !read_query_action_allowed(cmd.action) {
        return Err(ApiError::with_code(
            "ACTION_DENIED",
            format!("Action {:?} is not allowed on {}", cmd.action, surface),
        ));
    }

    for cte in &cmd.ctes {
        reject_non_read_action(&cte.base_query, surface)?;
        if let Some(ref recursive_query) = cte.recursive_query {
            reject_non_read_action(recursive_query, surface)?;
        }
    }
    for (_, set_query) in &cmd.set_ops {
        reject_non_read_action(set_query, surface)?;
    }
    if let Some(ref source_query) = cmd.source_query {
        reject_non_read_action(source_query, surface)?;
    }
    validate_embedded_subqueries(cmd, &mut |query| reject_non_read_action(query, surface))?;

    Ok(())
}

fn public_query_action_allowed(action: Action) -> bool {
    matches!(
        action,
        Action::Get
            | Action::Cnt
            | Action::Set
            | Action::Del
            | Action::Add
            | Action::Over
            | Action::Put
            | Action::JsonTable
            | Action::Export
            | Action::With
            | Action::Search
            | Action::Scroll
            | Action::Upsert
            | Action::Merge
            | Action::CreateCollection
            | Action::DeleteCollection
    )
}

fn read_query_action_allowed(action: Action) -> bool {
    matches!(
        action,
        Action::Get
            | Action::Cnt
            | Action::JsonTable
            | Action::With
            | Action::Search
            | Action::Scroll
    )
}

pub(crate) fn query_complexity(cmd: &qail_core::ast::Qail) -> (usize, usize, usize) {
    use qail_core::ast::CageKind;

    let mut depth = cmd.ctes.len() + cmd.set_ops.len() + usize::from(cmd.source_query.is_some());
    let mut filter_count: usize = cmd
        .cages
        .iter()
        .filter(|c| matches!(c.kind, CageKind::Filter))
        .map(|c| c.conditions.len())
        .sum();

    let mut join_count = cmd.joins.len();
    let mut nested_complexity = (0, 0, 0);

    for expr in &cmd.columns {
        add_complexity(&mut nested_complexity, expression_subquery_complexity(expr));
    }
    for expr in &cmd.distinct_on {
        add_complexity(&mut nested_complexity, expression_subquery_complexity(expr));
    }
    if let Some(returning) = &cmd.returning {
        for expr in returning {
            add_complexity(&mut nested_complexity, expression_subquery_complexity(expr));
        }
    }
    for cage in &cmd.cages {
        for condition in &cage.conditions {
            add_complexity(
                &mut nested_complexity,
                condition_subquery_complexity(condition),
            );
        }
    }
    for condition in &cmd.having {
        add_complexity(
            &mut nested_complexity,
            condition_subquery_complexity(condition),
        );
    }
    for join in &cmd.joins {
        if let Some(conditions) = &join.on {
            for condition in conditions {
                add_complexity(
                    &mut nested_complexity,
                    condition_subquery_complexity(condition),
                );
            }
        }
    }
    if let Some(on_conflict) = &cmd.on_conflict
        && let qail_core::ast::ConflictAction::DoUpdate { assignments } = &on_conflict.action
    {
        for (_, expr) in assignments {
            add_complexity(&mut nested_complexity, expression_subquery_complexity(expr));
        }
    }
    if let Some(merge) = &cmd.merge {
        if let MergeSource::Query { query, .. } = &merge.source {
            let child = query_complexity(query);
            add_complexity(&mut nested_complexity, (1 + child.0, child.1, child.2));
        }
        for condition in &merge.on {
            add_complexity(
                &mut nested_complexity,
                condition_subquery_complexity(condition),
            );
        }
        for clause in &merge.clauses {
            for condition in &clause.condition {
                add_complexity(
                    &mut nested_complexity,
                    condition_subquery_complexity(condition),
                );
            }
            match &clause.action {
                MergeAction::Update { assignments } => {
                    for (_, expr) in assignments {
                        add_complexity(
                            &mut nested_complexity,
                            expression_subquery_complexity(expr),
                        );
                    }
                }
                MergeAction::Insert { values, .. } => {
                    for expr in values {
                        add_complexity(
                            &mut nested_complexity,
                            expression_subquery_complexity(expr),
                        );
                    }
                }
                MergeAction::Delete | MergeAction::DoNothing => {}
            }
        }
    }

    for cte in &cmd.ctes {
        let (child_depth, child_filters, child_joins) = query_complexity(&cte.base_query);
        depth += child_depth;
        filter_count += child_filters;
        join_count += child_joins;

        if let Some(ref recursive_query) = cte.recursive_query {
            let (child_depth, child_filters, child_joins) = query_complexity(recursive_query);
            depth += child_depth;
            filter_count += child_filters;
            join_count += child_joins;
        }
    }

    if let Some(ref source_query) = cmd.source_query {
        let (child_depth, child_filters, child_joins) = query_complexity(source_query);
        depth += child_depth;
        filter_count += child_filters;
        join_count += child_joins;
    }

    for (_, set_query) in &cmd.set_ops {
        let (child_depth, child_filters, child_joins) = query_complexity(set_query);
        depth += child_depth;
        filter_count += child_filters;
        join_count += child_joins;
    }

    depth += nested_complexity.0;
    filter_count += nested_complexity.1;
    join_count += nested_complexity.2;

    (depth, filter_count, join_count)
}

pub(crate) fn qail_table_name(table_ref: &str) -> String {
    table_ref
        .split_whitespace()
        .next()
        .unwrap_or(table_ref)
        .trim_matches('"')
        .to_string()
}

pub(crate) fn cache_tables_for_qail(cmd: &qail_core::ast::Qail) -> Vec<String> {
    fn push_table(tables: &mut Vec<String>, table_ref: &str) {
        let table = qail_table_name(table_ref);
        if !table.is_empty() && !tables.iter().any(|existing| existing == &table) {
            tables.push(table);
        }
    }

    fn collect(cmd: &qail_core::ast::Qail, tables: &mut Vec<String>) {
        let cte_names: Vec<&str> = cmd.ctes.iter().map(|cte| cte.name.as_str()).collect();
        let base_table = qail_table_name(&cmd.table);
        if !cte_names.iter().any(|name| *name == base_table) {
            push_table(tables, &cmd.table);
        }

        for cte in &cmd.ctes {
            collect(&cte.base_query, tables);
            if let Some(ref recursive_query) = cte.recursive_query {
                collect(recursive_query, tables);
            }
        }

        if let Some(ref source_query) = cmd.source_query {
            collect(source_query, tables);
        }

        for (_, set_query) in &cmd.set_ops {
            collect(set_query, tables);
        }

        for join in &cmd.joins {
            let join_table = qail_table_name(&join.table);
            if !cte_names.iter().any(|name| *name == join_table) {
                push_table(tables, &join.table);
            }
            if let Some(conditions) = &join.on {
                for condition in conditions {
                    for_each_condition_subquery(condition, &mut |query| collect(query, tables));
                }
            }
        }

        for expr in &cmd.columns {
            for_each_expr_subquery(expr, &mut |query| collect(query, tables));
        }
        for expr in &cmd.distinct_on {
            for_each_expr_subquery(expr, &mut |query| collect(query, tables));
        }
        if let Some(returning) = &cmd.returning {
            for expr in returning {
                for_each_expr_subquery(expr, &mut |query| collect(query, tables));
            }
        }
        for cage in &cmd.cages {
            for condition in &cage.conditions {
                for_each_condition_subquery(condition, &mut |query| collect(query, tables));
            }
        }
        for condition in &cmd.having {
            for_each_condition_subquery(condition, &mut |query| collect(query, tables));
        }
        if let Some(on_conflict) = &cmd.on_conflict
            && let qail_core::ast::ConflictAction::DoUpdate { assignments } = &on_conflict.action
        {
            for (_, expr) in assignments {
                for_each_expr_subquery(expr, &mut |query| collect(query, tables));
            }
        }
        if let Some(merge) = &cmd.merge {
            if let MergeSource::Query { query, .. } = &merge.source {
                collect(query, tables);
            }
            for condition in &merge.on {
                for_each_condition_subquery(condition, &mut |query| collect(query, tables));
            }
            for clause in &merge.clauses {
                for condition in &clause.condition {
                    for_each_condition_subquery(condition, &mut |query| collect(query, tables));
                }
                match &clause.action {
                    MergeAction::Update { assignments } => {
                        for (_, expr) in assignments {
                            for_each_expr_subquery(expr, &mut |query| collect(query, tables));
                        }
                    }
                    MergeAction::Insert { values, .. } => {
                        for expr in values {
                            for_each_expr_subquery(expr, &mut |query| collect(query, tables));
                        }
                    }
                    MergeAction::Delete | MergeAction::DoNothing => {}
                }
            }
        }
    }

    let mut tables = Vec::new();
    collect(cmd, &mut tables);
    tables
}

/// Common query execution logic
pub(super) fn exact_cache_key(cmd: &qail_core::ast::Qail) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let payload = qail_core::wire::encode_cmd_text(cmd);
    let mut hasher = DefaultHasher::new();
    payload.hash(&mut hasher);
    format!("full:{:016x}", hasher.finish())
}

pub(super) fn auth_scoped_cache_key(
    auth: &crate::auth::AuthContext,
    cmd: &qail_core::ast::Qail,
) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let tenant = auth.tenant_id.as_deref().unwrap_or("_anon");
    let mut hasher = DefaultHasher::new();
    tenant.hash(&mut hasher);
    auth.user_id.hash(&mut hasher);
    auth.role.hash(&mut hasher);
    auth.is_authenticated().hash(&mut hasher);
    auth.is_denied().hash(&mut hasher);
    auth.is_platform_admin().hash(&mut hasher);

    let mut claims: Vec<_> = auth.claims.iter().collect();
    claims.sort_by_key(|(left, _)| *left);
    for (key, value) in claims {
        key.hash(&mut hasher);
        crate::auth::canonical_json_value(value).hash(&mut hasher);
    }

    format!(
        "qail:{}:{}:{:016x}:{}",
        tenant,
        auth.user_id,
        hasher.finish(),
        exact_cache_key(cmd)
    )
}

/// Clamp the LIMIT on a Qail command to at most `max_rows`.
///
/// SECURITY: This must be called **before** execution so PostgreSQL's planner can
/// use the LIMIT to cut off scanning. Post-fetch truncation does not prevent
/// memory exhaustion because all rows are already materialized.
///
/// - If the AST has no LIMIT cage, one is injected.
/// - If the existing LIMIT is higher than `max_rows`, it is lowered.
/// - If the existing LIMIT is already ≤ `max_rows`, nothing changes.
///
/// Only applies to read queries (Get/With/Cnt) — mutations are left untouched.
pub(crate) fn clamp_query_limit(cmd: &mut qail_core::ast::Qail, max_rows: usize) {
    use qail_core::ast::{Action, Cage, CageKind, LogicalOp};

    // Only clamp read actions — writes have RETURNING which is typically small.
    if !matches!(cmd.action, Action::Get | Action::With | Action::Cnt) {
        return;
    }

    // Find existing Limit cage
    for cage in &mut cmd.cages {
        if let CageKind::Limit(ref mut n) = cage.kind {
            if *n > max_rows {
                *n = max_rows;
            }
            return; // Already has a limit, clamped or already fine.
        }
    }

    // No limit cage — inject one.
    cmd.cages.push(Cage {
        kind: CageKind::Limit(max_rows),
        conditions: vec![],
        logical_op: LogicalOp::And,
    });
}

/// Check allow-list against multiple canonical forms.
pub(crate) fn is_query_allowed(
    allow_list: &crate::middleware::QueryAllowList,
    raw_query: Option<&str>,
    cmd: &qail_core::ast::Qail,
) -> bool {
    use qail_core::transpiler::ToSql;

    // Fast path: allow-list disabled.
    if !allow_list.is_enabled() {
        return true;
    }

    if let Some(raw) = raw_query
        && allow_list.is_allowed(raw)
    {
        return true;
    }

    // Canonical QAIL formatter (Display impl).
    let canonical_qail = cmd.to_string();
    if allow_list.is_allowed(&canonical_qail) {
        return true;
    }

    // SQL fallback for deployments that store SQL patterns.
    let sql = cmd.to_sql();
    allow_list.is_allowed(&sql)
}
