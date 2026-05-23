use super::*;

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
        serde_json::to_string(value)
            .unwrap_or_default()
            .hash(&mut hasher);
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
