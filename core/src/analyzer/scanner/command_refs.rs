use std::collections::HashSet;
use std::path::Path;

#[cfg(test)]
use crate::ast::CageKind;
use crate::ast::{Action, Cage, Condition, ConflictAction, Expr, MergeAction, Value};

use super::{CodeReference, QueryType};

fn command_to_reference(path: &Path, line: usize, cmd: &crate::Qail) -> Option<CodeReference> {
    if cmd.table.trim().is_empty() {
        return None;
    }

    let snippet = match cmd.action {
        Action::Get => format!("get {} fields ...", cmd.table),
        Action::Set => format!("set {} values ...", cmd.table),
        Action::Del => format!("del {}", cmd.table),
        Action::Add => format!("add {} fields ...", cmd.table),
        _ => return None,
    };
    let columns = collect_reference_columns(cmd);

    Some(CodeReference {
        file: path.to_path_buf(),
        line,
        table: cmd.table.clone(),
        columns,
        query_type: QueryType::Qail,
        snippet,
    })
}

pub(super) fn command_to_references(
    path: &Path,
    line: usize,
    cmd: &crate::Qail,
) -> Vec<CodeReference> {
    let mut refs = Vec::new();
    if let Some(reference) = command_to_reference(path, line, cmd) {
        refs.push(reference);
    }
    collect_subquery_references(path, line, cmd, &mut refs);
    refs
}

fn collect_subquery_references(
    path: &Path,
    line: usize,
    cmd: &crate::Qail,
    refs: &mut Vec<CodeReference>,
) {
    for expr in &cmd.columns {
        collect_expr_subquery_references(path, line, expr, refs);
    }
    for cage in &cmd.cages {
        collect_cage_subquery_references(path, line, cage, refs);
    }
    for join in &cmd.joins {
        if let Some(conditions) = &join.on {
            collect_conditions_subquery_references(path, line, conditions, refs);
        }
    }
    collect_conditions_subquery_references(path, line, &cmd.having, refs);
    for expr in &cmd.distinct_on {
        collect_expr_subquery_references(path, line, expr, refs);
    }
    if let Some(returning) = &cmd.returning {
        for expr in returning {
            collect_expr_subquery_references(path, line, expr, refs);
        }
    }
    if let Some(on_conflict) = &cmd.on_conflict
        && let ConflictAction::DoUpdate { assignments } = &on_conflict.action
    {
        for (_, expr) in assignments {
            collect_expr_subquery_references(path, line, expr, refs);
        }
    }
    if let Some(merge) = &cmd.merge {
        match &merge.source {
            crate::ast::MergeSource::Query { query, .. } => {
                refs.extend(command_to_references(path, line, query));
            }
            crate::ast::MergeSource::Table { .. } => {}
        }
        collect_conditions_subquery_references(path, line, &merge.on, refs);
        for clause in &merge.clauses {
            collect_conditions_subquery_references(path, line, &clause.condition, refs);
            match &clause.action {
                MergeAction::Update { assignments } => {
                    for (_, expr) in assignments {
                        collect_expr_subquery_references(path, line, expr, refs);
                    }
                }
                MergeAction::Insert { values, .. } => {
                    for expr in values {
                        collect_expr_subquery_references(path, line, expr, refs);
                    }
                }
                MergeAction::Delete | MergeAction::DoNothing => {}
            }
        }
    }
    if let Some(source_query) = &cmd.source_query {
        refs.extend(command_to_references(path, line, source_query));
    }
    for (_, set_query) in &cmd.set_ops {
        refs.extend(command_to_references(path, line, set_query));
    }
    for cte in &cmd.ctes {
        refs.extend(command_to_references(path, line, &cte.base_query));
        if let Some(recursive_query) = &cte.recursive_query {
            refs.extend(command_to_references(path, line, recursive_query));
        }
    }
}

fn collect_expr_subquery_references(
    path: &Path,
    line: usize,
    expr: &Expr,
    refs: &mut Vec<CodeReference>,
) {
    match expr {
        Expr::Aggregate { filter, .. } => {
            if let Some(conditions) = filter {
                collect_conditions_subquery_references(path, line, conditions, refs);
            }
        }
        Expr::Cast { expr, .. }
        | Expr::Mod { col: expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::FieldAccess { expr, .. } => {
            collect_expr_subquery_references(path, line, expr, refs)
        }
        Expr::Subscript { expr, index, .. } => {
            collect_expr_subquery_references(path, line, expr, refs);
            collect_expr_subquery_references(path, line, index, refs);
        }
        Expr::FunctionCall { args, .. } | Expr::ArrayConstructor { elements: args, .. } => {
            for arg in args {
                collect_expr_subquery_references(path, line, arg, refs);
            }
        }
        Expr::SpecialFunction { args, .. } => {
            for (_, arg) in args {
                collect_expr_subquery_references(path, line, arg, refs);
            }
        }
        Expr::Binary { left, right, .. } => {
            collect_expr_subquery_references(path, line, left, refs);
            collect_expr_subquery_references(path, line, right, refs);
        }
        Expr::Literal(value) => collect_value_subquery_references(path, line, value, refs),
        Expr::RowConstructor { elements, .. } => {
            for expr in elements {
                collect_expr_subquery_references(path, line, expr, refs);
            }
        }
        Expr::Case {
            when_clauses,
            else_value,
            ..
        } => {
            for (condition, value) in when_clauses {
                collect_condition_subquery_references(path, line, condition, refs);
                collect_expr_subquery_references(path, line, value, refs);
            }
            if let Some(value) = else_value {
                collect_expr_subquery_references(path, line, value, refs);
            }
        }
        Expr::Window { params, order, .. } => {
            for param in params {
                collect_expr_subquery_references(path, line, param, refs);
            }
            for cage in order {
                collect_cage_subquery_references(path, line, cage, refs);
            }
        }
        Expr::Subquery { query, .. } | Expr::Exists { query, .. } => {
            refs.extend(command_to_references(path, line, query));
        }
        Expr::Star
        | Expr::Named(_)
        | Expr::Aliased { .. }
        | Expr::JsonAccess { .. }
        | Expr::Def { .. } => {}
    }
}

fn collect_cage_subquery_references(
    path: &Path,
    line: usize,
    cage: &Cage,
    refs: &mut Vec<CodeReference>,
) {
    collect_conditions_subquery_references(path, line, &cage.conditions, refs);
}

fn collect_conditions_subquery_references(
    path: &Path,
    line: usize,
    conditions: &[Condition],
    refs: &mut Vec<CodeReference>,
) {
    for condition in conditions {
        collect_condition_subquery_references(path, line, condition, refs);
    }
}

fn collect_condition_subquery_references(
    path: &Path,
    line: usize,
    condition: &Condition,
    refs: &mut Vec<CodeReference>,
) {
    collect_expr_subquery_references(path, line, &condition.left, refs);
    collect_value_subquery_references(path, line, &condition.value, refs);
}

fn collect_value_subquery_references(
    path: &Path,
    line: usize,
    value: &Value,
    refs: &mut Vec<CodeReference>,
) {
    match value {
        Value::Array(values) => {
            for value in values {
                collect_value_subquery_references(path, line, value, refs);
            }
        }
        Value::Subquery(query) => refs.extend(command_to_references(path, line, query)),
        Value::Expr(expr) => collect_expr_subquery_references(path, line, expr, refs),
        _ => {}
    }
}

fn collect_reference_columns(cmd: &crate::Qail) -> Vec<String> {
    let mut cols = Vec::new();
    let mut seen = HashSet::new();

    collect_exprs_columns(&cmd.columns, &mut cols, &mut seen);
    for cage in &cmd.cages {
        collect_cage_columns(cage, &mut cols, &mut seen);
    }
    for join in &cmd.joins {
        if let Some(conditions) = &join.on {
            collect_conditions_columns(conditions, &mut cols, &mut seen);
        }
    }
    collect_conditions_columns(&cmd.having, &mut cols, &mut seen);
    collect_exprs_columns(&cmd.distinct_on, &mut cols, &mut seen);
    if let Some(returning) = &cmd.returning {
        collect_exprs_columns(returning, &mut cols, &mut seen);
    }
    if let Some(on_conflict) = &cmd.on_conflict {
        for column in &on_conflict.columns {
            push_column_ref(column, &mut cols, &mut seen);
        }
        if let ConflictAction::DoUpdate { assignments } = &on_conflict.action {
            for (column, expr) in assignments {
                push_column_ref(column, &mut cols, &mut seen);
                collect_expr_columns(expr, &mut cols, &mut seen);
            }
        }
    }
    if let Some(merge) = &cmd.merge {
        collect_conditions_columns(&merge.on, &mut cols, &mut seen);
        for clause in &merge.clauses {
            collect_conditions_columns(&clause.condition, &mut cols, &mut seen);
            match &clause.action {
                MergeAction::Update { assignments } => {
                    for (column, expr) in assignments {
                        push_column_ref(column, &mut cols, &mut seen);
                        collect_expr_columns(expr, &mut cols, &mut seen);
                    }
                }
                MergeAction::Insert { columns, values } => {
                    for column in columns {
                        push_column_ref(column, &mut cols, &mut seen);
                    }
                    collect_exprs_columns(values, &mut cols, &mut seen);
                }
                MergeAction::Delete | MergeAction::DoNothing => {}
            }
        }
    }

    cols
}

fn collect_exprs_columns(exprs: &[Expr], cols: &mut Vec<String>, seen: &mut HashSet<String>) {
    for expr in exprs {
        collect_expr_columns(expr, cols, seen);
    }
}

fn collect_cage_columns(cage: &Cage, cols: &mut Vec<String>, seen: &mut HashSet<String>) {
    collect_conditions_columns(&cage.conditions, cols, seen);
}

fn collect_conditions_columns(
    conditions: &[Condition],
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    for condition in conditions {
        collect_condition_columns(condition, cols, seen);
    }
}

fn collect_condition_columns(
    condition: &Condition,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    collect_expr_columns(&condition.left, cols, seen);
    collect_value_columns(&condition.value, cols, seen);
}

fn collect_expr_columns(expr: &Expr, cols: &mut Vec<String>, seen: &mut HashSet<String>) {
    match expr {
        Expr::Star => push_column_ref("*", cols, seen),
        Expr::Named(name) | Expr::Aliased { name, .. } => push_column_ref(name, cols, seen),
        Expr::Aggregate { col, filter, .. } => {
            push_column_ref(col, cols, seen);
            if let Some(conditions) = filter {
                collect_conditions_columns(conditions, cols, seen);
            }
        }
        Expr::JsonAccess { column, .. } => push_column_ref(column, cols, seen),
        Expr::Cast { expr, .. }
        | Expr::Mod { col: expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::FieldAccess { expr, .. } => collect_expr_columns(expr, cols, seen),
        Expr::Subscript { expr, index, .. } => {
            collect_expr_columns(expr, cols, seen);
            collect_expr_columns(index, cols, seen);
        }
        Expr::FunctionCall { args, .. } | Expr::ArrayConstructor { elements: args, .. } => {
            collect_exprs_columns(args, cols, seen);
        }
        Expr::SpecialFunction { args, .. } => {
            for (_, arg) in args {
                collect_expr_columns(arg, cols, seen);
            }
        }
        Expr::Binary { left, right, .. } => {
            collect_expr_columns(left, cols, seen);
            collect_expr_columns(right, cols, seen);
        }
        Expr::Literal(value) => collect_value_columns(value, cols, seen),
        Expr::RowConstructor { elements, .. } => collect_exprs_columns(elements, cols, seen),
        Expr::Case {
            when_clauses,
            else_value,
            ..
        } => {
            for (condition, value) in when_clauses {
                collect_condition_columns(condition, cols, seen);
                collect_expr_columns(value, cols, seen);
            }
            if let Some(value) = else_value {
                collect_expr_columns(value, cols, seen);
            }
        }
        Expr::Window {
            params,
            partition,
            order,
            ..
        } => {
            collect_exprs_columns(params, cols, seen);
            for column in partition {
                push_column_ref(column, cols, seen);
            }
            for cage in order {
                collect_cage_columns(cage, cols, seen);
            }
        }
        Expr::Def { .. } | Expr::Subquery { .. } | Expr::Exists { .. } => {}
    }
}

fn collect_value_columns(value: &Value, cols: &mut Vec<String>, seen: &mut HashSet<String>) {
    match value {
        Value::Column(column) => push_column_ref(column, cols, seen),
        Value::Expr(expr) => collect_expr_columns(expr, cols, seen),
        Value::Array(values) => {
            for value in values {
                collect_value_columns(value, cols, seen);
            }
        }
        _ => {}
    }
}

fn push_column_ref(name: &str, cols: &mut Vec<String>, seen: &mut HashSet<String>) {
    let name = name.trim();
    if !name.is_empty() && seen.insert(name.to_string()) {
        cols.push(name.to_string());
    }
}

#[cfg(test)]
fn extract_payload_columns(cmd: &crate::Qail) -> Vec<String> {
    let mut cols = Vec::new();
    let mut seen = HashSet::new();

    for cage in &cmd.cages {
        if !matches!(cage.kind, CageKind::Payload) {
            continue;
        }

        for cond in &cage.conditions {
            if let Expr::Named(name) = &cond.left
                && !name.is_empty()
                && seen.insert(name.clone())
            {
                cols.push(name.clone());
            }
        }
    }

    cols
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::parse;

    use super::*;

    #[test]
    fn test_set_payload_column_extraction() {
        let cmd = parse("set users values name = \"Alice\", status = \"active\" where id = $1")
            .expect("set parse");
        let columns = extract_payload_columns(&cmd);
        assert_eq!(columns, vec!["name", "status"]);
    }

    #[test]
    fn test_command_reference_tracks_filter_columns() {
        let cmd = parse("get users fields id where email = $1 order by created_at desc")
            .expect("get parse");
        let reference =
            command_to_reference(Path::new("src/users.ts"), 1, &cmd).expect("reference");

        assert_eq!(reference.table, "users");
        assert_eq!(reference.columns, vec!["id", "email", "created_at"]);
    }

    #[test]
    fn test_command_references_track_native_qail_subqueries() {
        let cmd =
            parse("get users fields id where exists (get orders fields user_id where total > $1)")
                .expect("get parse");
        let refs = command_to_references(Path::new("src/users.ts"), 1, &cmd);

        assert_eq!(refs.len(), 2, "{refs:?}");

        let users = refs
            .iter()
            .find(|reference| reference.table == "users")
            .expect("users reference");
        assert_eq!(users.columns, vec!["id"]);

        let orders = refs
            .iter()
            .find(|reference| reference.table == "orders")
            .expect("orders reference");
        assert_eq!(orders.columns, vec!["user_id", "total"]);
    }
}
