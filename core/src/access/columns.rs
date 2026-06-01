use std::collections::BTreeSet;

use crate::ast::{Action, CageKind, ConflictAction, Expr, MergeAction, Qail};

use super::error::{AccessError, AccessErrorKind};
use super::ident::{normalize_column_name, normalize_identifier_part};
use super::model::{AccessOperation, ColumnRule};

pub(super) fn projection_restricted_action(action: Action) -> bool {
    matches!(
        action,
        Action::Get | Action::Export | Action::With | Action::Search | Action::Scroll
    )
}

pub(super) fn check_projection_rule(
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

pub(super) fn create_columns(cmd: &Qail) -> Result<Vec<String>, AccessError> {
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

pub(super) fn update_columns(cmd: &Qail) -> Result<Vec<String>, AccessError> {
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

pub(super) fn expr_projects_all_columns(expr: &Expr) -> bool {
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

pub(super) fn check_named_read_column(
    table: &str,
    rule: &ColumnRule,
    target_refs: &BTreeSet<String>,
    name: &str,
    context: &'static str,
) -> Result<(), AccessError> {
    let Some(column_ref) = parse_column_ref(name) else {
        return Err(AccessError::new(
            table.to_string(),
            Some(AccessOperation::Read),
            AccessErrorKind::UnsupportedColumnExpression { context },
        ));
    };
    if !column_ref_matches_target(&column_ref, target_refs) {
        return Ok(());
    }
    if !rule.allows(&column_ref.column) {
        return Err(AccessError::new(
            table.to_string(),
            Some(AccessOperation::Read),
            AccessErrorKind::ColumnDenied {
                column: column_ref.column,
            },
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ColumnRef {
    qualifier: Option<String>,
    column: String,
}

fn parse_column_ref(name: &str) -> Option<ColumnRef> {
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

    let parts: Vec<String> = trimmed
        .split('.')
        .map(normalize_identifier_part)
        .filter(|part| !part.is_empty())
        .collect();
    let column = parts.last()?.clone();
    let qualifier = (parts.len() > 1).then(|| parts[..parts.len() - 1].join("."));
    Some(ColumnRef { qualifier, column })
}

fn column_ref_matches_target(column_ref: &ColumnRef, target_refs: &BTreeSet<String>) -> bool {
    let Some(qualifier) = &column_ref.qualifier else {
        return true;
    };
    target_refs.contains(qualifier)
}
