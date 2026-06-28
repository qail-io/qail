use qail_core::ast::{Expr, Operator, Value as QailValue};

use super::{is_safe_identifier, parse_select_columns};

pub(crate) const MAX_SORT_COLUMNS: usize = 32;

/// Apply parsed filters to a Qail command.
pub(crate) fn apply_filters(
    mut cmd: qail_core::ast::Qail,
    filters: &[(String, Operator, QailValue)],
) -> qail_core::ast::Qail {
    for (column, op, value) in filters {
        match op {
            Operator::IsNull => {
                cmd = cmd.is_null(column);
            }
            Operator::IsNotNull => {
                cmd = cmd.is_not_null(column);
            }
            Operator::In | Operator::NotIn => {
                if let QailValue::Array(vals) = value {
                    if matches!(op, Operator::In) {
                        cmd = cmd.in_vals(column, vals.clone());
                    } else {
                        cmd = cmd.filter(column, Operator::NotIn, value.clone());
                    }
                }
            }
            _ => {
                cmd = cmd.filter(column, *op, value.clone());
            }
        }
    }
    cmd
}

/// Apply parsed filters to a Qail command, consuming filter values.
pub(crate) fn apply_filters_owned(
    mut cmd: qail_core::ast::Qail,
    filters: Vec<(String, Operator, QailValue)>,
) -> qail_core::ast::Qail {
    for (column, op, value) in filters {
        match op {
            Operator::IsNull => {
                cmd = cmd.is_null(&column);
            }
            Operator::IsNotNull => {
                cmd = cmd.is_not_null(&column);
            }
            Operator::In | Operator::NotIn => {
                if let QailValue::Array(vals) = value {
                    if matches!(op, Operator::In) {
                        cmd = cmd.in_vals(&column, vals);
                    } else {
                        cmd = cmd.filter(&column, Operator::NotIn, QailValue::Array(vals));
                    }
                }
            }
            _ => {
                cmd = cmd.filter(&column, op, value);
            }
        }
    }
    cmd
}

/// Qualify unqualified base-table filter columns after flat JOIN expansion.
///
/// TextSearch stores a validated CSV column list in `Expr::Named`, so each
/// comma segment must be qualified independently.
pub(crate) fn qualify_base_filter_columns_for_join(
    cmd: &mut qail_core::ast::Qail,
    table_name: &str,
) {
    for cage in &mut cmd.cages {
        for cond in &mut cage.conditions {
            let Expr::Named(ref name) = cond.left else {
                continue;
            };

            if cond.op == Operator::TextSearch {
                cond.left = Expr::Named(qualify_text_search_columns(name, table_name));
            } else if !name.contains('.') {
                cond.left = Expr::Named(format!("{}.{}", table_name, name));
            }
        }
    }
}

fn qualify_text_search_columns(columns: &str, table_name: &str) -> String {
    columns
        .split(',')
        .map(str::trim)
        .filter(|column| !column.is_empty())
        .map(|column| {
            if column.contains('.') {
                column.to_string()
            } else {
                format!("{}.{}", table_name, column)
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Apply multi-column sorting.
pub(crate) fn apply_sorting(
    mut cmd: qail_core::ast::Qail,
    sort: &str,
) -> Result<qail_core::ast::Qail, String> {
    for (idx, part) in sort.split(',').enumerate() {
        if idx >= MAX_SORT_COLUMNS {
            return Err(format!(
                "Sort contains more than {} columns",
                MAX_SORT_COLUMNS
            ));
        }
        let part = part.trim();
        if part.is_empty() {
            return Err("Sort contains an empty entry".to_string());
        }

        if let Some(col) = part.strip_prefix('-') {
            let col = col.trim();
            if col.is_empty() || !is_safe_identifier(col) {
                return Err(format!("Invalid sort column '{}'", col));
            }
            cmd = cmd.order_desc(col);
            continue;
        }
        if let Some(col) = part.strip_prefix('+') {
            let col = col.trim();
            if col.is_empty() || !is_safe_identifier(col) {
                return Err(format!("Invalid sort column '{}'", col));
            }
            cmd = cmd.order_asc(col);
            continue;
        }

        if let Some((col, dir)) = part.split_once(':') {
            let col = col.trim();
            let dir = dir.trim();
            if col.is_empty() || !is_safe_identifier(col) {
                return Err(format!("Invalid sort column '{}'", col));
            }
            if dir.eq_ignore_ascii_case("desc") {
                cmd = cmd.order_desc(col);
            } else if dir.eq_ignore_ascii_case("asc") {
                cmd = cmd.order_asc(col);
            } else {
                return Err(format!("Invalid sort direction '{}'", dir));
            }
            continue;
        }

        if is_safe_identifier(part) {
            cmd = cmd.order_asc(part);
        } else {
            return Err(format!("Invalid sort column '{}'", part));
        }
    }
    Ok(cmd)
}

/// Apply returning clause to a mutation command.
pub(crate) fn apply_returning(
    mut cmd: qail_core::ast::Qail,
    returning: Option<&str>,
) -> Result<qail_core::ast::Qail, String> {
    if let Some(ret) = returning {
        let cols = parse_select_columns(ret)
            .map_err(|msg| format!("Invalid returning parameter: {}", msg))?;
        if cols.len() == 1 && cols[0] == "*" {
            cmd = cmd.returning_all();
        } else {
            cmd = cmd.returning(cols);
        }
    }
    Ok(cmd)
}
