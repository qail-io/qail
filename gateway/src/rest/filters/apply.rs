use qail_core::ast::{Operator, Value as QailValue};

use super::{is_safe_identifier, parse_select_columns};

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

/// Apply multi-column sorting.
pub(crate) fn apply_sorting(
    mut cmd: qail_core::ast::Qail,
    sort: &str,
) -> Result<qail_core::ast::Qail, String> {
    for part in sort.split(',') {
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
