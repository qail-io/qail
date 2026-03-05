use qail_core::ast::{Operator, Value as QailValue};

use super::is_safe_identifier;

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
pub(crate) fn apply_sorting(mut cmd: qail_core::ast::Qail, sort: &str) -> qail_core::ast::Qail {
    for part in sort.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        if let Some(col) = part.strip_prefix('-') {
            let col = col.trim();
            if !col.is_empty() && is_safe_identifier(col) {
                cmd = cmd.order_desc(col);
            }
            continue;
        }
        if let Some(col) = part.strip_prefix('+') {
            let col = col.trim();
            if !col.is_empty() && is_safe_identifier(col) {
                cmd = cmd.order_asc(col);
            }
            continue;
        }

        if let Some((col, dir)) = part.split_once(':') {
            let col = col.trim();
            let dir = dir.trim();
            if col.is_empty() || !is_safe_identifier(col) {
                continue;
            }
            if dir.eq_ignore_ascii_case("desc") {
                cmd = cmd.order_desc(col);
            } else {
                cmd = cmd.order_asc(col);
            }
            continue;
        }

        if is_safe_identifier(part) {
            cmd = cmd.order_asc(part);
        }
    }
    cmd
}

/// Apply returning clause to a mutation command.
pub(crate) fn apply_returning(
    mut cmd: qail_core::ast::Qail,
    returning: Option<&str>,
) -> qail_core::ast::Qail {
    if let Some(ret) = returning {
        if ret == "*" {
            cmd = cmd.returning_all();
        } else {
            let cols: Vec<&str> = ret
                .split(',')
                .map(|s| s.trim())
                .filter(|s| is_safe_identifier(s))
                .collect();
            if !cols.is_empty() {
                cmd = cmd.returning(cols);
            }
        }
    }
    cmd
}
