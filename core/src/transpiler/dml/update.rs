//! UPDATE SQL generation.

use crate::ast::*;
use crate::transpiler::conditions::ConditionToSql;
use crate::transpiler::dialect::Dialect;

/// Generate UPDATE SQL with SET, FROM, and WHERE clauses.
pub fn build_update(cmd: &Qail, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let mut sql = if cmd.only_table {
        String::from("UPDATE ONLY ")
    } else {
        String::from("UPDATE ")
    };
    sql.push_str(&generator.quote_identifier(&cmd.table));

    let mut set_clauses: Vec<String> = Vec::new();
    let mut where_groups: Vec<String> = Vec::new();

    for cage in &cmd.cages {
        match cage.kind {
            // V2 syntax: Payload cage contains SET values
            CageKind::Payload => {
                for cond in &cage.conditions {
                    let col_sql = match &cond.left {
                        Expr::Named(name) => generator.quote_identifier(name),
                        _ => "/* ERROR: Invalid update column */".to_string(),
                    };
                    set_clauses.push(format!(
                        "{} = {}",
                        col_sql,
                        cond.to_value_sql(generator.as_ref())
                    ));
                }
            }
            CageKind::Filter if !cage.conditions.is_empty() => {
                let joiner = match cage.logical_op {
                    LogicalOp::And => " AND ",
                    LogicalOp::Or => " OR ",
                };
                let conditions: Vec<String> = cage
                    .conditions
                    .iter()
                    .map(|c| c.to_sql(generator.as_ref(), Some(cmd)))
                    .collect();
                let group = conditions.join(joiner);
                if cage.logical_op == LogicalOp::Or && cage.conditions.len() > 1 {
                    where_groups.push(format!("({})", group));
                } else {
                    where_groups.push(group);
                }
            }
            _ => {}
        }
    }

    // SET clause
    if !set_clauses.is_empty() {
        sql.push_str(" SET ");
        sql.push_str(&set_clauses.join(", "));
    }

    // FROM clause (multi-table update)
    if !cmd.from_tables.is_empty() {
        sql.push_str(" FROM ");
        sql.push_str(
            &cmd.from_tables
                .iter()
                .map(|t| generator.quote_identifier(t))
                .collect::<Vec<_>>()
                .join(", "),
        );
    }

    if !where_groups.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_groups.join(" AND "));
    }

    if let Some(returning) = &cmd.returning
        && !returning.is_empty()
    {
        let cols: Vec<String> = returning
            .iter()
            .map(|expr| match expr {
                Expr::Star => "*".to_string(),
                Expr::Named(name) => generator.quote_identifier(name),
                other => render_returning_expr(other, generator.as_ref()),
            })
            .collect();
        sql.push_str(" RETURNING ");
        sql.push_str(&cols.join(", "));
    }

    sql
}

fn render_returning_expr(expr: &Expr, generator: &dyn crate::transpiler::SqlGenerator) -> String {
    match expr {
        Expr::Star => "*".to_string(),
        Expr::Named(name) => generator.quote_identifier(name),
        Expr::Literal(value) => value.to_string(),
        Expr::Cast {
            expr, target_type, ..
        } => {
            let Some(target_type) = checked_sql_type_fragment(target_type) else {
                return "/* ERROR: Invalid cast target type */".to_string();
            };
            format!(
                "{}::{}",
                render_returning_expr(expr, generator),
                target_type
            )
        }
        Expr::JsonAccess {
            column,
            path_segments,
            ..
        } => render_json_access(column, path_segments, generator),
        Expr::Collate {
            expr, collation, ..
        } => format!(
            "{} COLLATE {}",
            render_returning_expr(expr, generator),
            render_qualified_identifier(collation, generator)
        ),
        Expr::FieldAccess { expr, field, .. } => format!(
            "({}).{}",
            render_returning_expr(expr, generator),
            render_qualified_identifier(field, generator)
        ),
        _ => "/* ERROR: Invalid returning expression */".to_string(),
    }
}

fn checked_sql_type_fragment(fragment: &str) -> Option<String> {
    let fragment = fragment.trim();
    if fragment.is_empty()
        || fragment.contains('\0')
        || fragment.contains(';')
        || fragment.contains('\'')
        || fragment.contains('"')
        || fragment.contains("--")
        || fragment.contains("/*")
        || fragment.contains("*/")
        || !fragment.bytes().all(|b| {
            b.is_ascii_alphanumeric()
                || matches!(
                    b,
                    b'_' | b'.' | b' ' | b'(' | b')' | b',' | b'[' | b']' | b'%' | b'+' | b'-'
                )
        })
    {
        None
    } else {
        Some(fragment.to_string())
    }
}

fn render_qualified_identifier(
    value: &str,
    generator: &dyn crate::transpiler::SqlGenerator,
) -> String {
    if value.is_empty() || value.as_bytes().contains(&0) || value.split('.').any(str::is_empty) {
        "/* ERROR: Invalid identifier */".to_string()
    } else {
        generator.quote_identifier(value)
    }
}

fn render_json_access(
    column: &str,
    path_segments: &[(String, bool)],
    generator: &dyn crate::transpiler::SqlGenerator,
) -> String {
    let mut sql = generator.quote_identifier(column);
    for (path, as_text) in path_segments {
        let op = if *as_text { "->>" } else { "->" };
        if path.parse::<i64>().is_ok() {
            sql.push_str(&format!("{}{}", op, path));
        } else {
            sql.push_str(&format!(
                "{}'{}'",
                op,
                crate::transpiler::escape_sql_string_literal(path)
            ));
        }
    }
    sql
}
