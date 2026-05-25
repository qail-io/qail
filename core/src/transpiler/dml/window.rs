//! Window Function SQL generation.

use crate::ast::*;
use crate::transpiler::conditions::ConditionToSql;
use crate::transpiler::dialect::Dialect;
use crate::transpiler::{SqlGenerator, escape_sql_string_literal};

/// Generate Window Function SQL (Pillar 8).
pub fn build_window(cmd: &Qail, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let mut sql = String::from("SELECT ");

    let cols: Vec<String> = cmd
        .columns
        .iter()
        .map(|c| match c {
            Expr::Window {
                name,
                func,
                params,
                partition,
                order,
                frame,
            } => {
                let params_str = if params.is_empty() {
                    String::new()
                } else {
                    params
                        .iter()
                        .map(|v| render_window_expr(v, generator.as_ref(), cmd))
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                let Some(function) = render_function_name(func) else {
                    return "/* ERROR: Invalid window function name */".to_string();
                };

                let mut over_clause = String::from("OVER (");
                if !partition.is_empty() {
                    over_clause.push_str("PARTITION BY ");
                    let quoted_partition: Vec<String> = partition
                        .iter()
                        .map(|p| generator.quote_identifier(p))
                        .collect();
                    over_clause.push_str(&quoted_partition.join(", "));
                    if !order.is_empty() {
                        over_clause.push(' ');
                    }
                }
                if !order.is_empty() {
                    over_clause.push_str("ORDER BY ");
                    let order_parts: Vec<String> = order
                        .iter()
                        .map(|cage| {
                            let col_str = if let Some(cond) = cage.conditions.first() {
                                match &cond.left {
                                    Expr::Named(name) => generator.quote_identifier(name),
                                    expr => render_window_expr(expr, generator.as_ref(), cmd),
                                }
                            } else {
                                return String::new();
                            };

                            match &cage.kind {
                                CageKind::Sort(SortOrder::Asc) => format!("{} ASC", col_str),
                                CageKind::Sort(SortOrder::Desc) => format!("{} DESC", col_str),
                                CageKind::Sort(SortOrder::AscNullsFirst) => {
                                    format!("{} ASC NULLS FIRST", col_str)
                                }
                                CageKind::Sort(SortOrder::AscNullsLast) => {
                                    format!("{} ASC NULLS LAST", col_str)
                                }
                                CageKind::Sort(SortOrder::DescNullsFirst) => {
                                    format!("{} DESC NULLS FIRST", col_str)
                                }
                                CageKind::Sort(SortOrder::DescNullsLast) => {
                                    format!("{} DESC NULLS LAST", col_str)
                                }
                                _ => String::new(),
                            }
                        })
                        .filter(|s| !s.is_empty())
                        .collect();
                    over_clause.push_str(&order_parts.join(", "));
                }

                if let Some(fr) = frame {
                    over_clause.push(' ');
                    match fr {
                        WindowFrame::Rows { start, end } => {
                            over_clause.push_str(&format!(
                                "ROWS BETWEEN {} AND {}",
                                bound_to_sql(start),
                                bound_to_sql(end)
                            ));
                        }
                        WindowFrame::Range { start, end } => {
                            over_clause.push_str(&format!(
                                "RANGE BETWEEN {} AND {}",
                                bound_to_sql(start),
                                bound_to_sql(end)
                            ));
                        }
                    }
                }

                over_clause.push(')');

                format!(
                    "{}({}) {} AS {}",
                    function,
                    params_str,
                    over_clause,
                    generator.quote_identifier(name)
                )
            }
            _ => render_window_expr(c, generator.as_ref(), cmd),
        })
        .collect();

    sql.push_str(&cols.join(", "));
    sql.push_str(" FROM ");
    sql.push_str(&generator.quote_identifier(&cmd.table));

    let mut where_groups: Vec<String> = Vec::new();
    for cage in &cmd.cages {
        if let CageKind::Filter = cage.kind
            && !cage.conditions.is_empty()
        {
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
    }

    if !where_groups.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_groups.join(" AND "));
    }

    sql
}

fn bound_to_sql(bound: &FrameBound) -> String {
    match bound {
        FrameBound::UnboundedPreceding => "UNBOUNDED PRECEDING".to_string(),
        FrameBound::UnboundedFollowing => "UNBOUNDED FOLLOWING".to_string(),
        FrameBound::CurrentRow => "CURRENT ROW".to_string(),
        FrameBound::Preceding(n) => format!("{} PRECEDING", n),
        FrameBound::Following(n) => format!("{} FOLLOWING", n),
    }
}

fn render_window_expr(expr: &Expr, generator: &dyn SqlGenerator, cmd: &Qail) -> String {
    match expr {
        Expr::Star => "*".to_string(),
        Expr::Named(name) => render_named_expr(name, generator),
        Expr::Aliased { name, alias } => {
            format!(
                "{} AS {}",
                render_named_expr(name, generator),
                generator.quote_identifier(alias)
            )
        }
        Expr::Literal(value) => value.to_string(),
        Expr::Case {
            when_clauses,
            else_value,
            ..
        } => {
            let mut case_sql = String::from("CASE");
            for (cond, val) in when_clauses {
                case_sql.push_str(&format!(
                    " WHEN {} THEN {}",
                    cond.to_sql(generator, Some(cmd)),
                    render_window_expr(val, generator, cmd)
                ));
            }
            if let Some(value) = else_value {
                case_sql.push_str(&format!(
                    " ELSE {}",
                    render_window_expr(value, generator, cmd)
                ));
            }
            case_sql.push_str(" END");
            case_sql
        }
        Expr::Binary {
            left, op, right, ..
        } => {
            let left_sql = render_window_expr(left, generator, cmd);
            let right_sql = render_window_expr(right, generator, cmd);
            match op {
                BinaryOp::IsNull => format!("({left_sql} IS NULL)"),
                BinaryOp::IsNotNull => format!("({left_sql} IS NOT NULL)"),
                _ => format!("({left_sql} {op} {right_sql})"),
            }
        }
        Expr::FunctionCall { name, args, .. } => {
            let Some(function) = render_function_name(name) else {
                return "/* ERROR: Invalid function name */".to_string();
            };
            let args = args
                .iter()
                .map(|arg| render_window_expr(arg, generator, cmd))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{function}({args})")
        }
        Expr::Cast {
            expr, target_type, ..
        } => {
            let Some(target_type) = checked_sql_type_fragment(target_type) else {
                return "/* ERROR: Invalid cast target type */".to_string();
            };
            format!(
                "{}::{}",
                render_window_expr(expr, generator, cmd),
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
            render_window_expr(expr, generator, cmd),
            render_qualified_identifier(collation, generator)
        ),
        Expr::FieldAccess { expr, field, .. } => format!(
            "({}).{}",
            render_window_expr(expr, generator, cmd),
            render_qualified_identifier(field, generator)
        ),
        Expr::ArrayConstructor { elements, .. } => {
            let elements = elements
                .iter()
                .map(|element| render_window_expr(element, generator, cmd))
                .collect::<Vec<_>>()
                .join(", ");
            format!("ARRAY[{elements}]")
        }
        Expr::RowConstructor { elements, .. } => {
            let elements = elements
                .iter()
                .map(|element| render_window_expr(element, generator, cmd))
                .collect::<Vec<_>>()
                .join(", ");
            format!("ROW({elements})")
        }
        Expr::Subscript { expr, index, .. } => format!(
            "{}[{}]",
            render_window_expr(expr, generator, cmd),
            render_window_expr(index, generator, cmd)
        ),
        _ => "/* ERROR: Invalid window expression */".to_string(),
    }
}

fn render_named_expr(name: &str, generator: &dyn SqlGenerator) -> String {
    if name == "*"
        || name.starts_with('\'')
        || name.starts_with('"')
        || name.starts_with(':')
        || name.starts_with('$')
        || name.parse::<f64>().is_ok()
        || name.eq_ignore_ascii_case("NULL")
        || name.eq_ignore_ascii_case("TRUE")
        || name.eq_ignore_ascii_case("FALSE")
    {
        name.to_string()
    } else {
        generator.quote_identifier(name)
    }
}

fn render_function_name(name: &str) -> Option<String> {
    if name.is_empty()
        || name.contains('\0')
        || name.split('.').any(str::is_empty)
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.')
    {
        None
    } else {
        Some(name.to_uppercase())
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

fn render_qualified_identifier(value: &str, generator: &dyn SqlGenerator) -> String {
    if value.is_empty() || value.as_bytes().contains(&0) || value.split('.').any(str::is_empty) {
        "/* ERROR: Invalid identifier */".to_string()
    } else {
        generator.quote_identifier(value)
    }
}

fn render_json_access(
    column: &str,
    path_segments: &[(String, bool)],
    generator: &dyn SqlGenerator,
) -> String {
    let mut sql = generator.quote_identifier(column);
    for (path, as_text) in path_segments {
        let op = if *as_text { "->>" } else { "->" };
        if path.parse::<i64>().is_ok() {
            sql.push_str(&format!("{}{}", op, path));
        } else {
            sql.push_str(&format!("{}'{}'", op, escape_sql_string_literal(path)));
        }
    }
    sql
}
