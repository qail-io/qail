//! Window Function SQL generation.

use crate::ast::*;
use crate::transpiler::conditions::ConditionToSql;
use crate::transpiler::dialect::Dialect;

/// Generate Window Function SQL (Pillar 8).
pub fn build_window(cmd: &Qail, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let mut sql = String::from("SELECT ");

    let cols: Vec<String> = cmd
        .columns
        .iter()
        .map(|c| {
            match c {
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
                            .map(|v| v.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
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
                                        expr => expr.to_string(),
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
                        func,
                        params_str,
                        over_clause,
                        generator.quote_identifier(name)
                    )
                }
                _ => c.to_string(),
            }
        })
        .collect();

    sql.push_str(&cols.join(", "));
    sql.push_str(" FROM ");
    sql.push_str(&generator.quote_identifier(&cmd.table));

    let mut where_clauses: Vec<String> = Vec::new();
    for cage in &cmd.cages {
        if let CageKind::Filter = cage.kind {
            for cond in &cage.conditions {
                where_clauses.push(cond.to_sql(&generator, Some(cmd)));
            }
        }
    }

    if !where_clauses.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_clauses.join(" AND "));
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
