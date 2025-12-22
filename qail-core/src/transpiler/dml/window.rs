//! Window Function SQL generation.

use crate::ast::*;
use crate::transpiler::dialect::Dialect;
use crate::transpiler::conditions::ConditionToSql;

/// Generate Window Function SQL (Pillar 8).
pub fn build_window(cmd: &QailCmd, dialect: Dialect) -> String {
    let generator = dialect.generator();
    // Build SELECT with window function columns
    let mut sql = String::from("SELECT ");

    let cols: Vec<String> = cmd.columns.iter().map(|c| {
        match c {
            Column::Window { name, func, params, partition, order, frame } => {
                let params_str = if params.is_empty() {
                    String::new()
                } else {
                    params.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", ")
                };
                
                let mut over_clause = String::from("OVER (");
                if !partition.is_empty() {
                    over_clause.push_str("PARTITION BY ");
                    let quoted_partition: Vec<String> = partition.iter().map(|p| generator.quote_identifier(p)).collect();
                    over_clause.push_str(&quoted_partition.join(", "));
                    if !order.is_empty() {
                        over_clause.push(' ');
                    }
                }
                if !order.is_empty() {
                    over_clause.push_str("ORDER BY ");
                    let order_parts: Vec<String> = order.iter().map(|cage| {
                        match &cage.kind {
                            CageKind::Sort(SortOrder::Asc) => {
                                if let Some(cond) = cage.conditions.first() {
                                    format!("{} ASC", generator.quote_identifier(&cond.column))
                                } else {
                                    String::new()
                                }
                            }
                            CageKind::Sort(SortOrder::Desc) => {
                                if let Some(cond) = cage.conditions.first() {
                                    format!("{} DESC", generator.quote_identifier(&cond.column))
                                } else {
                                    String::new()
                                }
                            }
                            // Handle Nulls First/Last if needed here, but skipping for brevity of match
                             CageKind::Sort(SortOrder::AscNullsFirst) => {
                                if let Some(cond) = cage.conditions.first() { format!("{} ASC NULLS FIRST", generator.quote_identifier(&cond.column)) } else { "".to_string() }
                            }
                            CageKind::Sort(SortOrder::AscNullsLast) => {
                                if let Some(cond) = cage.conditions.first() { format!("{} ASC NULLS LAST", generator.quote_identifier(&cond.column)) } else { "".to_string() }
                            }
                            CageKind::Sort(SortOrder::DescNullsFirst) => {
                                if let Some(cond) = cage.conditions.first() { format!("{} DESC NULLS FIRST", generator.quote_identifier(&cond.column)) } else { "".to_string() }
                            }
                            CageKind::Sort(SortOrder::DescNullsLast) => {
                                if let Some(cond) = cage.conditions.first() { format!("{} DESC NULLS LAST", generator.quote_identifier(&cond.column)) } else { "".to_string() }
                            }
                            _ => String::new(),
                        }
                    }).filter(|s| !s.is_empty()).collect();
                    over_clause.push_str(&order_parts.join(", "));
                }
                
                // Add Frame Logic
                if let Some(fr) = frame {
                    over_clause.push(' ');
                    match fr {
                        WindowFrame::Rows { start, end } => {
                            over_clause.push_str(&format!("ROWS BETWEEN {} AND {}", bound_to_sql(&start), bound_to_sql(&end)));
                        }
                        WindowFrame::Range { start, end } => {
                             over_clause.push_str(&format!("RANGE BETWEEN {} AND {}", bound_to_sql(&start), bound_to_sql(&end)));
                        }
                    }
                }
                
                over_clause.push(')');
                
                format!("{}({}) {} AS {}", func, params_str, over_clause, generator.quote_identifier(name))
            }
            _ => c.to_string(),
        }
    }).collect();

    sql.push_str(&cols.join(", "));
    sql.push_str(" FROM ");
    sql.push_str(&generator.quote_identifier(&cmd.table));

    // Handle cages (WHERE, LIMIT, etc.)
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
