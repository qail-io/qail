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
            Column::Window { name, func, params, partition, order } => {
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
                            _ => String::new(),
                        }
                    }).filter(|s| !s.is_empty()).collect();
                    over_clause.push_str(&order_parts.join(", "));
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
