//! UPDATE SQL generation.

use crate::ast::*;
use crate::transpiler::dialect::Dialect;
use crate::transpiler::conditions::ConditionToSql;

/// Generate UPDATE SQL.
pub fn build_update(cmd: &QailCmd, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let mut sql = String::from("UPDATE ");
    sql.push_str(&generator.quote_identifier(&cmd.table));

    let mut set_clauses: Vec<String> = Vec::new();
    let mut where_clauses: Vec<String> = Vec::new();

    for cage in &cmd.cages {
        match cage.kind {
            // V2 syntax: Payload cage contains SET values
            CageKind::Payload => {
                for cond in &cage.conditions {
                    let col_sql = match &cond.left {
                        Expr::Named(name) => generator.quote_identifier(name),
                        expr => expr.to_string(),
                    };
                    set_clauses.push(format!("{} = {}", col_sql, cond.to_value_sql(&generator)));
                }
            }
            // Filter cage contains WHERE conditions
            CageKind::Filter => {
                for cond in &cage.conditions {
                    where_clauses.push(cond.to_sql(&generator, Some(cmd)));
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

    // WHERE clause
    if !where_clauses.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_clauses.join(" AND "));
    }

    sql
}

