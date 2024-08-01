//! UPDATE SQL generation.

use crate::ast::*;
use crate::transpiler::dialect::Dialect;
use crate::transpiler::conditions::ConditionToSql;

/// Generate UPDATE SQL.
pub fn build_update(cmd: &QailCmd, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let mut sql = String::from("UPDATE ");
    sql.push_str(&generator.quote_identifier(&cmd.table));

    // For SET queries, first cage is payload, rest are filters
    let mut set_clauses: Vec<String> = Vec::new();
    let mut where_clauses: Vec<String> = Vec::new();
    let mut is_first_filter = true;

    for cage in &cmd.cages {
        if let CageKind::Filter = cage.kind {
            if is_first_filter {
                // First filter cage is the SET payload
                for cond in &cage.conditions {
                    set_clauses.push(format!("{} = {}", generator.quote_identifier(&cond.column), cond.to_value_sql(&generator)));
                }
                is_first_filter = false;
            } else {
                // Subsequent filter cages are WHERE conditions
                for cond in &cage.conditions {
                    where_clauses.push(cond.to_sql(&generator, Some(cmd)));
                }
            }
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
