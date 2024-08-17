//! DELETE SQL generation.

use crate::ast::*;
use crate::transpiler::conditions::ConditionToSql;
use crate::transpiler::dialect::Dialect;

/// Generate DELETE SQL.
pub fn build_delete(cmd: &QailCmd, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let mut sql = String::from("DELETE FROM ");
    sql.push_str(&generator.quote_identifier(&cmd.table));

    // Process WHERE clauses
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
