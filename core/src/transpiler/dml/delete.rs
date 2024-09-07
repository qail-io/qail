//! DELETE SQL generation.

use crate::ast::*;
use crate::transpiler::conditions::ConditionToSql;
use crate::transpiler::dialect::Dialect;

pub fn build_delete(cmd: &Qail, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let mut sql = if cmd.only_table {
        String::from("DELETE FROM ONLY ")
    } else {
        String::from("DELETE FROM ")
    };
    sql.push_str(&generator.quote_identifier(&cmd.table));

    // USING clause (multi-table delete)
    if !cmd.using_tables.is_empty() {
        sql.push_str(" USING ");
        sql.push_str(
            &cmd.using_tables
                .iter()
                .map(|t| generator.quote_identifier(t))
                .collect::<Vec<_>>()
                .join(", "),
        );
    }

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
