//! DELETE SQL generation.

use crate::ast::*;
use crate::transpiler::conditions::ConditionToSql;
use crate::transpiler::dialect::Dialect;

/// Generate DELETE FROM SQL with optional USING and WHERE clauses.
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
                .map(|c| c.to_sql(&generator, Some(cmd)))
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
