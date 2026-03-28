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
                        expr => expr.to_string(),
                    };
                    set_clauses.push(format!("{} = {}", col_sql, cond.to_value_sql(&generator)));
                }
            }
            CageKind::Filter => {
                if !cage.conditions.is_empty() {
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

    sql
}
