//! UPDATE SQL generation.

use crate::ast::write_payload::{check_update_shape, update_assignments, update_write_target};
use crate::ast::*;
use crate::transpiler::conditions::{ConditionToSql, output_expr_sql, returning_clause_sql};
use crate::transpiler::dialect::Dialect;
use crate::transpiler::identifier::render_table_reference;

/// Generate UPDATE SQL with SET, FROM, and WHERE clauses.
pub fn build_update(cmd: &Qail, dialect: Dialect) -> String {
    if let Err(error) = check_update_shape(cmd, update_write_target, |message| message) {
        return crate::transpiler::dml::insert::shape_error_comment(&error);
    }

    let generator = dialect.generator();
    let mut sql = super::cte::build_write_with_prefix(cmd, dialect);
    sql.push_str(if cmd.only_table {
        "UPDATE ONLY "
    } else {
        "UPDATE "
    });
    sql.push_str(&render_table_reference(&cmd.table, generator.as_ref()));

    let set_clauses: Vec<String> = update_assignments(cmd)
        .into_iter()
        .map(|(column, cond)| {
            let col_sql = render_update_target(column, generator.as_ref())
                .unwrap_or_else(|| "/* ERROR: Invalid update column */".to_string());
            format!("{} = {}", col_sql, cond.to_value_sql(generator.as_ref()))
        })
        .collect();
    let mut where_groups: Vec<String> = Vec::new();

    for cage in &cmd.cages {
        match cage.kind {
            CageKind::Filter if !cage.conditions.is_empty() => {
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
                .map(|t| render_table_reference(t, generator.as_ref()))
                .collect::<Vec<_>>()
                .join(", "),
        );
    }

    if !where_groups.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_groups.join(" AND "));
    }

    sql.push_str(&returning_clause_sql(cmd, generator.as_ref(), |expr| {
        output_expr_sql(expr, generator.as_ref())
    }));

    sql
}

/// SET target: `col`, `col[1]`, `col[idx_col]`, `col.field`, chained. PostgreSQL
/// rejects table qualifiers and parentheses in this position.
fn render_update_target(
    expr: &Expr,
    generator: &dyn crate::transpiler::SqlGenerator,
) -> Option<String> {
    match expr {
        Expr::Named(name) => Some(generator.quote_identifier(name)),
        Expr::Subscript {
            expr,
            index,
            alias: None,
        } => {
            let index = match index.as_ref() {
                Expr::Literal(Value::Int(n)) => n.to_string(),
                Expr::Named(column) if column.split('.').all(is_target_atom) => {
                    generator.quote_identifier(column)
                }
                _ => return None,
            };
            Some(format!(
                "{}[{}]",
                render_update_target_base(expr, generator)?,
                index
            ))
        }
        Expr::FieldAccess {
            expr,
            field,
            alias: None,
        } if is_target_atom(field) => Some(format!(
            "{}.{}",
            render_update_target_base(expr, generator)?,
            generator.quote_identifier(field)
        )),
        _ => None,
    }
}

fn render_update_target_base(
    expr: &Expr,
    generator: &dyn crate::transpiler::SqlGenerator,
) -> Option<String> {
    match expr {
        // A dotted base would read as `column.field`, not `table.column`.
        Expr::Named(name) if !is_target_atom(name) => None,
        other => render_update_target(other, generator),
    }
}

/// Same atom rule as the native encoder: letters, digits, underscore.
fn is_target_atom(name: &str) -> bool {
    !name.is_empty() && name.len() <= 63 && name.chars().all(|c| c.is_alphanumeric() || c == '_')
}
