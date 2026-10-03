//! CTE (Common Table Expression) SQL generation.

use crate::ast::*;
use crate::transpiler::dialect::Dialect;
use crate::transpiler::dml::select::{
    build_select, build_select_without_cte_prefix, build_set_operand,
};

/// Generate CTE SQL with support for multiple CTEs and RECURSIVE.
/// Supports:
/// - Single CTE: `WITH x AS (...) SELECT ...`
/// - Multiple CTEs: `WITH x AS (...), y AS (...), z AS (...) SELECT ...`
/// - Recursive CTEs: `WITH RECURSIVE x AS (base UNION ALL recursive) SELECT ...`
pub fn build_cte(cmd: &Qail, dialect: Dialect) -> String {
    // If no CTEs, just return a select
    if cmd.ctes.is_empty() {
        return build_select(cmd, dialect);
    }

    let mut sql = String::from("WITH ");

    let has_recursive = cmd.ctes.iter().any(|c| c.recursive);
    if has_recursive {
        sql.push_str("RECURSIVE ");
    }

    let cte_parts: Vec<String> = cmd
        .ctes
        .iter()
        .map(|cte| build_single_cte(cte, dialect))
        .collect();

    sql.push_str(&cte_parts.join(", "));

    sql.push(' ');
    if cmd.table.is_empty()
        && let Some(final_table) = cmd.ctes.last().map(|cte| &cte.name)
    {
        let mut final_query = cmd.clone();
        final_query.ctes.clear();
        final_query.table = final_table.clone();
        sql.push_str(&build_select_without_cte_prefix(&final_query, dialect));
    } else {
        sql.push_str(&build_select_without_cte_prefix(cmd, dialect));
    }

    sql
}

/// `WITH [RECURSIVE] ... ` prefix for INSERT, UPDATE, and DELETE, or empty.
pub(crate) fn build_write_with_prefix(cmd: &Qail, dialect: Dialect) -> String {
    if cmd.ctes.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = cmd
        .ctes
        .iter()
        .map(|cte| build_single_cte(cte, dialect))
        .collect();
    if cmd.ctes.iter().any(|c| c.recursive) {
        format!("WITH RECURSIVE {} ", parts.join(", "))
    } else {
        format!("WITH {} ", parts.join(", "))
    }
}

/// Build a single CTE definition (without the WITH keyword)
pub fn build_single_cte(cte: &CTEDef, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let mut sql = String::new();

    // CTE name and optional column list
    sql.push_str(&generator.quote_identifier(&cte.name));
    if !cte.columns.is_empty() {
        sql.push('(');
        let cols: Vec<String> = cte
            .columns
            .iter()
            .map(|c| generator.quote_identifier(c))
            .collect();
        sql.push_str(&cols.join(", "));
        sql.push(')');
    }

    sql.push_str(" AS (");

    // Data-modifying bodies keep their own statement and RETURNING relation.
    if matches!(
        cte.base_query.action,
        Action::Add | Action::Set | Action::Del
    ) {
        use crate::transpiler::ToSql;
        if cte.recursive_query.is_some() {
            sql.push_str("/* ERROR: data-modifying CTE cannot have a recursive arm */");
        } else {
            sql.push_str(&cte.base_query.to_sql_with_dialect(dialect));
        }
        sql.push(')');
        return sql;
    }

    sql.push_str(&build_set_operand(&cte.base_query, dialect));

    // Recursive part (if RECURSIVE)
    if cte.recursive
        && let Some(ref recursive_query) = cte.recursive_query
    {
        sql.push_str(" UNION ALL ");
        sql.push_str(&build_set_operand(recursive_query, dialect));
    }

    sql.push(')');
    sql
}
