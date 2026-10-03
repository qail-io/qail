//! CTE (Common Table Expression) SQL generation.

use crate::ast::*;
use crate::transpiler::SqlGenerator;
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

    sql.push_str(match cte.materialization {
        None => " AS (",
        Some(CteMaterialization::Materialized) => " AS MATERIALIZED (",
        Some(CteMaterialization::NotMaterialized) => " AS NOT MATERIALIZED (",
    });

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

    match cte_search_cycle_sql(cte, generator.as_ref()) {
        Ok(suffix) => sql.push_str(&suffix),
        Err(reason) => sql.push_str(&format!(" /* ERROR: {reason} */")),
    }
    sql
}

/// ` SEARCH ... CYCLE ...` suffix of a CTE, or why PostgreSQL would reject it.
fn cte_search_cycle_sql(cte: &CTEDef, generator: &dyn SqlGenerator) -> Result<String, String> {
    if cte.search.is_none() && cte.cycle.is_none() {
        return Ok(String::new());
    }
    if !cte.recursive || cte.recursive_query.is_none() {
        return Err("SEARCH/CYCLE requires a recursive CTE".to_string());
    }
    let names = |names: &[String]| -> Result<String, String> {
        if names.is_empty() {
            return Err("SEARCH/CYCLE requires at least one column".to_string());
        }
        names
            .iter()
            .map(|name| checked_name(name, generator))
            .collect::<Result<Vec<_>, _>>()
            .map(|names| names.join(", "))
    };
    let mut sql = String::new();
    if let Some(search) = &cte.search {
        let order = match search.order {
            CteSearchOrder::DepthFirst => "DEPTH",
            CteSearchOrder::BreadthFirst => "BREADTH",
        };
        sql.push_str(&format!(
            " SEARCH {order} FIRST BY {} SET {}",
            names(&search.by)?,
            checked_name(&search.set_column, generator)?
        ));
    }
    if let Some(cycle) = &cte.cycle {
        sql.push_str(&format!(
            " CYCLE {} SET {} USING {}",
            names(&cycle.columns)?,
            checked_name(&cycle.set_column, generator)?,
            checked_name(&cycle.using_column, generator)?
        ));
    }
    Ok(sql)
}

fn checked_name(name: &str, generator: &dyn SqlGenerator) -> Result<String, String> {
    if name.is_empty() || name.contains('.') || name.contains('\0') {
        Err("invalid SEARCH/CYCLE column name".to_string())
    } else {
        Ok(generator.quote_identifier(name))
    }
}
