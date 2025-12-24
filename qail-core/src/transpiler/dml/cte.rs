//! CTE (Common Table Expression) SQL generation.

use crate::ast::*;
use crate::transpiler::dialect::Dialect;
use crate::transpiler::dml::select::build_select;

/// Generate CTE SQL with support for multiple CTEs and RECURSIVE.
/// 
/// Supports:
/// - Single CTE: `WITH x AS (...) SELECT ...`
/// - Multiple CTEs: `WITH x AS (...), y AS (...), z AS (...) SELECT ...`
/// - Recursive CTEs: `WITH RECURSIVE x AS (base UNION ALL recursive) SELECT ...`
pub fn build_cte(cmd: &QailCmd, dialect: Dialect) -> String {
    let generator = dialect.generator();
    
    // If no CTEs, just return a select
    if cmd.ctes.is_empty() {
        return build_select(cmd, dialect);
    }
    
    let mut sql = String::from("WITH ");
    
    // Check if ANY CTE is recursive (Postgres requires RECURSIVE keyword once)
    let has_recursive = cmd.ctes.iter().any(|c| c.recursive);
    if has_recursive {
        sql.push_str("RECURSIVE ");
    }
    
    // Build each CTE
    let cte_parts: Vec<String> = cmd.ctes.iter()
        .map(|cte| build_single_cte(cte, dialect))
        .collect();
    
    sql.push_str(&cte_parts.join(", "));
    
    // Final SELECT from the last CTE (or the cmd's table)
    let final_table = if !cmd.ctes.is_empty() {
        &cmd.ctes.last().unwrap().name
    } else {
        &cmd.table
    };
    
    sql.push_str(" SELECT * FROM ");
    sql.push_str(&generator.quote_identifier(final_table));

    sql
}

/// Build a single CTE definition (without the WITH keyword)
pub fn build_single_cte(cte: &CTEDef, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let mut sql = String::new();
    
    // CTE name and optional column list
    sql.push_str(&generator.quote_identifier(&cte.name));
    if !cte.columns.is_empty() {
        sql.push('(');
        let cols: Vec<String> = cte.columns.iter()
            .map(|c| generator.quote_identifier(c))
            .collect();
        sql.push_str(&cols.join(", "));
        sql.push(')');
    }
    
    sql.push_str(" AS (");
    
    // Base query - check if it's raw SQL passthrough
    // Raw SQL is stored when table contains SQL keywords and columns are empty/star
    let is_raw_sql = cte.base_query.columns.iter().all(|c| matches!(c, Expr::Star))
        && (cte.base_query.table.to_lowercase().starts_with("select ")
            || cte.base_query.table.to_lowercase().contains(" from "));
    
    if is_raw_sql {
        // Pass through raw SQL directly
        sql.push_str(&cte.base_query.table);
    } else {
        sql.push_str(&build_select(&cte.base_query, dialect));
    }
    
    // Recursive part (if RECURSIVE)
    if cte.recursive {
        if let Some(ref recursive_query) = cte.recursive_query {
            sql.push_str(" UNION ALL ");
            sql.push_str(&build_select(recursive_query, dialect));
        }
    }
    
    sql.push(')');
    sql
}

