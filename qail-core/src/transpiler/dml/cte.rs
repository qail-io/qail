//! CTE (Common Table Expression) SQL generation.

use crate::ast::*;
use crate::transpiler::dialect::Dialect;
use crate::transpiler::dml::select::build_select;

/// Generate CTE SQL with RECURSIVE support.
pub fn build_cte(cmd: &QailCmd, dialect: Dialect) -> String {
    let generator = dialect.generator();
    
    // Check if we have a CTEDef with RECURSIVE support
    if let Some(cte_def) = &cmd.cte {
        return build_cte_from_def(cte_def, cmd, dialect);
    }
    
    // Legacy: Simple CTE from table name
    let mut sql = String::from("WITH ");
    sql.push_str(&generator.quote_identifier(&cmd.table));
    sql.push_str(" AS (");
    sql.push_str(&build_select(cmd, dialect));
    sql.push_str(") SELECT * FROM ");
    sql.push_str(&generator.quote_identifier(&cmd.table));

    sql
}

/// Build CTE from CTEDef structure (supports RECURSIVE)
fn build_cte_from_def(cte: &CTEDef, _outer_cmd: &QailCmd, dialect: Dialect) -> String {
    let generator = dialect.generator();
    
    let mut sql = String::from("WITH ");
    
    // RECURSIVE keyword
    if cte.recursive {
        sql.push_str("RECURSIVE ");
    }
    
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
    
    // Base query (anchor member)
    sql.push_str(&build_select(&cte.base_query, dialect));
    
    // Recursive part (if RECURSIVE)
    if cte.recursive {
        if let Some(ref recursive_query) = cte.recursive_query {
            sql.push_str(" UNION ALL ");
            sql.push_str(&build_select(recursive_query, dialect));
        }
    }
    
    sql.push_str(") SELECT * FROM ");
    sql.push_str(&generator.quote_identifier(&cte.name));

    sql
}
