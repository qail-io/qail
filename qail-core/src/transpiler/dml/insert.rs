//! INSERT SQL generation.

use crate::ast::*;
use crate::transpiler::dialect::Dialect;
use crate::transpiler::conditions::ConditionToSql;

/// Generate INSERT SQL.
pub fn build_insert(cmd: &QailCmd, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let mut sql = String::from("INSERT INTO ");
    sql.push_str(&generator.quote_identifier(&cmd.table));

    // For ADD queries, we use columns and first cage contains values
    let cols: Vec<String> = cmd.columns.iter().map(|c| c.to_string()).collect(); 
    
    if !cols.is_empty() {
        sql.push_str(" (");
        sql.push_str(&cols.join(", "));
        sql.push(')');
    }

    // Values from first payload/filter cage
    if let Some(cage) = cmd.cages.first() {
        let values: Vec<String> = cage.conditions.iter().map(|c| c.to_value_sql(&generator)).collect();
        
        if !values.is_empty() {
            sql.push_str(" VALUES (");
            sql.push_str(&values.join(", "));
            sql.push(')');
        }
    }

    // RETURNING clause - configurable
    match &cmd.returning {
        None => sql.push_str(" RETURNING *"), // Default: return all
        Some(cols) if cols.is_empty() => {}, // Explicitly no RETURNING
        Some(cols) => {
            let col_strs: Vec<String> = cols.iter().map(|e| e.to_string()).collect();
            sql.push_str(" RETURNING ");
            sql.push_str(&col_strs.join(", "));
        }
    }

    sql
}
