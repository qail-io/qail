//! JSON_TABLE SQL generation.

use crate::ast::*;
use crate::transpiler::dialect::Dialect;

/// Generate JSON_TABLE SQL.
/// 
/// QAIL Syntax: `jtable::orders.items [$[*]] :product_name=$.name,quantity=$.qty`
/// 
/// Generates:
/// ```sql
/// SELECT jt.* FROM orders,
/// JSON_TABLE(orders.items, '$[*]' COLUMNS (
///     product_name TEXT PATH '$.name',
///     quantity INT PATH '$.qty'
/// )) AS jt;
/// ```
pub fn build_json_table(cmd: &QailCmd, dialect: Dialect) -> String {
    let generator = dialect.generator();
    
    // Parse source: "orders.items" -> table: "orders", col: "items"
    let parts: Vec<&str> = cmd.table.split('.').collect();
    let (source_table, source_col) = if parts.len() >= 2 {
        (parts[0], parts[1..].join("."))
    } else {
        // If no table is specified, treat as a column reference.
        ("_", cmd.table.clone())
    };
    
    // Extract path from first cage (e.g., [$[*]])
    let path = if let Some(cage) = cmd.cages.first() {
        if let CageKind::Filter = cage.kind {
            if let Some(cond) = cage.conditions.first() {
                // The "column" is actually the path without leading $
                match &cond.left {
                    Expr::Named(col) => {
                        if col.starts_with('$') {
                            col.clone()
                        } else {
                            format!("${}", col)
                        }
                    },
                    _ => "$[*]".to_string()
                }
            } else {
                "$[*]".to_string()
            }
        } else {
            "$[*]".to_string()
        }
    } else {
        "$[*]".to_string()
    };
    
    // Extract column definitions from cmd.columns
    // Column::Named("product_name=$.name") -> product_name TEXT PATH '$.name'
    let column_defs: Vec<String> = cmd.columns.iter().filter_map(|c| {
        match c {
            Expr::Named(def) => {
                // Parse "name=$.path" or just "name"
                if let Some((name, json_path)) = def.split_once('=') {
                    // Default type TEXT
                    Some(format!("{} TEXT PATH '{}'", generator.quote_identifier(name), json_path))
                } else {
                    // If no path specified, use $.name
                    Some(format!("{} TEXT PATH '$.{}'", generator.quote_identifier(def), def))
                }
            },
            Expr::Def { name, data_type, .. } => {
                // If using Column::Def, data_type might contain the path
                Some(format!("{} {} PATH '$.{}'", generator.quote_identifier(name), data_type, name))
            },
            _ => None
        }
    }).collect();
    
    if column_defs.is_empty() {
        return "/* ERROR: JSON_TABLE requires column definitions (e.g., :name=$.path) */".to_string();
    }
    
    // Build the SQL
    let source_ref = if source_table == "_" {
        source_col.clone()
    } else {
        format!("{}.{}", generator.quote_identifier(source_table), generator.quote_identifier(&source_col))
    };
    
    let sql = format!(
        "SELECT jt.* FROM {}, JSON_TABLE({}, '{}' COLUMNS ({})) AS jt",
        if source_table == "_" { "dual".to_string() } else { generator.quote_identifier(source_table) },
        source_ref,
        path,
        column_defs.join(", ")
    );
    
    sql
}
