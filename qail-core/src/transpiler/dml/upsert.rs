//! Upsert (INSERT ... ON CONFLICT) SQL generation.

use crate::ast::*;
use crate::transpiler::dialect::Dialect;
use crate::transpiler::conditions::ConditionToSql;

/// Generate UPSERT SQL.
/// Supports Postgres ON CONFLICT, MySQL ON DUPLICATE KEY, and Oracle/SQL Server MERGE.
pub fn build_upsert(cmd: &QailCmd, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let table = generator.quote_identifier(&cmd.table);
    
    // 1. Identify PK (Conflict Target) from command columns (put::table:pk)
    let pk_cols: Vec<String> = cmd.columns.iter().filter_map(|c| match c {
        Expr::Named(n) => Some(n.clone()),
        _ => None,
    }).collect();
    
    if pk_cols.is_empty() {
        return "/* ERROR: Upsert requires specifying PK column (put::table:pk) */".to_string();
    }
    
    // 2. Extract Data from Cage
    let (data_cols, data_vals): (Vec<String>, Vec<String>) = if let Some(cage) = cmd.cages.first() {
        cage.conditions.iter().map(|c| (
            match &c.left {
                Expr::Named(name) => name.clone(),
                expr => expr.to_string(),
            },
            c.to_value_sql(&generator)
        )).unzip()
    } else {
        return "/* ERROR: No data to upsert */".to_string();
    };
    
    if data_cols.is_empty() { return "/* ERROR: Empty data */".to_string(); }
    
    // 3. Build INSERT part
    let mut sql = format!("INSERT INTO {} ({}) VALUES ({})", 
        table,
        data_cols.iter().map(|c| generator.quote_identifier(c)).collect::<Vec<_>>().join(", "),
        data_vals.join(", ")
    );
    
    // 4. Build CONFLICT part
    match dialect {
        Dialect::Postgres | Dialect::SQLite => {
            let conflict_target = pk_cols.iter()
                .map(|c| generator.quote_identifier(c))
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(" ON CONFLICT ({}) DO UPDATE SET ", conflict_target));
            
            let updates: Vec<String> = data_cols.iter()
                .filter(|c| !pk_cols.contains(c)) // Don't update PK columns
                .map(|c| {
                    let quoted = generator.quote_identifier(c);
                    format!("{} = EXCLUDED.{}", quoted, quoted)
                }).collect();
                
            if updates.is_empty() {
                sql.push_str(&pk_cols.iter().map(|c| format!("{} = EXCLUDED.{}", c, c)).collect::<Vec<_>>().join(", "));
            } else {
                sql.push_str(&updates.join(", "));
            }
            // Postgres 17+ supports RETURNING on upsert
            sql.push_str(" RETURNING *");
        },
        Dialect::MySQL | Dialect::MariaDB => {
             // MySQL doesn't use Conflict Target, it just uses ON DUPLICATE KEY
             sql.push_str(" ON DUPLICATE KEY UPDATE ");
             let updates: Vec<String> = data_cols.iter()
                .filter(|c| !pk_cols.contains(c)) 
                .map(|c| {
                    let quoted = generator.quote_identifier(c);
                    format!("{} = VALUES({})", quoted, quoted)
                }).collect();
             sql.push_str(&updates.join(", "));
        },
        Dialect::Oracle => {
            // MERGE INTO target t USING (SELECT 1 as id, 'val' as col FROM dual) s ON (t.id = s.id) ...
            let source_select_parts: Vec<String> = data_cols.iter().zip(data_vals.iter()).map(|(col, val)| {
                format!("{} AS {}", val, generator.quote_identifier(col))
            }).collect();
            let source_query = format!("SELECT {} FROM dual", source_select_parts.join(", "));
            
            sql = format!("MERGE INTO {} t USING ({}) s ON ({})", 
                table,
                source_query,
                pk_cols.iter().map(|c| format!("t.{} = s.{}", generator.quote_identifier(c), generator.quote_identifier(c))).collect::<Vec<_>>().join(" AND ")
            );
            
            // UPDATE clause
            let updates: Vec<String> = data_cols.iter()
                .filter(|c| !pk_cols.contains(c))
                .map(|c| {
                    let quoted = generator.quote_identifier(c);
                    format!("t.{} = s.{}", quoted, quoted)
                }).collect();
            
            if !updates.is_empty() {
                sql.push_str(&format!(" WHEN MATCHED THEN UPDATE SET {}", updates.join(", ")));
            }
            
            // INSERT clause
            sql.push_str(&format!(" WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})",
                 data_cols.iter().map(|c| generator.quote_identifier(c)).collect::<Vec<_>>().join(", "),
                 data_cols.iter().map(|c| format!("s.{}", generator.quote_identifier(c))).collect::<Vec<_>>().join(", ")
            ));
        },
        Dialect::SqlServer => {
            // MERGE INTO target t USING (VALUES (1, 'val')) AS s(id, col) ON t.id = s.id ...
            sql = format!("MERGE INTO {} t USING (VALUES ({})) AS s({}) ON ({})", 
                table,
                data_vals.join(", "),
                data_cols.iter().map(|c| generator.quote_identifier(c)).collect::<Vec<_>>().join(", "),
                pk_cols.iter().map(|c| format!("t.{} = s.{}", generator.quote_identifier(c), generator.quote_identifier(c))).collect::<Vec<_>>().join(" AND ")
            );
             
            // UPDATE clause
            let updates: Vec<String> = data_cols.iter()
                .filter(|c| !pk_cols.contains(c))
                .map(|c| {
                    let quoted = generator.quote_identifier(c);
                    format!("t.{} = s.{}", quoted, quoted)
                }).collect();
                
            if !updates.is_empty() {
                sql.push_str(&format!(" WHEN MATCHED THEN UPDATE SET {}", updates.join(", ")));
            }
             
            sql.push_str(&format!(" WHEN NOT MATCHED THEN INSERT ({}) VALUES ({});", 
                 data_cols.iter().map(|c| generator.quote_identifier(c)).collect::<Vec<_>>().join(", "),
                 data_cols.iter().map(|c| format!("s.{}", generator.quote_identifier(c))).collect::<Vec<_>>().join(", ")
            ));
        },
        _ => {
            return format!("/* UPSERT NOT SUPPORTED FOR {:?} */", dialect);
        }
    }
    
    sql
}
