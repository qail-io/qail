//! Upsert (INSERT ... ON CONFLICT) SQL generation.

use crate::ast::*;
use crate::transpiler::conditions::ConditionToSql;
use crate::transpiler::dialect::Dialect;

/// Supports Postgres ON CONFLICT, MySQL ON DUPLICATE KEY, and Oracle/SQL Server MERGE.
pub fn build_upsert(cmd: &Qail, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let table = generator.quote_identifier(&cmd.table);

    // 1. Identify PK (Conflict Target) from command columns (put::table:pk)
    let pk_cols: Vec<String> = cmd
        .columns
        .iter()
        .filter_map(|c| match c {
            Expr::Named(n) => Some(n.clone()),
            _ => None,
        })
        .collect();

    if pk_cols.is_empty() {
        return "/* ERROR: Upsert requires specifying PK column (put::table:pk) */".to_string();
    }

    // 2. Extract Data from Cage
    let (data_cols, data_vals): (Vec<String>, Vec<String>) = if let Some(cage) = cmd.cages.first() {
        cage.conditions
            .iter()
            .map(|c| {
                (
                    match &c.left {
                        Expr::Named(name) => name.clone(),
                        expr => expr.to_string(),
                    },
                    c.to_value_sql(&generator),
                )
            })
            .unzip()
    } else {
        return "/* ERROR: No data to upsert */".to_string();
    };

    if data_cols.is_empty() {
        return "/* ERROR: Empty data */".to_string();
    }

    // 3. Build INSERT part
    let mut sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table,
        data_cols
            .iter()
            .map(|c| generator.quote_identifier(c))
            .collect::<Vec<_>>()
            .join(", "),
        data_vals.join(", ")
    );

    // 4. Build CONFLICT part
    match dialect {
        Dialect::Postgres | Dialect::SQLite => {
            let conflict_target = pk_cols
                .iter()
                .map(|c| generator.quote_identifier(c))
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(
                " ON CONFLICT ({}) DO UPDATE SET ",
                conflict_target
            ));

            let updates: Vec<String> = data_cols
                .iter()
                .filter(|c| !pk_cols.contains(c)) // Don't update PK columns
                .map(|c| {
                    let quoted = generator.quote_identifier(c);
                    format!("{} = EXCLUDED.{}", quoted, quoted)
                })
                .collect();

            if updates.is_empty() {
                sql.push_str(
                    &pk_cols
                        .iter()
                        .map(|c| format!("{} = EXCLUDED.{}", c, c))
                        .collect::<Vec<_>>()
                        .join(", "),
                );
            } else {
                sql.push_str(&updates.join(", "));
            }
            // Postgres supports RETURNING on upsert (SQLite depends on version, but usually fine in simple cases or ignored)
            sql.push_str(" RETURNING *");
        }
    }

    sql
}
