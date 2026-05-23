//! JSON_TABLE SQL generation.

use crate::ast::*;
use crate::transpiler::dialect::Dialect;
use crate::transpiler::traits::SqlGenerator;

struct JsonTableColumn {
    name: String,
    data_type: String,
    path: String,
}

/// QAIL Syntax: `jtable::orders.items [$[*]] :product_name=$.name,quantity=$.qty`
/// Generates:
/// ```sql
/// SELECT jt.* FROM orders,
/// JSON_TABLE(orders.items, '$[*]' COLUMNS (
///     product_name TEXT PATH '$.name',
///     quantity INT PATH '$.qty'
/// )) AS jt;
/// ```
pub fn build_json_table(cmd: &Qail, dialect: Dialect) -> String {
    let generator = dialect.generator();

    let parts: Vec<&str> = cmd.table.split('.').collect();
    let (source_table, source_col) = if parts.len() >= 2 {
        (parts[0], parts[1..].join("."))
    } else {
        // If no table is specified, treat as a column reference.
        ("_", cmd.table.clone())
    };

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
                    }
                    _ => "$[*]".to_string(),
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

    let json_columns = json_table_columns(cmd);
    let column_defs: Vec<String> = json_columns
        .iter()
        .map(|column| {
            format!(
                "{} {} PATH '{}'",
                generator.quote_identifier(&column.name),
                column.data_type,
                escape_sql_string(&column.path)
            )
        })
        .collect();

    if column_defs.is_empty() {
        return "/* ERROR: JSON_TABLE requires column definitions (e.g., :name=$.path) */"
            .to_string();
    }

    let source_ref = if source_table == "_" {
        source_col
    } else {
        format!(
            "{}.{}",
            generator.quote_identifier(source_table),
            generator.quote_identifier(&source_col)
        )
    };

    match dialect {
        Dialect::Postgres => {
            build_postgres_json_table(&*generator, source_table, &source_ref, &path, &column_defs)
        }
        Dialect::SQLite => format!(
            "SELECT jt.* FROM {}, JSON_TABLE({}, '{}' COLUMNS ({})) AS jt",
            if source_table == "_" {
                "dual".to_string()
            } else {
                generator.quote_identifier(source_table)
            },
            source_ref,
            path,
            column_defs.join(", ")
        ),
    }
}

fn json_table_columns(cmd: &Qail) -> Vec<JsonTableColumn> {
    cmd.columns
        .iter()
        .filter_map(|c| {
            match c {
                Expr::Named(def) => {
                    if let Some((name, json_path)) = def.split_once('=') {
                        // Default type TEXT
                        Some(JsonTableColumn {
                            name: name.to_string(),
                            data_type: "TEXT".to_string(),
                            path: json_path.to_string(),
                        })
                    } else {
                        // If no path specified, use $.name
                        Some(JsonTableColumn {
                            name: def.to_string(),
                            data_type: "TEXT".to_string(),
                            path: format!("$.{}", def),
                        })
                    }
                }
                Expr::Def {
                    name, data_type, ..
                } => Some(JsonTableColumn {
                    name: name.to_string(),
                    data_type: data_type.to_string(),
                    path: format!("$.{}", name),
                }),
                _ => None,
            }
        })
        .collect()
}

fn build_postgres_json_table(
    generator: &dyn SqlGenerator,
    source_table: &str,
    source_ref: &str,
    path: &str,
    column_defs: &[String],
) -> String {
    let json_table = format!(
        "JSON_TABLE({}, '{}' COLUMNS ({})) AS jt",
        source_ref,
        escape_sql_string(path),
        column_defs.join(", ")
    );

    if source_table == "_" {
        format!("SELECT jt.* FROM {}", json_table)
    } else {
        format!(
            "SELECT jt.* FROM {}, {}",
            generator.quote_identifier(source_table),
            json_table
        )
    }
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}
