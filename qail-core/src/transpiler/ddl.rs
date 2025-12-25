use crate::ast::*;
use super::dialect::Dialect;
// use super::traits::SqlGenerator;

/// Generate CREATE TABLE SQL.
pub fn build_create_table(cmd: &QailCmd, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let mut sql = String::new();
    sql.push_str("CREATE TABLE ");
    sql.push_str(&generator.quote_identifier(&cmd.table));
    sql.push_str(" (\n");

    let mut defs = Vec::new();
    for col in &cmd.columns {
        if let Expr::Def {
            name,
            data_type,
            constraints,
        } = col
        {
            let sql_type = map_type(data_type);
            let mut line = format!("    {} {}", generator.quote_identifier(name), sql_type);

            // Default to NOT NULL unless Nullable (?) constraint is present
            let is_nullable = constraints.contains(&Constraint::Nullable);
            if !is_nullable {
                line.push_str(" NOT NULL");
            }

            // Handle DEFAULT value
            for constraint in constraints {
                if let Constraint::Default(val) = constraint {
                    line.push_str(" DEFAULT ");
                    // Map common functions to SQL equivalents
                    let sql_default = match val.as_str() {
                        "uuid()" => "gen_random_uuid()",
                        "now()" => "NOW()",
                        other => other,
                    };
                    line.push_str(sql_default);
                }
            }

            if constraints.contains(&Constraint::PrimaryKey) {
                line.push_str(" PRIMARY KEY");
            }
            if constraints.contains(&Constraint::Unique) {
                line.push_str(" UNIQUE");
            }

            // Handle CHECK constraint
            for constraint in constraints {
                if let Constraint::Check(vals) = constraint {
                    line.push_str(&format!(
                        " CHECK ({} IN ({}))",
                        generator.quote_identifier(name),
                        vals.iter().map(|v| format!("'{}'", v)).collect::<Vec<_>>().join(", ")
                    ));
                }
            }

            defs.push(line);
        }
    }
    
    // Add table-level constraints
    for tc in &cmd.table_constraints {
        match tc {
            TableConstraint::Unique(cols) => {
                let col_list = cols.iter()
                    .map(|c| generator.quote_identifier(c))
                    .collect::<Vec<_>>()
                    .join(", ");
                defs.push(format!("    UNIQUE ({})", col_list));
            }
            TableConstraint::PrimaryKey(cols) => {
                let col_list = cols.iter()
                    .map(|c| generator.quote_identifier(c))
                    .collect::<Vec<_>>()
                    .join(", ");
                defs.push(format!("    PRIMARY KEY ({})", col_list));
            }
        }
    }
    
    sql.push_str(&defs.join(",\n"));
    sql.push_str("\n)");
    
    // Generate COMMENT ON statements
    let mut comments = Vec::new();
    for col in &cmd.columns {
        if let Expr::Def { name, constraints, .. } = col {
            for c in constraints {
                if let Constraint::Comment(text) = c {
                    comments.push(format!(
                        "COMMENT ON COLUMN {}.{} IS '{}'",
                        generator.quote_identifier(&cmd.table),
                        generator.quote_identifier(name),
                        text.replace('\'', "''")
                    ));
                }
            }
        }
    }
    if !comments.is_empty() {
        sql.push_str(";\n");
        sql.push_str(&comments.join(";\n"));
    }
    
    sql
}

/// Generate ALTER TABLE SQL.
pub fn build_alter_table(cmd: &QailCmd, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let mut stmts = Vec::new();
    let table_name = generator.quote_identifier(&cmd.table);

    for col in &cmd.columns {
        match col {
            Expr::Mod { kind, col } => {
                match kind {
                    ModKind::Add => {
                        if let Expr::Def { name, data_type, constraints } = col.as_ref() {
                            let sql_type = map_type(data_type);
                            let mut line = format!("ALTER TABLE {} ADD COLUMN {} {}", table_name, generator.quote_identifier(name), sql_type);
                            
                            let is_nullable = constraints.contains(&Constraint::Nullable);
                            if !is_nullable {
                                line.push_str(" NOT NULL");
                            }

                            if constraints.contains(&Constraint::Unique) {
                                line.push_str(" UNIQUE");
                            }
                            stmts.push(line);
                        }
                    }
                    ModKind::Drop => {
                        if let Expr::Named(name) = col.as_ref() {
                            stmts.push(format!("ALTER TABLE {} DROP COLUMN {}", table_name, generator.quote_identifier(name)));
                        }
                    }
                }
            }
            // Handle rename: "old_name -> new_name" format
            Expr::Named(rename_expr) if rename_expr.contains(" -> ") => {
                let parts: Vec<&str> = rename_expr.split(" -> ").collect();
                if parts.len() == 2 {
                    let old_name = parts[0].trim();
                    let new_name = parts[1].trim();
                    stmts.push(format!(
                        "ALTER TABLE {} RENAME COLUMN {} TO {}",
                        table_name,
                        generator.quote_identifier(old_name),
                        generator.quote_identifier(new_name)
                    ));
                }
            }
            _ => {}
        }
    }
    stmts.join(";\n")
}

/// Generate CREATE INDEX SQL.
pub fn build_create_index(cmd: &QailCmd, dialect: Dialect) -> String {
    let generator = dialect.generator();
    match &cmd.index_def {
        Some(idx) => {
            let unique = if idx.unique { "UNIQUE " } else { "" };
            let cols = idx.columns.iter()
                .map(|c| generator.quote_identifier(c))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "CREATE {}INDEX {} ON {} ({})",
                unique,
                generator.quote_identifier(&idx.name),
                generator.quote_identifier(&idx.table),
                cols
            )
        }
        None => String::new(),
    }
}

fn map_type(t: &str) -> &str {
    match t {
        "str" | "text" | "string" => "VARCHAR(255)",
        "int" | "i32" => "INT",
        "bigint" | "i64" => "BIGINT",
        "uuid" => "UUID",
        "bool" | "boolean" => "BOOLEAN",
        "dec" | "decimal" => "DECIMAL",
        "float" | "f64" => "DOUBLE PRECISION",
        "serial" => "SERIAL",
        "timestamp" | "time" => "TIMESTAMP",
        "json" | "jsonb" => "JSONB",
        _ => t,
    }
}

// Stub
pub fn build_alter_column(cmd: &QailCmd, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let table = generator.quote_identifier(&cmd.table);
    
    // Identified columns (target column)
    let cols: Vec<String> = cmd.columns.iter().filter_map(|c| match c {
        Expr::Named(n) => Some(n.clone()),
        _ => None,
    }).collect();
    
    if cols.is_empty() { return "/* ERROR: Column required */".to_string(); }
    let col_name = &cols[0];
    let quoted_col = generator.quote_identifier(col_name);
    
    match cmd.action {
        Action::DropCol => {
            format!("ALTER TABLE {} DROP COLUMN {}", table, quoted_col)
        },
        Action::RenameCol => {
            // Find "to" or "new" in cages
            // Syntax: rename::users:old[to=new]
            let new_name_opt = cmd.cages.iter()
                .flat_map(|c| &c.conditions)
                .find(|c| {
                    let col = match &c.left { Expr::Named(n) => n.as_str(), _ => "" };
                    matches!(col, "to" | "new" | "rename")
                })
                .map(|c| match &c.value {
                    Value::String(s) => s.clone(),
                    Value::Param(_) => "PARAM".to_string(), // unsupported
                    _ => c.value.to_string(),
                });
            
            if let Some(new_name) = new_name_opt {
                 let quoted_new = generator.quote_identifier(&new_name);
                 format!("ALTER TABLE {} RENAME COLUMN {} TO {}", table, quoted_col, quoted_new)
            } else {
                "/* ERROR: New name required (e.g. [to=new_name]) */".to_string()
            }
        },
        _ => "/* ERROR: Unknown Column Action */".to_string()
    }
}

/// Generate ALTER TABLE ADD COLUMN SQL (for migrations).
pub fn build_alter_add_column(cmd: &QailCmd, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let table = generator.quote_identifier(&cmd.table);
    
    let mut parts = Vec::new();
    
    for col in &cmd.columns {
        if let Expr::Def { name, data_type, constraints } = col {
            let sql_type = map_type(data_type);
            let quoted_name = generator.quote_identifier(name);
            
            let mut col_def = format!("{} {}", quoted_name, sql_type);
            
            let is_nullable = constraints.contains(&Constraint::Nullable);
            if !is_nullable {
                col_def.push_str(" NOT NULL");
            }
            
            for constraint in constraints {
                if let Constraint::Default(val) = constraint {
                    col_def.push_str(" DEFAULT ");
                    let sql_default = match val.as_str() {
                        "uuid()" => "gen_random_uuid()",
                        "now()" => "NOW()",
                        other => other,
                    };
                    col_def.push_str(sql_default);
                }
            }
            
            parts.push(format!("ALTER TABLE {} ADD COLUMN {}", table, col_def));
        }
    }
    
    parts.join(";\n")
}

/// Generate ALTER TABLE DROP COLUMN SQL (for migrations).
pub fn build_alter_drop_column(cmd: &QailCmd, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let table = generator.quote_identifier(&cmd.table);
    
    let mut parts = Vec::new();
    
    for col in &cmd.columns {
        let col_name = match col {
            Expr::Named(n) => n.clone(),
            Expr::Def { name, .. } => name.clone(),
            _ => continue,
        };
        
        let quoted_col = generator.quote_identifier(&col_name);
        parts.push(format!("ALTER TABLE {} DROP COLUMN {}", table, quoted_col));
    }
    
    parts.join(";\n")
}
