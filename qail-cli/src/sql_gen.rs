//! SQL generation utilities for migrations

use qail_core::prelude::*;

/// Convert QailCmd to SQL string for preview.
pub fn cmd_to_sql(cmd: &QailCmd) -> String {
    // Generate SQL for migration DDL operations
    match cmd.action {
        Action::Make => {
            // CREATE TABLE
            let mut sql = format!("CREATE TABLE {} (", cmd.table);
            let cols: Vec<String> = cmd.columns.iter().filter_map(|col| {
                if let Expr::Def { name, data_type, constraints } = col {
                    let mut col_def = format!("{} {}", name, data_type);
                    for c in constraints {
                        match c {
                            Constraint::PrimaryKey => col_def.push_str(" PRIMARY KEY"),
                            Constraint::Nullable => {},
                            Constraint::Unique => col_def.push_str(" UNIQUE"),
                            Constraint::Default(v) => col_def.push_str(&format!(" DEFAULT {}", v)),
                            _ => {},
                        }
                    }
                    Some(col_def)
                } else {
                    None
                }
            }).collect();
            sql.push_str(&cols.join(", "));
            sql.push(')');
            sql
        },
        Action::Drop => {
            format!("DROP TABLE IF EXISTS {}", cmd.table)
        },
        Action::Alter => {
            // ADD COLUMN
            if let Some(Expr::Def { name, data_type, constraints }) = cmd.columns.first() {
                let mut sql = format!("ALTER TABLE {} ADD COLUMN {} {}", cmd.table, name, data_type);
                for c in constraints {
                    match c {
                        Constraint::Nullable => {},
                        Constraint::Unique => sql.push_str(" UNIQUE"),
                        Constraint::Default(v) => sql.push_str(&format!(" DEFAULT {}", v)),
                        _ => {},
                    }
                }
                return sql;
            }
            format!("ALTER TABLE {} ADD COLUMN ...", cmd.table)
        },
        Action::AlterDrop => {
            // DROP COLUMN
            if let Some(Expr::Named(name)) = cmd.columns.first() {
                return format!("ALTER TABLE {} DROP COLUMN {}", cmd.table, name);
            }
            if let Some(Expr::Def { name, .. }) = cmd.columns.first() {
                return format!("ALTER TABLE {} DROP COLUMN {}", cmd.table, name);
            }
            format!("ALTER TABLE {} DROP COLUMN ...", cmd.table)
        },
        Action::Index => {
            if let Some(ref idx) = cmd.index_def {
                let unique = if idx.unique { "UNIQUE " } else { "" };
                return format!("CREATE {}INDEX {} ON {} ({})", 
                    unique, idx.name, cmd.table, idx.columns.join(", "));
            }
            format!("CREATE INDEX ON {} (...)", cmd.table)
        },
        Action::DropIndex => {
            if let Some(ref idx) = cmd.index_def {
                return format!("DROP INDEX IF EXISTS {}", idx.name);
            }
            "DROP INDEX ...".to_string()
        },
        Action::Mod => {
            // RENAME COLUMN
            format!("ALTER TABLE {} RENAME COLUMN ... TO ...", cmd.table)
        },
        Action::AlterType => {
            // ALTER COLUMN TYPE
            if let Some(Expr::Def { name, data_type, .. }) = cmd.columns.first() {
                return format!("ALTER TABLE {} ALTER COLUMN {} TYPE {}", cmd.table, name, data_type);
            }
            format!("ALTER TABLE {} ALTER COLUMN ... TYPE ...", cmd.table)
        },
        _ => format!("-- Unsupported action: {:?}", cmd.action),
    }
}

/// Generate rollback SQL for a command.
pub fn generate_rollback_sql(cmd: &QailCmd) -> String {
    match cmd.action {
        Action::Make => {
            format!("DROP TABLE IF EXISTS {}", cmd.table)
        },
        Action::Drop => {
            format!("-- Cannot auto-rollback DROP TABLE {} (data lost)", cmd.table)
        },
        Action::Alter => {
            // ADD COLUMN -> DROP COLUMN
            if let Some(col) = cmd.columns.first() {
                if let Expr::Def { name, .. } = col {
                    return format!("ALTER TABLE {} DROP COLUMN {}", cmd.table, name);
                }
            }
            format!("-- Cannot determine rollback for ALTER on {}", cmd.table)
        },
        Action::AlterDrop => {
            // DROP COLUMN -> cannot easily reverse
            format!("-- Cannot auto-rollback DROP COLUMN on {} (data lost)", cmd.table)
        },
        Action::Index => {
            if let Some(ref idx) = cmd.index_def {
                return format!("DROP INDEX IF EXISTS {}", idx.name);
            }
            "-- Cannot determine index name for rollback".to_string()
        },
        Action::DropIndex => {
            format!("-- Cannot auto-rollback DROP INDEX (need original definition)")
        },
        Action::Mod => {
            format!("-- RENAME operation: reverse manually")
        },
        Action::AlterType => {
            // ALTER COLUMN TYPE -> cannot easily reverse (may lose data)
            format!("-- Cannot auto-rollback TYPE change on {} (may need USING clause)", cmd.table)
        },
        _ => format!("-- No rollback for {:?}", cmd.action),
    }
}

/// Generate DOWN SQL for a migration command.
pub fn generate_down_sql(cmd: &QailCmd) -> String {
    generate_rollback_sql(cmd)
}
