//! SQL generation utilities for migrations

use qail_core::prelude::*;

/// Convert Qail to SQL string for preview.
pub fn cmd_to_sql(cmd: &Qail) -> String {
    match cmd.action {
        Action::Make => {
            // CREATE TABLE
            let mut sql = format!("CREATE TABLE {} (", cmd.table);
            let cols: Vec<String> = cmd
                .columns
                .iter()
                .filter_map(|col| {
                    if let Expr::Def {
                        name,
                        data_type,
                        constraints,
                    } = col
                    {
                        let mut col_def = format!("{} {}", name, data_type);

                        let is_pk = constraints
                            .iter()
                            .any(|c| matches!(c, Constraint::PrimaryKey));
                        let is_nullable = constraints
                            .iter()
                            .any(|c| matches!(c, Constraint::Nullable));

                        for c in constraints {
                            match c {
                                Constraint::PrimaryKey => col_def.push_str(" PRIMARY KEY"),
                                Constraint::Nullable => {} // Columns are NOT NULL by default unless marked Nullable
                                Constraint::Unique => col_def.push_str(" UNIQUE"),
                                Constraint::Default(v) => {
                                    col_def.push_str(&format!(" DEFAULT {}", v))
                                }
                                Constraint::References(target) => {
                                    col_def.push_str(&format!(" REFERENCES {}", target))
                                }
                                _ => {}
                            }
                        }

                        if !is_pk && !is_nullable {
                            col_def.push_str(" NOT NULL");
                        }

                        Some(col_def)
                    } else {
                        None
                    }
                })
                .collect();
            sql.push_str(&cols.join(", "));
            sql.push(')');
            sql
        }
        Action::Drop => {
            format!("DROP TABLE IF EXISTS {}", cmd.table)
        }
        Action::Alter => {
            // ADD COLUMN
            if let Some(Expr::Def {
                name,
                data_type,
                constraints,
            }) = cmd.columns.first()
            {
                let mut sql = format!(
                    "ALTER TABLE {} ADD COLUMN {} {}",
                    cmd.table, name, data_type
                );
                if !constraints.contains(&Constraint::Nullable) {
                    sql.push_str(" NOT NULL");
                }
                for c in constraints {
                    match c {
                        Constraint::Nullable => {}
                        Constraint::Unique => sql.push_str(" UNIQUE"),
                        Constraint::Default(v) => sql.push_str(&format!(" DEFAULT {}", v)),
                        _ => {}
                    }
                }
                return sql;
            }
            format!("ALTER TABLE {} ADD COLUMN ...", cmd.table)
        }
        Action::AlterDrop => {
            // DROP COLUMN
            if let Some(Expr::Named(name)) = cmd.columns.first() {
                return format!("ALTER TABLE {} DROP COLUMN {}", cmd.table, name);
            }
            if let Some(Expr::Def { name, .. }) = cmd.columns.first() {
                return format!("ALTER TABLE {} DROP COLUMN {}", cmd.table, name);
            }
            format!("ALTER TABLE {} DROP COLUMN ...", cmd.table)
        }
        Action::Index => {
            if let Some(ref idx) = cmd.index_def {
                let unique = if idx.unique { "UNIQUE " } else { "" };
                let table = if idx.table.trim().is_empty() {
                    cmd.table.as_str()
                } else {
                    idx.table.as_str()
                };
                let mut sql = format!(
                    "CREATE {}INDEX {} ON {} ({})",
                    unique,
                    idx.name,
                    table,
                    idx.columns.join(", ")
                );
                if let Some(method) = &idx.index_type
                    && !method.trim().is_empty()
                {
                    sql = format!(
                        "CREATE {}INDEX {} ON {} USING {} ({})",
                        unique,
                        idx.name,
                        table,
                        method.trim(),
                        idx.columns.join(", ")
                    );
                }
                if let Some(where_clause) = &idx.where_clause
                    && !where_clause.trim().is_empty()
                {
                    sql.push_str(" WHERE ");
                    sql.push_str(where_clause.trim());
                }
                return sql;
            }
            format!("CREATE INDEX ON {} (...)", cmd.table)
        }
        Action::DropIndex => {
            if let Some(ref idx) = cmd.index_def {
                return format!("DROP INDEX IF EXISTS {}", idx.name);
            }
            if !cmd.table.trim().is_empty() {
                return format!("DROP INDEX IF EXISTS {}", cmd.table);
            }
            "DROP INDEX ...".to_string()
        }
        Action::Mod => {
            // RENAME COLUMN
            format!("ALTER TABLE {} RENAME COLUMN ... TO ...", cmd.table)
        }
        Action::AlterType => {
            // ALTER COLUMN TYPE
            if let Some(Expr::Def {
                name, data_type, ..
            }) = cmd.columns.first()
            {
                return format!(
                    "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
                    cmd.table, name, data_type
                );
            }
            format!("ALTER TABLE {} ALTER COLUMN ... TYPE ...", cmd.table)
        }
        Action::AlterSetNotNull => {
            if let Some(Expr::Named(col)) = cmd.columns.first() {
                format!(
                    "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL",
                    cmd.table, col
                )
            } else {
                format!("ALTER TABLE {} ALTER COLUMN ... SET NOT NULL", cmd.table)
            }
        }
        Action::AlterDropNotNull => {
            if let Some(Expr::Named(col)) = cmd.columns.first() {
                format!(
                    "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL",
                    cmd.table, col
                )
            } else {
                format!("ALTER TABLE {} ALTER COLUMN ... DROP NOT NULL", cmd.table)
            }
        }
        Action::AlterSetDefault => {
            if let Some(Expr::Named(col)) = cmd.columns.first() {
                let default_expr = cmd.payload.as_deref().unwrap_or("NULL");
                format!(
                    "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {}",
                    cmd.table, col, default_expr
                )
            } else {
                format!("ALTER TABLE {} ALTER COLUMN ... SET DEFAULT ...", cmd.table)
            }
        }
        Action::AlterDropDefault => {
            if let Some(Expr::Named(col)) = cmd.columns.first() {
                format!(
                    "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT",
                    cmd.table, col
                )
            } else {
                format!("ALTER TABLE {} ALTER COLUMN ... DROP DEFAULT", cmd.table)
            }
        }
        Action::AlterEnableRls => {
            format!("ALTER TABLE {} ENABLE ROW LEVEL SECURITY", cmd.table)
        }
        Action::AlterDisableRls => {
            format!("ALTER TABLE {} DISABLE ROW LEVEL SECURITY", cmd.table)
        }
        Action::AlterForceRls => {
            format!("ALTER TABLE {} FORCE ROW LEVEL SECURITY", cmd.table)
        }
        Action::AlterNoForceRls => {
            format!("ALTER TABLE {} NO FORCE ROW LEVEL SECURITY", cmd.table)
        }
        _ => format!("-- Unsupported action: {:?}", cmd.action),
    }
}

/// Generate rollback SQL for a command.
pub fn generate_rollback_sql(cmd: &Qail) -> String {
    match cmd.action {
        Action::Make => {
            format!("DROP TABLE IF EXISTS {}", cmd.table)
        }
        Action::Drop => {
            format!(
                "-- Cannot auto-rollback DROP TABLE {} (data lost)",
                cmd.table
            )
        }
        Action::Alter => {
            // ADD COLUMN -> DROP COLUMN
            if let Some(col) = cmd.columns.first()
                && let Expr::Def { name, .. } = col
            {
                return format!("ALTER TABLE {} DROP COLUMN {}", cmd.table, name);
            }
            format!("-- Cannot determine rollback for ALTER on {}", cmd.table)
        }
        Action::AlterDrop => {
            // DROP COLUMN -> cannot easily reverse
            format!(
                "-- Cannot auto-rollback DROP COLUMN on {} (data lost)",
                cmd.table
            )
        }
        Action::Index => {
            if let Some(ref idx) = cmd.index_def {
                return format!("DROP INDEX IF EXISTS {}", idx.name);
            }
            "-- Cannot determine index name for rollback".to_string()
        }
        Action::DropIndex => {
            "-- Cannot auto-rollback DROP INDEX (need original definition)".to_string()
        }
        Action::Mod => "-- RENAME operation: reverse manually".to_string(),
        Action::AlterType => {
            // ALTER COLUMN TYPE -> cannot easily reverse (may lose data)
            format!(
                "-- Cannot auto-rollback TYPE change on {} (may need USING clause)",
                cmd.table
            )
        }
        _ => format!("-- No rollback for {:?}", cmd.action),
    }
}

/// Generate DOWN SQL for a migration command.
pub fn generate_down_sql(cmd: &Qail) -> String {
    generate_rollback_sql(cmd)
}

#[cfg(test)]
mod tests {
    use super::*;
    use qail_core::ast::IndexDef;

    #[test]
    fn alter_add_column_sql_renders_not_null_by_default() {
        let cmd = Qail {
            action: Action::Alter,
            table: "users".to_string(),
            columns: vec![Expr::Def {
                name: "email".to_string(),
                data_type: "TEXT".to_string(),
                constraints: vec![],
            }],
            ..Default::default()
        };

        let sql = cmd_to_sql(&cmd);

        assert!(
            sql.contains("email TEXT NOT NULL"),
            "add-column SQL should render NOT NULL by default, got: {sql}"
        );
    }

    #[test]
    fn index_sql_uses_index_def_table_when_command_table_is_empty() {
        let cmd = Qail {
            action: Action::Index,
            table: String::new(),
            index_def: Some(IndexDef {
                name: "idx_users_email".to_string(),
                table: "users".to_string(),
                columns: vec!["email".to_string()],
                unique: false,
                index_type: None,
                where_clause: None,
            }),
            ..Default::default()
        };

        let sql = cmd_to_sql(&cmd);

        assert!(
            sql.contains("ON users (email)"),
            "index SQL should use IndexDef table, got: {sql}"
        );
    }

    #[test]
    fn index_sql_renders_method_and_where_clause() {
        let cmd = Qail {
            action: Action::Index,
            table: String::new(),
            index_def: Some(IndexDef {
                name: "idx_users_active_email".to_string(),
                table: "users".to_string(),
                columns: vec!["email".to_string()],
                unique: true,
                index_type: Some("gin".to_string()),
                where_clause: Some("deleted_at IS NULL".to_string()),
            }),
            ..Default::default()
        };

        let sql = cmd_to_sql(&cmd);

        assert_eq!(
            sql,
            "CREATE UNIQUE INDEX idx_users_active_email ON users USING gin (email) WHERE deleted_at IS NULL"
        );
    }

    #[test]
    fn drop_index_sql_uses_command_table_when_index_def_is_absent() {
        let cmd = Qail {
            action: Action::DropIndex,
            table: "idx_users_email".to_string(),
            index_def: None,
            ..Default::default()
        };

        let sql = cmd_to_sql(&cmd);

        assert_eq!(sql, "DROP INDEX IF EXISTS idx_users_email");
        assert!(
            !sql.contains("..."),
            "drop-index SQL should not contain placeholder, got: {sql}"
        );
    }
}
