//! Schema Diff Visitor
//!
//! Computes the difference between two schemas and generates QailCmd operations.
//! Now with intent-awareness from MigrationHint.

use super::schema::{Schema, MigrationHint};
use crate::ast::{QailCmd, Action, Expr, Constraint, IndexDef};

/// Compute the difference between two schemas.
/// 
/// Returns a Vec<QailCmd> representing the operations needed to migrate
/// from `old` to `new`. Respects MigrationHint for intent-aware diffing.
pub fn diff_schemas(old: &Schema, new: &Schema) -> Vec<QailCmd> {
    let mut cmds = Vec::new();

    // Process migration hints first (intent-aware)
    for hint in &new.migrations {
        match hint {
            MigrationHint::Rename { from, to } => {
                // Parse "table.column" format
                if let (Some((from_table, from_col)), Some((to_table, to_col))) = 
                    (parse_table_col(from), parse_table_col(to)) 
                {
                    if from_table == to_table {
                        // Same table rename - use ALTER TABLE RENAME COLUMN
                        cmds.push(QailCmd {
                            action: Action::Mod,
                            table: from_table.to_string(),
                            columns: vec![Expr::Named(format!("{} -> {}", from_col, to_col))],
                            ..Default::default()
                        });
                    }
                }
            }
            MigrationHint::Transform { expression, target } => {
                // Transform requires manual SQL - we generate a comment
                if let Some((table, _col)) = parse_table_col(target) {
                    cmds.push(QailCmd {
                        action: Action::Set,
                        table: table.to_string(),
                        columns: vec![Expr::Named(format!("/* TRANSFORM: {} */", expression))],
                        ..Default::default()
                    });
                }
            }
            MigrationHint::Drop { target, confirmed: true } => {
                if target.contains('.') {
                    // Drop column
                    if let Some((table, col)) = parse_table_col(target) {
                        cmds.push(QailCmd {
                            action: Action::AlterDrop,
                            table: table.to_string(),
                            columns: vec![Expr::Named(col.to_string())],
                            ..Default::default()
                        });
                    }
                } else {
                    // Drop table
                    cmds.push(QailCmd {
                        action: Action::Drop,
                        table: target.clone(),
                        ..Default::default()
                    });
                }
            }
            _ => {}
        }
    }

    // Detect new tables
    for (name, table) in &new.tables {
        if !old.tables.contains_key(name) {
            // New table - CREATE TABLE
            let columns: Vec<Expr> = table.columns.iter().map(|col| {
                let mut constraints = Vec::new();
                if col.primary_key {
                    constraints.push(Constraint::PrimaryKey);
                }
                if col.nullable {
                    constraints.push(Constraint::Nullable);
                }
                if col.unique {
                    constraints.push(Constraint::Unique);
                }
                if let Some(def) = &col.default {
                    constraints.push(Constraint::Default(def.clone()));
                }
                if let Some(ref fk) = col.foreign_key {
                    constraints.push(Constraint::References(format!("{}({})", fk.table, fk.column)));
                }
                
                Expr::Def {
                    name: col.name.clone(),
                    data_type: col.data_type.to_pg_type(),
                    constraints,
                }
            }).collect();

            cmds.push(QailCmd {
                action: Action::Make,
                table: name.clone(),
                columns,
                ..Default::default()
            });
        }
    }

    // Detect dropped tables (only if not already handled by hints)
    for name in old.tables.keys() {
        if !new.tables.contains_key(name) {
            let already_dropped = new.migrations.iter().any(|h| {
                matches!(h, MigrationHint::Drop { target, confirmed: true } if target == name)
            });
            if !already_dropped {
                cmds.push(QailCmd {
                    action: Action::Drop,
                    table: name.clone(),
                    ..Default::default()
                });
            }
        }
    }

    // Detect column changes in existing tables
    for (name, new_table) in &new.tables {
        if let Some(old_table) = old.tables.get(name) {
            let old_cols: std::collections::HashSet<_> = old_table.columns.iter().map(|c| &c.name).collect();
            let new_cols: std::collections::HashSet<_> = new_table.columns.iter().map(|c| &c.name).collect();

            // New columns
            for col in &new_table.columns {
                if !old_cols.contains(&col.name) {
                    // Check if this is a rename target
                    let is_rename_target = new.migrations.iter().any(|h| {
                        matches!(h, MigrationHint::Rename { to, .. } if to.ends_with(&format!(".{}", col.name)))
                    });
                    
                    if !is_rename_target {
                        let mut constraints = Vec::new();
                        if col.nullable {
                            constraints.push(Constraint::Nullable);
                        }
                        if col.unique {
                            constraints.push(Constraint::Unique);
                        }
                        if let Some(def) = &col.default {
                            constraints.push(Constraint::Default(def.clone()));
                        }

                        cmds.push(QailCmd {
                            action: Action::Alter,
                            table: name.clone(),
                            columns: vec![Expr::Def {
                                name: col.name.clone(),
                                data_type: col.data_type.to_pg_type(),
                                constraints,
                            }],
                            ..Default::default()
                        });
                    }
                }
            }

            // Dropped columns (not handled by hints)
            for col in &old_table.columns {
                if !new_cols.contains(&col.name) {
                    let is_rename_source = new.migrations.iter().any(|h| {
                        matches!(h, MigrationHint::Rename { from, .. } if from.ends_with(&format!(".{}", col.name)))
                    });
                    
                    if !is_rename_source {
                        cmds.push(QailCmd {
                            action: Action::AlterDrop,
                            table: name.clone(),
                            columns: vec![Expr::Named(col.name.clone())],
                            ..Default::default()
                        });
                    }
                }
            }

            // Detect type changes in existing columns
            for new_col in &new_table.columns {
                if let Some(old_col) = old_table.columns.iter().find(|c| c.name == new_col.name) {
                    let old_type = old_col.data_type.to_pg_type();
                    let new_type = new_col.data_type.to_pg_type();
                    
                    if old_type != new_type {
                        // Type changed - ALTER COLUMN TYPE
                        cmds.push(QailCmd {
                            action: Action::AlterType,
                            table: name.clone(),
                            columns: vec![Expr::Def {
                                name: new_col.name.clone(),
                                data_type: new_type,
                                constraints: vec![],
                            }],
                            ..Default::default()
                        });
                    }
                }
            }
        }
    }

    // Detect new indexes
    for new_idx in &new.indexes {
        let exists = old.indexes.iter().any(|i| i.name == new_idx.name);
        if !exists {
            cmds.push(QailCmd {
                action: Action::Index,
                table: String::new(),
                index_def: Some(IndexDef {
                    name: new_idx.name.clone(),
                    table: new_idx.table.clone(),
                    columns: new_idx.columns.clone(),
                    unique: new_idx.unique,
                }),
                ..Default::default()
            });
        }
    }

    // Detect dropped indexes
    for old_idx in &old.indexes {
        let exists = new.indexes.iter().any(|i| i.name == old_idx.name);
        if !exists {
            cmds.push(QailCmd {
                action: Action::DropIndex,
                table: old_idx.name.clone(),
                ..Default::default()
            });
        }
    }

    cmds
}

/// Parse "table.column" format
fn parse_table_col(s: &str) -> Option<(&str, &str)> {
    let parts: Vec<&str> = s.splitn(2, '.').collect();
    if parts.len() == 2 {
        Some((parts[0], parts[1]))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::schema::{Table, Column};

    #[test]
    fn test_diff_new_table() {
        use super::super::types::ColumnType;
        let old = Schema::default();
        let mut new = Schema::default();
        new.add_table(Table::new("users")
            .column(Column::new("id", ColumnType::Serial).primary_key())
            .column(Column::new("name", ColumnType::Text).not_null()));
        
        let cmds = diff_schemas(&old, &new);
        assert_eq!(cmds.len(), 1);
        assert!(matches!(cmds[0].action, Action::Make));
    }

    #[test]
    fn test_diff_rename_with_hint() {
        use super::super::types::ColumnType;
        let mut old = Schema::default();
        old.add_table(Table::new("users")
            .column(Column::new("username", ColumnType::Text)));
        
        let mut new = Schema::default();
        new.add_table(Table::new("users")
            .column(Column::new("name", ColumnType::Text)));
        new.add_hint(MigrationHint::Rename {
            from: "users.username".into(),
            to: "users.name".into(),
        });
        
        let cmds = diff_schemas(&old, &new);
        // Should have rename, NOT drop + add
        assert!(cmds.iter().any(|c| matches!(c.action, Action::Mod)));
        assert!(!cmds.iter().any(|c| matches!(c.action, Action::AlterDrop)));
    }
}
