//! Schema Diff Visitor
//!
//! Computes the difference between two schemas and generates Qail operations.
//! Now with intent-awareness from MigrationHint.

use super::schema::{MigrationHint, Schema};
use crate::ast::{Action, Constraint, Expr, IndexDef, Qail};

/// Compute the difference between two schemas.
/// Returns a Vec<Qail> representing the operations needed to migrate
/// from `old` to `new`. Respects MigrationHint for intent-aware diffing.
pub fn diff_schemas(old: &Schema, new: &Schema) -> Vec<Qail> {
    let mut cmds = Vec::new();

    // Process migration hints first (intent-aware)
    for hint in &new.migrations {
        match hint {
            MigrationHint::Rename { from, to } => {
                if let (Some((from_table, from_col)), Some((to_table, to_col))) =
                    (parse_table_col(from), parse_table_col(to))
                    && from_table == to_table
                {
                    // Same table rename - use ALTER TABLE RENAME COLUMN
                    cmds.push(Qail {
                        action: Action::Mod,
                        table: from_table.to_string(),
                        columns: vec![Expr::Named(format!("{} -> {}", from_col, to_col))],
                        ..Default::default()
                    });
                }
            }
            MigrationHint::Transform { expression, target } => {
                if let Some((table, _col)) = parse_table_col(target) {
                    cmds.push(Qail {
                        action: Action::Set,
                        table: table.to_string(),
                        columns: vec![Expr::Named(format!("/* TRANSFORM: {} */", expression))],
                        ..Default::default()
                    });
                }
            }
            MigrationHint::Drop {
                target,
                confirmed: true,
            } => {
                if target.contains('.') {
                    // Drop column
                    if let Some((table, col)) = parse_table_col(target) {
                        cmds.push(Qail {
                            action: Action::AlterDrop,
                            table: table.to_string(),
                            columns: vec![Expr::Named(col.to_string())],
                            ..Default::default()
                        });
                    }
                } else {
                    // Drop table
                    cmds.push(Qail {
                        action: Action::Drop,
                        table: target.clone(),
                        ..Default::default()
                    });
                }
            }
            _ => {}
        }
    }

    // Collect new tables (not in old schema), sorted by FK dependencies
    let mut new_table_names: Vec<&String> = new
        .tables
        .keys()
        .filter(|name| !old.tables.contains_key(*name))
        .collect();

    // Simple FK-aware sort: tables with no FK deps first, then others
    // This handles the common case of parent -> child relationships
    new_table_names.sort_by_key(|name| {
        new.tables
            .get(*name)
            .map(|t| t.columns.iter().filter(|c| c.foreign_key.is_some()).count())
            .unwrap_or(0)
    });

    // Generate CREATE TABLE commands in dependency order
    for name in new_table_names {
        let table = &new.tables[name];
        let columns: Vec<Expr> = table
            .columns
            .iter()
            .map(|col| {
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
                    constraints.push(Constraint::References(format!(
                        "{}({})",
                        fk.table, fk.column
                    )));
                }

                Expr::Def {
                    name: col.name.clone(),
                    data_type: col.data_type.to_pg_type(),
                    constraints,
                }
            })
            .collect();

        cmds.push(Qail {
            action: Action::Make,
            table: name.clone(),
            columns,
            ..Default::default()
        });
    }

    // Detect dropped tables (only if not already handled by hints)
    for name in old.tables.keys() {
        if !new.tables.contains_key(name) {
            let already_dropped = new.migrations.iter().any(
                |h| matches!(h, MigrationHint::Drop { target, confirmed: true } if target == name),
            );
            if !already_dropped {
                cmds.push(Qail {
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
            let old_cols: std::collections::HashSet<_> =
                old_table.columns.iter().map(|c| &c.name).collect();
            let new_cols: std::collections::HashSet<_> =
                new_table.columns.iter().map(|c| &c.name).collect();

            // New columns
            for col in &new_table.columns {
                if !old_cols.contains(&col.name) {
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
                        // SERIAL is a pseudo-type only valid in CREATE TABLE
                        // For ALTER TABLE ADD COLUMN, convert to INTEGER/BIGINT
                        let data_type = match &col.data_type {
                            super::types::ColumnType::Serial => "INTEGER".to_string(),
                            super::types::ColumnType::BigSerial => "BIGINT".to_string(),
                            other => other.to_pg_type(),
                        };

                        cmds.push(Qail {
                            action: Action::Alter,
                            table: name.clone(),
                            columns: vec![Expr::Def {
                                name: col.name.clone(),
                                data_type,
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
                        cmds.push(Qail {
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
                        // SERIAL is pseudo-type only valid in CREATE TABLE
                        let safe_new_type = match &new_col.data_type {
                            super::types::ColumnType::Serial => "INTEGER".to_string(),
                            super::types::ColumnType::BigSerial => "BIGINT".to_string(),
                            _ => new_type,
                        };
                        
                        cmds.push(Qail {
                            action: Action::AlterType,
                            table: name.clone(),
                            columns: vec![Expr::Def {
                                name: new_col.name.clone(),
                                data_type: safe_new_type,
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
            cmds.push(Qail {
                action: Action::Index,
                table: String::new(),
                index_def: Some(IndexDef {
                    name: new_idx.name.clone(),
                    table: new_idx.table.clone(),
                    columns: new_idx.columns.clone(),
                    unique: new_idx.unique,
                    index_type: None,
                }),
                ..Default::default()
            });
        }
    }

    // Detect dropped indexes
    for old_idx in &old.indexes {
        let exists = new.indexes.iter().any(|i| i.name == old_idx.name);
        if !exists {
            cmds.push(Qail {
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
    use super::super::schema::{Column, Table};
    use super::*;

    #[test]
    fn test_diff_new_table() {
        use super::super::types::ColumnType;
        let old = Schema::default();
        let mut new = Schema::default();
        new.add_table(
            Table::new("users")
                .column(Column::new("id", ColumnType::Serial).primary_key())
                .column(Column::new("name", ColumnType::Text).not_null()),
        );

        let cmds = diff_schemas(&old, &new);
        assert_eq!(cmds.len(), 1);
        assert!(matches!(cmds[0].action, Action::Make));
    }

    #[test]
    fn test_diff_rename_with_hint() {
        use super::super::types::ColumnType;
        let mut old = Schema::default();
        old.add_table(Table::new("users").column(Column::new("username", ColumnType::Text)));

        let mut new = Schema::default();
        new.add_table(Table::new("users").column(Column::new("name", ColumnType::Text)));
        new.add_hint(MigrationHint::Rename {
            from: "users.username".into(),
            to: "users.name".into(),
        });

        let cmds = diff_schemas(&old, &new);
        // Should have rename, NOT drop + add
        assert!(cmds.iter().any(|c| matches!(c.action, Action::Mod)));
        assert!(!cmds.iter().any(|c| matches!(c.action, Action::AlterDrop)));
    }

    /// Regression test: FK parent tables must be created before child tables
    #[test]
    fn test_fk_ordering_parent_before_child() {
        use super::super::types::ColumnType;
        
        let old = Schema::default();
        
        let mut new = Schema::default();
        // Child table with FK to parent
        new.add_table(
            Table::new("child")
                .column(Column::new("id", ColumnType::Serial).primary_key())
                .column(Column::new("parent_id", ColumnType::Int).references("parent", "id")),
        );
        // Parent table (no FK)
        new.add_table(
            Table::new("parent")
                .column(Column::new("id", ColumnType::Serial).primary_key())
                .column(Column::new("name", ColumnType::Text)),
        );

        let cmds = diff_schemas(&old, &new);
        
        // Should have 2 CREATE TABLE commands
        let make_cmds: Vec<_> = cmds.iter().filter(|c| matches!(c.action, Action::Make)).collect();
        assert_eq!(make_cmds.len(), 2);
        
        // Parent (0 FKs) should come BEFORE child (1 FK)
        let parent_idx = make_cmds.iter().position(|c| c.table == "parent").unwrap();
        let child_idx = make_cmds.iter().position(|c| c.table == "child").unwrap();
        assert!(parent_idx < child_idx, "parent table should be created before child with FK");
    }

    /// Regression test: Multiple FK dependencies should be sorted correctly
    #[test]
    fn test_fk_ordering_multiple_dependencies() {
        use super::super::types::ColumnType;
        
        let old = Schema::default();
        
        let mut new = Schema::default();
        // Table with 2 FKs (should be last)
        new.add_table(
            Table::new("order_items")
                .column(Column::new("id", ColumnType::Serial).primary_key())
                .column(Column::new("order_id", ColumnType::Int).references("orders", "id"))
                .column(Column::new("product_id", ColumnType::Int).references("products", "id")),
        );
        // Table with 1 FK (should be middle)
        new.add_table(
            Table::new("orders")
                .column(Column::new("id", ColumnType::Serial).primary_key())
                .column(Column::new("user_id", ColumnType::Int).references("users", "id")),
        );
        // Table with 0 FKs (should be first)
        new.add_table(
            Table::new("users")
                .column(Column::new("id", ColumnType::Serial).primary_key()),
        );
        new.add_table(
            Table::new("products")
                .column(Column::new("id", ColumnType::Serial).primary_key()),
        );

        let cmds = diff_schemas(&old, &new);
        
        let make_cmds: Vec<_> = cmds.iter().filter(|c| matches!(c.action, Action::Make)).collect();
        assert_eq!(make_cmds.len(), 4);
        
        // Get positions
        let users_idx = make_cmds.iter().position(|c| c.table == "users").unwrap();
        let products_idx = make_cmds.iter().position(|c| c.table == "products").unwrap();
        let orders_idx = make_cmds.iter().position(|c| c.table == "orders").unwrap();
        let items_idx = make_cmds.iter().position(|c| c.table == "order_items").unwrap();
        
        // Tables with 0 FKs should come first
        assert!(users_idx < orders_idx, "users (0 FK) before orders (1 FK)");
        assert!(products_idx < items_idx, "products (0 FK) before order_items (2 FK)");
        
        // orders (1 FK) should come before order_items (2 FKs)
        assert!(orders_idx < items_idx, "orders (1 FK) before order_items (2 FK)");
    }
}

