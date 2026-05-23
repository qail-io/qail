//! Schema Diff Visitor
//!
//! Computes the difference between two schemas and generates Qail operations.
//! Now with intent-awareness from MigrationHint.

use super::schema::{
    MigrationHint, Schema, check_expr_to_sql, foreign_key_to_sql, index_method_str,
    multi_column_fk_to_alter_command,
};
use crate::ast::{Action, Constraint, Expr, IndexDef, Qail};
use std::collections::BTreeSet;

/// Return unsupported non-table object families present in a schema.
///
/// State-based diff currently covers table/index/migration-hint operations only.
fn unsupported_state_diff_features(schema: &Schema) -> BTreeSet<&'static str> {
    let mut out = BTreeSet::new();
    if !schema.extensions.is_empty() {
        out.insert("extensions");
    }
    if !schema.comments.is_empty() {
        out.insert("comments");
    }
    if !schema.sequences.is_empty() {
        out.insert("sequences");
    }
    if !schema.enums.is_empty() {
        out.insert("enums");
    }
    if !schema.views.is_empty() {
        out.insert("views");
    }
    if !schema.functions.is_empty() {
        out.insert("functions");
    }
    if !schema.triggers.is_empty() {
        out.insert("triggers");
    }
    if !schema.grants.is_empty() {
        out.insert("grants");
    }
    if !schema.policies.is_empty() {
        out.insert("policies");
    }
    if !schema.resources.is_empty() {
        out.insert("resources");
    }
    out
}

fn existing_column_check_diffs(old: &Schema, new: &Schema) -> Vec<String> {
    let mut changes = Vec::new();

    for (table_name, new_table) in &new.tables {
        let Some(old_table) = old.tables.get(table_name) else {
            continue;
        };

        for new_col in &new_table.columns {
            let Some(old_col) = old_table
                .columns
                .iter()
                .find(|old_col| old_col.name == new_col.name)
            else {
                continue;
            };

            if check_signature(&old_col.check) != check_signature(&new_col.check) {
                changes.push(format!("{}.{}", table_name, new_col.name));
            }
        }
    }

    changes.sort();
    changes
}

fn existing_column_foreign_key_diffs(old: &Schema, new: &Schema) -> Vec<String> {
    let mut changes = Vec::new();

    for (table_name, new_table) in &new.tables {
        let Some(old_table) = old.tables.get(table_name) else {
            continue;
        };

        for new_col in &new_table.columns {
            let Some(old_col) = old_table
                .columns
                .iter()
                .find(|old_col| old_col.name == new_col.name)
            else {
                continue;
            };

            if foreign_key_signature(&old_col.foreign_key)
                != foreign_key_signature(&new_col.foreign_key)
            {
                changes.push(format!("{}.{}", table_name, new_col.name));
            }
        }
    }

    changes.sort();
    changes
}

fn existing_column_unique_diffs(old: &Schema, new: &Schema) -> Vec<String> {
    let mut changes = Vec::new();

    for (table_name, new_table) in &new.tables {
        let Some(old_table) = old.tables.get(table_name) else {
            continue;
        };

        for new_col in &new_table.columns {
            let Some(old_col) = old_table
                .columns
                .iter()
                .find(|old_col| old_col.name == new_col.name)
            else {
                continue;
            };

            if old_col.unique != new_col.unique {
                changes.push(format!("{}.{}", table_name, new_col.name));
            }
        }
    }

    changes.sort();
    changes
}

fn existing_column_primary_key_diffs(old: &Schema, new: &Schema) -> Vec<String> {
    let mut changes = Vec::new();

    for (table_name, new_table) in &new.tables {
        let Some(old_table) = old.tables.get(table_name) else {
            continue;
        };

        for new_col in &new_table.columns {
            let Some(old_col) = old_table
                .columns
                .iter()
                .find(|old_col| old_col.name == new_col.name)
            else {
                continue;
            };

            if old_col.primary_key != new_col.primary_key {
                changes.push(format!("{}.{}", table_name, new_col.name));
            }
        }
    }

    changes.sort();
    changes
}

fn new_column_primary_key_additions(old: &Schema, new: &Schema) -> Vec<String> {
    let mut changes = Vec::new();

    for (table_name, new_table) in &new.tables {
        let Some(old_table) = old.tables.get(table_name) else {
            continue;
        };

        for new_col in &new_table.columns {
            if new_col.primary_key
                && !old_table
                    .columns
                    .iter()
                    .any(|old_col| old_col.name == new_col.name)
            {
                changes.push(format!("{}.{}", table_name, new_col.name));
            }
        }
    }

    changes.sort();
    changes
}

fn same_name_index_definition_diffs(old: &Schema, new: &Schema) -> Vec<String> {
    let mut changes = Vec::new();

    for new_idx in &new.indexes {
        let Some(old_idx) = old
            .indexes
            .iter()
            .find(|old_idx| old_idx.name == new_idx.name)
        else {
            continue;
        };

        if index_signature(old_idx) != index_signature(new_idx) {
            changes.push(new_idx.name.clone());
        }
    }

    changes.sort();
    changes.dedup();
    changes
}

fn check_signature(check: &Option<super::schema::CheckConstraint>) -> Option<String> {
    check
        .as_ref()
        .map(|check| format!("{:?}:{:?}", check.name, check.expr))
}

fn foreign_key_signature(fk: &Option<super::schema::ForeignKey>) -> Option<String> {
    fk.as_ref().map(|fk| format!("{:?}", fk))
}

fn index_signature(idx: &super::schema::Index) -> String {
    format!(
        "table={:?};columns={:?};expressions={:?};unique={};method={};where={:?};include={:?};concurrently={}",
        idx.table,
        idx.columns,
        idx.expressions,
        idx.unique,
        index_method_str(&idx.method),
        idx.where_clause.as_ref().map(check_expr_to_sql),
        idx.include,
        idx.concurrently
    )
}

fn table_references_table(table: &super::schema::Table, target: &str) -> bool {
    table.columns.iter().any(|col| {
        col.foreign_key
            .as_ref()
            .is_some_and(|fk| fk.table == target)
    }) || table
        .multi_column_fks
        .iter()
        .any(|fk| fk.ref_table == target)
}

/// Validate that a schema pair is fully supported by state-based diff.
///
/// Returns an error when object families outside table/index/hint coverage are present.
pub fn validate_state_diff_support(old: &Schema, new: &Schema) -> Result<(), String> {
    let mut unsupported = unsupported_state_diff_features(old);
    unsupported.extend(unsupported_state_diff_features(new));

    if !unsupported.is_empty() {
        let detail = unsupported.into_iter().collect::<Vec<_>>().join(", ");
        return Err(format!(
            "State-based diff currently supports tables, columns, indexes, and migration hints only. \
             Unsupported schema object families present: {}. \
             Use folder-based strict migrations for these objects.",
            detail
        ));
    }

    let index_diffs = same_name_index_definition_diffs(old, new);
    if !index_diffs.is_empty() {
        return Err(format!(
            "State-based diff cannot safely replace existing indexes with changed definitions: {}. \
             Use an explicit migration for DROP INDEX/CREATE INDEX replacement.",
            index_diffs.join(", ")
        ));
    }

    let check_diffs = existing_column_check_diffs(old, new);
    if !check_diffs.is_empty() {
        return Err(format!(
            "State-based diff cannot safely alter CHECK constraints on existing columns: {}. \
             Use an explicit migration for ADD/DROP/replace CHECK constraints.",
            check_diffs.join(", ")
        ));
    }

    let unique_diffs = existing_column_unique_diffs(old, new);
    if !unique_diffs.is_empty() {
        return Err(format!(
            "State-based diff cannot safely alter UNIQUE constraints on existing columns: {}. \
             Use an explicit migration for ADD/DROP/replace UNIQUE constraints.",
            unique_diffs.join(", ")
        ));
    }

    let pk_diffs = existing_column_primary_key_diffs(old, new);
    if !pk_diffs.is_empty() {
        return Err(format!(
            "State-based diff cannot safely alter PRIMARY KEY constraints on existing columns: {}. \
             Use an explicit migration for ADD/DROP/replace PRIMARY KEY constraints.",
            pk_diffs.join(", ")
        ));
    }

    let new_pk_columns = new_column_primary_key_additions(old, new);
    if !new_pk_columns.is_empty() {
        return Err(format!(
            "State-based diff cannot safely add PRIMARY KEY columns to existing tables: {}. \
             Use an explicit migration to backfill data and add the PRIMARY KEY constraint.",
            new_pk_columns.join(", ")
        ));
    }

    let fk_diffs = existing_column_foreign_key_diffs(old, new);
    if !fk_diffs.is_empty() {
        return Err(format!(
            "State-based diff cannot safely alter single-column foreign keys on existing columns: {}. \
             Use an explicit migration for ADD/DROP/replace FOREIGN KEY constraints.",
            fk_diffs.join(", ")
        ));
    }

    Ok(())
}

/// Checked variant of [`diff_schemas`] that rejects unsupported object families.
pub fn diff_schemas_checked(old: &Schema, new: &Schema) -> Result<Vec<Qail>, String> {
    validate_state_diff_support(old, new)?;
    Ok(diff_schemas(old, new))
}

/// Compute the difference between two schemas.
/// Returns a `Vec<Qail>` representing the operations needed to migrate
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
    let new_table_names: Vec<&String> = new
        .tables
        .keys()
        .filter(|name| !old.tables.contains_key(*name))
        .collect();

    // Simple FK-aware sort: tables with no FK deps first, then others
    // This handles the common case of parent -> child relationships
    // Use iterative topological sort: in each round, emit tables whose FK targets
    // are either already emitted or not in this batch (pre-existing tables).
    let new_set: std::collections::HashSet<&str> =
        new_table_names.iter().map(|n| n.as_str()).collect();
    let mut emitted: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut sorted: Vec<&String> = Vec::with_capacity(new_table_names.len());
    let mut remaining = new_table_names;

    loop {
        let before = sorted.len();
        remaining.retain(|name| {
            let deps_satisfied = new.tables.get(*name).is_none_or(|t| {
                t.columns.iter().all(|c| {
                    c.foreign_key.as_ref().is_none_or(|fk| {
                        !new_set.contains(fk.table.as_str()) || emitted.contains(fk.table.as_str())
                    })
                }) && t.multi_column_fks.iter().all(|fk| {
                    !new_set.contains(fk.ref_table.as_str())
                        || emitted.contains(fk.ref_table.as_str())
                })
            });
            if deps_satisfied {
                emitted.insert(name.as_str());
                sorted.push(name);
                false // remove from remaining
            } else {
                true // keep in remaining
            }
        });
        if remaining.is_empty() || sorted.len() == before {
            // Either done or circular deps — append remaining as-is
            sorted.extend(remaining);
            break;
        }
    }

    let new_table_names = sorted;

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
                    constraints.push(Constraint::References(foreign_key_to_sql(fk)));
                }
                if let Some(check) = &col.check {
                    let check_sql = check_expr_to_sql(&check.expr);
                    if let Some(name) = &check.name {
                        constraints.push(Constraint::Check(vec![format!(
                            "CONSTRAINT {} CHECK ({})",
                            name, check_sql
                        )]));
                    } else {
                        constraints.push(Constraint::Check(vec![check_sql]));
                    }
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

        if table.enable_rls {
            cmds.push(Qail {
                action: Action::AlterEnableRls,
                table: name.clone(),
                ..Default::default()
            });
        }
        if table.force_rls {
            cmds.push(Qail {
                action: Action::AlterForceRls,
                table: name.clone(),
                ..Default::default()
            });
        }
    }

    // Detect dropped tables (only if not already handled by hints)
    let mut dropped_tables: Vec<&String> = old
        .tables
        .keys()
        .filter(|name| {
            !new.tables.contains_key(*name) && !new.migrations.iter().any(
                |h| matches!(h, MigrationHint::Drop { target, confirmed: true } if target == *name),
            )
        })
        .collect();

    dropped_tables.sort();
    let mut remaining = dropped_tables;
    let mut dropped_tables = Vec::with_capacity(remaining.len());
    while !remaining.is_empty() {
        let before = dropped_tables.len();
        let remaining_names: Vec<String> = remaining.iter().map(|name| (*name).clone()).collect();
        let mut next_remaining = Vec::new();

        for name in remaining {
            let has_dropped_dependent = remaining_names.iter().any(|other| {
                other.as_str() != name.as_str()
                    && old
                        .tables
                        .get(other)
                        .is_some_and(|table| table_references_table(table, name))
            });

            if has_dropped_dependent {
                next_remaining.push(name);
            } else {
                dropped_tables.push(name);
            }
        }

        if dropped_tables.len() == before {
            next_remaining.sort();
            dropped_tables.extend(next_remaining);
            break;
        }

        remaining = next_remaining;
    }

    for name in dropped_tables {
        cmds.push(Qail {
            action: Action::Drop,
            table: name.clone(),
            ..Default::default()
        });
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
                    let col_path = format!("{}.{}", name, col.name);
                    let is_rename_target = new
                        .migrations
                        .iter()
                        .any(|h| matches!(h, MigrationHint::Rename { to, .. } if to == &col_path));

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
                        if let Some(fk) = &col.foreign_key {
                            constraints.push(Constraint::References(foreign_key_to_sql(fk)));
                        }
                        if let Some(check) = &col.check {
                            let check_sql = check_expr_to_sql(&check.expr);
                            if let Some(name) = &check.name {
                                constraints.push(Constraint::Check(vec![format!(
                                    "CONSTRAINT {} CHECK ({})",
                                    name, check_sql
                                )]));
                            } else {
                                constraints.push(Constraint::Check(vec![check_sql]));
                            }
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
                    let col_path = format!("{}.{}", name, col.name);
                    let is_rename_source = new.migrations.iter().any(
                        |h| matches!(h, MigrationHint::Rename { from, .. } if from == &col_path),
                    );

                    let is_drop_hinted = new.migrations.iter().any(|h| {
                        matches!(h, MigrationHint::Drop { target, confirmed: true } if target == &col_path)
                    });

                    if !is_rename_source && !is_drop_hinted {
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

                    // Detect NOT NULL changes
                    if old_col.nullable && !new_col.nullable && !new_col.primary_key {
                        // Was nullable, now NOT NULL → SET NOT NULL
                        cmds.push(Qail {
                            action: Action::AlterSetNotNull,
                            table: name.clone(),
                            columns: vec![Expr::Named(new_col.name.clone())],
                            ..Default::default()
                        });
                    } else if !old_col.nullable && new_col.nullable && !old_col.primary_key {
                        // Was NOT NULL, now nullable → DROP NOT NULL
                        cmds.push(Qail {
                            action: Action::AlterDropNotNull,
                            table: name.clone(),
                            columns: vec![Expr::Named(new_col.name.clone())],
                            ..Default::default()
                        });
                    }

                    // Detect DEFAULT changes
                    match (&old_col.default, &new_col.default) {
                        (None, Some(new_default)) => {
                            // No default before, now has one → SET DEFAULT
                            cmds.push(Qail {
                                action: Action::AlterSetDefault,
                                table: name.clone(),
                                columns: vec![Expr::Named(new_col.name.clone())],
                                payload: Some(new_default.clone()),
                                ..Default::default()
                            });
                        }
                        (Some(_), None) => {
                            // Had default, now removed → DROP DEFAULT
                            cmds.push(Qail {
                                action: Action::AlterDropDefault,
                                table: name.clone(),
                                columns: vec![Expr::Named(new_col.name.clone())],
                                ..Default::default()
                            });
                        }
                        (Some(old_default), Some(new_default)) if old_default != new_default => {
                            // Default value changed → SET DEFAULT (new)
                            cmds.push(Qail {
                                action: Action::AlterSetDefault,
                                table: name.clone(),
                                columns: vec![Expr::Named(new_col.name.clone())],
                                payload: Some(new_default.clone()),
                                ..Default::default()
                            });
                        }
                        _ => {} // Same or both None
                    }
                }
            }

            // Detect RLS changes
            if !old_table.enable_rls && new_table.enable_rls {
                cmds.push(Qail {
                    action: Action::AlterEnableRls,
                    table: name.clone(),
                    ..Default::default()
                });
            } else if old_table.enable_rls && !new_table.enable_rls {
                cmds.push(Qail {
                    action: Action::AlterDisableRls,
                    table: name.clone(),
                    ..Default::default()
                });
            }

            if !old_table.force_rls && new_table.force_rls {
                cmds.push(Qail {
                    action: Action::AlterForceRls,
                    table: name.clone(),
                    ..Default::default()
                });
            } else if old_table.force_rls && !new_table.force_rls {
                cmds.push(Qail {
                    action: Action::AlterNoForceRls,
                    table: name.clone(),
                    ..Default::default()
                });
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
                    columns: if !new_idx.expressions.is_empty() {
                        new_idx.expressions.clone()
                    } else {
                        new_idx.columns.clone()
                    },
                    unique: new_idx.unique,
                    index_type: Some(index_method_str(&new_idx.method).to_string()),
                    where_clause: new_idx.where_clause.as_ref().map(check_expr_to_sql),
                }),
                ..Default::default()
            });
        }
    }

    let mut fk_table_names: Vec<&String> = new
        .tables
        .iter()
        .filter(|(_, table)| !table.multi_column_fks.is_empty())
        .map(|(name, _)| name)
        .collect();
    fk_table_names.sort();
    for name in fk_table_names {
        let new_table = &new.tables[name];
        if let Some(old_table) = old.tables.get(name) {
            for fk in &new_table.multi_column_fks {
                if !old_table.multi_column_fks.contains(fk) {
                    cmds.push(multi_column_fk_to_alter_command(name, fk));
                }
            }
        } else {
            for fk in &new_table.multi_column_fks {
                cmds.push(multi_column_fk_to_alter_command(name, fk));
            }
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
    use super::super::schema::{
        CheckExpr, Column, FkAction, Index, IndexMethod, MultiColumnForeignKey, Table, ViewDef,
    };
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
    fn state_diff_support_rejects_non_table_object_families() {
        let old = Schema::default();
        let mut new = Schema::default();
        new.add_view(ViewDef::new("active_users", "SELECT 1"));

        let err = validate_state_diff_support(&old, &new)
            .expect_err("state-based diff should reject unsupported view objects");
        assert!(
            err.contains("views"),
            "error should include unsupported family name"
        );
    }

    #[test]
    fn state_diff_checked_passes_for_table_index_only_schema() {
        use super::super::types::ColumnType;
        let old = Schema::default();
        let mut new = Schema::default();
        new.add_table(Table::new("users").column(Column::new("id", ColumnType::Serial)));
        let cmds = diff_schemas_checked(&old, &new).expect("table/index-only schema should pass");
        assert!(
            cmds.iter().any(|c| matches!(c.action, Action::Make)),
            "checked diff should still produce normal table commands"
        );
    }

    fn schema_with_users_index(index: Index) -> Schema {
        use super::super::types::ColumnType;

        let mut schema = Schema::default();
        schema.add_table(
            Table::new("users")
                .column(Column::new("email", ColumnType::Text))
                .column(Column::new("username", ColumnType::Text))
                .column(Column::new("deleted_at", ColumnType::Text)),
        );
        schema.add_index(index);
        schema
    }

    #[test]
    fn state_diff_checked_rejects_same_name_index_unique_change() {
        let old = schema_with_users_index(Index::new(
            "idx_users_email",
            "users",
            vec!["email".to_string()],
        ));
        let new = schema_with_users_index(
            Index::new("idx_users_email", "users", vec!["email".to_string()]).unique(),
        );

        let err = diff_schemas_checked(&old, &new)
            .expect_err("same-name index unique change should fail closed");
        assert!(err.contains("replace existing indexes"));
        assert!(err.contains("idx_users_email"));
    }

    #[test]
    fn state_diff_checked_rejects_same_name_index_predicate_change() {
        let old = schema_with_users_index(
            Index::new("idx_users_email", "users", vec!["email".to_string()])
                .partial(CheckExpr::Sql("deleted_at IS NULL".to_string())),
        );
        let new = schema_with_users_index(
            Index::new("idx_users_email", "users", vec!["email".to_string()])
                .partial(CheckExpr::Sql("deleted_at IS NOT NULL".to_string())),
        );

        let err = diff_schemas_checked(&old, &new)
            .expect_err("same-name index predicate change should fail closed");
        assert!(err.contains("replace existing indexes"));
        assert!(err.contains("idx_users_email"));
    }

    #[test]
    fn state_diff_checked_rejects_same_name_index_method_change() {
        let old = schema_with_users_index(Index::new(
            "idx_users_email",
            "users",
            vec!["email".to_string()],
        ));
        let new = schema_with_users_index(
            Index::new("idx_users_email", "users", vec!["email".to_string()])
                .using(IndexMethod::Hash),
        );

        let err = diff_schemas_checked(&old, &new)
            .expect_err("same-name index method change should fail closed");
        assert!(err.contains("replace existing indexes"));
        assert!(err.contains("idx_users_email"));
    }

    #[test]
    fn state_diff_checked_rejects_same_name_index_column_change() {
        let old = schema_with_users_index(Index::new(
            "idx_users_email",
            "users",
            vec!["email".to_string()],
        ));
        let new = schema_with_users_index(Index::new(
            "idx_users_email",
            "users",
            vec!["username".to_string()],
        ));

        let err = diff_schemas_checked(&old, &new)
            .expect_err("same-name index column change should fail closed");
        assert!(err.contains("replace existing indexes"));
        assert!(err.contains("idx_users_email"));
    }

    #[test]
    fn state_diff_checked_rejects_existing_column_check_addition() {
        use super::super::types::ColumnType;

        let mut old = Schema::default();
        old.add_table(
            Table::new("inventory").column(Column::new("quantity", ColumnType::Int).not_null()),
        );

        let mut new = Schema::default();
        new.add_table(
            Table::new("inventory").column(
                Column::new("quantity", ColumnType::Int).not_null().check(
                    CheckExpr::GreaterOrEqual {
                        column: "quantity".to_string(),
                        value: 0,
                    },
                ),
            ),
        );

        let err = diff_schemas_checked(&old, &new)
            .expect_err("existing-column CHECK change should fail closed");
        assert!(err.contains("CHECK constraints"));
        assert!(err.contains("inventory.quantity"));
    }

    #[test]
    fn state_diff_checked_rejects_existing_column_unique_addition() {
        use super::super::types::ColumnType;

        let mut old = Schema::default();
        old.add_table(
            Table::new("users").column(Column::new("email", ColumnType::Text).not_null()),
        );

        let mut new = Schema::default();
        new.add_table(
            Table::new("users").column(Column::new("email", ColumnType::Text).not_null().unique()),
        );

        let err = diff_schemas_checked(&old, &new)
            .expect_err("existing-column UNIQUE change should fail closed");
        assert!(err.contains("UNIQUE constraints"));
        assert!(err.contains("users.email"));
    }

    #[test]
    fn state_diff_checked_rejects_existing_column_primary_key_addition() {
        use super::super::types::ColumnType;

        let mut old = Schema::default();
        old.add_table(Table::new("api_keys").column(Column::new("key", ColumnType::Text)));

        let mut new = Schema::default();
        new.add_table(
            Table::new("api_keys").column(Column::new("key", ColumnType::Text).primary_key()),
        );

        let err = diff_schemas_checked(&old, &new)
            .expect_err("existing-column PRIMARY KEY addition should fail closed");
        assert!(err.contains("PRIMARY KEY constraints"));
        assert!(err.contains("api_keys.key"));
    }

    #[test]
    fn state_diff_checked_rejects_existing_column_primary_key_removal() {
        use super::super::types::ColumnType;

        let mut old = Schema::default();
        old.add_table(
            Table::new("api_keys").column(Column::new("key", ColumnType::Text).primary_key()),
        );

        let mut new = Schema::default();
        new.add_table(Table::new("api_keys").column(Column::new("key", ColumnType::Text)));

        let err = diff_schemas_checked(&old, &new)
            .expect_err("existing-column PRIMARY KEY removal should fail closed");
        assert!(err.contains("PRIMARY KEY constraints"));
        assert!(err.contains("api_keys.key"));
    }

    #[test]
    fn state_diff_checked_rejects_new_primary_key_column_on_existing_table() {
        use super::super::types::ColumnType;

        let mut old = Schema::default();
        old.add_table(Table::new("api_keys").column(Column::new("label", ColumnType::Text)));

        let mut new = old.clone();
        new.tables
            .get_mut("api_keys")
            .expect("api_keys table should exist")
            .columns
            .push(Column::new("key", ColumnType::Text).primary_key());

        let err = diff_schemas_checked(&old, &new)
            .expect_err("new PRIMARY KEY column on existing table should fail closed");
        assert!(err.contains("add PRIMARY KEY columns"));
        assert!(err.contains("api_keys.key"));
    }

    #[test]
    fn state_diff_checked_rejects_existing_column_foreign_key_addition() {
        use super::super::types::ColumnType;

        let mut old = Schema::default();
        old.add_table(Table::new("tenants").column(Column::new("id", ColumnType::Int)));
        old.add_table(Table::new("orders").column(Column::new("tenant_id", ColumnType::Int)));

        let mut new = Schema::default();
        new.add_table(Table::new("tenants").column(Column::new("id", ColumnType::Int)));
        new.add_table(
            Table::new("orders")
                .column(Column::new("tenant_id", ColumnType::Int).references("tenants", "id")),
        );

        let err = diff_schemas_checked(&old, &new)
            .expect_err("existing-column single-column FK change should fail closed");
        assert!(err.contains("single-column foreign keys"));
        assert!(err.contains("orders.tenant_id"));
    }

    #[test]
    fn diff_new_column_preserves_foreign_key_reference() {
        use super::super::types::ColumnType;
        use crate::transpiler::ToSql;

        let mut old = Schema::default();
        old.add_table(Table::new("tenants").column(Column::new("id", ColumnType::Int)));
        old.add_table(Table::new("orders").column(Column::new("id", ColumnType::Int)));

        let mut new = old.clone();
        new.tables
            .get_mut("orders")
            .expect("orders table should exist")
            .columns
            .push(
                Column::new("tenant_id", ColumnType::Int)
                    .references("tenants", "id")
                    .on_delete(FkAction::Cascade)
                    .on_update(FkAction::Restrict)
                    .initially_deferred(),
            );

        let cmds = diff_schemas_checked(&old, &new).expect("new referenced column should diff");
        let add_col = cmds
            .iter()
            .find(|cmd| matches!(cmd.action, Action::Alter) && cmd.table == "orders")
            .expect("add-column command should be present");

        let Expr::Def { constraints, .. } = &add_col.columns[0] else {
            panic!("expected added column def");
        };
        assert!(constraints.iter().any(|constraint| {
            matches!(
                constraint,
                Constraint::References(target)
                    if target == "tenants(id) ON DELETE CASCADE ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED"
            )
        }));

        let sql = add_col.to_sql();
        assert!(
            sql.contains(
                "REFERENCES tenants(id) ON DELETE CASCADE ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED"
            ),
            "add-column SQL should preserve FK reference, got: {sql}"
        );
    }

    #[test]
    fn diff_new_column_preserves_check_constraint() {
        use super::super::types::ColumnType;
        use crate::transpiler::ToSql;

        let mut old = Schema::default();
        old.add_table(Table::new("players").column(Column::new("id", ColumnType::Int)));

        let mut new = old.clone();
        new.tables
            .get_mut("players")
            .expect("players table should exist")
            .columns
            .push(
                Column::new("score", ColumnType::Int).check(CheckExpr::GreaterOrEqual {
                    column: "score".to_string(),
                    value: 0,
                }),
            );

        let cmds = diff_schemas_checked(&old, &new).expect("new checked column should diff");
        let add_col = cmds
            .iter()
            .find(|cmd| matches!(cmd.action, Action::Alter) && cmd.table == "players")
            .expect("add-column command should be present");

        let Expr::Def { constraints, .. } = &add_col.columns[0] else {
            panic!("expected score column definition");
        };
        assert!(constraints.iter().any(|constraint| {
            matches!(
                constraint,
                Constraint::Check(vals) if vals.len() == 1 && vals[0] == "score >= 0"
            )
        }));

        let sql = add_col.to_sql();
        assert!(
            sql.contains("CHECK (score >= 0)"),
            "add-column SQL should preserve CHECK constraint, got: {sql}"
        );
    }

    #[test]
    fn diff_new_column_preserves_unique_constraint() {
        use super::super::types::ColumnType;
        use crate::transpiler::ToSql;

        let mut old = Schema::default();
        old.add_table(Table::new("users").column(Column::new("id", ColumnType::Int)));

        let mut new = old.clone();
        new.tables
            .get_mut("users")
            .expect("users table should exist")
            .columns
            .push(Column::new("email", ColumnType::Text).unique());

        let cmds = diff_schemas_checked(&old, &new).expect("new unique column should diff");
        let add_col = cmds
            .iter()
            .find(|cmd| matches!(cmd.action, Action::Alter) && cmd.table == "users")
            .expect("add-column command should be present");

        let Expr::Def { constraints, .. } = &add_col.columns[0] else {
            panic!("expected email column definition");
        };
        assert!(constraints.contains(&Constraint::Unique));

        let sql = add_col.to_sql();
        assert!(
            sql.contains("UNIQUE"),
            "add-column SQL should preserve UNIQUE constraint, got: {sql}"
        );
    }

    #[test]
    fn diff_new_table_preserves_foreign_key_actions() {
        use super::super::types::ColumnType;
        use crate::transpiler::ToSql;

        let old = Schema::default();
        let mut new = Schema::default();
        new.add_table(Table::new("tenants").column(Column::new("id", ColumnType::Int)));
        new.add_table(
            Table::new("orders").column(
                Column::new("tenant_id", ColumnType::Int)
                    .references("tenants", "id")
                    .on_delete(FkAction::Cascade)
                    .on_update(FkAction::Restrict),
            ),
        );

        let cmds = diff_schemas_checked(&old, &new).expect("new table with FK should diff");
        let make_cmd = cmds
            .iter()
            .find(|cmd| matches!(cmd.action, Action::Make) && cmd.table == "orders")
            .expect("orders create-table command should be present");

        let Expr::Def { constraints, .. } = &make_cmd.columns[0] else {
            panic!("expected tenant_id column definition");
        };
        assert!(constraints.iter().any(|constraint| {
            matches!(
                constraint,
                Constraint::References(target)
                    if target == "tenants(id) ON DELETE CASCADE ON UPDATE RESTRICT"
            )
        }));

        let sql = make_cmd.to_sql();
        assert!(
            sql.contains("REFERENCES tenants(id) ON DELETE CASCADE ON UPDATE RESTRICT"),
            "create-table SQL should preserve FK action clauses, got: {sql}"
        );
    }

    #[test]
    fn diff_new_table_emits_rls_commands_after_create() {
        use super::super::types::ColumnType;

        let old = Schema::default();
        let mut new = Schema::default();
        let mut docs = Table::new("docs").column(Column::new("id", ColumnType::Int));
        docs.enable_rls = true;
        docs.force_rls = true;
        new.add_table(docs);

        let cmds = diff_schemas_checked(&old, &new).expect("new RLS table should diff");
        let make_idx = cmds
            .iter()
            .position(|cmd| matches!(cmd.action, Action::Make) && cmd.table == "docs")
            .expect("create-table command should be present");
        let enable_idx = cmds
            .iter()
            .position(|cmd| matches!(cmd.action, Action::AlterEnableRls) && cmd.table == "docs")
            .expect("enable RLS command should be present");
        let force_idx = cmds
            .iter()
            .position(|cmd| matches!(cmd.action, Action::AlterForceRls) && cmd.table == "docs")
            .expect("force RLS command should be present");

        assert!(make_idx < enable_idx);
        assert!(enable_idx < force_idx);
    }

    #[test]
    fn diff_dropped_tables_orders_child_before_parent_by_incoming_fk_topology() {
        use super::super::types::ColumnType;

        let mut old = Schema::default();
        old.add_table(Table::new("root_a").column(Column::new("id", ColumnType::Int)));
        old.add_table(Table::new("root_b").column(Column::new("id", ColumnType::Int)));
        old.add_table(
            Table::new("parent")
                .column(Column::new("id", ColumnType::Int))
                .column(Column::new("root_a_id", ColumnType::Int).references("root_a", "id"))
                .column(Column::new("root_b_id", ColumnType::Int).references("root_b", "id")),
        );
        old.add_table(
            Table::new("child")
                .column(Column::new("id", ColumnType::Int))
                .column(Column::new("parent_id", ColumnType::Int).references("parent", "id")),
        );

        let mut new = Schema::default();
        new.add_table(Table::new("root_a").column(Column::new("id", ColumnType::Int)));
        new.add_table(Table::new("root_b").column(Column::new("id", ColumnType::Int)));

        let cmds = diff_schemas_checked(&old, &new).expect("dropped tables should diff");
        let child_drop_idx = cmds
            .iter()
            .position(|cmd| matches!(cmd.action, Action::Drop) && cmd.table == "child")
            .expect("child drop should be present");
        let parent_drop_idx = cmds
            .iter()
            .position(|cmd| matches!(cmd.action, Action::Drop) && cmd.table == "parent")
            .expect("parent drop should be present");

        assert!(
            child_drop_idx < parent_drop_idx,
            "child table must be dropped before referenced parent table"
        );
    }

    #[test]
    fn diff_new_table_preserves_column_check_constraint() {
        use super::super::types::ColumnType;
        use crate::transpiler::ToSql;

        let old = Schema::default();
        let mut new = Schema::default();
        new.add_table(
            Table::new("inventory").column(
                Column::new("quantity", ColumnType::Int).not_null().check(
                    CheckExpr::GreaterOrEqual {
                        column: "quantity".to_string(),
                        value: 0,
                    },
                ),
            ),
        );

        let cmds =
            diff_schemas_checked(&old, &new).expect("new table with checked column should diff");
        let make_cmd = cmds
            .iter()
            .find(|cmd| matches!(cmd.action, Action::Make) && cmd.table == "inventory")
            .expect("create-table command should be present");

        let Expr::Def { constraints, .. } = &make_cmd.columns[0] else {
            panic!("expected quantity column definition");
        };
        assert!(constraints.iter().any(|constraint| {
            matches!(
                constraint,
                Constraint::Check(vals) if vals.len() == 1 && vals[0] == "quantity >= 0"
            )
        }));

        let sql = make_cmd.to_sql();
        assert!(
            sql.contains("CHECK (quantity >= 0)"),
            "create-table SQL should preserve CHECK constraint, got: {sql}"
        );
    }

    #[test]
    fn diff_new_partial_unique_index_preserves_predicate() {
        use super::super::types::ColumnType;
        use crate::transpiler::ToSql;

        let mut old = Schema::default();
        old.add_table(
            Table::new("users")
                .column(Column::new("email", ColumnType::Text))
                .column(Column::new("deleted_at", ColumnType::Text)),
        );

        let mut new = old.clone();
        new.add_index(
            Index::new("idx_users_email_active", "users", vec!["email".to_string()])
                .unique()
                .partial(CheckExpr::Sql("deleted_at IS NULL".to_string())),
        );

        let cmds = diff_schemas_checked(&old, &new).expect("new partial index should diff");
        let index_cmd = cmds
            .iter()
            .find(|cmd| matches!(cmd.action, Action::Index))
            .expect("index command should be present");
        let index_def = index_cmd
            .index_def
            .as_ref()
            .expect("index command should carry index definition");

        assert!(index_def.unique);
        assert_eq!(index_def.index_type.as_deref(), Some("btree"));
        assert_eq!(
            index_def.where_clause.as_deref(),
            Some("deleted_at IS NULL")
        );

        let sql = index_cmd.to_sql();
        assert!(
            sql.contains("WHERE deleted_at IS NULL"),
            "index SQL should preserve partial predicate, got: {sql}"
        );
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

    #[test]
    fn rename_hint_does_not_suppress_same_named_add_column_in_other_table() {
        use super::super::types::ColumnType;

        let mut old = Schema::default();
        old.add_table(Table::new("users").column(Column::new("username", ColumnType::Text)));
        old.add_table(Table::new("profiles").column(Column::new("id", ColumnType::Int)));

        let mut new = Schema::default();
        new.add_table(Table::new("users").column(Column::new("name", ColumnType::Text)));
        new.add_table(
            Table::new("profiles")
                .column(Column::new("id", ColumnType::Int))
                .column(Column::new("name", ColumnType::Text)),
        );
        new.add_hint(MigrationHint::Rename {
            from: "users.username".into(),
            to: "users.name".into(),
        });

        let cmds = diff_schemas_checked(&old, &new).expect("schema should diff");

        assert!(cmds.iter().any(|cmd| {
            matches!(cmd.action, Action::Alter)
                && cmd.table == "profiles"
                && matches!(
                    cmd.columns.first(),
                    Some(Expr::Def { name, .. }) if name == "name"
                )
        }));
    }

    #[test]
    fn rename_hint_does_not_suppress_same_named_drop_column_in_other_table() {
        use super::super::types::ColumnType;

        let mut old = Schema::default();
        old.add_table(Table::new("users").column(Column::new("username", ColumnType::Text)));
        old.add_table(
            Table::new("profiles")
                .column(Column::new("id", ColumnType::Int))
                .column(Column::new("username", ColumnType::Text)),
        );

        let mut new = Schema::default();
        new.add_table(Table::new("users").column(Column::new("name", ColumnType::Text)));
        new.add_table(Table::new("profiles").column(Column::new("id", ColumnType::Int)));
        new.add_hint(MigrationHint::Rename {
            from: "users.username".into(),
            to: "users.name".into(),
        });

        let cmds = diff_schemas_checked(&old, &new).expect("schema should diff");

        assert!(cmds.iter().any(|cmd| {
            matches!(cmd.action, Action::AlterDrop)
                && cmd.table == "profiles"
                && matches!(
                    cmd.columns.first(),
                    Some(Expr::Named(name)) if name == "username"
                )
        }));
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
        let make_cmds: Vec<_> = cmds
            .iter()
            .filter(|c| matches!(c.action, Action::Make))
            .collect();
        assert_eq!(make_cmds.len(), 2);

        // Parent (0 FKs) should come BEFORE child (1 FK)
        let parent_idx = make_cmds.iter().position(|c| c.table == "parent").unwrap();
        let child_idx = make_cmds.iter().position(|c| c.table == "child").unwrap();
        assert!(
            parent_idx < child_idx,
            "parent table should be created before child with FK"
        );
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
            Table::new("users").column(Column::new("id", ColumnType::Serial).primary_key()),
        );
        new.add_table(
            Table::new("products").column(Column::new("id", ColumnType::Serial).primary_key()),
        );

        let cmds = diff_schemas(&old, &new);

        let make_cmds: Vec<_> = cmds
            .iter()
            .filter(|c| matches!(c.action, Action::Make))
            .collect();
        assert_eq!(make_cmds.len(), 4);

        // Get positions
        let users_idx = make_cmds.iter().position(|c| c.table == "users").unwrap();
        let products_idx = make_cmds
            .iter()
            .position(|c| c.table == "products")
            .unwrap();
        let orders_idx = make_cmds.iter().position(|c| c.table == "orders").unwrap();
        let items_idx = make_cmds
            .iter()
            .position(|c| c.table == "order_items")
            .unwrap();

        // Tables with 0 FKs should come first
        assert!(users_idx < orders_idx, "users (0 FK) before orders (1 FK)");
        assert!(
            products_idx < items_idx,
            "products (0 FK) before order_items (2 FK)"
        );

        // orders (1 FK) should come before order_items (2 FKs)
        assert!(
            orders_idx < items_idx,
            "orders (1 FK) before order_items (2 FK)"
        );
    }

    #[test]
    fn diff_new_table_preserves_multi_column_foreign_key() {
        use super::super::types::ColumnType;
        use crate::transpiler::ToSql;

        let old = Schema::default();

        let mut new = Schema::default();
        new.add_table(
            Table::new("schedules")
                .column(Column::new("route_id", ColumnType::Text))
                .column(Column::new("schedule_id", ColumnType::Text)),
        );
        new.add_index(
            Index::new(
                "idx_schedules_route_schedule",
                "schedules",
                vec!["route_id".to_string(), "schedule_id".to_string()],
            )
            .unique(),
        );
        new.add_table(
            Table::new("trips")
                .column(Column::new("route_id", ColumnType::Text))
                .column(Column::new("schedule_id", ColumnType::Text))
                .foreign_key(MultiColumnForeignKey::new(
                    vec!["route_id".to_string(), "schedule_id".to_string()],
                    "schedules",
                    vec!["route_id".to_string(), "schedule_id".to_string()],
                )),
        );

        let cmds = diff_schemas(&old, &new);
        let schedules_idx = cmds
            .iter()
            .position(|c| matches!(c.action, Action::Make) && c.table == "schedules")
            .expect("schedules create command should exist");
        let trips_idx = cmds
            .iter()
            .position(|c| matches!(c.action, Action::Make) && c.table == "trips")
            .expect("trips create command should exist");
        let unique_idx = cmds
            .iter()
            .position(|c| {
                matches!(c.action, Action::Index)
                    && c.index_def
                        .as_ref()
                        .is_some_and(|idx| idx.name == "idx_schedules_route_schedule")
            })
            .expect("unique index command should exist");
        let add_fk_idx = cmds
            .iter()
            .position(|c| matches!(c.action, Action::Alter) && c.table == "trips")
            .expect("composite FK ALTER command should exist");

        assert!(schedules_idx < unique_idx);
        assert!(trips_idx < unique_idx);
        assert!(unique_idx < add_fk_idx);

        let trips_cmd = cmds
            .iter()
            .find(|c| matches!(c.action, Action::Make) && c.table == "trips")
            .expect("trips create command should exist");
        assert!(
            trips_cmd.table_constraints.is_empty(),
            "composite foreign keys should not be emitted inline on CREATE TABLE"
        );

        let add_fk_cmd = &cmds[add_fk_idx];
        assert!(
            add_fk_cmd
                .table_constraints
                .iter()
                .any(|constraint| matches!(
                    constraint,
                    crate::ast::TableConstraint::ForeignKey {
                        columns,
                        ref_table,
                        ref_columns,
                        ..
                    } if columns == &["route_id", "schedule_id"]
                        && ref_table == "schedules"
                        && ref_columns == &["route_id", "schedule_id"]
                )),
            "diff should preserve composite FK table constraint"
        );

        let sql = add_fk_cmd.to_sql();
        assert!(
            sql.contains(
                "ALTER TABLE trips ADD FOREIGN KEY (route_id, schedule_id) REFERENCES schedules(route_id, schedule_id)"
            ),
            "generated SQL should include composite foreign key, got: {sql}"
        );
    }

    #[test]
    fn diff_existing_table_adds_multi_column_foreign_key() {
        use super::super::types::ColumnType;
        use crate::transpiler::ToSql;

        let mut old = Schema::default();
        old.add_table(
            Table::new("schedules")
                .column(Column::new("route_id", ColumnType::Text))
                .column(Column::new("schedule_id", ColumnType::Text)),
        );
        old.add_table(
            Table::new("trips")
                .column(Column::new("route_id", ColumnType::Text))
                .column(Column::new("schedule_id", ColumnType::Text)),
        );

        let mut new = old.clone();
        new.add_index(
            Index::new(
                "idx_schedules_route_schedule",
                "schedules",
                vec!["route_id".to_string(), "schedule_id".to_string()],
            )
            .unique(),
        );
        new.tables
            .get_mut("trips")
            .expect("trips table should exist")
            .multi_column_fks
            .push(MultiColumnForeignKey::new(
                vec!["route_id".to_string(), "schedule_id".to_string()],
                "schedules",
                vec!["route_id".to_string(), "schedule_id".to_string()],
            ));

        let cmds = diff_schemas(&old, &new);
        let unique_idx = cmds
            .iter()
            .position(|c| {
                matches!(c.action, Action::Index)
                    && c.index_def
                        .as_ref()
                        .is_some_and(|idx| idx.name == "idx_schedules_route_schedule")
            })
            .expect("unique index command should exist");
        let add_fk_idx = cmds
            .iter()
            .position(|c| matches!(c.action, Action::Alter) && c.table == "trips")
            .expect("composite FK ALTER command should exist");
        assert!(unique_idx < add_fk_idx);

        let add_fk_cmd = &cmds[add_fk_idx];
        let sql = add_fk_cmd.to_sql();
        assert!(
            sql.contains(
                "ALTER TABLE trips ADD FOREIGN KEY (route_id, schedule_id) REFERENCES schedules(route_id, schedule_id)"
            ),
            "generated SQL should add composite foreign key, got: {sql}"
        );
    }
}
