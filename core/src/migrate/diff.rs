//! Schema Diff Visitor
//!
//! Computes the difference between two schemas and generates Qail operations.
//! Now with intent-awareness from MigrationHint.

use super::schema::{
    Generated, MigrationHint, Schema, check_expr_to_sql, foreign_key_to_sql, index_method_str,
    multi_column_fk_to_alter_command,
};
use super::types::ColumnType;
use crate::ast::{Action, ColumnGeneration, Constraint, Expr, IndexDef, Qail};
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

fn unconfirmed_drop_hints(schema: &Schema) -> Vec<String> {
    let mut hints = schema
        .migrations
        .iter()
        .filter_map(|hint| match hint {
            MigrationHint::Drop {
                target,
                confirmed: false,
            } => Some(target.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    hints.sort();
    hints
}

fn existing_column_check_diffs(old: &Schema, new: &Schema) -> Vec<String> {
    let mut changes = Vec::new();

    for (table_name, new_table) in &new.tables {
        let Some(old_table) = old.tables.get(table_name) else {
            continue;
        };

        let old_column_names = old_table
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let new_column_names = new_table
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let existing_column_names = old_column_names
            .intersection(&new_column_names)
            .copied()
            .collect::<std::collections::BTreeSet<_>>();

        let old_checks = table_check_signatures(old_table, &existing_column_names);
        let new_checks = table_check_signatures(new_table, &existing_column_names);

        for (signature, column) in &new_checks {
            if !old_checks.contains_key(signature) {
                changes.push(format!("{}.{}", table_name, column));
            }
        }

        for (signature, column) in &old_checks {
            if !new_checks.contains_key(signature) {
                changes.push(format!("{}.{}", table_name, column));
            }
        }
    }

    changes.sort();
    changes.dedup();
    changes
}

fn table_check_signatures(
    table: &super::schema::Table,
    existing_column_names: &std::collections::BTreeSet<&str>,
) -> std::collections::BTreeMap<String, String> {
    table
        .columns
        .iter()
        .filter(|column| existing_column_names.contains(column.name.as_str()))
        .filter_map(|column| {
            check_signature(&column.check).map(|signature| (signature, column.name.clone()))
        })
        .collect()
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

fn removed_or_changed_multi_column_foreign_keys(old: &Schema, new: &Schema) -> Vec<String> {
    let mut changes = Vec::new();

    for (table_name, old_table) in &old.tables {
        let Some(new_table) = new.tables.get(table_name) else {
            continue;
        };

        for old_fk in &old_table.multi_column_fks {
            if !new_table.multi_column_fks.contains(old_fk) {
                changes.push(format!(
                    "{}.{}",
                    table_name,
                    multi_column_fk_signature(old_fk)
                ));
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

fn existing_column_set_not_null_diffs(old: &Schema, new: &Schema) -> Vec<String> {
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

            if old_col.nullable && !new_col.nullable && !new_col.primary_key {
                changes.push(format!("{}.{}", table_name, new_col.name));
            }
        }
    }

    changes.sort();
    changes
}

fn existing_column_generated_diffs(old: &Schema, new: &Schema) -> Vec<String> {
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

            if generated_signature(&old_col.generated) != generated_signature(&new_col.generated) {
                changes.push(format!("{}.{}", table_name, new_col.name));
            }
        }
    }

    changes.sort();
    changes
}

fn unsupported_existing_column_type_diffs(old: &Schema, new: &Schema) -> Vec<String> {
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

            if old_col.data_type != new_col.data_type
                && !is_safe_existing_column_type_change(&old_col.data_type, &new_col.data_type)
            {
                changes.push(format!(
                    "{}.{} ({} -> {})",
                    table_name,
                    new_col.name,
                    old_col.data_type.to_pg_type(),
                    new_col.data_type.to_pg_type()
                ));
            }
        }
    }

    changes.sort();
    changes
}

fn is_safe_existing_column_type_change(old: &ColumnType, new: &ColumnType) -> bool {
    if old == new {
        return true;
    }

    if is_serial_pseudo_type(old) || is_serial_pseudo_type(new) {
        return false;
    }

    match (old, new) {
        (ColumnType::Int, ColumnType::BigInt) => true,
        (old, ColumnType::Text) if is_unbounded_character_type(old) => true,
        (ColumnType::Text, ColumnType::Varchar(None)) => true,
        (ColumnType::Varchar(None), ColumnType::Text) => true,
        (ColumnType::Varchar(Some(old_len)), ColumnType::Varchar(Some(new_len))) => {
            new_len >= old_len
        }
        (ColumnType::Varchar(Some(_)), ColumnType::Varchar(None)) => true,
        (old, ColumnType::Int | ColumnType::BigInt) if is_smallint_type(old) => true,
        _ => false,
    }
}

fn is_serial_pseudo_type(ty: &ColumnType) -> bool {
    matches!(ty, ColumnType::Serial | ColumnType::BigSerial)
}

fn is_unbounded_character_type(ty: &ColumnType) -> bool {
    matches!(ty, ColumnType::Varchar(_) | ColumnType::Text)
}

fn is_smallint_type(ty: &ColumnType) -> bool {
    matches!(ty, ColumnType::Range(name) if name.eq_ignore_ascii_case("SMALLINT"))
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

fn new_serial_pseudo_type_column_additions(old: &Schema, new: &Schema) -> Vec<String> {
    let mut changes = Vec::new();

    for (table_name, new_table) in &new.tables {
        let Some(old_table) = old.tables.get(table_name) else {
            continue;
        };

        for new_col in &new_table.columns {
            if is_serial_pseudo_type(&new_col.data_type)
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

fn new_required_column_additions_without_value(old: &Schema, new: &Schema) -> Vec<String> {
    let mut changes = Vec::new();

    for (table_name, new_table) in &new.tables {
        let Some(old_table) = old.tables.get(table_name) else {
            continue;
        };

        for new_col in &new_table.columns {
            if !new_col.nullable
                && !new_col.primary_key
                && !column_has_value_source(new_col)
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

fn column_has_value_source(column: &super::schema::Column) -> bool {
    column.default.is_some() || column.generated.is_some()
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

        let reasons = index_difference_reasons(old_idx, new_idx);
        if !reasons.is_empty() {
            changes.push(format!("{} ({})", new_idx.name, reasons.join("; ")));
        }
    }

    changes.sort();
    changes.dedup();
    changes
}

fn check_signature(check: &Option<super::schema::CheckConstraint>) -> Option<String> {
    check
        .as_ref()
        .map(|check| normalize_index_sql_fragment(&check_expr_to_sql(&check.expr)))
}

fn foreign_key_signature(fk: &Option<super::schema::ForeignKey>) -> Option<String> {
    fk.as_ref().map(|fk| format!("{:?}", fk))
}

fn multi_column_fk_signature(fk: &super::schema::MultiColumnForeignKey) -> String {
    match &fk.name {
        Some(name) => format!("constraint:{name}"),
        None => format!("{:?}->{:?}.{:?}", fk.columns, fk.ref_table, fk.ref_columns),
    }
}

fn generated_signature(generated: &Option<Generated>) -> Option<String> {
    match generated {
        Some(Generated::AlwaysStored(expr)) => Some(format!("stored:{expr}")),
        Some(Generated::AlwaysIdentity) => Some("identity:always".to_string()),
        Some(Generated::ByDefaultIdentity) => Some("identity:by_default".to_string()),
        None => None,
    }
}

fn generated_to_constraint(generated: &Generated) -> Constraint {
    match generated {
        Generated::AlwaysStored(expr) => {
            Constraint::Generated(ColumnGeneration::Stored(expr.clone()))
        }
        Generated::AlwaysIdentity => {
            Constraint::Generated(ColumnGeneration::Stored("identity".to_string()))
        }
        Generated::ByDefaultIdentity => {
            Constraint::Generated(ColumnGeneration::Stored("identity_by_default".to_string()))
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ComparableIndex {
    table: String,
    columns: Vec<String>,
    expressions: Vec<String>,
    unique: bool,
    method: &'static str,
    where_clause: Option<String>,
    include: Vec<String>,
}

fn comparable_index(idx: &super::schema::Index) -> ComparableIndex {
    ComparableIndex {
        table: idx.table.clone(),
        columns: normalized_index_fragments(&idx.columns),
        expressions: normalized_index_fragments(&idx.expressions),
        unique: idx.unique,
        method: index_method_str(&idx.method),
        where_clause: idx
            .where_clause
            .as_ref()
            .map(check_expr_to_sql)
            .map(|fragment| normalize_index_sql_fragment(&fragment)),
        include: normalized_index_fragments(&idx.include),
    }
}

fn index_difference_reasons(
    old_idx: &super::schema::Index,
    new_idx: &super::schema::Index,
) -> Vec<String> {
    let old = comparable_index(old_idx);
    let new = comparable_index(new_idx);
    let mut reasons = Vec::new();

    push_index_diff(&mut reasons, "table", &old.table, &new.table);
    push_index_diff(&mut reasons, "columns", &old.columns, &new.columns);
    push_index_diff(
        &mut reasons,
        "expressions",
        &old.expressions,
        &new.expressions,
    );
    push_index_diff(&mut reasons, "unique", &old.unique, &new.unique);
    push_index_diff(&mut reasons, "method", &old.method, &new.method);
    push_index_diff(&mut reasons, "where", &old.where_clause, &new.where_clause);
    push_index_diff(&mut reasons, "include", &old.include, &new.include);

    reasons
}

fn push_index_diff<T>(reasons: &mut Vec<String>, label: &str, old: &T, new: &T)
where
    T: std::fmt::Debug + PartialEq,
{
    if old != new {
        reasons.push(format!("{label}: {old:?} -> {new:?}"));
    }
}

fn normalized_index_fragments(values: &[String]) -> Vec<String> {
    values
        .iter()
        .map(|value| normalize_index_sql_fragment(value))
        .collect()
}

fn normalize_index_sql_fragment(input: &str) -> String {
    let mut normalized = compact_sql_for_index_compare(input);
    normalized = normalized.replace("!=", "<>");
    normalized = normalized.replace("::charactervarying", "");
    normalized = normalized.replace("::varchar", "");
    normalized = normalized.replace("::text", "");

    loop {
        let stripped = strip_redundant_outer_parens(&normalized);
        let simplified = simplify_parenthesized_identifiers(&stripped);
        if simplified == normalized {
            normalized = simplified;
            break;
        }
        normalized = simplified;
    }

    normalize_any_array_predicate(&normalized)
}

fn compact_sql_for_index_compare(input: &str) -> String {
    let mut out = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut chars = input.trim().chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\'' if !in_double => {
                out.push(ch);
                if in_single && chars.peek().is_some_and(|next| *next == '\'') {
                    out.push('\'');
                    chars.next();
                } else {
                    in_single = !in_single;
                }
            }
            '"' if !in_single => {
                out.push(ch);
                if in_double && chars.peek().is_some_and(|next| *next == '"') {
                    out.push('"');
                    chars.next();
                } else {
                    in_double = !in_double;
                }
            }
            _ if !in_single && !in_double && ch.is_whitespace() => {}
            _ if !in_single && !in_double => out.extend(ch.to_lowercase()),
            _ => out.push(ch),
        }
    }

    out
}

fn strip_redundant_outer_parens(input: &str) -> String {
    let mut s = input;
    while s.starts_with('(') && s.ends_with(')') && outer_parens_wrap_entire_fragment(s) {
        s = &s[1..s.len() - 1];
    }
    s.to_string()
}

fn outer_parens_wrap_entire_fragment(input: &str) -> bool {
    let mut depth = 0_i32;
    let mut in_single = false;
    let mut in_double = false;
    let mut chars = input.char_indices().peekable();

    while let Some((idx, ch)) = chars.next() {
        match ch {
            '\'' if !in_double => {
                if in_single && chars.peek().is_some_and(|(_, next)| *next == '\'') {
                    chars.next();
                } else {
                    in_single = !in_single;
                }
            }
            '"' if !in_single => {
                if in_double && chars.peek().is_some_and(|(_, next)| *next == '"') {
                    chars.next();
                } else {
                    in_double = !in_double;
                }
            }
            '(' if !in_single && !in_double => depth += 1,
            ')' if !in_single && !in_double => {
                depth -= 1;
                if depth == 0 && idx != input.len() - 1 {
                    return false;
                }
            }
            _ => {}
        }
    }

    depth == 0 && !in_single && !in_double
}

fn simplify_parenthesized_identifiers(input: &str) -> String {
    let mut out = String::new();
    let chars: Vec<char> = input.chars().collect();
    let mut idx = 0;

    while idx < chars.len() {
        if chars[idx] != '(' {
            out.push(chars[idx]);
            idx += 1;
            continue;
        }

        let Some(end) = matching_paren_chars(&chars, idx) else {
            out.push(chars[idx]);
            idx += 1;
            continue;
        };
        let inner: String = chars[idx + 1..end].iter().collect();
        let preceded_by_identifier = idx > 0 && is_compact_identifier_char(chars[idx - 1]);
        if !preceded_by_identifier && is_compact_identifier_path(&inner) {
            out.push_str(&inner);
            idx = end + 1;
        } else {
            out.push(chars[idx]);
            idx += 1;
        }
    }

    out
}

fn matching_paren_chars(chars: &[char], start: usize) -> Option<usize> {
    let mut depth = 0_i32;
    let mut in_single = false;
    let mut in_double = false;
    let mut idx = start;

    while idx < chars.len() {
        match chars[idx] {
            '\'' if !in_double => {
                if in_single && chars.get(idx + 1).is_some_and(|next| *next == '\'') {
                    idx += 1;
                } else {
                    in_single = !in_single;
                }
            }
            '"' if !in_single => {
                if in_double && chars.get(idx + 1).is_some_and(|next| *next == '"') {
                    idx += 1;
                } else {
                    in_double = !in_double;
                }
            }
            '(' if !in_single && !in_double => depth += 1,
            ')' if !in_single && !in_double => {
                depth -= 1;
                if depth == 0 {
                    return Some(idx);
                }
            }
            _ => {}
        }
        idx += 1;
    }

    None
}

fn is_compact_identifier_path(input: &str) -> bool {
    !input.is_empty() && input.chars().all(is_compact_identifier_char)
}

fn is_compact_identifier_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == '.'
}

fn normalize_any_array_predicate(input: &str) -> String {
    const ANY_ARRAY: &str = "=any(array[";

    let Some(pos) = input.find(ANY_ARRAY) else {
        return input.to_string();
    };
    if !input.ends_with("])") {
        return input.to_string();
    }

    let left = &input[..pos];
    let values = &input[pos + ANY_ARRAY.len()..input.len() - 2];
    format!("{left}in({values})")
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

    let unconfirmed_drops = unconfirmed_drop_hints(new);
    if !unconfirmed_drops.is_empty() {
        return Err(format!(
            "State-based diff refuses unconfirmed destructive drop hints: {}. \
             Add `confirm` to the drop hint or restore the object.",
            unconfirmed_drops.join(", ")
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

    let set_not_null_diffs = existing_column_set_not_null_diffs(old, new);
    if !set_not_null_diffs.is_empty() {
        return Err(format!(
            "State-based diff cannot safely set NOT NULL on existing columns: {}. \
             Use an explicit migration to backfill/validate data before SET NOT NULL.",
            set_not_null_diffs.join(", ")
        ));
    }

    let type_diffs = unsupported_existing_column_type_diffs(old, new);
    if !type_diffs.is_empty() {
        return Err(format!(
            "State-based diff cannot safely alter existing column types without an explicit cast plan: {}. \
             Use an explicit migration with USING/backfill steps for narrowing casts, pseudo-type changes, or data-validating conversions.",
            type_diffs.join(", ")
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

    let new_serial_columns = new_serial_pseudo_type_column_additions(old, new);
    if !new_serial_columns.is_empty() {
        return Err(format!(
            "State-based diff cannot safely add SERIAL/BIGSERIAL columns to existing tables: {}. \
             Use an explicit migration to create the sequence/default or use an identity column plan.",
            new_serial_columns.join(", ")
        ));
    }

    let new_required_columns = new_required_column_additions_without_value(old, new);
    if !new_required_columns.is_empty() {
        return Err(format!(
            "State-based diff cannot safely add required columns without a default/generated value to existing tables: {}. \
             Use an explicit migration to add the column nullable, backfill, then set NOT NULL.",
            new_required_columns.join(", ")
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

    let multi_fk_diffs = removed_or_changed_multi_column_foreign_keys(old, new);
    if !multi_fk_diffs.is_empty() {
        return Err(format!(
            "State-based diff cannot safely drop or replace multi-column foreign keys on existing tables: {}. \
             Use an explicit migration for DROP CONSTRAINT/ADD CONSTRAINT replacement.",
            multi_fk_diffs.join(", ")
        ));
    }

    let generated_diffs = existing_column_generated_diffs(old, new);
    if !generated_diffs.is_empty() {
        return Err(format!(
            "State-based diff cannot safely alter GENERATED/IDENTITY clauses on existing columns: {}. \
             Use an explicit migration for GENERATED/IDENTITY changes.",
            generated_diffs.join(", ")
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
                if let Some(generated) = &col.generated {
                    constraints.push(generated_to_constraint(generated));
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
                        if let Some(generated) = &col.generated {
                            constraints.push(generated_to_constraint(generated));
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
                    include: new_idx.include.clone(),
                    concurrently: new_idx.concurrently,
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
    use super::super::types::ColumnType;
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

    #[test]
    fn state_diff_checked_rejects_unconfirmed_drop_hint() {
        let mut old = Schema::default();
        old.add_table(Table::new("users").column(Column::new("id", ColumnType::Int)));

        let mut new = Schema::default();
        new.add_hint(MigrationHint::Drop {
            target: "users".to_string(),
            confirmed: false,
        });

        let err =
            diff_schemas_checked(&old, &new).expect_err("unconfirmed drop hint should fail closed");
        assert!(err.contains("unconfirmed destructive drop hints"));
        assert!(err.contains("users"));
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
        assert!(err.contains("where:"));
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
    fn state_diff_index_compare_ignores_concurrently_execution_option() {
        let old = schema_with_users_index(Index::new(
            "idx_users_email",
            "users",
            vec!["email".to_string()],
        ));
        let new = schema_with_users_index(
            Index::new("idx_users_email", "users", vec!["email".to_string()]).concurrently(),
        );

        let cmds = diff_schemas_checked(&old, &new)
            .expect("CONCURRENTLY is an execution option, not index definition drift");
        assert!(cmds.is_empty());
    }

    #[test]
    fn state_diff_index_compare_ignores_postgres_predicate_parentheses() {
        let old = schema_with_users_index(
            Index::new(
                "audit_log_session",
                "audit_log",
                vec!["impersonation_session_id".to_string()],
            )
            .partial(CheckExpr::Sql(
                "(impersonation_session_id IS NOT NULL)".to_string(),
            )),
        );
        let new = schema_with_users_index(
            Index::new(
                "audit_log_session",
                "audit_log",
                vec!["impersonation_session_id".to_string()],
            )
            .partial(CheckExpr::Sql(
                "impersonation_session_id IS NOT NULL".to_string(),
            )),
        );

        let cmds = diff_schemas_checked(&old, &new)
            .expect("equivalent partial index predicates should not fail closed");
        assert!(cmds.is_empty());
    }

    #[test]
    fn state_diff_index_compare_ignores_postgres_text_casts() {
        let old = schema_with_users_index(
            Index::expression(
                "users_email_unique_ci",
                "users",
                vec!["lower((email)::text)".to_string()],
            )
            .unique(),
        );
        let new = schema_with_users_index(
            Index::expression(
                "users_email_unique_ci",
                "users",
                vec!["lower(email)".to_string()],
            )
            .unique(),
        );

        let cmds = diff_schemas_checked(&old, &new)
            .expect("equivalent expression index casts should not fail closed");
        assert!(cmds.is_empty());
    }

    #[test]
    fn state_diff_index_compare_ignores_in_any_array_canonicalization() {
        let old = schema_with_users_index(
            Index::new(
                "idx_outbox_due",
                "whatsapp_outbox",
                vec!["next_attempt_at".to_string()],
            )
            .partial(CheckExpr::Sql(
                "status = ANY (ARRAY['pending'::text, 'failed'::text])".to_string(),
            )),
        );
        let new = schema_with_users_index(
            Index::new(
                "idx_outbox_due",
                "whatsapp_outbox",
                vec!["next_attempt_at".to_string()],
            )
            .partial(CheckExpr::Sql(
                "status IN ('pending', 'failed')".to_string(),
            )),
        );

        let cmds = diff_schemas_checked(&old, &new)
            .expect("equivalent IN predicate forms should not fail closed");
        assert!(cmds.is_empty());
    }

    #[test]
    fn state_diff_index_compare_ignores_not_equal_canonicalization() {
        let old = schema_with_users_index(
            Index::new(
                "idx_car_availability_overlap",
                "car_availability",
                vec![
                    "vehicle_id".to_string(),
                    "service_date".to_string(),
                    "start_time".to_string(),
                    "end_time".to_string(),
                ],
            )
            .partial(CheckExpr::Sql(
                "((status)::text <> 'completed'::text)".to_string(),
            )),
        );
        let new = schema_with_users_index(
            Index::new(
                "idx_car_availability_overlap",
                "car_availability",
                vec![
                    "vehicle_id".to_string(),
                    "service_date".to_string(),
                    "start_time".to_string(),
                    "end_time".to_string(),
                ],
            )
            .partial(CheckExpr::Sql("status != 'completed'".to_string())),
        );

        let cmds = diff_schemas_checked(&old, &new)
            .expect("equivalent not-equal predicates should not fail closed");
        assert!(cmds.is_empty());
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
    fn state_diff_check_compare_is_table_scoped_not_column_anchor_scoped() {
        use super::super::types::ColumnType;

        let check = CheckExpr::Sql(
            "((segment_id IS NOT NULL) AND (virtual_segment_id IS NULL)) OR ((segment_id IS NULL) AND (virtual_segment_id IS NOT NULL))"
                .to_string(),
        );

        let mut old = Schema::default();
        old.add_table(
            Table::new("pricing_plans")
                .column(
                    Column::new("segment_id", ColumnType::Uuid)
                        .check_named("pricing_plans_single_source_of_truth", check.clone()),
                )
                .column(Column::new("virtual_segment_id", ColumnType::Uuid)),
        );

        let mut new = Schema::default();
        new.add_table(
            Table::new("pricing_plans")
                .column(Column::new("segment_id", ColumnType::Uuid))
                .column(
                    Column::new("virtual_segment_id", ColumnType::Uuid)
                        .check_named("pricing_plans_single_source_of_truth", check),
                ),
        );

        let cmds = diff_schemas_checked(&old, &new)
            .expect("same table-level CHECK should not depend on inline column anchor");
        assert!(cmds.is_empty());
    }

    #[test]
    fn state_diff_check_compare_normalizes_sql_and_ast_equivalent_checks() {
        let mut old = Schema::default();
        old.add_table(Table::new("inventory").column(
            Column::new("quantity", ColumnType::Int).check_named(
                "inventory_quantity_check",
                CheckExpr::Sql("((quantity >= 0))".to_string()),
            ),
        ));

        let mut new = Schema::default();
        new.add_table(Table::new("inventory").column(
            Column::new("quantity", ColumnType::Int).check(CheckExpr::GreaterOrEqual {
                column: "quantity".to_string(),
                value: 0,
            }),
        ));

        let cmds = diff_schemas_checked(&old, &new)
            .expect("equivalent SQL and AST-native CHECK predicates should not fail closed");
        assert!(cmds.is_empty());
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
    fn state_diff_checked_rejects_existing_column_set_not_null() {
        let mut old = Schema::default();
        old.add_table(Table::new("users").column(Column::new("email", ColumnType::Text)));

        let mut new = Schema::default();
        new.add_table(
            Table::new("users").column(Column::new("email", ColumnType::Text).not_null()),
        );

        let err = diff_schemas_checked(&old, &new)
            .expect_err("SET NOT NULL should require explicit backfill/validation");
        assert!(err.contains("set NOT NULL"));
        assert!(err.contains("users.email"));
    }

    #[test]
    fn state_diff_checked_rejects_new_primary_key_column_on_existing_table() {
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
    fn state_diff_checked_rejects_new_required_column_without_value_source() {
        let mut old = Schema::default();
        old.add_table(Table::new("users").column(Column::new("id", ColumnType::Int)));

        let mut new = old.clone();
        new.tables
            .get_mut("users")
            .expect("users table should exist")
            .columns
            .push(Column::new("email", ColumnType::Text).not_null());

        let err = diff_schemas_checked(&old, &new)
            .expect_err("required column without default should require explicit migration");
        assert!(err.contains("required columns"));
        assert!(err.contains("users.email"));
    }

    #[test]
    fn state_diff_checked_allows_new_required_column_with_default() {
        use crate::transpiler::ToSql;

        let mut old = Schema::default();
        old.add_table(Table::new("users").column(Column::new("id", ColumnType::Int)));

        let mut new = old.clone();
        new.tables
            .get_mut("users")
            .expect("users table should exist")
            .columns
            .push(
                Column::new("status", ColumnType::Text)
                    .not_null()
                    .default("'active'"),
            );

        let cmds = diff_schemas_checked(&old, &new)
            .expect("required column with default should be auto-planned");
        let add_col = cmds
            .iter()
            .find(|cmd| cmd.action == Action::Alter && cmd.table == "users")
            .expect("add-column command should be present");

        let sql = add_col.to_sql();
        assert!(
            sql.contains("ADD COLUMN status TEXT NOT NULL DEFAULT 'active'"),
            "add-column SQL should preserve default-backed NOT NULL, got: {sql}"
        );
    }

    #[test]
    fn state_diff_checked_rejects_new_serial_pseudo_type_column_on_existing_table() {
        let mut old = Schema::default();
        old.add_table(Table::new("events").column(Column::new("name", ColumnType::Text)));

        let mut new = old.clone();
        new.tables
            .get_mut("events")
            .expect("events table should exist")
            .columns
            .push(Column::new("id", ColumnType::Serial));

        let err = diff_schemas_checked(&old, &new)
            .expect_err("SERIAL add-column cannot be represented by ALTER ADD COLUMN INTEGER");
        assert!(err.contains("SERIAL/BIGSERIAL"));
        assert!(err.contains("events.id"));
    }

    #[test]
    fn state_diff_checked_rejects_unsafe_existing_column_type_change() {
        let mut old = Schema::default();
        old.add_table(Table::new("events").column(Column::new("external_id", ColumnType::Text)));

        let mut new = Schema::default();
        new.add_table(Table::new("events").column(Column::new("external_id", ColumnType::Uuid)));

        let err = diff_schemas_checked(&old, &new)
            .expect_err("TEXT -> UUID should require an explicit cast plan");
        assert!(err.contains("existing column types"));
        assert!(err.contains("events.external_id"));
        assert!(err.contains("TEXT -> UUID"));
    }

    #[test]
    fn state_diff_checked_rejects_existing_column_serial_pseudo_type_change() {
        let mut old = Schema::default();
        old.add_table(Table::new("events").column(Column::new("id", ColumnType::Int)));

        let mut new = Schema::default();
        new.add_table(Table::new("events").column(Column::new("id", ColumnType::Serial)));

        let err = diff_schemas_checked(&old, &new)
            .expect_err("INT -> SERIAL cannot be represented by ALTER COLUMN TYPE");
        assert!(err.contains("existing column types"));
        assert!(err.contains("events.id"));
        assert!(err.contains("INT -> SERIAL"));
    }

    #[test]
    fn state_diff_checked_allows_safe_existing_column_type_widening() {
        use crate::transpiler::ToSql;

        let mut old = Schema::default();
        old.add_table(Table::new("events").column(Column::new("counter", ColumnType::Int)));

        let mut new = Schema::default();
        new.add_table(Table::new("events").column(Column::new("counter", ColumnType::BigInt)));

        let cmds = diff_schemas_checked(&old, &new).expect("INT -> BIGINT should be auto-planned");
        let type_cmd = cmds
            .iter()
            .find(|cmd| cmd.action == Action::AlterType && cmd.table == "events")
            .expect("ALTER TYPE command should be present");

        assert_eq!(
            type_cmd.to_sql(),
            "ALTER TABLE events ALTER COLUMN counter TYPE BIGINT"
        );
    }

    #[test]
    fn state_diff_checked_rejects_varchar_length_narrowing() {
        let mut old = Schema::default();
        old.add_table(
            Table::new("users").column(Column::new("display_name", ColumnType::Varchar(Some(255)))),
        );

        let mut new = Schema::default();
        new.add_table(
            Table::new("users").column(Column::new("display_name", ColumnType::Varchar(Some(64)))),
        );

        let err = diff_schemas_checked(&old, &new)
            .expect_err("VARCHAR length shrink should require explicit validation");
        assert!(err.contains("existing column types"));
        assert!(err.contains("users.display_name"));
    }

    #[test]
    fn state_diff_checked_rejects_existing_column_foreign_key_addition() {
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
    fn diff_new_column_preserves_generated_constraint() {
        use super::super::types::ColumnType;
        use crate::transpiler::ToSql;

        let mut old = Schema::default();
        old.add_table(
            Table::new("people")
                .column(Column::new("first_name", ColumnType::Text))
                .column(Column::new("last_name", ColumnType::Text)),
        );

        let mut new = old.clone();
        new.tables
            .get_mut("people")
            .expect("people table should exist")
            .columns
            .push(
                Column::new("full_name", ColumnType::Text)
                    .generated_stored("first_name || ' ' || last_name"),
            );

        let cmds = diff_schemas_checked(&old, &new).expect("new generated column should diff");
        let add_col = cmds
            .iter()
            .find(|cmd| matches!(cmd.action, Action::Alter) && cmd.table == "people")
            .expect("add-column command should be present");

        let Expr::Def { constraints, .. } = &add_col.columns[0] else {
            panic!("expected generated column definition");
        };
        assert!(constraints.iter().any(|constraint| {
            matches!(
                constraint,
                Constraint::Generated(ColumnGeneration::Stored(expr))
                    if expr == "first_name || ' ' || last_name"
            )
        }));

        let sql = add_col.to_sql();
        assert!(
            sql.contains("GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED"),
            "add-column SQL should preserve GENERATED clause, got: {sql}"
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
    fn diff_new_table_preserves_generated_and_identity_columns() {
        use super::super::types::ColumnType;
        use crate::transpiler::ToSql;

        let old = Schema::default();
        let mut new = Schema::default();
        new.add_table(
            Table::new("people")
                .column(Column::new("first_name", ColumnType::Text))
                .column(Column::new("last_name", ColumnType::Text))
                .column(
                    Column::new("full_name", ColumnType::Text)
                        .generated_stored("first_name || ' ' || last_name"),
                )
                .column(Column::new("row_seq", ColumnType::BigInt).generated_by_default()),
        );

        let cmds = diff_schemas_checked(&old, &new).expect("new table should diff");
        let make_cmd = cmds
            .iter()
            .find(|cmd| matches!(cmd.action, Action::Make) && cmd.table == "people")
            .expect("create-table command should be present");

        let sql = make_cmd.to_sql();
        assert!(
            sql.contains("GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED"),
            "create-table SQL should preserve GENERATED clause, got: {sql}"
        );
        assert!(
            sql.contains("GENERATED BY DEFAULT AS IDENTITY"),
            "create-table SQL should preserve IDENTITY clause, got: {sql}"
        );
    }

    #[test]
    fn state_diff_rejects_generated_changes_on_existing_columns() {
        use super::super::types::ColumnType;

        let mut old = Schema::default();
        old.add_table(Table::new("people").column(Column::new("full_name", ColumnType::Text)));

        let mut new = Schema::default();
        new.add_table(
            Table::new("people").column(
                Column::new("full_name", ColumnType::Text)
                    .generated_stored("first_name || ' ' || last_name"),
            ),
        );

        let err = validate_state_diff_support(&old, &new)
            .expect_err("generated changes on existing columns should fail closed");
        assert!(err.contains("GENERATED/IDENTITY"), "{err}");
        assert!(err.contains("people.full_name"), "{err}");
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
    fn diff_new_covering_concurrent_index_preserves_options() {
        use super::super::types::ColumnType;
        use crate::transpiler::ToSql;

        let mut old = Schema::default();
        old.add_table(
            Table::new("users")
                .column(Column::new("email", ColumnType::Text))
                .column(Column::new("name", ColumnType::Text))
                .column(Column::new("created_at", ColumnType::Timestamp)),
        );

        let mut new = old.clone();
        new.add_index(
            Index::new("idx_users_email_cover", "users", vec!["email".to_string()])
                .include(vec!["name".to_string(), "created_at".to_string()])
                .concurrently(),
        );

        let cmds =
            diff_schemas_checked(&old, &new).expect("new covering concurrent index should diff");
        let index_cmd = cmds
            .iter()
            .find(|cmd| matches!(cmd.action, Action::Index))
            .expect("index command should be present");
        let index_def = index_cmd
            .index_def
            .as_ref()
            .expect("index command should carry index definition");

        assert!(index_def.concurrently);
        assert_eq!(
            index_def.include,
            vec!["name".to_string(), "created_at".to_string()]
        );

        let sql = index_cmd.to_sql();
        assert!(
            sql.contains("CREATE INDEX CONCURRENTLY idx_users_email_cover"),
            "index SQL should preserve CONCURRENTLY, got: {sql}"
        );
        assert!(
            sql.contains("INCLUDE (name, created_at)"),
            "index SQL should preserve INCLUDE columns, got: {sql}"
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

    #[test]
    fn state_diff_support_rejects_removed_multi_column_foreign_key() {
        use super::super::types::ColumnType;

        let mut old = Schema::default();
        old.add_table(
            Table::new("schedules")
                .column(Column::new("route_id", ColumnType::Text))
                .column(Column::new("schedule_id", ColumnType::Text)),
        );
        old.add_table(
            Table::new("trips")
                .column(Column::new("route_id", ColumnType::Text))
                .column(Column::new("schedule_id", ColumnType::Text))
                .foreign_key(MultiColumnForeignKey::new(
                    vec!["route_id".to_string(), "schedule_id".to_string()],
                    "schedules",
                    vec!["route_id".to_string(), "schedule_id".to_string()],
                )),
        );

        let mut new = old.clone();
        new.tables
            .get_mut("trips")
            .expect("trips table should exist")
            .multi_column_fks
            .clear();

        let err =
            diff_schemas_checked(&old, &new).expect_err("removed composite FK should fail closed");
        assert!(err.contains("multi-column foreign keys"));
        assert!(err.contains("trips."));
    }

    #[test]
    fn state_diff_support_rejects_changed_multi_column_foreign_key() {
        use super::super::types::ColumnType;

        let mut old = Schema::default();
        old.add_table(
            Table::new("schedules")
                .column(Column::new("route_id", ColumnType::Text))
                .column(Column::new("schedule_id", ColumnType::Text)),
        );
        old.add_table(
            Table::new("trips")
                .column(Column::new("route_id", ColumnType::Text))
                .column(Column::new("schedule_id", ColumnType::Text))
                .foreign_key(MultiColumnForeignKey::new(
                    vec!["route_id".to_string(), "schedule_id".to_string()],
                    "schedules",
                    vec!["route_id".to_string(), "schedule_id".to_string()],
                )),
        );

        let mut new = old.clone();
        new.tables
            .get_mut("trips")
            .expect("trips table should exist")
            .multi_column_fks[0] = MultiColumnForeignKey::new(
            vec!["route_id".to_string(), "schedule_id".to_string()],
            "schedules",
            vec!["schedule_id".to_string(), "route_id".to_string()],
        );

        let err =
            diff_schemas_checked(&old, &new).expect_err("changed composite FK should fail closed");
        assert!(err.contains("multi-column foreign keys"));
        assert!(err.contains("trips."));
    }
}
