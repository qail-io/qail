//! Post-apply verification gates for migration safety.

use crate::colors::*;
use anyhow::{Result, anyhow, bail};
use qail_core::ast::{Operator, Qail};
use qail_core::migrate::{Schema, policy::PolicyPermissiveness};
use qail_pg::driver::PgDriver;
use std::collections::{BTreeSet, HashSet};

/// Run post-apply verification before migration record/commit.
pub async fn post_apply_verify(
    driver: &mut PgDriver,
    expected_schema: &Schema,
    cmds: &[Qail],
) -> Result<()> {
    println!();
    println!("{}", "🧪 Post-Apply Verification".cyan().bold());

    let live_schema = crate::shadow::introspect_schema(driver).await?;

    verify_schema_fingerprint(expected_schema, &live_schema)?;
    println!("  {} Schema fingerprint match", "✓".green());

    verify_foreign_keys(expected_schema, &live_schema)?;
    println!("  {} Foreign key validation passed", "✓".green());

    verify_indexes(expected_schema, &live_schema)?;
    println!("  {} Index validation passed", "✓".green());

    verify_policies(driver, expected_schema).await?;
    println!("  {} Policy validation passed", "✓".green());

    smoke_read_checks(driver, expected_schema, cmds).await?;
    println!("  {} Smoke query checks passed", "✓".green());

    Ok(())
}

fn verify_schema_fingerprint(expected: &Schema, live: &Schema) -> Result<()> {
    let expected_lines = schema_fingerprint_lines(expected);
    let live_lines = schema_fingerprint_lines(live);

    let expected_fp = crate::time::md5_hex(&expected_lines.join("\n"));
    let live_fp = crate::time::md5_hex(&live_lines.join("\n"));

    if expected_fp == live_fp {
        return Ok(());
    }

    let expected_set: BTreeSet<String> = expected_lines.into_iter().collect();
    let live_set: BTreeSet<String> = live_lines.into_iter().collect();

    let missing: Vec<String> = expected_set
        .difference(&live_set)
        .take(8)
        .cloned()
        .collect();
    let unexpected: Vec<String> = live_set
        .difference(&expected_set)
        .take(8)
        .cloned()
        .collect();

    let mut details = String::new();
    if !missing.is_empty() {
        details.push_str(&format!("\n  Missing ({}) sample:", missing.len()));
        for line in &missing {
            details.push_str(&format!("\n    - {}", line));
        }
    }
    if !unexpected.is_empty() {
        details.push_str(&format!("\n  Unexpected ({}) sample:", unexpected.len()));
        for line in &unexpected {
            details.push_str(&format!("\n    - {}", line));
        }
    }

    bail!(
        "Schema fingerprint mismatch after apply (expected={}, live={}).{}",
        expected_fp,
        live_fp,
        details
    );
}

fn schema_fingerprint_lines(schema: &Schema) -> Vec<String> {
    let mut lines = Vec::new();

    let mut table_names: Vec<String> = schema.tables.keys().cloned().collect();
    table_names.sort();
    for table_name in table_names {
        let table = schema.tables.get(&table_name).expect("table exists");
        lines.push(format!(
            "T|{}|rls={}|force={}",
            table.name, table.enable_rls, table.force_rls
        ));

        let mut columns = table.columns.clone();
        columns.sort_by(|a, b| a.name.cmp(&b.name));
        for col in columns {
            let fk = col
                .foreign_key
                .as_ref()
                .map(|fk| {
                    format!(
                        "{}:{}:{:?}:{:?}",
                        fk.table, fk.column, fk.on_delete, fk.on_update
                    )
                })
                .unwrap_or_else(|| "-".to_string());
            lines.push(format!(
                "C|{}|{}|{}|nullable={}|pk={}|unique={}|fk={}",
                table.name,
                col.name,
                col.data_type.to_pg_type(),
                col.nullable,
                col.primary_key,
                col.unique,
                fk
            ));
        }
    }

    let mut indexes = schema.indexes.clone();
    indexes.sort_by(|a, b| a.table.cmp(&b.table).then(a.name.cmp(&b.name)));
    for idx in indexes {
        // Fingerprint only simple indexes currently represented by introspection.
        if !idx.expressions.is_empty() || idx.where_clause.is_some() || !idx.include.is_empty() {
            continue;
        }
        lines.push(format!(
            "I|{}|{}|unique={}|cols={}",
            idx.table,
            idx.name,
            idx.unique,
            idx.columns
                .iter()
                .map(|c| normalize_ident(c))
                .collect::<Vec<_>>()
                .join(",")
        ));
    }

    lines
}

fn verify_foreign_keys(expected: &Schema, live: &Schema) -> Result<()> {
    let mut errors = Vec::<String>::new();
    for (table_name, expected_table) in &expected.tables {
        let Some(live_table) = live.tables.get(table_name) else {
            errors.push(format!("table '{}' missing in live schema", table_name));
            continue;
        };

        for expected_col in &expected_table.columns {
            let Some(expected_fk) = &expected_col.foreign_key else {
                continue;
            };
            let live_col = live_table
                .columns
                .iter()
                .find(|c| c.name == expected_col.name);
            let Some(live_col) = live_col else {
                errors.push(format!(
                    "column '{}.{}' missing in live schema",
                    table_name, expected_col.name
                ));
                continue;
            };

            match &live_col.foreign_key {
                Some(live_fk)
                    if live_fk.table == expected_fk.table
                        && live_fk.column == expected_fk.column
                        && live_fk.on_delete == expected_fk.on_delete
                        && live_fk.on_update == expected_fk.on_update => {}
                Some(live_fk) => {
                    errors.push(format!(
                        "FK mismatch for '{}.{}': expected {}.{} ({:?}/{:?}), got {}.{} ({:?}/{:?})",
                        table_name,
                        expected_col.name,
                        expected_fk.table,
                        expected_fk.column,
                        expected_fk.on_delete,
                        expected_fk.on_update,
                        live_fk.table,
                        live_fk.column,
                        live_fk.on_delete,
                        live_fk.on_update
                    ));
                }
                None => errors.push(format!(
                    "FK missing for '{}.{}' (expected {}.{})",
                    table_name, expected_col.name, expected_fk.table, expected_fk.column
                )),
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "Foreign key verification failed:\n{}",
            errors.join("\n")
        ))
    }
}

fn verify_indexes(expected: &Schema, live: &Schema) -> Result<()> {
    let mut live_index_keys = HashSet::<String>::new();
    for idx in &live.indexes {
        let key = index_key(
            idx.table.as_str(),
            idx.name.as_str(),
            idx.unique,
            &idx.columns,
        );
        live_index_keys.insert(key);
    }

    let mut missing = Vec::<String>::new();
    let mut skipped = 0usize;
    for idx in &expected.indexes {
        if !idx.expressions.is_empty() || idx.where_clause.is_some() || !idx.include.is_empty() {
            skipped += 1;
            continue;
        }
        let key = index_key(
            idx.table.as_str(),
            idx.name.as_str(),
            idx.unique,
            &idx.columns,
        );
        if !live_index_keys.contains(&key) {
            missing.push(format!(
                "{} on {} ({})",
                idx.name,
                idx.table,
                idx.columns.join(", ")
            ));
        }
    }

    if !missing.is_empty() {
        return Err(anyhow!(
            "Index verification failed. Missing indexes:\n{}",
            missing.join("\n")
        ));
    }

    if skipped > 0 {
        println!(
            "  {} Skipped {} advanced index check(s) (expression/partial/include)",
            "⚠".yellow(),
            skipped
        );
    }

    Ok(())
}

async fn verify_policies(driver: &mut PgDriver, expected_schema: &Schema) -> Result<()> {
    if expected_schema.policies.is_empty() {
        return Ok(());
    }

    let rows = Qail::get("pg_policies")
        .columns(["tablename", "policyname", "cmd", "permissive"])
        .filter("schemaname", Operator::Eq, "public");
    let rows = driver
        .fetch_all(&rows)
        .await
        .map_err(|e| anyhow!("Failed to query pg_policies: {}", e))?;

    let mut live_policies = HashSet::<String>::new();
    for row in rows {
        let table = row.get_string(0).unwrap_or_default();
        let name = row.get_string(1).unwrap_or_default();
        let cmd = normalize_policy_cmd(&row.get_string(2).unwrap_or_default());
        let permissive = normalize_policy_permissive(&row.get_string(3).unwrap_or_default());
        live_policies.insert(format!("{}|{}|{}|{}", table, name, cmd, permissive));
    }

    let mut missing = Vec::<String>::new();
    for p in &expected_schema.policies {
        let cmd = normalize_policy_cmd(&p.target.to_string());
        let permissive = match p.permissiveness {
            PolicyPermissiveness::Permissive => "PERMISSIVE".to_string(),
            PolicyPermissiveness::Restrictive => "RESTRICTIVE".to_string(),
        };
        let key = format!("{}|{}|{}|{}", p.table, p.name, cmd, permissive);
        if !live_policies.contains(&key) {
            missing.push(format!(
                "{} on {} (cmd={}, {})",
                p.name, p.table, cmd, permissive
            ));
        }
    }

    if missing.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "Policy verification failed. Missing policies:\n{}",
            missing.join("\n")
        ))
    }
}

async fn smoke_read_checks(
    driver: &mut PgDriver,
    expected_schema: &Schema,
    cmds: &[Qail],
) -> Result<()> {
    let expected_tables: HashSet<String> = expected_schema.tables.keys().cloned().collect();
    let mut touched = BTreeSet::<String>::new();
    for cmd in cmds {
        if expected_tables.contains(&cmd.table) {
            touched.insert(cmd.table.clone());
        }
    }

    // Fallback: if no touched tables in target state, sample the first few tables.
    let mut smoke_tables: Vec<String> = touched.into_iter().collect();
    if smoke_tables.is_empty() {
        smoke_tables = expected_schema
            .tables
            .keys()
            .take(5)
            .cloned()
            .collect::<Vec<_>>();
    }
    smoke_tables.sort();
    if smoke_tables.len() > 20 {
        smoke_tables.truncate(20);
    }

    for table in &smoke_tables {
        let cmd = Qail::get(table).column("1").limit(1);
        driver
            .fetch_all(&cmd)
            .await
            .map_err(|e| anyhow!("Smoke query failed on table '{}': {}", table, e))?;
    }

    Ok(())
}

fn index_key(table: &str, name: &str, unique: bool, cols: &[String]) -> String {
    format!(
        "{}|{}|{}|{}",
        normalize_ident(table),
        normalize_ident(name),
        unique,
        cols.iter()
            .map(|c| normalize_ident(c))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn normalize_ident(s: &str) -> String {
    s.trim()
        .trim_matches('"')
        .replace('\"', "")
        .to_ascii_lowercase()
}

fn normalize_policy_cmd(s: &str) -> String {
    let up = s.trim().to_ascii_uppercase();
    match up.as_str() {
        "*" => "ALL".to_string(),
        other => other.to_string(),
    }
}

fn normalize_policy_permissive(s: &str) -> String {
    let up = s.trim().to_ascii_uppercase();
    match up.as_str() {
        "T" | "TRUE" => "PERMISSIVE".to_string(),
        "F" | "FALSE" => "RESTRICTIVE".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_ident, normalize_policy_cmd, normalize_policy_permissive,
        schema_fingerprint_lines,
    };
    use qail_core::migrate::{Column, ColumnType, Schema, Table};

    #[test]
    fn normalize_ident_is_stable() {
        assert_eq!(normalize_ident("\"Users\""), "users");
        assert_eq!(normalize_ident("  Posts "), "posts");
    }

    #[test]
    fn policy_normalization_handles_variants() {
        assert_eq!(normalize_policy_cmd("*"), "ALL");
        assert_eq!(normalize_policy_cmd("select"), "SELECT");
        assert_eq!(normalize_policy_permissive("t"), "PERMISSIVE");
        assert_eq!(normalize_policy_permissive("RESTRICTIVE"), "RESTRICTIVE");
    }

    #[test]
    fn schema_fingerprint_is_order_stable() {
        let users = Table::new("users")
            .column(Column::new("email", ColumnType::Text))
            .column(Column::new("id", ColumnType::Uuid).primary_key());
        let posts = Table::new("posts").column(Column::new("id", ColumnType::Uuid).primary_key());

        let mut a = Schema::new();
        a.add_table(users.clone());
        a.add_table(posts.clone());

        let mut b = Schema::new();
        b.add_table(posts);
        b.add_table(users);

        assert_eq!(schema_fingerprint_lines(&a), schema_fingerprint_lines(&b));
    }
}
