//! Migration file discovery and helper functions.

use super::types::{MigrateDirection, MigrationFile, MigrationPhase};
use crate::colors::*;
use anyhow::Result;
use std::fs;
use std::path::Path;

pub(super) fn phase_rank(phase: MigrationPhase) -> u8 {
    match phase {
        MigrationPhase::Expand => 0,
        MigrationPhase::Backfill => 1,
        MigrationPhase::Contract => 2,
    }
}

pub(super) fn detect_phase(name: &str) -> MigrationPhase {
    let lower = name.to_ascii_lowercase();
    if lower.contains("contract") {
        MigrationPhase::Contract
    } else if lower.contains("backfill") {
        MigrationPhase::Backfill
    } else {
        MigrationPhase::Expand
    }
}

pub(super) fn normalize_group_key(name: &str) -> String {
    let lower = name.to_ascii_lowercase();
    let mut base = lower
        .trim_end_matches(".qail")
        .trim_end_matches(".up")
        .trim_end_matches(".down")
        .to_string();
    for token in [
        ".expand",
        ".backfill",
        ".contract",
        "_expand",
        "_backfill",
        "_contract",
        "-expand",
        "-backfill",
        "-contract",
    ] {
        if let Some(stripped) = base.strip_suffix(token) {
            base = stripped.to_string();
        }
    }
    base
}

pub(super) fn is_valid_ident(ident: &str) -> bool {
    let mut parts = ident.split('.');
    let mut seen = false;
    for part in &mut parts {
        seen = true;
        if part.is_empty()
            || !part.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            || !part
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        {
            return false;
        }
    }
    seen
}

pub(super) fn unquote_sql_ident(token: &str) -> String {
    let cleaned = token
        .trim()
        .trim_end_matches(',')
        .trim_end_matches(';')
        .trim_matches('"')
        .trim_matches('`')
        .to_string();
    cleaned
        .split('.')
        .next_back()
        .unwrap_or(cleaned.as_str())
        .trim_matches('"')
        .trim_matches('`')
        .to_ascii_lowercase()
}

pub(super) fn parse_drop_targets(sql: &str) -> (Vec<String>, Vec<(String, String)>) {
    let mut tables = Vec::new();
    let mut columns = Vec::new();

    for stmt in sql.split(';') {
        let normalized = stmt.replace(['\n', '\t'], " ");
        let tokens: Vec<String> = normalized
            .split_whitespace()
            .map(|t| t.trim().to_string())
            .collect();
        if tokens.is_empty() {
            continue;
        }
        let upper: Vec<String> = tokens.iter().map(|t| t.to_ascii_uppercase()).collect();

        if upper.len() >= 3 && upper[0] == "DROP" && upper[1] == "TABLE" {
            let mut idx = 2usize;
            if upper.get(idx).is_some_and(|t| t == "IF")
                && upper.get(idx + 1).is_some_and(|t| t == "EXISTS")
            {
                idx += 2;
            }
            if let Some(name) = tokens.get(idx) {
                tables.push(unquote_sql_ident(name));
            }
            continue;
        }

        if upper.len() >= 6 && upper[0] == "ALTER" && upper[1] == "TABLE" {
            let mut table_idx = 2usize;
            if upper.get(table_idx).is_some_and(|t| t == "ONLY") {
                table_idx += 1;
            }
            let Some(table_token) = tokens.get(table_idx) else {
                continue;
            };
            let table = unquote_sql_ident(table_token);

            let drop_idx = upper.iter().position(|t| t == "DROP");
            let col_idx = upper.iter().position(|t| t == "COLUMN");
            if let (Some(d_idx), Some(c_idx)) = (drop_idx, col_idx)
                && d_idx + 1 == c_idx
            {
                let mut idx = c_idx + 1;
                if upper.get(idx).is_some_and(|t| t == "IF")
                    && upper.get(idx + 1).is_some_and(|t| t == "EXISTS")
                {
                    idx += 2;
                }
                if let Some(col_token) = tokens.get(idx) {
                    columns.push((table, unquote_sql_ident(col_token)));
                }
            }
        }
    }

    (tables, columns)
}

/// Discover migration files in both flat and subdirectory layouts.
///
/// Supported layouts:
///   Flat:   `migrations/001_name.up.qail`
///   Subdir: `migrations/20251207000000_name/up.qail`
///
/// Raw `.sql` files are rejected to enforce the type-safe barrier.
pub(crate) fn discover_migrations(
    migrations_dir: &Path,
    direction: MigrateDirection,
) -> Result<Vec<MigrationFile>> {
    let suffix = match direction {
        MigrateDirection::Up => "up",
        MigrateDirection::Down => "down",
    };

    let mut migrations = Vec::new();

    for entry in fs::read_dir(migrations_dir)?.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name_str = name.to_string_lossy().to_string();

        if path.is_dir() {
            // Subdirectory layout:
            //   legacy:   <dir>/up.qail
            //   phased:   <dir>/expand.qail, backfill.qail, contract.qail
            if matches!(direction, MigrateDirection::Up) {
                let phased = [
                    ("expand.qail", MigrationPhase::Expand),
                    ("backfill.qail", MigrationPhase::Backfill),
                    ("contract.qail", MigrationPhase::Contract),
                ];
                let mut has_phased_files = false;

                for (filename, phase) in phased {
                    let qail_file = path.join(filename);
                    let sql_file = path.join(filename.replace(".qail", ".sql"));
                    if sql_file.exists() && !qail_file.exists() {
                        eprintln!(
                            "  {} {}/{} found but .sql is not supported — convert to .qail",
                            "⚠".yellow(),
                            name_str,
                            filename.replace(".qail", ".sql")
                        );
                    }
                    if qail_file.exists() {
                        has_phased_files = true;
                        migrations.push(MigrationFile {
                            group_key: name_str.clone(),
                            sort_key: format!("{}/{}", name_str, filename),
                            display_name: format!("{}/{}", name_str, filename),
                            path: qail_file,
                            phase,
                        });
                    }
                }

                // Legacy fallback when phased files do not exist
                if !has_phased_files {
                    let qail_file = path.join("up.qail");
                    let sql_file = path.join("up.sql");
                    if sql_file.exists() && !qail_file.exists() {
                        eprintln!(
                            "  {} {}/up.sql found but .sql is not supported — convert to .qail",
                            "⚠".yellow(),
                            name_str
                        );
                        continue;
                    }
                    if qail_file.exists() {
                        migrations.push(MigrationFile {
                            group_key: name_str.clone(),
                            sort_key: name_str.clone(),
                            display_name: format!("{}/up.qail", name_str),
                            path: qail_file,
                            phase: MigrationPhase::Expand,
                        });
                    }
                }
            } else {
                // Down direction: keep single rollback file
                let qail_file = path.join(format!("{}.qail", suffix));
                let sql_file = path.join(format!("{}.sql", suffix));
                if sql_file.exists() && !qail_file.exists() {
                    eprintln!(
                        "  {} {}/{}.sql found but .sql is not supported — convert to .qail",
                        "⚠".yellow(),
                        name_str,
                        suffix
                    );
                    continue;
                }
                if qail_file.exists() {
                    migrations.push(MigrationFile {
                        group_key: name_str.clone(),
                        sort_key: name_str.clone(),
                        display_name: format!("{}/{}.qail", name_str, suffix),
                        path: qail_file,
                        phase: detect_phase(&name_str),
                    });
                }
            }
        } else if path.is_file() {
            // Flat layout: NNN_name.up.qail / NNN_name.down.qail
            let flat_suffix = format!(".{}.qail", suffix);
            if name_str.ends_with(&flat_suffix) {
                let group_name = name_str.trim_end_matches(&flat_suffix);
                migrations.push(MigrationFile {
                    group_key: normalize_group_key(group_name),
                    sort_key: name_str.clone(),
                    display_name: name_str.clone(),
                    path: path.clone(),
                    phase: detect_phase(&name_str),
                });
            } else if name_str.ends_with(&format!(".{}.sql", suffix)) {
                eprintln!(
                    "  {} {} — .sql migrations are not supported, convert to .qail",
                    "⚠".yellow(),
                    name_str
                );
            }
        }
    }

    // Sort by group key + phase order to enforce expand -> backfill -> contract.
    migrations.sort_by(|a, b| {
        a.group_key
            .cmp(&b.group_key)
            .then_with(|| phase_rank(a.phase).cmp(&phase_rank(b.phase)))
            .then_with(|| a.sort_key.cmp(&b.sort_key))
    });

    Ok(migrations)
}
