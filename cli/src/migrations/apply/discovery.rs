//! Migration file discovery and helper functions.

use super::types::{MigrateDirection, MigrationFile, MigrationPhase};
use anyhow::{Result, anyhow};
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
    if lower
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .any(|part| part == "contract")
    {
        MigrationPhase::Contract
    } else if lower
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .any(|part| part == "backfill")
    {
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

    for stmt in split_sql_statements(sql) {
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
            if upper.get(idx).is_some_and(|t| t == "ONLY") {
                idx += 1;
            }
            while let Some(name) = tokens.get(idx) {
                if upper
                    .get(idx)
                    .is_some_and(|t| matches!(t.as_str(), "CASCADE" | "RESTRICT"))
                {
                    break;
                }
                for part in name.split(',') {
                    let target = unquote_sql_ident(part);
                    if !target.is_empty() {
                        tables.push(target);
                    }
                }
                idx += 1;
            }
            continue;
        }

        if upper.len() >= 6 && upper[0] == "ALTER" && upper[1] == "TABLE" {
            let mut table_idx = 2usize;
            if upper.get(table_idx).is_some_and(|t| t == "IF")
                && upper.get(table_idx + 1).is_some_and(|t| t == "EXISTS")
            {
                table_idx += 2;
            }
            if upper.get(table_idx).is_some_and(|t| t == "ONLY") {
                table_idx += 1;
            }
            let Some(table_token) = tokens.get(table_idx) else {
                continue;
            };
            let table = unquote_sql_ident(table_token);

            let mut idx = table_idx + 1;
            while idx < upper.len() {
                let Some(relative_drop_idx) = upper[idx..].iter().position(|t| t == "DROP") else {
                    break;
                };
                idx += relative_drop_idx + 1;
                if upper.get(idx).is_some_and(|t| t == "COLUMN") {
                    idx += 1;
                }
                if upper.get(idx).is_some_and(|t| t == "IF")
                    && upper.get(idx + 1).is_some_and(|t| t == "EXISTS")
                {
                    idx += 2;
                }
                if upper
                    .get(idx)
                    .is_some_and(|t| matches!(t.as_str(), "CONSTRAINT" | "INDEX"))
                {
                    idx += 1;
                    continue;
                }
                if let Some(col_token) = tokens.get(idx) {
                    columns.push((table.clone(), unquote_sql_ident(col_token)));
                }
                idx += 1;
            }
        }
    }

    (tables, columns)
}

fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let mut dollar_quote: Option<String> = None;
    let mut i = 0usize;

    while i < sql.len() {
        if in_line_comment {
            let Some(ch) = sql[i..].chars().next() else {
                break;
            };
            if ch == '\n' {
                in_line_comment = false;
                current.push(' ');
            }
            i += ch.len_utf8();
            continue;
        }

        if in_block_comment {
            if sql[i..].starts_with("*/") {
                in_block_comment = false;
                i += 2;
            } else {
                i += sql[i..].chars().next().map(char::len_utf8).unwrap_or(1);
            }
            continue;
        }

        if let Some(delim) = dollar_quote.as_deref() {
            if sql[i..].starts_with(delim) {
                current.push_str(delim);
                i += delim.len();
                dollar_quote = None;
            } else if let Some(ch) = sql[i..].chars().next() {
                current.push(ch);
                i += ch.len_utf8();
            }
            continue;
        }

        let Some(ch) = sql[i..].chars().next() else {
            break;
        };

        if in_single {
            current.push(ch);
            if ch == '\'' {
                if sql[i + ch.len_utf8()..].starts_with('\'') {
                    current.push('\'');
                    i += ch.len_utf8() + 1;
                } else {
                    i += ch.len_utf8();
                    in_single = false;
                }
            } else {
                i += ch.len_utf8();
            }
            continue;
        }

        if in_double {
            current.push(ch);
            if ch == '"' {
                if sql[i + ch.len_utf8()..].starts_with('"') {
                    current.push('"');
                    i += ch.len_utf8() + 1;
                } else {
                    i += ch.len_utf8();
                    in_double = false;
                }
            } else {
                i += ch.len_utf8();
            }
            continue;
        }

        match ch {
            '\'' => {
                in_single = true;
                current.push(ch);
                i += ch.len_utf8();
            }
            '"' => {
                in_double = true;
                current.push(ch);
                i += ch.len_utf8();
            }
            '$' => {
                let Some(delim) = sql_dollar_quote_delimiter_at(sql, i) else {
                    current.push(ch);
                    i += ch.len_utf8();
                    continue;
                };
                current.push_str(delim);
                i += delim.len();
                dollar_quote = Some(delim.to_string());
            }
            '-' if sql[i + ch.len_utf8()..].starts_with('-') => {
                in_line_comment = true;
                current.push(' ');
                i += ch.len_utf8() + 1;
            }
            '/' if sql[i + ch.len_utf8()..].starts_with('*') => {
                in_block_comment = true;
                current.push(' ');
                i += ch.len_utf8() + 1;
            }
            ';' => {
                let statement = current.trim();
                if !statement.is_empty() {
                    statements.push(statement.to_string());
                }
                current.clear();
                i += ch.len_utf8();
            }
            _ => {
                current.push(ch);
                i += ch.len_utf8();
            }
        }
    }

    let tail = current.trim();
    if !tail.is_empty() {
        statements.push(tail.to_string());
    }

    statements
}

fn sql_dollar_quote_delimiter_at(raw: &str, idx: usize) -> Option<&str> {
    let bytes = raw.as_bytes();
    if bytes.get(idx) != Some(&b'$') {
        return None;
    }

    let mut end = idx + 1;
    while end < bytes.len() {
        match bytes[end] {
            b'$' => return Some(&raw[idx..=end]),
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_' => end += 1,
            _ => return None,
        }
    }

    None
}

/// Discover migration files in both flat and subdirectory layouts.
///
/// Supported layouts:
///   Flat:   `deltas/001_name.up.qail`
///   Subdir: `deltas/20251207000000_name/{expand,backfill,contract}.qail`
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
    let mut unsupported_sql = Vec::new();

    for entry in fs::read_dir(migrations_dir)? {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name();
        let name_str = name.to_string_lossy().to_string();

        if path.is_dir() {
            // Subdirectory layout:
            //   phased:   <dir>/expand.qail, backfill.qail, contract.qail
            if matches!(direction, MigrateDirection::Up) {
                let phased = [
                    ("expand.qail", MigrationPhase::Expand),
                    ("backfill.qail", MigrationPhase::Backfill),
                    ("contract.qail", MigrationPhase::Contract),
                ];
                let mut found_phased = false;

                for (filename, phase) in phased {
                    let qail_file = path.join(filename);
                    let sql_file = path.join(filename.replace(".qail", ".sql"));
                    if sql_file.exists() && !qail_file.exists() {
                        unsupported_sql.push(format!(
                            "{}/{}",
                            name_str,
                            filename.replace(".qail", ".sql")
                        ));
                    }
                    if qail_file.exists() {
                        found_phased = true;
                        migrations.push(MigrationFile {
                            group_key: name_str.clone(),
                            sort_key: format!("{}/{}", name_str, filename),
                            display_name: format!("{}/{}", name_str, filename),
                            path: qail_file,
                            phase,
                        });
                    }
                }

                // Legacy subdirectory layout:
                //   <dir>/up.qail + <dir>/down.qail
                //
                // If a group has phased files, those are authoritative and
                // legacy up.qail is ignored. This keeps historical receipts
                // visible without double-discovering migrated phased groups.
                let qail_file = path.join("up.qail");
                let sql_file = path.join("up.sql");
                if sql_file.exists() && !qail_file.exists() {
                    unsupported_sql.push(format!("{}/up.sql", name_str));
                    continue;
                }
                if !found_phased && qail_file.exists() {
                    migrations.push(MigrationFile {
                        group_key: name_str.clone(),
                        sort_key: format!("{}/up.qail", name_str),
                        display_name: format!("{}/up.qail", name_str),
                        path: qail_file,
                        phase: detect_phase(&name_str),
                    });
                }
            } else {
                // Down direction: keep single rollback file
                let qail_file = path.join(format!("{}.qail", suffix));
                let sql_file = path.join(format!("{}.sql", suffix));
                if sql_file.exists() && !qail_file.exists() {
                    unsupported_sql.push(format!("{}/{}.sql", name_str, suffix));
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
                unsupported_sql.push(name_str.clone());
            }
        }
    }

    if !unsupported_sql.is_empty() {
        return Err(anyhow!(
            ".sql migrations are not supported; convert to .qail: {}",
            unsupported_sql.join(", ")
        ));
    }

    // Sort by group key + phase order to enforce expand -> backfill -> contract for UP.
    // For DOWN, execute in reverse discovery order so latest groups roll back first.
    migrations.sort_by(|a, b| {
        a.group_key
            .cmp(&b.group_key)
            .then_with(|| phase_rank(a.phase).cmp(&phase_rank(b.phase)))
            .then_with(|| a.sort_key.cmp(&b.sort_key))
    });
    if matches!(direction, MigrateDirection::Down) {
        migrations.reverse();
    }

    Ok(migrations)
}
