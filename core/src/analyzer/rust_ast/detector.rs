//! Semantic Rust analyzer using shared scanner/IR utilities.
//!
//! This module intentionally avoids `syn` so analyzer mode and build mode
//! use one semantic extraction path for QAIL usage and SQL-literal detection.

use std::fs;
use std::path::Path;

use crate::analyzer::{CodeReference, QueryType};

use super::sql_semantics::classify_sql_kind;

/// Rust source analyzer backed by QAIL semantic scanner.
pub struct RustAnalyzer;

impl RustAnalyzer {
    /// Scan a Rust file for QAIL patterns.
    pub fn scan_file(path: &Path) -> Vec<CodeReference> {
        let content = match fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };

        let mut usages = Vec::new();
        crate::build::scanner::scan_file_silent(&path.display().to_string(), &content, &mut usages);

        usages
            .into_iter()
            .map(|usage| CodeReference {
                file: path.to_path_buf(),
                line: usage.line,
                table: usage.table.clone(),
                columns: usage.columns,
                query_type: QueryType::Qail,
                snippet: usage_to_snippet(&usage.action, &usage.table),
            })
            .collect()
    }

    /// Check if this is a Rust project (has Cargo.toml)
    pub fn is_rust_project(path: &Path) -> bool {
        let cargo_toml = if path.is_file() {
            path.parent().map(|p| p.join("Cargo.toml"))
        } else {
            Some(path.join("Cargo.toml"))
        };

        cargo_toml.map(|p| p.exists()).unwrap_or(false)
    }

    /// Scan a directory for Rust files.
    pub fn scan_directory(dir: &Path) -> Vec<CodeReference> {
        let mut refs = Vec::new();
        Self::scan_dir_recursive(dir, &mut refs);
        refs
    }

    fn scan_dir_recursive(dir: &Path, refs: &mut Vec<CodeReference>) {
        let entries = match fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };

        for entry in entries.flatten() {
            let path = entry.path();

            if path.is_dir() {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name == "target" || name == ".git" || name == "node_modules" {
                    continue;
                }
                Self::scan_dir_recursive(&path, refs);
            } else if path.extension().is_some_and(|e| e == "rs") {
                refs.extend(Self::scan_file(&path));
            }
        }
    }
}

fn usage_to_snippet(action: &str, table: &str) -> String {
    match action {
        "GET" => format!("Qail::get(\"{}\")", table),
        "ADD" => format!("Qail::add(\"{}\")", table),
        "SET" => format!("Qail::set(\"{}\")", table),
        "DEL" => format!("Qail::del(\"{}\")", table),
        "PUT" => format!("Qail::put(\"{}\")", table),
        "TYPED" => format!("Qail::typed(/* {} */)", table),
        _ => format!("Qail::get(\"{}\")", table),
    }
}

// =============================================================================
// Raw SQL Detection (for VS Code extension)
// =============================================================================

/// A raw SQL statement detected in Rust source code.
#[derive(Debug, Clone)]
pub struct RawSqlMatch {
    /// Line number (1-indexed)
    pub line: usize,
    pub column: usize,
    /// End line number (1-indexed)
    pub end_line: usize,
    /// End column number (0-indexed, exclusive)
    pub end_column: usize,
    /// Type of SQL statement
    pub sql_type: String,
    /// The raw SQL content
    pub raw_sql: String,
    /// Suggested QAIL equivalent
    pub suggested_qail: String,
}

#[derive(Debug, Clone)]
struct StringLiteralMatch {
    start_offset: usize,
    end_offset: usize,
    value: String,
}

/// Detect raw SQL strings in Rust source code.
pub fn detect_raw_sql(source: &str) -> Vec<RawSqlMatch> {
    let line_starts = compute_line_starts(source);
    let literals = scan_rust_string_literals(source);

    let mut out = Vec::new();
    for lit in literals {
        let Some(sql_type) = classify_sql_type(&lit.value) else {
            continue;
        };

        let (line, column) = offset_to_line_col(&line_starts, lit.start_offset);
        let (end_line, end_column) = offset_to_line_col(&line_starts, lit.end_offset);

        out.push(RawSqlMatch {
            line,
            column,
            end_line,
            end_column,
            sql_type: sql_type.to_string(),
            raw_sql: lit.value.clone(),
            suggested_qail: super::transformer::sql_to_qail(&lit.value)
                .unwrap_or_else(|_| "// Could not parse SQL".to_string()),
        });
    }

    out
}

/// Detect raw SQL strings in a file by path.
pub fn detect_raw_sql_in_file(path: &Path) -> Vec<RawSqlMatch> {
    match fs::read_to_string(path) {
        Ok(source) => detect_raw_sql(&source),
        Err(_) => Vec::new(),
    }
}

fn compute_line_starts(source: &str) -> Vec<usize> {
    let mut starts = Vec::with_capacity(source.lines().count() + 1);
    starts.push(0);
    for (idx, b) in source.bytes().enumerate() {
        if b == b'\n' {
            starts.push(idx + 1);
        }
    }
    starts
}

fn offset_to_line_col(line_starts: &[usize], offset: usize) -> (usize, usize) {
    let idx = line_starts.partition_point(|&start| start <= offset);
    let line_idx = idx.saturating_sub(1);
    let line_start = line_starts.get(line_idx).copied().unwrap_or(0);
    (line_idx + 1, offset.saturating_sub(line_start))
}

fn classify_sql_type(value: &str) -> Option<&'static str> {
    classify_sql_kind(value).map(|kind| kind.as_str())
}

fn scan_rust_string_literals(source: &str) -> Vec<StringLiteralMatch> {
    let bytes = source.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;

    while i < bytes.len() {
        if starts_with(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }

        if starts_with(bytes, i, b"/*") {
            i += 2;
            let mut depth = 1usize;
            while i < bytes.len() && depth > 0 {
                if starts_with(bytes, i, b"/*") {
                    depth += 1;
                    i += 2;
                } else if starts_with(bytes, i, b"*/") {
                    depth = depth.saturating_sub(1);
                    i += 2;
                } else {
                    i += 1;
                }
            }
            continue;
        }

        if let Some((prefix_start, content_start, hashes)) = raw_string_prefix(bytes, i) {
            if let Some(end_quote) = find_raw_string_end(bytes, content_start, hashes) {
                let end_offset = end_quote + 1 + hashes;
                if let Some(raw) = source.get(content_start..end_quote) {
                    out.push(StringLiteralMatch {
                        start_offset: prefix_start,
                        end_offset,
                        value: raw.to_string(),
                    });
                }
                i = end_offset;
                continue;
            }
            break;
        }

        if bytes[i] == b'"' || starts_with(bytes, i, b"b\"") {
            let start_offset = i;
            let quote_offset = if bytes[i] == b'"' { i } else { i + 1 };
            let mut j = quote_offset + 1;

            while j < bytes.len() {
                if bytes[j] == b'\\' {
                    j = (j + 2).min(bytes.len());
                    continue;
                }
                if bytes[j] == b'"' {
                    let end_offset = j + 1;
                    if let Some(raw) = source.get(quote_offset + 1..j) {
                        out.push(StringLiteralMatch {
                            start_offset,
                            end_offset,
                            value: unescape_rust_string(raw),
                        });
                    }
                    i = end_offset;
                    break;
                }
                j += 1;
            }

            if j >= bytes.len() {
                break;
            }
            continue;
        }

        if bytes[i] == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' {
                    i = (i + 2).min(bytes.len());
                    continue;
                }
                if bytes[i] == b'\'' {
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }

        i += 1;
    }

    out
}

fn starts_with(haystack: &[u8], idx: usize, needle: &[u8]) -> bool {
    haystack
        .get(idx..idx.saturating_add(needle.len()))
        .is_some_and(|s| s == needle)
}

fn raw_string_prefix(bytes: &[u8], idx: usize) -> Option<(usize, usize, usize)> {
    if bytes.get(idx).copied() == Some(b'r') {
        let mut j = idx + 1;
        while bytes.get(j).copied() == Some(b'#') {
            j += 1;
        }
        if bytes.get(j).copied() == Some(b'"') {
            let hashes = j - (idx + 1);
            return Some((idx, j + 1, hashes));
        }
        return None;
    }

    if bytes.get(idx).copied() == Some(b'b') && bytes.get(idx + 1).copied() == Some(b'r') {
        let mut j = idx + 2;
        while bytes.get(j).copied() == Some(b'#') {
            j += 1;
        }
        if bytes.get(j).copied() == Some(b'"') {
            let hashes = j - (idx + 2);
            return Some((idx, j + 1, hashes));
        }
    }

    None
}

fn find_raw_string_end(bytes: &[u8], mut idx: usize, hashes: usize) -> Option<usize> {
    while idx < bytes.len() {
        if bytes[idx] == b'"' {
            let mut ok = true;
            for off in 0..hashes {
                if bytes.get(idx + 1 + off).copied() != Some(b'#') {
                    ok = false;
                    break;
                }
            }
            if ok {
                return Some(idx);
            }
        }
        idx += 1;
    }
    None
}

fn unescape_rust_string(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars();

    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }

        match chars.next() {
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('t') => out.push('\t'),
            Some('0') => out.push('\0'),
            Some('"') => out.push('"'),
            Some('\\') => out.push('\\'),
            Some('x') => {
                let h1 = chars.next();
                let h2 = chars.next();
                if let (Some(a), Some(b)) = (h1, h2)
                    && let (Some(ha), Some(hb)) = (a.to_digit(16), b.to_digit(16))
                    && let Some(decoded) = char::from_u32((ha * 16) + hb)
                {
                    out.push(decoded);
                    continue;
                }
                out.push('\\');
                out.push('x');
                if let Some(a) = h1 {
                    out.push(a);
                }
                if let Some(b) = h2 {
                    out.push(b);
                }
            }
            Some(other) => {
                // Keep unknown escapes stable for downstream SQL parsing.
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_qail_scan_file() {
        let tmp_name = format!(
            "qail_detector_test_{}_{}.rs",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);

        let code = r#"
            fn query() {
                let cmd = Qail::get("users")
                    .filter("status", Operator::Eq, "active")
                    .columns(["id", "name", "email"]);
            }
        "#;

        fs::write(&path, code).expect("write temp rust file");
        let refs = RustAnalyzer::scan_file(&path);
        let _ = fs::remove_file(&path);

        assert!(!refs.is_empty());
        assert!(refs.iter().any(|r| r.table == "users"));
        assert!(
            refs.iter()
                .any(|r| r.columns.contains(&"status".to_string()))
        );
    }

    #[test]
    fn test_detect_raw_sql() {
        let code = r#"
            fn query() {
                let sql = "SELECT id, name FROM users WHERE status = 'active'";
                sqlx::query(sql);
            }
        "#;

        let matches = detect_raw_sql(code);
        assert!(!matches.is_empty());
        assert_eq!(matches[0].sql_type, "SELECT");
        assert!(matches[0].suggested_qail.contains("Qail::get"));
    }

    #[test]
    fn test_detect_raw_multiline_cte_sql() {
        let code = r##"
            fn get_insights() {
                let sql = r#"
                    WITH stats AS (
                        SELECT COUNT(*) FILTER (WHERE direction = 'outbound'
                        AND created_at > NOW() - INTERVAL '24 hours') AS sent
                        FROM messages
                    )
                    SELECT sent FROM stats
                "#;
            }
        "##;

        let matches = detect_raw_sql(code);
        assert!(!matches.is_empty());

        let qail = &matches[0].suggested_qail;
        assert!(
            qail.contains("CTE 'stats'") || qail.contains("stats_cte"),
            "Should generate CTE variable: {}",
            qail
        );
        assert!(
            qail.contains("messages"),
            "Should find source table 'messages': {}",
            qail
        );
    }

    #[test]
    fn ignores_sql_in_comments() {
        let code = r#"
            // SELECT id FROM users
            /*
              DELETE FROM sessions
            */
            fn ok() {
                let msg = "just text";
            }
        "#;

        let matches = detect_raw_sql(code);
        assert!(matches.is_empty(), "matches: {matches:?}");
    }
}
