//! Build-time raw SQL policy guard.
//!
//! Enforces "QAIL-first" by detecting common raw SQL entry points.

use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SqlUsageDiagnostic {
    pub(crate) file: String,
    pub(crate) line: usize,
    pub(crate) column: usize,
    pub(crate) code: &'static str,
    pub(crate) message: String,
}

mod semantic_impl {
    use super::SqlUsageDiagnostic;

    pub(super) fn detect_in_source(file: &str, source: &str) -> Vec<SqlUsageDiagnostic> {
        if source.contains("qail:allow(raw_sql)") {
            return Vec::new();
        }

        let line_starts = compute_line_starts(source);
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
                i = consume_block_comment(bytes, i);
                continue;
            }

            if let Some(next) = consume_rust_literal(bytes, i) {
                i = next;
                continue;
            }

            if starts_with(bytes, i, b"Qail::raw_sql") {
                let after = skip_ws(bytes, i + "Qail::raw_sql".len());
                if bytes.get(after).copied() == Some(b'(') {
                    let (line, column) = offset_to_line_col(&line_starts, i);
                    out.push(SqlUsageDiagnostic {
                        file: file.to_string(),
                        line,
                        column: column + 1,
                        code: "SQL-001",
                        message: "Qail::raw_sql(...) bypasses QAIL structural validation"
                            .to_string(),
                    });
                    i += "Qail::raw_sql".len();
                    continue;
                }
            }

            let mut matched_sqlx = false;
            for name in ["sqlx::query_scalar", "sqlx::query_as", "sqlx::query"] {
                if !starts_with(bytes, i, name.as_bytes()) {
                    continue;
                }
                let after = skip_ws(bytes, i + name.len());
                let code = match bytes.get(after).copied() {
                    Some(b'!') => Some("SQL-003"),
                    Some(b'(') => Some("SQL-002"),
                    _ => None,
                };

                let Some(code) = code else {
                    continue;
                };

                let (line, column) = offset_to_line_col(&line_starts, i);
                let message = if code == "SQL-003" {
                    "sqlx::query!* macro detected; use QAIL DSL instead".to_string()
                } else {
                    "sqlx::query* detected; use QAIL DSL instead of raw SQL APIs".to_string()
                };

                out.push(SqlUsageDiagnostic {
                    file: file.to_string(),
                    line,
                    column: column + 1,
                    code,
                    message,
                });
                i += name.len();
                matched_sqlx = true;
                break;
            }

            if !matched_sqlx {
                i += 1;
            }
        }
        out
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

    fn starts_with(haystack: &[u8], idx: usize, needle: &[u8]) -> bool {
        haystack
            .get(idx..idx.saturating_add(needle.len()))
            .is_some_and(|s| s == needle)
    }

    fn skip_ws(bytes: &[u8], mut idx: usize) -> usize {
        while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }
        idx
    }

    fn consume_block_comment(bytes: &[u8], start: usize) -> usize {
        let mut i = start + 2;
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

        i
    }

    fn consume_rust_literal(bytes: &[u8], start: usize) -> Option<usize> {
        if let Some((_, content_start, hashes)) = raw_string_prefix(bytes, start) {
            let end_quote = find_raw_string_end(bytes, content_start, hashes)?;
            return Some(end_quote + 1 + hashes);
        }

        if bytes.get(start).copied() == Some(b'"') || starts_with(bytes, start, b"b\"") {
            let quote_offset = if bytes.get(start).copied() == Some(b'"') {
                start
            } else {
                start + 1
            };

            let mut i = quote_offset + 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' {
                    i = (i + 2).min(bytes.len());
                    continue;
                }
                if bytes[i] == b'"' {
                    return Some(i + 1);
                }
                i += 1;
            }
            return Some(bytes.len());
        }

        if bytes.get(start).copied() == Some(b'\'') {
            let mut i = start + 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' {
                    i = (i + 2).min(bytes.len());
                    continue;
                }
                if bytes[i] == b'\'' {
                    return Some(i + 1);
                }
                i += 1;
            }
            return Some(bytes.len());
        }

        None
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
}

fn detect_sql_in_file(path: &Path) -> Vec<SqlUsageDiagnostic> {
    let Ok(source) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    #[cfg(feature = "analyzer")]
    let mut out = semantic_impl::detect_in_source(&path.display().to_string(), &source);
    #[cfg(not(feature = "analyzer"))]
    let out = semantic_impl::detect_in_source(&path.display().to_string(), &source);

    #[cfg(feature = "analyzer")]
    {
        // Blend mode: union API-name detections with analyzer literal scans.
        // Analyzer catches additional SQL literals missed by API-name heuristics.
        use std::collections::HashSet;

        let mut seen: HashSet<(usize, usize, &'static str)> =
            out.iter().map(|d| (d.line, d.column, d.code)).collect();
        for m in crate::analyzer::detect_raw_sql(&source) {
            let key = (m.line, m.column + 1, "SQL-005");
            if !seen.insert(key) {
                continue;
            }
            out.push(SqlUsageDiagnostic {
                file: path.display().to_string(),
                line: m.line,
                column: m.column + 1,
                code: "SQL-005",
                message: format!(
                    "raw SQL literal detected ({}); migrate to QAIL DSL",
                    m.sql_type
                ),
            });
        }
    }

    out
}

pub(crate) fn detect_sql_usage_in_dir(dir: &Path) -> Vec<SqlUsageDiagnostic> {
    let mut out = Vec::new();
    let mut files = Vec::<PathBuf>::new();
    collect_rust_files(dir, &mut files);
    for file in files {
        out.extend(detect_sql_in_file(&file));
    }
    out
}

fn collect_rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str())
                && (name == "target" || name == ".git" || name == "node_modules")
            {
                continue;
            }
            collect_rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::semantic_impl::detect_in_source;

    #[test]
    fn detects_qail_raw_sql() {
        let src = r#"fn x(){ let _ = Qail::raw_sql("SELECT 1"); }"#;
        let hits = detect_in_source("x.rs", src);
        assert!(hits.iter().any(|d| d.code == "SQL-001"), "{hits:?}");
    }

    #[test]
    fn detects_sqlx_query_macro_and_call() {
        let src = r#"
fn x() {
    let _ = sqlx::query("SELECT id FROM users");
    let _ = sqlx::query_as!("SELECT id FROM users");
}
"#;
        let hits = detect_in_source("x.rs", src);
        assert!(hits.iter().any(|d| d.code == "SQL-002"), "{hits:?}");
        assert!(hits.iter().any(|d| d.code == "SQL-003"), "{hits:?}");
    }

    #[test]
    fn allows_file_with_raw_sql_allow_comment() {
        let src = r#"
// qail:allow(raw_sql)
fn x() {
    let _ = Qail::raw_sql("SELECT 1");
}
"#;
        let hits = detect_in_source("x.rs", src);
        assert!(hits.is_empty(), "{hits:?}");
    }

    #[test]
    fn ignores_markers_inside_strings_and_comments() {
        let src = r#"
fn x() {
    let _ = "sqlx::query(SELECT 1)";
    // sqlx::query_as!("SELECT 1")
    /* Qail::raw_sql("SELECT 1") */
}
"#;
        let hits = detect_in_source("x.rs", src);
        assert!(hits.is_empty(), "{hits:?}");
    }
}
