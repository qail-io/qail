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
    use super::super::rust_lex::{mask_non_code, starts_with_bytes as starts_with};
    use super::SqlUsageDiagnostic;

    pub(super) fn detect_in_source(file: &str, source: &str) -> Vec<SqlUsageDiagnostic> {
        if source.contains("qail:allow(raw_sql)") {
            return Vec::new();
        }

        let masked = mask_non_code(source);
        let line_starts = compute_line_starts(&masked);
        let bytes = masked.as_bytes();
        let mut out = Vec::new();

        let mut i = 0usize;
        while i < bytes.len() {
            if is_ident_boundary(bytes, i, "Qail::raw_sql")
                && starts_with(bytes, i, b"Qail::raw_sql")
            {
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

            i += 1;
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

    fn is_ident_char(b: u8) -> bool {
        b.is_ascii_alphanumeric() || b == b'_'
    }

    fn is_ident_boundary(bytes: &[u8], idx: usize, needle: &str) -> bool {
        if !starts_with(bytes, idx, needle.as_bytes()) {
            return false;
        }
        let prev_ok = idx == 0 || !is_ident_char(bytes[idx - 1]);
        let after = idx + needle.len();
        let next_ok = after >= bytes.len() || !is_ident_char(bytes[after]);
        prev_ok && next_ok
    }

    fn skip_ws(bytes: &[u8], mut idx: usize) -> usize {
        while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }
        idx
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
        // Blend mode: union structural detections with analyzer literal scans.
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
    fn ignores_generic_query_names_by_themselves() {
        let src = r#"
fn x() {
    let _ = query("SELECT id FROM users");
    let _ = query_as!("SELECT id FROM users");
}
"#;
        let hits = detect_in_source("x.rs", src);
        assert!(hits.is_empty(), "{hits:?}");
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
    let _ = "query(SELECT 1)";
    // query_as!("SELECT 1")
    /* Qail::raw_sql("SELECT 1") */
}
"#;
        let hits = detect_in_source("x.rs", src);
        assert!(hits.is_empty(), "{hits:?}");
    }
}
