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

mod syn_impl {
    use super::SqlUsageDiagnostic;

    pub(super) fn detect_in_source(file: &str, source: &str) -> Vec<SqlUsageDiagnostic> {
        if source.contains("qail:allow(raw_sql)") {
            return Vec::new();
        }

        let mut out = Vec::new();
        for (idx, line) in source.lines().enumerate() {
            if line.contains("Qail::raw_sql(") {
                out.push(SqlUsageDiagnostic {
                    file: file.to_string(),
                    line: idx + 1,
                    column: line.find("Qail::raw_sql(").unwrap_or(0) + 1,
                    code: "SQL-001",
                    message: "Qail::raw_sql(...) bypasses QAIL structural validation".to_string(),
                });
            }
            if line.contains("sqlx::query(")
                || line.contains("sqlx::query_as(")
                || line.contains("sqlx::query_scalar(")
            {
                out.push(SqlUsageDiagnostic {
                    file: file.to_string(),
                    line: idx + 1,
                    column: line.find("sqlx::query").unwrap_or(0) + 1,
                    code: "SQL-002",
                    message: "sqlx::query* detected; use QAIL DSL instead of raw SQL APIs"
                        .to_string(),
                });
            }
            if line.contains("sqlx::query!(")
                || line.contains("sqlx::query_as!(")
                || line.contains("sqlx::query_scalar!(")
            {
                out.push(SqlUsageDiagnostic {
                    file: file.to_string(),
                    line: idx + 1,
                    column: line.find("sqlx::query").unwrap_or(0) + 1,
                    code: "SQL-003",
                    message: "sqlx::query!* macro detected; use QAIL DSL instead".to_string(),
                });
            }
        }
        out
    }
}

fn detect_sql_in_file(path: &Path) -> Vec<SqlUsageDiagnostic> {
    let Ok(source) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    #[cfg(feature = "analyzer")]
    let mut out = syn_impl::detect_in_source(&path.display().to_string(), &source);
    #[cfg(not(feature = "analyzer"))]
    let out = syn_impl::detect_in_source(&path.display().to_string(), &source);

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
    use super::syn_impl::detect_in_source;

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
}
