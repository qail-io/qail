//! Source code scanner for QAIL and SQL queries.

mod command_refs;
mod sql_refs;

use std::fs;
use std::path::{Path, PathBuf};

use crate::parse;

use self::command_refs::command_to_references;
use self::sql_refs::{normalize_whitespace, parse_sql_references, sanitize_sql_for_reference_scan};
use super::rust_ast::RustAnalyzer;
use super::rust_ast::detect_raw_sql_in_file;
#[cfg(test)]
use super::text_qail::extract_qail_candidate_from_line;
use super::text_qail::{
    TextLiteral, extract_text_literals, literal_offset_to_line_col, looks_like_qail_query,
    trim_query_bounds,
};

/// Analysis mode for the codebase scanner
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AnalysisMode {
    /// Semantic Rust source analysis (shared with build scanner)
    RustAST,
    /// Text-source semantic scan for non-Rust files.
    TextSemantic,
}

/// Type of query found in source code.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryType {
    /// Native QAIL query in modern text form (e.g. `get users fields ...`).
    Qail,
    RawSql,
}

/// A reference to a query in source code.
#[derive(Debug, Clone)]
pub struct CodeReference {
    pub file: PathBuf,
    pub line: usize,
    pub table: String,
    pub columns: Vec<String>,
    pub query_type: QueryType,
    pub snippet: String,
}

/// Analysis result for a single file
#[derive(Debug, Clone)]
pub struct FileAnalysis {
    pub file: PathBuf,
    pub mode: AnalysisMode,
    pub ref_count: usize,
    pub safe: bool,
}

/// Complete scan result with per-file breakdown
#[derive(Debug, Default)]
pub struct ScanResult {
    pub refs: Vec<CodeReference>,
    pub files: Vec<FileAnalysis>,
}

/// Scanner for finding QAIL and SQL references in source code.
pub struct CodebaseScanner;

impl Default for CodebaseScanner {
    fn default() -> Self {
        Self::new()
    }
}

impl CodebaseScanner {
    /// Create a new scanner.
    pub fn new() -> Self {
        Self
    }

    /// Scan a directory for all QAIL and SQL references.
    pub fn scan(&self, path: &Path) -> Vec<CodeReference> {
        self.scan_with_details(path).refs
    }

    /// Scan a directory with detailed per-file breakdown.
    pub fn scan_with_details(&self, path: &Path) -> ScanResult {
        let mut result = ScanResult::default();

        if path.is_file() {
            if let Some(ext) = path.extension()
                && is_supported_source_extension(ext)
            {
                let mode = mode_for_extension(ext);
                let file_refs = self.scan_file(path);
                let ref_count = file_refs.len();

                result.files.push(FileAnalysis {
                    file: path.to_path_buf(),
                    mode,
                    ref_count,
                    safe: true, // Will be updated after impact analysis
                });
                result.refs.extend(file_refs);
            }
        } else if path.is_dir() {
            self.scan_dir_with_details(path, &mut result);
        }

        result
    }

    /// Recursively scan a directory with per-file tracking.
    fn scan_dir_with_details(&self, dir: &Path, result: &mut ScanResult) {
        let entries = match fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };

        for entry in entries.flatten() {
            let path = entry.path();

            // Skip common non-source directories
            if path.is_dir() {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name == "target"
                    || name == "node_modules"
                    || name == ".git"
                    || name == "vendor"
                    || name == "__pycache__"
                    || name == "dist"
                {
                    continue;
                }
                self.scan_dir_with_details(&path, result);
            } else if let Some(ext) = path.extension()
                && is_supported_source_extension(ext)
            {
                let mode = mode_for_extension(ext);
                let file_refs = self.scan_file(&path);
                let ref_count = file_refs.len();

                result.files.push(FileAnalysis {
                    file: path.clone(),
                    mode,
                    ref_count,
                    safe: true,
                });
                result.refs.extend(file_refs);
            }
        }
    }

    /// Scan a single file for references.
    /// Uses semantic Rust analysis for `.rs` files and parser-based textual
    /// extraction for non-Rust sources.
    fn scan_file(&self, path: &Path) -> Vec<CodeReference> {
        if path.extension().map(|e| e == "rs").unwrap_or(false) {
            let mut refs = RustAnalyzer::scan_file(path);
            refs.extend(self.scan_rust_raw_sql(path));
            return refs;
        }

        let content = match fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };

        self.scan_text_file(path, &content)
    }

    fn scan_text_file(&self, path: &Path, content: &str) -> Vec<CodeReference> {
        if path.extension().is_some_and(|ext| ext == "sql") {
            return self.scan_sql_document(path, content);
        }

        let mut refs = Vec::new();

        for literal in extract_text_literals(content) {
            refs.extend(self.scan_text_literal(path, &literal));
        }

        refs
    }

    fn scan_sql_document(&self, path: &Path, content: &str) -> Vec<CodeReference> {
        self.scan_sql_fragment(path, 1, content)
    }

    fn scan_sql_fragment(
        &self,
        path: &Path,
        base_line: usize,
        content: &str,
    ) -> Vec<CodeReference> {
        let mut refs = Vec::new();
        let sanitized = sanitize_sql_for_reference_scan(content);

        for (line_number, statement) in split_sql_document_statements(&sanitized) {
            let Some((start, end)) = trim_query_bounds(&statement) else {
                continue;
            };
            let Some(candidate) = statement.get(start..end) else {
                continue;
            };
            let normalized = normalize_whitespace(candidate);
            if normalized.is_empty() {
                continue;
            }
            let statement_line = base_line + line_number - 1
                + statement[..start].bytes().filter(|b| *b == b'\n').count();

            for (_kind, table, columns) in parse_sql_references(&normalized) {
                refs.push(CodeReference {
                    file: path.to_path_buf(),
                    line: statement_line,
                    table,
                    columns,
                    query_type: QueryType::RawSql,
                    snippet: normalized.chars().take(60).collect(),
                });
            }
        }

        refs
    }

    fn scan_text_literal(&self, path: &Path, literal: &TextLiteral) -> Vec<CodeReference> {
        let mut refs = Vec::new();
        let Some((start, end)) = trim_query_bounds(&literal.text) else {
            return refs;
        };
        let Some(candidate) = literal.text.get(start..end) else {
            return refs;
        };

        // Keep scans bounded for very large embedded literals.
        if candidate.len() > 16384 {
            return refs;
        }
        let (line_number, _) = literal_offset_to_line_col(literal, start);

        if looks_like_qail_query(candidate)
            && let Ok(cmd) = parse(candidate)
        {
            refs.extend(command_to_references(path, line_number, &cmd));
        }

        let normalized = normalize_whitespace(candidate);
        refs.extend(self.scan_sql_fragment(path, line_number, &normalized));

        refs
    }

    fn scan_rust_raw_sql(&self, path: &Path) -> Vec<CodeReference> {
        let mut refs = Vec::new();

        for sql_match in detect_raw_sql_in_file(path) {
            let normalized = normalize_whitespace(&sql_match.raw_sql);
            if normalized.is_empty() {
                continue;
            }

            refs.extend(self.scan_sql_fragment(path, sql_match.line, &normalized));
        }

        refs
    }
}

fn mode_for_extension(ext: &std::ffi::OsStr) -> AnalysisMode {
    if ext == "rs" {
        AnalysisMode::RustAST
    } else {
        AnalysisMode::TextSemantic
    }
}

fn is_supported_source_extension(ext: &std::ffi::OsStr) -> bool {
    matches!(
        ext.to_str(),
        Some("rs" | "ts" | "tsx" | "js" | "jsx" | "mjs" | "cjs" | "mts" | "cts" | "py" | "sql")
    )
}

fn split_sql_document_statements(content: &str) -> Vec<(usize, String)> {
    let bytes = content.as_bytes();
    let mut statements = Vec::new();
    let mut start = 0usize;
    let mut start_line = 1usize;
    let mut line = 1usize;
    let mut i = 0usize;
    let mut in_quote: Option<u8> = None;

    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\n' {
            line += 1;
        }

        if let Some(quote) = in_quote {
            if b == quote {
                if matches!(quote, b'\'' | b'"') && bytes.get(i + 1).copied() == Some(quote) {
                    i += 2;
                    continue;
                }
                in_quote = None;
            } else if b == b'\\' && i + 1 < bytes.len() {
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }

        match b {
            b'\'' | b'"' | b'`' => in_quote = Some(b),
            b';' => {
                if let Some(statement) = content.get(start..i) {
                    statements.push((start_line, statement.to_string()));
                }
                start = i + 1;
                start_line = line;
            }
            _ => {}
        }

        i += 1;
    }

    if let Some(statement) = content.get(start..)
        && !statement.trim().is_empty()
    {
        statements.push((start_line, statement.to_string()));
    }

    statements
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_qail_candidate_from_line() {
        let line = r#"const q = "get users fields name, email where id = $1";"#;
        let (_, query) = extract_qail_candidate_from_line(line).expect("qail candidate expected");
        assert_eq!(query, "get users fields name, email where id = $1");
    }

    #[test]
    fn test_non_rust_scan_uses_parser_and_sql_classifier() {
        let scanner = CodebaseScanner::new();
        let tmp_name = format!(
            "qail_scanner_text_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);

        let source = r#"
            const q = "get users fields id, email where active = true";
            const s = "SELECT id, email FROM users WHERE active = true";
        "#;

        std::fs::write(&path, source).expect("write temp ts file");
        let refs = scanner.scan(&path);
        let _ = std::fs::remove_file(&path);

        let qail_refs = refs
            .iter()
            .filter(|r| r.query_type == QueryType::Qail)
            .collect::<Vec<_>>();
        assert_eq!(qail_refs.len(), 1);
        assert_eq!(qail_refs[0].table, "users");
        assert_eq!(qail_refs[0].columns, vec!["id", "email", "active"]);

        let raw_sql_refs = refs
            .iter()
            .filter(|r| r.query_type == QueryType::RawSql)
            .collect::<Vec<_>>();
        assert_eq!(raw_sql_refs.len(), 1);
        assert_eq!(raw_sql_refs[0].table, "users");
        assert_eq!(raw_sql_refs[0].columns, vec!["id", "email", "active"]);
    }

    #[test]
    fn test_non_rust_scan_supports_multiline_literals() {
        let scanner = CodebaseScanner::new();
        let tmp_name = format!(
            "qail_scanner_text_multiline_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);

        let source = r#"const q = `
get users
fields id, email
where active = true
`;
const s = "
SELECT id, email
FROM users
WHERE active = true
";"#;

        std::fs::write(&path, source).expect("write temp ts file");
        let refs = scanner.scan(&path);
        let _ = std::fs::remove_file(&path);

        let qail_refs = refs
            .iter()
            .filter(|r| r.query_type == QueryType::Qail)
            .collect::<Vec<_>>();
        assert_eq!(qail_refs.len(), 1);
        assert_eq!(qail_refs[0].table, "users");
        assert_eq!(qail_refs[0].columns, vec!["id", "email", "active"]);

        let raw_sql_refs = refs
            .iter()
            .filter(|r| r.query_type == QueryType::RawSql)
            .collect::<Vec<_>>();
        assert_eq!(raw_sql_refs.len(), 1);
        assert_eq!(raw_sql_refs[0].table, "users");
        assert_eq!(raw_sql_refs[0].columns, vec!["id", "email", "active"]);
    }

    #[test]
    fn test_sql_file_scan_tracks_raw_sql_document_statements() {
        let scanner = CodebaseScanner::new();
        let tmp_name = format!(
            "qail_scanner_sql_document_{}_{}.sql",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);

        let source = r#"
SELECT id, email FROM users WHERE active = true;
UPDATE orders SET status = $1 WHERE id = $2;
"#;

        std::fs::write(&path, source).expect("write temp sql file");
        let refs = scanner.scan(&path);
        let _ = std::fs::remove_file(&path);

        assert_eq!(refs.len(), 2, "{refs:?}");

        let users = refs
            .iter()
            .find(|reference| reference.table == "users")
            .expect("users reference");
        assert_eq!(users.line, 2);
        assert_eq!(users.columns, vec!["id", "email", "active"]);

        let orders = refs
            .iter()
            .find(|reference| reference.table == "orders")
            .expect("orders reference");
        assert_eq!(orders.line, 3);
        assert_eq!(orders.columns, vec!["status", "id"]);
    }

    #[test]
    fn test_sql_file_scan_ignores_comments_and_dollar_quoted_semicolons() {
        let scanner = CodebaseScanner::new();
        let tmp_name = format!(
            "qail_scanner_sql_comments_dollar_{}_{}.sql",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);

        let source = r#"
-- SELECT id FROM ghosts;
SELECT id FROM users WHERE note = $$fake; sql;$$;
/* SELECT id FROM block_users; */
SELECT total FROM orders WHERE status = 'paid';
"#;

        std::fs::write(&path, source).expect("write temp sql file");
        let refs = scanner.scan(&path);
        let _ = std::fs::remove_file(&path);

        assert_eq!(refs.len(), 2, "{refs:?}");

        let users = refs
            .iter()
            .find(|reference| reference.table == "users")
            .expect("users reference");
        assert_eq!(users.columns, vec!["id", "note"]);

        let orders = refs
            .iter()
            .find(|reference| reference.table == "orders")
            .expect("orders reference");
        assert_eq!(orders.columns, vec!["total", "status"]);
    }

    #[test]
    fn test_non_rust_scan_tracks_multiple_sql_statements_in_one_literal() {
        let scanner = CodebaseScanner::new();
        let tmp_name = format!(
            "qail_scanner_text_multi_sql_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);

        let source = r#"
            const sql = `
                SELECT id FROM users WHERE active = true;
                UPDATE orders SET status = $1 WHERE id = $2;
            `;
        "#;

        std::fs::write(&path, source).expect("write temp text file");
        let refs = scanner.scan(&path);
        let _ = std::fs::remove_file(&path);

        assert_eq!(refs.len(), 2, "{refs:?}");
        assert!(refs.iter().any(|reference| reference.table == "users"));
        assert!(refs.iter().any(|reference| reference.table == "orders"));
    }

    #[test]
    fn test_rust_scan_uses_semantic_sql_detection() {
        let scanner = CodebaseScanner::new();
        let tmp_name = format!(
            "qail_scanner_rust_sql_{}_{}.rs",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);

        let source = r#"
            // SELECT id FROM comments_should_not_match
            fn demo() {
                let sql = "SELECT id, email FROM users WHERE active = true";
                let _ = query(sql);
            }
        "#;

        std::fs::write(&path, source).expect("write temp rust file");
        let refs = scanner.scan(&path);
        let _ = std::fs::remove_file(&path);

        let raw_sql_refs = refs
            .iter()
            .filter(|r| r.query_type == QueryType::RawSql)
            .collect::<Vec<_>>();

        assert_eq!(raw_sql_refs.len(), 1, "{raw_sql_refs:?}");
        assert_eq!(raw_sql_refs[0].table, "users");
        assert_eq!(raw_sql_refs[0].columns, vec!["id", "email", "active"]);
    }

    #[test]
    fn test_non_rust_scan_tracks_raw_sql_cte_base_table() {
        let scanner = CodebaseScanner::new();
        let tmp_name = format!(
            "qail_scanner_text_cte_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);

        let source = r#"
            const sql = `
                WITH active_users AS (
                    SELECT id, email FROM users WHERE status = $1
                )
                SELECT id FROM active_users
            `;
        "#;

        std::fs::write(&path, source).expect("write temp text file");
        let refs = scanner.scan(&path);
        let _ = std::fs::remove_file(&path);

        let raw_sql_refs = refs
            .iter()
            .filter(|r| r.query_type == QueryType::RawSql)
            .collect::<Vec<_>>();

        assert_eq!(raw_sql_refs.len(), 1, "{raw_sql_refs:?}");
        assert_eq!(raw_sql_refs[0].table, "users");
        assert_eq!(raw_sql_refs[0].columns, vec!["id", "email", "status"]);
    }

    #[test]
    fn test_non_rust_scan_ignores_comment_markers() {
        let scanner = CodebaseScanner::new();
        let tmp_name = format!(
            "qail_scanner_text_comments_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);

        let source = r#"
            // "SELECT id, email FROM users"
            -- "DELETE FROM users"
            /*
            const q = "get block_users fields id";
            const s = "DELETE FROM block_users";
            */
            const msg = "ok";
            # "DELETE FROM users"
        "#;

        std::fs::write(&path, source).expect("write temp text file");
        let refs = scanner.scan(&path);
        let _ = std::fs::remove_file(&path);

        assert!(refs.is_empty(), "{refs:?}");
    }

    #[test]
    fn test_non_rust_scan_preserves_js_private_field_queries() {
        let scanner = CodebaseScanner::new();
        let tmp_name = format!(
            "qail_scanner_text_private_fields_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);

        let source = r#"
            class Store {
                #qail = "get users fields id";
                #sql = "SELECT id FROM users";
            }
        "#;

        std::fs::write(&path, source).expect("write temp text file");
        let refs = scanner.scan(&path);
        let _ = std::fs::remove_file(&path);

        assert_eq!(refs.len(), 2, "{refs:?}");
        assert!(refs.iter().any(|r| r.query_type == QueryType::Qail));
        assert!(refs.iter().any(|r| r.query_type == QueryType::RawSql));
    }

    #[test]
    fn test_non_rust_scan_preserves_js_decrement_operator_queries() {
        let scanner = CodebaseScanner::new();
        let tmp_name = format!(
            "qail_scanner_text_decrement_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);

        let source = r#"
            let counter = 1;
            counter--; const q = "get users fields id";
        "#;

        std::fs::write(&path, source).expect("write temp text file");
        let refs = scanner.scan(&path);
        let _ = std::fs::remove_file(&path);

        assert_eq!(refs.len(), 1, "{refs:?}");
        assert_eq!(refs[0].query_type, QueryType::Qail);
        assert_eq!(refs[0].table, "users");
        assert_eq!(refs[0].columns, vec!["id"]);
    }

    #[test]
    fn test_scan_with_details_includes_zero_ref_files_in_directories() {
        let scanner = CodebaseScanner::new();
        let root = std::env::temp_dir().join(format!(
            "qail_scanner_details_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("mkdir temp root");

        let with_ref = root.join("with_ref.ts");
        std::fs::write(&with_ref, r#"const q = "get users fields id";"#).expect("write with_ref");
        let no_ref = root.join("no_ref.ts");
        std::fs::write(&no_ref, r#"const msg = "hello";"#).expect("write no_ref");

        let result = scanner.scan_with_details(&root);

        let mut entries = result
            .files
            .iter()
            .map(|f| {
                (
                    f.file.file_name().and_then(|n| n.to_str()).unwrap_or(""),
                    f.mode,
                    f.ref_count,
                )
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|(name, _, _)| *name);

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, "no_ref.ts");
        assert_eq!(entries[0].1, AnalysisMode::TextSemantic);
        assert_eq!(entries[0].2, 0);
        assert_eq!(entries[1].0, "with_ref.ts");
        assert_eq!(entries[1].1, AnalysisMode::TextSemantic);
        assert_eq!(entries[1].2, 1);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn test_scan_with_details_includes_tsx_and_jsx_files() {
        let scanner = CodebaseScanner::new();
        let root = std::env::temp_dir().join(format!(
            "qail_scanner_jsx_tsx_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("mkdir temp root");

        let tsx = root.join("widget.tsx");
        std::fs::write(&tsx, r#"const q = "get users fields id";"#).expect("write tsx");
        let jsx = root.join("panel.jsx");
        std::fs::write(&jsx, r#"const s = "SELECT id FROM users";"#).expect("write jsx");

        let result = scanner.scan_with_details(&root);

        let mut files = result
            .files
            .iter()
            .map(|f| {
                (
                    f.file.file_name().and_then(|n| n.to_str()).unwrap_or(""),
                    f.ref_count,
                )
            })
            .collect::<Vec<_>>();
        files.sort_by_key(|(name, _)| *name);

        assert_eq!(files, vec![("panel.jsx", 1), ("widget.tsx", 1)]);
        assert_eq!(result.refs.len(), 2, "{:?}", result.refs);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn test_scan_with_details_includes_modern_js_module_files() {
        let scanner = CodebaseScanner::new();
        let root = std::env::temp_dir().join(format!(
            "qail_scanner_modern_js_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("mkdir temp root");

        for ext in ["mjs", "cjs", "mts", "cts"] {
            std::fs::write(
                root.join(format!("query.{ext}")),
                r#"const sql = "SELECT id FROM users";"#,
            )
            .expect("write module source");
        }

        let result = scanner.scan_with_details(&root);

        let mut files = result
            .files
            .iter()
            .filter_map(|file| file.file.extension().and_then(|ext| ext.to_str()))
            .collect::<Vec<_>>();
        files.sort_unstable();

        assert_eq!(files, vec!["cjs", "cts", "mjs", "mts"]);
        assert_eq!(result.refs.len(), 4, "{:?}", result.refs);

        let _ = std::fs::remove_dir_all(&root);
    }
}
