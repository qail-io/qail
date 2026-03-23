//! Source code scanner for QAIL and SQL queries.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use crate::ast::{Action, CageKind, Expr};
use crate::parse;

use super::rust_ast::RustAnalyzer;
use super::rust_ast::detect_raw_sql_in_file;
use super::rust_ast::sql_semantics::{SqlStmtKind, classify_sql_kind};
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
    /// Legacy alias retained for API compatibility.
    #[doc(hidden)]
    Regex,
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
                && (ext == "rs" || ext == "ts" || ext == "js" || ext == "py")
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
                && (ext == "rs" || ext == "ts" || ext == "js" || ext == "py")
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
        let mut refs = Vec::new();

        for literal in extract_text_literals(content) {
            refs.extend(self.scan_text_literal(path, &literal));
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
            && let Some(qail_ref) = command_to_reference(path, line_number, &cmd)
        {
            refs.push(qail_ref);
        }

        let normalized = normalize_whitespace(candidate);
        if let Some((_kind, table, columns)) = parse_sql_reference(&normalized) {
            refs.push(CodeReference {
                file: path.to_path_buf(),
                line: line_number,
                table,
                columns,
                query_type: QueryType::RawSql,
                snippet: normalized.chars().take(60).collect(),
            });
        }

        refs
    }

    fn scan_rust_raw_sql(&self, path: &Path) -> Vec<CodeReference> {
        let mut refs = Vec::new();

        for sql_match in detect_raw_sql_in_file(path) {
            let normalized = normalize_whitespace(&sql_match.raw_sql);
            if normalized.is_empty() {
                continue;
            }

            let Some((_kind, table, columns)) = parse_sql_reference(&normalized) else {
                continue;
            };

            refs.push(CodeReference {
                file: path.to_path_buf(),
                line: sql_match.line,
                table,
                columns,
                query_type: QueryType::RawSql,
                snippet: normalized.chars().take(60).collect(),
            });
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

fn command_to_reference(path: &Path, line: usize, cmd: &crate::Qail) -> Option<CodeReference> {
    if cmd.table.trim().is_empty() {
        return None;
    }

    let (snippet, columns) = match cmd.action {
        Action::Get => (
            format!("get {} fields ...", cmd.table),
            extract_columns_from_exprs(&cmd.columns),
        ),
        Action::Set => (
            format!("set {} values ...", cmd.table),
            extract_payload_columns(cmd),
        ),
        Action::Del => (format!("del {}", cmd.table), vec![]),
        Action::Add => (
            format!("add {} fields ...", cmd.table),
            extract_columns_from_exprs(&cmd.columns),
        ),
        _ => return None,
    };

    Some(CodeReference {
        file: path.to_path_buf(),
        line,
        table: cmd.table.clone(),
        columns,
        query_type: QueryType::Qail,
        snippet,
    })
}

fn extract_columns_from_exprs(exprs: &[Expr]) -> Vec<String> {
    let mut cols = Vec::new();
    let mut seen = HashSet::new();

    for expr in exprs {
        let name = match expr {
            Expr::Star => "*".to_string(),
            Expr::Named(name) => name.clone(),
            Expr::Aliased { name, .. } => name.clone(),
            Expr::Aggregate { col, .. } => col.clone(),
            Expr::JsonAccess { column, .. } => column.clone(),
            _ => continue,
        };

        if !name.is_empty() && seen.insert(name.clone()) {
            cols.push(name);
        }
    }

    cols
}

fn extract_payload_columns(cmd: &crate::Qail) -> Vec<String> {
    let mut cols = Vec::new();
    let mut seen = HashSet::new();

    for cage in &cmd.cages {
        if !matches!(cage.kind, CageKind::Payload) {
            continue;
        }

        for cond in &cage.conditions {
            if let Expr::Named(name) = &cond.left
                && !name.is_empty()
                && seen.insert(name.clone())
            {
                cols.push(name.clone());
            }
        }
    }

    cols
}

fn is_ident_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn normalize_whitespace(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn parse_sql_reference(sql: &str) -> Option<(SqlStmtKind, String, Vec<String>)> {
    let normalized = normalize_whitespace(sql);
    let kind = classify_sql_kind(&normalized)?;

    match kind {
        SqlStmtKind::Select => {
            let select_idx = find_keyword_top_level_from(&normalized, "SELECT", 0)?;
            let from_idx =
                find_keyword_top_level_from(&normalized, "FROM", select_idx + "SELECT".len())?;

            let columns_raw = normalized
                .get(select_idx + "SELECT".len()..from_idx)?
                .trim();
            let table = parse_sql_object_name(&normalized, from_idx + "FROM".len())?;

            let columns = if columns_raw == "*" {
                vec!["*".to_string()]
            } else {
                split_sql_top_level(columns_raw, ',')
                    .into_iter()
                    .map(|c| c.trim().to_string())
                    .filter_map(|c| normalize_projection_column(&c))
                    .collect()
            };

            Some((kind, table, columns))
        }
        SqlStmtKind::Insert => {
            let insert_idx = find_keyword_top_level_from(&normalized, "INSERT", 0)?;
            let into_idx =
                find_keyword_top_level_from(&normalized, "INTO", insert_idx + "INSERT".len())?;
            let table = parse_sql_object_name(&normalized, into_idx + "INTO".len())?;
            Some((kind, table, vec![]))
        }
        SqlStmtKind::Update => {
            let update_idx = find_keyword_top_level_from(&normalized, "UPDATE", 0)?;
            let table = parse_sql_object_name(&normalized, update_idx + "UPDATE".len())?;
            Some((kind, table, vec![]))
        }
        SqlStmtKind::Delete => {
            let delete_idx = find_keyword_top_level_from(&normalized, "DELETE", 0)?;
            let from_idx =
                find_keyword_top_level_from(&normalized, "FROM", delete_idx + "DELETE".len())?;
            let table = parse_sql_object_name(&normalized, from_idx + "FROM".len())?;
            Some((kind, table, vec![]))
        }
    }
}

fn parse_sql_object_name(sql: &str, start: usize) -> Option<String> {
    let bytes = sql.as_bytes();
    let mut cursor = skip_sql_ws(bytes, start);
    if cursor >= bytes.len() {
        return None;
    }

    let mut segments = Vec::new();
    loop {
        if cursor >= bytes.len() {
            break;
        }

        let (segment, next) = if matches!(bytes[cursor], b'"' | b'`') {
            let quote = bytes[cursor];
            let start_seg = cursor + 1;
            cursor += 1;
            while cursor < bytes.len() {
                if bytes[cursor] == quote {
                    break;
                }
                cursor += 1;
            }
            let seg = sql.get(start_seg..cursor)?.to_string();
            let next = if cursor < bytes.len() {
                cursor + 1
            } else {
                cursor
            };
            (seg, next)
        } else {
            let start_seg = cursor;
            while cursor < bytes.len()
                && (bytes[cursor].is_ascii_alphanumeric() || bytes[cursor] == b'_')
            {
                cursor += 1;
            }
            (sql.get(start_seg..cursor)?.to_string(), cursor)
        };

        if segment.is_empty() {
            break;
        }
        segments.push(segment);
        cursor = skip_sql_ws(bytes, next);
        if cursor < bytes.len() && bytes[cursor] == b'.' {
            cursor = skip_sql_ws(bytes, cursor + 1);
            continue;
        }
        break;
    }

    let tail = segments.last()?.trim();
    if tail.is_empty() {
        None
    } else {
        Some(tail.to_string())
    }
}

fn normalize_projection_column(expr: &str) -> Option<String> {
    let expr = expr.trim();
    if expr.is_empty() {
        return None;
    }
    if expr == "*" {
        return Some("*".to_string());
    }

    let mut base = expr;
    if let Some(as_idx) = find_keyword_top_level_from(expr, "AS", 0) {
        base = expr.get(..as_idx).unwrap_or(expr).trim();
    }
    let token = base.split_whitespace().next().unwrap_or(base).trim();
    if token.is_empty() {
        return None;
    }

    let normalized = token.trim_matches('"').trim_matches('`');
    let tail = normalized.rsplit('.').next().unwrap_or(normalized).trim();
    if tail.is_empty() {
        None
    } else {
        Some(tail.to_string())
    }
}

fn split_sql_top_level(input: &str, delimiter: char) -> Vec<&str> {
    let bytes = input.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    let mut depth = 0i32;
    let mut in_quote: Option<u8> = None;
    let mut escaped = false;

    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = in_quote {
            if escaped {
                escaped = false;
                i += 1;
                continue;
            }
            if b == b'\\' {
                escaped = true;
                i += 1;
                continue;
            }
            if b == q {
                in_quote = None;
            }
            i += 1;
            continue;
        }

        match b {
            b'\'' | b'"' | b'`' => in_quote = Some(b),
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }

        if b == delimiter as u8 && depth == 0 {
            out.push(input.get(start..i).unwrap_or_default());
            start = i + 1;
        }
        i += 1;
    }
    out.push(input.get(start..).unwrap_or_default());
    out
}

fn find_keyword_top_level_from(sql: &str, keyword: &str, min_idx: usize) -> Option<usize> {
    if keyword.is_empty() {
        return None;
    }

    let bytes = sql.as_bytes();
    let upper = bytes
        .iter()
        .map(|b| b.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let kw = keyword
        .as_bytes()
        .iter()
        .map(|b| b.to_ascii_uppercase())
        .collect::<Vec<_>>();

    let mut i = 0usize;
    let mut depth = 0i32;
    let mut in_quote: Option<u8> = None;
    let mut escaped = false;

    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = in_quote {
            if escaped {
                escaped = false;
                i += 1;
                continue;
            }
            if b == b'\\' {
                escaped = true;
                i += 1;
                continue;
            }
            if b == q {
                in_quote = None;
            }
            i += 1;
            continue;
        }

        match b {
            b'\'' | b'"' | b'`' => in_quote = Some(b),
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }

        if depth == 0
            && i >= min_idx
            && upper
                .get(i..i.saturating_add(kw.len()))
                .is_some_and(|slice| slice == kw)
        {
            let before_ok = if i == 0 {
                true
            } else {
                !is_ident_char(upper[i - 1] as char)
            };
            let after = i + kw.len();
            let after_ok = if after >= upper.len() {
                true
            } else {
                !is_ident_char(upper[after] as char)
            };

            if before_ok && after_ok {
                return Some(i);
            }
        }

        i += 1;
    }

    None
}

fn skip_sql_ws(bytes: &[u8], mut idx: usize) -> usize {
    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }
    idx
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
    fn test_parse_sql_reference_select() {
        let sql = "SELECT name, email FROM users WHERE id = $1";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Select);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["name", "email"]);
    }

    #[test]
    fn test_parse_sql_reference_quoted_schema_table() {
        let sql = r#"SELECT "id", "email" FROM "public"."users" WHERE "id" = $1"#;
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Select);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["id", "email"]);
    }

    #[test]
    fn test_set_payload_column_extraction() {
        let cmd = parse("set users values name = \"Alice\", status = \"active\" where id = $1")
            .expect("set parse");
        let columns = extract_payload_columns(&cmd);
        assert_eq!(columns, vec!["name", "status"]);
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
        assert_eq!(qail_refs[0].columns, vec!["id", "email"]);

        let raw_sql_refs = refs
            .iter()
            .filter(|r| r.query_type == QueryType::RawSql)
            .collect::<Vec<_>>();
        assert_eq!(raw_sql_refs.len(), 1);
        assert_eq!(raw_sql_refs[0].table, "users");
        assert_eq!(raw_sql_refs[0].columns, vec!["id", "email"]);
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
        assert_eq!(qail_refs[0].columns, vec!["id", "email"]);

        let raw_sql_refs = refs
            .iter()
            .filter(|r| r.query_type == QueryType::RawSql)
            .collect::<Vec<_>>();
        assert_eq!(raw_sql_refs.len(), 1);
        assert_eq!(raw_sql_refs[0].table, "users");
        assert_eq!(raw_sql_refs[0].columns, vec!["id", "email"]);
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
        assert_eq!(raw_sql_refs[0].columns, vec!["id", "email"]);
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
            const msg = "ok";
            # "DELETE FROM users"
        "#;

        std::fs::write(&path, source).expect("write temp text file");
        let refs = scanner.scan(&path);
        let _ = std::fs::remove_file(&path);

        assert!(refs.is_empty(), "{refs:?}");
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
}
