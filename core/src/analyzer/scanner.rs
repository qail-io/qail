//! Source code scanner for QAIL and SQL queries.

use regex::Regex;
use std::fs;
use std::path::{Path, PathBuf};

use super::rust_ast::{RustAnalyzer, detect_raw_sql_in_file};

/// Analysis mode for the codebase scanner
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AnalysisMode {
    /// Semantic Rust source analysis (shared with build scanner)
    RustAST,
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
pub struct CodebaseScanner {
    /// Regex patterns for modern QAIL text syntax
    qail_v2_get_pattern: Regex,
    qail_v2_set_pattern: Regex,
    qail_v2_del_pattern: Regex,
    qail_v2_add_pattern: Regex,
    sql_select_pattern: Regex,
    sql_insert_pattern: Regex,
    sql_update_pattern: Regex,
    sql_delete_pattern: Regex,
}

impl Default for CodebaseScanner {
    fn default() -> Self {
        Self::new()
    }
}

impl CodebaseScanner {
    /// Create a new scanner with default patterns.
    pub fn new() -> Self {
        // SAFETY: All regex patterns below are compile-time constant strings.
        // They have been validated and will never fail to compile, so .expect() is infallible.
        Self {
            qail_v2_get_pattern: Regex::new(
                r"\bget\s+(\w+)\s+fields\s+([^\n]+?)(?:\s+where|\s+order|\s+limit|$)",
            )
            .expect("valid v2 get regex"),
            qail_v2_set_pattern: Regex::new(r"\bset\s+(\w+)\s+values\s+([^\n]+?)(?:\s+where|$)")
                .expect("valid v2 set regex"),
            qail_v2_del_pattern: Regex::new(r"\bdel\s+(\w+)(?:\s+where|$)")
                .expect("valid v2 del regex"),
            qail_v2_add_pattern: Regex::new(r"\badd\s+(\w+)\s+fields\s+([^\n]+?)\s+values")
                .expect("valid v2 add regex"),
            sql_select_pattern: Regex::new(r"(?i)SELECT\s+([^\n]+?)\s+FROM\s+(\w+)")
                .expect("valid sql select regex"),
            sql_insert_pattern: Regex::new(r"(?i)INSERT\s+INTO\s+(\w+)")
                .expect("valid sql insert regex"),
            sql_update_pattern: Regex::new(r"(?i)UPDATE\s+(\w+)\s+SET")
                .expect("valid sql update regex"),
            sql_delete_pattern: Regex::new(r"(?i)DELETE\s+FROM\s+(\w+)")
                .expect("valid sql delete regex"),
        }
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
                let mode = if ext == "rs" {
                    AnalysisMode::RustAST
                } else {
                    AnalysisMode::Regex
                };
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
                let mode = if ext == "rs" {
                    AnalysisMode::RustAST
                } else {
                    AnalysisMode::Regex
                };
                let file_refs = self.scan_file(&path);
                let ref_count = file_refs.len();

                if ref_count > 0 {
                    result.files.push(FileAnalysis {
                        file: path.clone(),
                        mode,
                        ref_count,
                        safe: true,
                    });
                }
                result.refs.extend(file_refs);
            }
        }
    }

    /// Scan a single file for references.
    /// Uses semantic Rust analysis for `.rs` files and regex scanning for
    /// non-Rust sources.
    fn scan_file(&self, path: &Path) -> Vec<CodeReference> {
        let mut refs = Vec::new();

        if path.extension().map(|e| e == "rs").unwrap_or(false) {
            refs.extend(RustAnalyzer::scan_file(path));
            refs.extend(self.scan_rust_raw_sql(path));
            return refs;
        }

        let content = match fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => return refs,
        };

        for (line_num, line) in content.lines().enumerate() {
            let line_number = line_num + 1;

            // R7-ReDoS: Skip excessively long lines to bound regex backtracking
            if line.len() > 4096 {
                continue;
            }

            for cap in self.qail_v2_get_pattern.captures_iter(line) {
                let table = cap.get(1).map(|m| m.as_str()).unwrap_or("");
                let columns_str = cap.get(2).map(|m| m.as_str()).unwrap_or("");
                let columns = Self::parse_v2_columns(columns_str);

                refs.push(CodeReference {
                    file: path.to_path_buf(),
                    line: line_number,
                    table: table.to_string(),
                    columns,
                    query_type: QueryType::Qail,
                    snippet: format!("get {} fields ...", table),
                });
            }

            for cap in self.qail_v2_set_pattern.captures_iter(line) {
                let table = cap.get(1).map(|m| m.as_str()).unwrap_or("");
                let columns_str = cap.get(2).map(|m| m.as_str()).unwrap_or("");
                let columns = Self::parse_v2_set_columns(columns_str);

                refs.push(CodeReference {
                    file: path.to_path_buf(),
                    line: line_number,
                    table: table.to_string(),
                    columns,
                    query_type: QueryType::Qail,
                    snippet: format!("set {} values ...", table),
                });
            }

            for cap in self.qail_v2_del_pattern.captures_iter(line) {
                let table = cap.get(1).map(|m| m.as_str()).unwrap_or("");

                refs.push(CodeReference {
                    file: path.to_path_buf(),
                    line: line_number,
                    table: table.to_string(),
                    columns: vec![],
                    query_type: QueryType::Qail,
                    snippet: format!("del {}", table),
                });
            }

            for cap in self.qail_v2_add_pattern.captures_iter(line) {
                let table = cap.get(1).map(|m| m.as_str()).unwrap_or("");
                let columns_str = cap.get(2).map(|m| m.as_str()).unwrap_or("");
                let columns = Self::parse_v2_columns(columns_str);

                refs.push(CodeReference {
                    file: path.to_path_buf(),
                    line: line_number,
                    table: table.to_string(),
                    columns,
                    query_type: QueryType::Qail,
                    snippet: format!("add {} fields ...", table),
                });
            }

            for cap in self.sql_select_pattern.captures_iter(line) {
                let columns_str = cap.get(1).map(|m| m.as_str()).unwrap_or("");
                let table = cap.get(2).map(|m| m.as_str()).unwrap_or("");

                let columns = if columns_str.trim() == "*" {
                    vec!["*".to_string()]
                } else {
                    columns_str
                        .split(',')
                        .map(|c| c.trim().to_string())
                        .filter(|c| !c.is_empty())
                        .collect()
                };

                refs.push(CodeReference {
                    file: path.to_path_buf(),
                    line: line_number,
                    table: table.to_string(),
                    columns,
                    query_type: QueryType::RawSql,
                    snippet: line.trim().chars().take(60).collect(),
                });
            }

            for cap in self.sql_insert_pattern.captures_iter(line) {
                let table = cap.get(1).map(|m| m.as_str()).unwrap_or("");
                refs.push(CodeReference {
                    file: path.to_path_buf(),
                    line: line_number,
                    table: table.to_string(),
                    columns: vec![],
                    query_type: QueryType::RawSql,
                    snippet: line.trim().chars().take(60).collect(),
                });
            }

            for cap in self.sql_update_pattern.captures_iter(line) {
                let table = cap.get(1).map(|m| m.as_str()).unwrap_or("");
                refs.push(CodeReference {
                    file: path.to_path_buf(),
                    line: line_number,
                    table: table.to_string(),
                    columns: vec![],
                    query_type: QueryType::RawSql,
                    snippet: line.trim().chars().take(60).collect(),
                });
            }

            for cap in self.sql_delete_pattern.captures_iter(line) {
                let table = cap.get(1).map(|m| m.as_str()).unwrap_or("");
                refs.push(CodeReference {
                    file: path.to_path_buf(),
                    line: line_number,
                    table: table.to_string(),
                    columns: vec![],
                    query_type: QueryType::RawSql,
                    snippet: line.trim().chars().take(60).collect(),
                });
            }
        }

        refs
    }

    fn scan_rust_raw_sql(&self, path: &Path) -> Vec<CodeReference> {
        let mut refs = Vec::new();
        for sql_match in detect_raw_sql_in_file(path) {
            let snippet = sql_match
                .raw_sql
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ");
            if snippet.is_empty() {
                continue;
            }

            let (table, columns) = if let Some(cap) = self.sql_select_pattern.captures(&snippet) {
                let columns_str = cap.get(1).map(|m| m.as_str()).unwrap_or_default();
                let table = cap
                    .get(2)
                    .map(|m| m.as_str())
                    .unwrap_or_default()
                    .to_string();
                let columns = if columns_str.trim() == "*" {
                    vec!["*".to_string()]
                } else {
                    columns_str
                        .split(',')
                        .map(|c| c.trim().to_string())
                        .filter(|c| !c.is_empty())
                        .collect()
                };
                (table, columns)
            } else if let Some(cap) = self.sql_insert_pattern.captures(&snippet) {
                (
                    cap.get(1)
                        .map(|m| m.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    vec![],
                )
            } else if let Some(cap) = self.sql_update_pattern.captures(&snippet) {
                (
                    cap.get(1)
                        .map(|m| m.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    vec![],
                )
            } else if let Some(cap) = self.sql_delete_pattern.captures(&snippet) {
                (
                    cap.get(1)
                        .map(|m| m.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    vec![],
                )
            } else {
                (String::new(), vec![])
            };

            refs.push(CodeReference {
                file: path.to_path_buf(),
                line: sql_match.line,
                table,
                columns,
                query_type: QueryType::RawSql,
                snippet: snippet.chars().take(60).collect(),
            });
        }
        refs
    }

    /// Parse v2 column list: "id, name, email" or "*"
    fn parse_v2_columns(columns_str: &str) -> Vec<String> {
        if columns_str.trim() == "*" {
            return vec!["*".to_string()];
        }
        columns_str
            .split(',')
            .map(|c| c.trim().to_string())
            .filter(|c| !c.is_empty() && !c.starts_with('$'))
            .collect()
    }

    /// Parse v2 SET column assignments: "name = 'Alice', status = 'active'"
    fn parse_v2_set_columns(columns_str: &str) -> Vec<String> {
        columns_str
            .split(',')
            .filter_map(|assignment| {
                let parts: Vec<&str> = assignment.split('=').collect();
                if !parts.is_empty() {
                    Some(parts[0].trim().to_string())
                } else {
                    None
                }
            })
            .filter(|c| !c.is_empty())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_v2_get_pattern() {
        let scanner = CodebaseScanner::new();
        let line = "get users fields name, email where id = $1";

        assert!(scanner.qail_v2_get_pattern.is_match(line));

        let cap = scanner.qail_v2_get_pattern.captures(line).unwrap();
        assert_eq!(cap.get(1).unwrap().as_str(), "users");
        assert_eq!(cap.get(2).unwrap().as_str(), "name, email");
    }

    #[test]
    fn test_sql_select_pattern() {
        let scanner = CodebaseScanner::new();
        let line = r#"sqlx::query("SELECT name, email FROM users WHERE id = $1")"#;

        assert!(scanner.sql_select_pattern.is_match(line));

        let cap = scanner.sql_select_pattern.captures(line).unwrap();
        assert_eq!(cap.get(2).unwrap().as_str(), "users");
    }

    #[test]
    fn test_v2_set_column_extraction() {
        let columns = CodebaseScanner::parse_v2_set_columns("name = 'Alice', status = 'active'");
        assert_eq!(columns, vec!["name", "status"]);
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
                let _ = sqlx::query(sql);
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
}
