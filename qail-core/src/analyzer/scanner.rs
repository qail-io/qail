//! Source code scanner for QAIL and SQL queries.

use regex::Regex;
use std::fs;
use std::path::{Path, PathBuf};

/// Type of query found in source code.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryType {
    /// Native QAIL query (get::, set::, del::, add::)
    Qail,
    /// Raw SQL query (SELECT, INSERT, UPDATE, DELETE)
    RawSql,
}

/// A reference to a query in source code.
#[derive(Debug, Clone)]
pub struct CodeReference {
    /// File path where the reference was found
    pub file: PathBuf,
    /// Line number (1-indexed)
    pub line: usize,
    /// Table name referenced
    pub table: String,
    /// Column names referenced (if any)
    pub columns: Vec<String>,
    /// Type of query
    pub query_type: QueryType,
    /// Code snippet containing the reference
    pub snippet: String,
}

/// Scanner for finding QAIL and SQL references in source code.
pub struct CodebaseScanner {
    /// Regex patterns for QAIL queries (legacy symbol syntax)
    qail_action_pattern: Regex,
    qail_column_pattern: Regex,
    /// Regex patterns for QAIL queries (v2 keyword syntax)
    qail_v2_get_pattern: Regex,
    qail_v2_set_pattern: Regex,
    qail_v2_del_pattern: Regex,
    qail_v2_add_pattern: Regex,
    /// Regex patterns for SQL queries
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
        Self {
            // QAIL legacy patterns: get::table, set::table, del::table, add::table
            qail_action_pattern: Regex::new(r"(get|set|del|add)::(\w+)").unwrap(),
            // QAIL column: 'column_name
            qail_column_pattern: Regex::new(r"'(\w+)").unwrap(),
            // QAIL v2 keyword patterns
            qail_v2_get_pattern: Regex::new(r"\bget\s+(\w+)\s+fields\s+(.+?)(?:\s+where|\s+order|\s+limit|$)").unwrap(),
            qail_v2_set_pattern: Regex::new(r"\bset\s+(\w+)\s+values\s+(.+?)(?:\s+where|$)").unwrap(),
            qail_v2_del_pattern: Regex::new(r"\bdel\s+(\w+)(?:\s+where|$)").unwrap(),
            qail_v2_add_pattern: Regex::new(r"\badd\s+(\w+)\s+fields\s+(.+?)\s+values").unwrap(),
            // SQL patterns
            sql_select_pattern: Regex::new(r"(?i)SELECT\s+(.+?)\s+FROM\s+(\w+)").unwrap(),
            sql_insert_pattern: Regex::new(r"(?i)INSERT\s+INTO\s+(\w+)").unwrap(),
            sql_update_pattern: Regex::new(r"(?i)UPDATE\s+(\w+)\s+SET").unwrap(),
            sql_delete_pattern: Regex::new(r"(?i)DELETE\s+FROM\s+(\w+)").unwrap(),
        }
    }

    /// Scan a directory for all QAIL and SQL references.
    pub fn scan(&self, path: &Path) -> Vec<CodeReference> {
        let mut refs = Vec::new();

        if path.is_file() {
            if let Some(ext) = path.extension()
                && (ext == "rs" || ext == "ts" || ext == "js" || ext == "py")
            {
                refs.extend(self.scan_file(path));
            }
        } else if path.is_dir() {
            self.scan_dir_recursive(path, &mut refs);
        }

        refs
    }

    /// Recursively scan a directory.
    fn scan_dir_recursive(&self, dir: &Path, refs: &mut Vec<CodeReference>) {
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
                {
                    continue;
                }
                self.scan_dir_recursive(&path, refs);
            } else if let Some(ext) = path.extension()
                && (ext == "rs" || ext == "ts" || ext == "js" || ext == "py")
            {
                refs.extend(self.scan_file(&path));
            }
        }
    }

    /// Scan a single file for references.
    fn scan_file(&self, path: &Path) -> Vec<CodeReference> {
        let mut refs = Vec::new();

        let content = match fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => return refs,
        };

        for (line_num, line) in content.lines().enumerate() {
            let line_number = line_num + 1;

            // Check for QAIL queries (legacy symbol syntax)
            for cap in self.qail_action_pattern.captures_iter(line) {
                let action = cap.get(1).map(|m| m.as_str()).unwrap_or("");
                let table = cap.get(2).map(|m| m.as_str()).unwrap_or("");

                // Extract column references from the same line
                let columns: Vec<String> = self
                    .qail_column_pattern
                    .captures_iter(line)
                    .filter_map(|c| c.get(1).map(|m| m.as_str().to_string()))
                    .collect();

                refs.push(CodeReference {
                    file: path.to_path_buf(),
                    line: line_number,
                    table: table.to_string(),
                    columns,
                    query_type: QueryType::Qail,
                    snippet: format!("{}::{}", action, table),
                });
            }

            // Check for QAIL v2 keyword syntax: get table fields ...
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

            // Check for QAIL v2: set table values ...
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

            // Check for QAIL v2: del table where ...
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

            // Check for QAIL v2: add table fields ... values ...
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

            // Check for SQL SELECT
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

            // Check for SQL INSERT
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

            // Check for SQL UPDATE
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

            // Check for SQL DELETE
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
    fn test_qail_pattern() {
        let scanner = CodebaseScanner::new();
        let line = r#"let result = qail!("get::users:'name'email[id=$1]");"#;

        assert!(scanner.qail_action_pattern.is_match(line));

        let cap = scanner.qail_action_pattern.captures(line).unwrap();
        assert_eq!(cap.get(1).unwrap().as_str(), "get");
        assert_eq!(cap.get(2).unwrap().as_str(), "users");
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
    fn test_column_extraction() {
        let scanner = CodebaseScanner::new();
        let line = r#"get::users:'name'email'created_at"#;

        let columns: Vec<String> = scanner
            .qail_column_pattern
            .captures_iter(line)
            .filter_map(|c| c.get(1).map(|m| m.as_str().to_string()))
            .collect();

        assert_eq!(columns, vec!["name", "email", "created_at"]);
    }
}
