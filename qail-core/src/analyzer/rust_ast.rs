//! Rust AST analyzer using `syn`.
//!
//! Provides 100% accurate detection of QAIL patterns in Rust source code
//! by parsing the actual AST instead of using regex.

use std::fs;
use std::path::Path;
use syn::visit::Visit;
use syn::{Expr, ExprCall, ExprMethodCall, Lit, LitStr};

use super::{CodeReference, QueryType};

// Re-export proc_macro2 span type for line number extraction
use proc_macro2::Span;

/// Patterns we're looking for in Rust code
#[derive(Debug, Clone)]
pub struct RustPattern {
    /// Table name referenced
    pub table: String,
    /// Column names referenced
    pub columns: Vec<String>,
    /// Line number (approximation based on span)
    pub line: usize,
    /// Code snippet
    pub snippet: String,
}

/// Visitor that walks Rust AST to find QAIL patterns
struct QailVisitor {
    patterns: Vec<RustPattern>,
    #[allow(dead_code)]
    source: String,
}

impl QailVisitor {
    fn new(source: String) -> Self {
        Self {
            patterns: Vec::new(),
            source,
        }
    }

    /// Extract string value from a string literal
    fn extract_string(lit: &LitStr) -> String {
        lit.value()
    }

    /// Approximate line number from span
    fn line_from_span(&self, span: Span) -> usize {
        span.start().line
    }

    /// Check if this is a QailCmd constructor call
    fn check_qailcmd_call(&mut self, path: &syn::ExprPath, args: &syn::punctuated::Punctuated<Expr, syn::token::Comma>) {
        // Look for QailCmd::get, QailCmd::set, QailCmd::del, QailCmd::add
        let segments: Vec<_> = path.path.segments.iter().map(|s| s.ident.to_string()).collect();
        
        if segments.len() >= 2 && segments[0] == "QailCmd" {
            let action = &segments[1];
            if matches!(action.as_str(), "get" | "set" | "del" | "add") {
                // First argument should be table name
                if let Some(Expr::Lit(expr_lit)) = args.first() {
                    if let Lit::Str(lit_str) = &expr_lit.lit {
                        let table = Self::extract_string(lit_str);
                        self.patterns.push(RustPattern {
                            table: table.clone(),
                            columns: vec![],
                            line: self.line_from_span(lit_str.span()),
                            snippet: format!("QailCmd::{}(\"{}\")", action, table),
                        });
                    }
                }
            }
        }
    }

    /// Check method calls for column references
    fn check_method_call(&mut self, method: &str, args: &syn::punctuated::Punctuated<Expr, syn::token::Comma>, span: Span) {
        match method {
            // .filter("column", op, value) - column is first arg
            "filter" | "where_eq" | "where_ne" | "where_gt" | "where_lt" => {
                if let Some(Expr::Lit(expr_lit)) = args.first() {
                    if let Lit::Str(lit_str) = &expr_lit.lit {
                        let column = Self::extract_string(lit_str);
                        self.patterns.push(RustPattern {
                            table: String::new(), // Will be merged with parent
                            columns: vec![column.clone()],
                            line: self.line_from_span(span),
                            snippet: format!(".{}(\"{}\"...)", method, column),
                        });
                    }
                }
            }
            // .columns(["a", "b", "c"]) - array of columns
            "columns" | "select" => {
                if let Some(Expr::Array(arr)) = args.first() {
                    let columns: Vec<String> = arr.elems.iter().filter_map(|elem| {
                        if let Expr::Lit(expr_lit) = elem {
                            if let Lit::Str(lit_str) = &expr_lit.lit {
                                return Some(Self::extract_string(lit_str));
                            }
                        }
                        None
                    }).collect();
                    
                    if !columns.is_empty() {
                        // Show actual column names in snippet
                        let cols_display = if columns.len() > 3 {
                            format!("{}, {} +{}", columns[0], columns[1], columns.len() - 2)
                        } else {
                            columns.join(", ")
                        };
                        self.patterns.push(RustPattern {
                            table: String::new(),
                            columns,
                            line: self.line_from_span(span),
                            snippet: format!(".{}([{}])", method, cols_display),
                        });
                    }
                }
            }
            // .set_value("column", value)
            "set_value" => {
                if let Some(Expr::Lit(expr_lit)) = args.first() {
                    if let Lit::Str(lit_str) = &expr_lit.lit {
                        let column = Self::extract_string(lit_str);
                        self.patterns.push(RustPattern {
                            table: String::new(),
                            columns: vec![column.clone()],
                            line: self.line_from_span(span),
                            snippet: format!(".set_value(\"{}\"...)", column),
                        });
                    }
                }
            }
            // .order_asc("column"), .order_desc("column")
            "order_asc" | "order_desc" => {
                if let Some(Expr::Lit(expr_lit)) = args.first() {
                    if let Lit::Str(lit_str) = &expr_lit.lit {
                        let column = Self::extract_string(lit_str);
                        self.patterns.push(RustPattern {
                            table: String::new(),
                            columns: vec![column.clone()],
                            line: self.line_from_span(span),
                            snippet: format!(".{}(\"{}\")", method, column),
                        });
                    }
                }
            }
            _ => {}
        }
    }
}

impl<'ast> Visit<'ast> for QailVisitor {
    fn visit_expr_call(&mut self, node: &'ast ExprCall) {
        // Check for QailCmd::get("table") style calls
        if let Expr::Path(path) = &*node.func {
            self.check_qailcmd_call(path, &node.args);
        }
        // Continue visiting children
        syn::visit::visit_expr_call(self, node);
    }

    fn visit_expr_method_call(&mut self, node: &'ast ExprMethodCall) {
        let method = node.method.to_string();
        self.check_method_call(&method, &node.args, node.method.span());
        // Continue visiting children
        syn::visit::visit_expr_method_call(self, node);
    }
}

/// Rust AST Analyzer
pub struct RustAnalyzer;

impl RustAnalyzer {
    /// Scan a Rust file for QAIL patterns using AST parsing
    pub fn scan_file(path: &Path) -> Vec<CodeReference> {
        let content = match fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => return vec![],
        };

        let syntax = match syn::parse_file(&content) {
            Ok(s) => s,
            Err(_) => return vec![], // Fall back to regex if parse fails
        };

        let mut visitor = QailVisitor::new(content);
        visitor.visit_file(&syntax);

        // Post-process: merge column patterns with their preceding table pattern
        let mut merged_refs: Vec<CodeReference> = Vec::new();
        let mut current_table = String::new();

        for p in visitor.patterns {
            if !p.table.is_empty() {
                // This is a table reference (QailCmd::get("table"))
                current_table = p.table.clone();
                merged_refs.push(CodeReference {
                    file: path.to_path_buf(),
                    line: p.line,
                    table: p.table,
                    columns: p.columns,
                    query_type: QueryType::Qail,
                    snippet: p.snippet,
                });
            } else if !current_table.is_empty() {
                // This is a column reference (.filter("col"), .columns([...]))
                // Associate it with the current table
                merged_refs.push(CodeReference {
                    file: path.to_path_buf(),
                    line: p.line,
                    table: current_table.clone(), // <-- Associate with parent table!
                    columns: p.columns,
                    query_type: QueryType::Qail,
                    snippet: p.snippet,
                });
            }
        }

        merged_refs
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

    /// Scan a directory for Rust files
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

            // Skip common non-source directories
            if path.is_dir() {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name == "target" || name == ".git" || name == "node_modules" {
                    continue;
                }
                Self::scan_dir_recursive(&path, refs);
            } else if path.extension().map(|e| e == "rs").unwrap_or(false) {
                refs.extend(Self::scan_file(&path));
            }
        }
    }
}

// =============================================================================
// Raw SQL Detection (for VS Code extension)
// =============================================================================

/// A raw SQL statement detected in Rust source code
#[derive(Debug, Clone, serde::Serialize)]
pub struct RawSqlMatch {
    /// Line number (1-indexed)
    pub line: usize,
    /// Column number (1-indexed)
    pub column: usize,
    /// End line number (1-indexed)
    pub end_line: usize,
    /// End column number (1-indexed)
    pub end_column: usize,
    /// Type of SQL statement
    pub sql_type: String,
    /// The raw SQL content
    pub raw_sql: String,
    /// Suggested QAIL equivalent
    pub suggested_qail: String,
}

/// Visitor that finds raw SQL strings in Rust code
struct SqlDetectorVisitor {
    matches: Vec<RawSqlMatch>,
}

impl SqlDetectorVisitor {
    fn new() -> Self {
        Self { matches: Vec::new() }
    }

    /// Check if a string literal contains SQL
    fn check_string_literal(&mut self, lit: &LitStr) {
        let value = lit.value();
        let upper = value.to_uppercase();
        
        // Check for SQL keywords
        let sql_type = if upper.contains("SELECT") && upper.contains("FROM") {
            "SELECT"
        } else if upper.contains("INSERT INTO") {
            "INSERT"
        } else if upper.contains("UPDATE") && upper.contains("SET") {
            "UPDATE"
        } else if upper.contains("DELETE FROM") {
            "DELETE"
        } else {
            return; // Not SQL
        };

        let span = lit.span();
        let start = span.start();
        let end = span.end();

        // The span includes the quotes, so we use the exact positions
        // Column is 0-indexed in syn, LSP uses 0-indexed too
        // But we need to ensure we capture the entire literal including quotes
        self.matches.push(RawSqlMatch {
            line: start.line,
            column: start.column, // 0-indexed, includes opening quote
            end_line: end.line,
            end_column: end.column, // 0-indexed, should be after closing quote
            sql_type: sql_type.to_string(),
            raw_sql: value.clone(),
            suggested_qail: Self::generate_qail(&value, sql_type),
        });
    }

    /// Generate QAIL equivalent for SQL
    fn generate_qail(sql: &str, sql_type: &str) -> String {
        match sql_type {
            "SELECT" => {
                // Extract table from FROM clause
                let table = sql
                    .to_uppercase()
                    .find("FROM ")
                    .and_then(|i| {
                        let rest = &sql[i + 5..];
                        rest.split_whitespace().next()
                    })
                    .unwrap_or("table")
                    .to_lowercase();
                
                format!("QailCmd::get(\"{}\")\n    .columns([\"*\"])", table)
            }
            "INSERT" => {
                let table = sql
                    .to_uppercase()
                    .find("INTO ")
                    .and_then(|i| {
                        let rest = &sql[i + 5..];
                        rest.split(|c: char| !c.is_alphanumeric() && c != '_').next()
                    })
                    .unwrap_or("table")
                    .to_lowercase();
                
                format!("QailCmd::add(\"{}\")\n    // TODO: add .set_value() calls", table)
            }
            "UPDATE" => {
                let table = sql
                    .to_uppercase()
                    .find("UPDATE ")
                    .and_then(|i| {
                        let rest = &sql[i + 7..];
                        rest.split_whitespace().next()
                    })
                    .unwrap_or("table")
                    .to_lowercase();
                
                format!("QailCmd::set(\"{}\")\n    // TODO: add .set_value() and .filter() calls", table)
            }
            "DELETE" => {
                let table = sql
                    .to_uppercase()
                    .find("FROM ")
                    .and_then(|i| {
                        let rest = &sql[i + 5..];
                        rest.split_whitespace().next()
                    })
                    .unwrap_or("table")
                    .to_lowercase();
                
                format!("QailCmd::del(\"{}\")\n    // TODO: add .filter() call", table)
            }
            _ => format!("// TODO: Convert to QAIL")
        }
    }
}

impl<'ast> Visit<'ast> for SqlDetectorVisitor {
    fn visit_lit(&mut self, lit: &'ast Lit) {
        if let Lit::Str(lit_str) = lit {
            self.check_string_literal(lit_str);
        }
        syn::visit::visit_lit(self, lit);
    }
}

/// Detect raw SQL strings in a Rust source file
pub fn detect_raw_sql(source: &str) -> Vec<RawSqlMatch> {
    match syn::parse_file(source) {
        Ok(syntax) => {
            let mut visitor = SqlDetectorVisitor::new();
            visitor.visit_file(&syntax);
            visitor.matches
        }
        Err(_) => Vec::new(),
    }
}

/// Detect raw SQL strings in a file by path
pub fn detect_raw_sql_in_file(path: &Path) -> Vec<RawSqlMatch> {
    match fs::read_to_string(path) {
        Ok(source) => detect_raw_sql(&source),
        Err(_) => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_qailcmd_get() {
        let code = r#"
            fn query() {
                let cmd = QailCmd::get("users")
                    .filter("status", Operator::Eq, "active")
                    .columns(["id", "name", "email"]);
            }
        "#;

        let syntax = syn::parse_file(code).unwrap();
        let mut visitor = QailVisitor::new(code.to_string());
        visitor.visit_file(&syntax);

        assert!(!visitor.patterns.is_empty());
        // Should find "users" table
        assert!(visitor.patterns.iter().any(|p| p.table == "users"));
        // Should find "status" column
        assert!(visitor.patterns.iter().any(|p| p.columns.contains(&"status".to_string())));
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
        assert!(matches[0].suggested_qail.contains("QailCmd::get"));
    }
}

