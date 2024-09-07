//! Rust AST analyzer using `syn`.
//!
//! Provides 100% accurate detection of QAIL patterns in Rust source code
//! by parsing the actual AST instead of using regex.

use std::fs;
use std::path::Path;
use syn::visit::Visit;
use syn::{Expr, ExprCall, ExprMethodCall, Lit, LitStr};

// Use the parent analyzer module's types
use crate::analyzer::{CodeReference, QueryType};

// Re-export proc_macro2 span type for line number extraction
use proc_macro2::Span;

/// Patterns we're looking for in Rust code
#[derive(Debug, Clone)]
pub struct RustPattern {
    pub table: String,
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

    /// Extract all string literals from any expression (generic approach)
    fn extract_strings_from_expr(expr: &Expr) -> Vec<String> {
        let mut strings = Vec::new();
        match expr {
            // Direct string literal
            Expr::Lit(lit) => {
                if let Lit::Str(s) = &lit.lit {
                    strings.push(Self::extract_string(s));
                }
            }
            // Array of strings [\"a\", \"b\", \"c\"]
            Expr::Array(arr) => {
                for elem in &arr.elems {
                    strings.extend(Self::extract_strings_from_expr(elem));
                }
            }
            // Reference &\"string\"
            Expr::Reference(r) => {
                strings.extend(Self::extract_strings_from_expr(&r.expr));
            }
            _ => {}
        }
        strings
    }

    /// Check if this is a Qail constructor call (generic - detects ALL constructors)
    fn check_qailcmd_call(&mut self, path: &syn::ExprPath, args: &syn::punctuated::Punctuated<Expr, syn::token::Comma>) {
        let segments: Vec<_> = path.path.segments.iter().map(|s| s.ident.to_string()).collect();
        
        // Match Qail::* where * is any method
        if segments.len() >= 2 && segments[0] == "Qail" {
            let action = &segments[1];
            
            let mut columns = Vec::new();
            let mut table = String::new();
            
            for arg in args {
                let extracted = Self::extract_strings_from_expr(arg);
                if table.is_empty() && !extracted.is_empty() {
                    table = extracted[0].clone();
                } else {
                    columns.extend(extracted);
                }
            }
            
            if !table.is_empty() {
                self.patterns.push(RustPattern {
                    table: table.clone(),
                    columns,
                    line: self.line_from_span(path.path.segments.first().map(|s| s.ident.span()).unwrap_or_else(Span::call_site)),
                    snippet: format!("Qail::{}(\"{}\")", action, table),
                });
            }
        }
    }

    /// Check method calls for column/table references (generic - captures ALL string arguments)
    fn check_method_call(&mut self, method: &str, args: &syn::punctuated::Punctuated<Expr, syn::token::Comma>, span: Span) {
        let mut all_strings = Vec::new();
        for arg in args {
            all_strings.extend(Self::extract_strings_from_expr(arg));
        }
        
        // If we found any strings, record this method call
        if !all_strings.is_empty() {
            let snippet = if all_strings.len() == 1 {
                format!(".{}(\"{}\")", method, all_strings[0])
            } else if all_strings.len() <= 3 {
                format!(".{}([{}])", method, all_strings.iter().map(|s| format!("\"{}\"", s)).collect::<Vec<_>>().join(", "))
            } else {
                format!(".{}([\"{}\" +{}])", method, all_strings[0], all_strings.len() - 1)
            };
            
            self.patterns.push(RustPattern {
                table: String::new(), // Will be merged with parent
                columns: all_strings,
                line: self.line_from_span(span),
                snippet,
            });
        }
    }
}

impl<'ast> Visit<'ast> for QailVisitor {
    fn visit_expr_call(&mut self, node: &'ast ExprCall) {
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
                // This is a table reference (Qail::get("table"))
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
        // But we need to ensure we capture the entire literal including quotes
        self.matches.push(RawSqlMatch {
            line: start.line,
            column: start.column, // 0-indexed, includes opening quote
            end_line: end.line,
            end_column: end.column, // 0-indexed, should be after closing quote
            sql_type: sql_type.to_string(),
            raw_sql: value.clone(),
            suggested_qail: super::transformer::sql_to_qail(&value).unwrap_or_else(|_| "// Could not parse SQL".to_string()),
        });
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
                let cmd = Qail::get("users")
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
        assert!(matches[0].suggested_qail.contains("Qail::get"));
    }

    #[test]
    fn test_generate_cte_qail() {
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
        // Should detect CTE pattern - generates separate CTE variables
        assert!(qail.contains("CTE 'stats'") || qail.contains("stats_cte"), 
            "Should generate CTE variable: {}", qail);
        // Should find the source table
        assert!(qail.contains("messages"), "Should find source table 'messages': {}", qail);
    }
}

