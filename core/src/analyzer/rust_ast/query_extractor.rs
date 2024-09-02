//! Query pattern extractor using syn.
//!
//! Detects database query calls and extracts:
//! - Full span of the entire call (for replacement)
//! - SQL string content
//! - Bind parameters with their expressions
//! - Return type (from turbofish)

#![allow(dead_code)]  // Module under development, will be used by LSP

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::visit::Visit;
use syn::{Expr, ExprAwait, ExprPath, Lit};

/// A detected database query call
#[derive(Debug, Clone)]
pub struct QueryCall {
    /// Start line (1-indexed)
    pub start_line: usize,
    /// Start column (0-indexed)
    pub start_column: usize,
    /// End line (1-indexed)
    pub end_line: usize,
    /// End column (0-indexed)
    pub end_column: usize,
    /// The raw SQL string
    pub sql: String,
    /// Bind parameters in order (the expression source code)
    pub binds: Vec<String>,
    pub return_type: Option<String>,
    /// The query function name (query, query_as, query_scalar)
    pub query_fn: String,
}

/// Visitor that finds database query calls
struct QueryVisitor {
    matches: Vec<QueryCall>,
    source: String,
}

impl QueryVisitor {
    fn new(source: String) -> Self {
        Self {
            matches: Vec::new(),
            source,
        }
    }

    /// Check if this is a sqlx query call and extract info
    fn check_expr(&mut self, expr: &Expr) {
        // Look for await expressions that end the chain
        if let Expr::Await(ExprAwait { base, .. }) = expr {
            // Walk back through the method chain
            if let Some(call) = self.extract_query_chain(base) {
                self.matches.push(call);
            }
        }
    }

    /// Extract query call info from a method chain
    fn extract_query_chain(&self, expr: &Expr) -> Option<QueryCall> {
        let mut binds = Vec::new();
        let mut current = expr.clone();
        let mut end_span: Option<Span> = None;

        // Walk through method chain backwards collecting .bind() calls
        loop {
            match &current {
                Expr::MethodCall(method) => {
                    if end_span.is_none() {
                        end_span = Some(method.method.span());
                    }
                    
                    let method_name = method.method.to_string();
                    
                    if method_name == "bind"
                        && let Some(arg) = method.args.first()
                    {
                        let arg_str = self.expr_to_source(arg);
                        binds.insert(0, arg_str);
                    }
                    
                    current = (*method.receiver).clone();
                }
                Expr::Call(call) => {
                    if let Expr::Path(path) = &*call.func {
                        let path_str = path_to_string(&path.path);
                        
                        if path_str.starts_with("sqlx::query") || path_str == "query" || path_str == "query_as" {
                            // Found the sqlx call!
                            let query_fn = path_str.split("::").last().unwrap_or("query").to_string();
                            
                            let sql = call.args.first().and_then(|arg| {
                                extract_string_literal(arg)
                            });
                            
                            let return_type = extract_turbofish_type(path);
                            
                            let start = call.func.span().start();
                            let end = end_span.map(|s| s.end()).unwrap_or(start);
                            
                            if let Some(sql) = sql {
                                return Some(QueryCall {
                                    start_line: start.line,
                                    start_column: start.column,
                                    end_line: end.line,
                                    end_column: end.column,
                                    sql,
                                    binds,
                                    return_type,
                                    query_fn,
                                });
                            }
                        }
                    }
                    break;
                }
                _ => break,
            }
        }
        
        None
    }

    /// Convert expression back to source code string
    fn expr_to_source(&self, expr: &Expr) -> String {
        // Format the expression using Display trait
        format!("{}", quote::ToTokens::to_token_stream(expr))
    }
}

impl<'ast> Visit<'ast> for QueryVisitor {
    fn visit_expr(&mut self, expr: &'ast Expr) {
        self.check_expr(expr);
        syn::visit::visit_expr(self, expr);
    }
}

/// Convert syn path to string
fn path_to_string(path: &syn::Path) -> String {
    path.segments
        .iter()
        .map(|seg| seg.ident.to_string())
        .collect::<Vec<_>>()
        .join("::")
}

/// Extract string literal from expression
fn extract_string_literal(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Lit(lit) => {
            if let Lit::Str(s) = &lit.lit {
                Some(s.value())
            } else {
                None
            }
        }
        Expr::Reference(r) => extract_string_literal(&r.expr),
        _ => None,
    }
}

/// Extract return type from turbofish syntax
fn extract_turbofish_type(path: &ExprPath) -> Option<String> {
    // Look for ::<_, RowType> in the path
    for seg in &path.path.segments {
        if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
            let types: Vec<_> = args.args.iter().filter_map(|arg| {
                if let syn::GenericArgument::Type(ty) = arg {
                    Some(format!("{}", quote::ToTokens::to_token_stream(ty)))
                } else {
                    None
                }
            }).collect();
            
            // Return the second type (Row type, after _)
            if types.len() >= 2 {
                return Some(types[1].clone());
            }
        }
    }
    None
}

/// Detect database query calls in Rust source code
pub fn detect_query_calls(source: &str) -> Vec<QueryCall> {
    match syn::parse_file(source) {
        Ok(syntax) => {
            let mut visitor = QueryVisitor::new(source.to_string());
            visitor.visit_file(&syntax);
            visitor.matches
        }
        Err(_) => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_simple_query() {
        let code = r#"
            async fn test() {
                let rows = sqlx::query_as::<_, User>("SELECT * FROM users")
                    .fetch_all(&pool)
                    .await;
            }
        "#;
        
        let calls = detect_query_calls(code);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].sql, "SELECT * FROM users");
        assert_eq!(calls[0].query_fn, "query_as");
    }

    #[test]
    fn test_detect_query_with_binds() {
        let code = r#"
            async fn test() {
                let rows = sqlx::query_as::<_, User>("SELECT * FROM users WHERE id = $1")
                    .bind(user_id)
                    .fetch_all(&pool)
                    .await;
            }
        "#;
        
        let calls = detect_query_calls(code);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].binds.len(), 1);
        assert!(calls[0].binds[0].contains("user_id"));
    }

    #[test]
    fn test_detect_multiple_binds() {
        let code = r#"
            async fn test() {
                let rows = sqlx::query("SELECT * FROM users WHERE name = $1 AND age > $2")
                    .bind(name)
                    .bind(min_age)
                    .fetch_all(&pool)
                    .await;
            }
        "#;
        
        let calls = detect_query_calls(code);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].binds.len(), 2);
    }
}
