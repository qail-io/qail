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

#[cfg(feature = "syn-scanner")]
mod syn_impl {
    use std::collections::HashSet;

    use super::SqlUsageDiagnostic;
    use syn::spanned::Spanned;
    use syn::visit::Visit;

    pub(super) fn detect_in_source(file: &str, source: &str) -> Vec<SqlUsageDiagnostic> {
        if source.contains("qail:allow(raw_sql)") {
            return Vec::new();
        }

        let Ok(syntax) = syn::parse_file(source) else {
            return detect_with_text_fallback(file, source);
        };

        struct SqlVisitor {
            file: String,
            diagnostics: Vec<SqlUsageDiagnostic>,
            seen: HashSet<(usize, usize, &'static str)>,
        }

        impl SqlVisitor {
            fn new(file: &str) -> Self {
                Self {
                    file: file.to_string(),
                    diagnostics: Vec::new(),
                    seen: HashSet::new(),
                }
            }

            fn push_diag(&mut self, code: &'static str, message: String, span: proc_macro2::Span) {
                let start = span.start();
                let key = (start.line, start.column + 1, code);
                if !self.seen.insert(key) {
                    return;
                }
                self.diagnostics.push(SqlUsageDiagnostic {
                    file: self.file.clone(),
                    line: start.line,
                    column: start.column + 1,
                    code,
                    message,
                });
            }
        }

        impl<'ast> Visit<'ast> for SqlVisitor {
            fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
                let syn::Expr::Path(path_expr) = &*node.func else {
                    syn::visit::visit_expr_call(self, node);
                    return;
                };

                if is_qail_raw_call(&path_expr.path) {
                    self.push_diag(
                        "SQL-001",
                        "Qail::raw_sql(...) bypasses QAIL structural validation".to_string(),
                        node.span(),
                    );
                } else if is_sqlx_query_call(&path_expr.path) {
                    self.push_diag(
                        "SQL-002",
                        "sqlx::query* detected; use QAIL DSL instead of raw SQL APIs".to_string(),
                        node.span(),
                    );
                }

                syn::visit::visit_expr_call(self, node);
            }

            fn visit_expr_macro(&mut self, node: &'ast syn::ExprMacro) {
                if is_sqlx_query_macro(&node.mac.path) {
                    self.push_diag(
                        "SQL-003",
                        "sqlx::query!* macro detected; use QAIL DSL instead".to_string(),
                        node.span(),
                    );
                }
                syn::visit::visit_expr_macro(self, node);
            }

            fn visit_macro(&mut self, node: &'ast syn::Macro) {
                if is_sqlx_query_macro(&node.path) {
                    self.push_diag(
                        "SQL-003",
                        "sqlx::query!* macro detected; use QAIL DSL instead".to_string(),
                        node.span(),
                    );
                }
                syn::visit::visit_macro(self, node);
            }

            fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
                let name = node.method.to_string();
                if looks_like_raw_sql_method(&name)
                    && let Some(first_arg) = node.args.first()
                    && expr_contains_sql_literal(first_arg)
                {
                    self.push_diag(
                        "SQL-004",
                        format!(
                            "raw SQL string passed to .{}(...); use QAIL AST/query builder APIs",
                            name
                        ),
                        node.span(),
                    );
                }
                syn::visit::visit_expr_method_call(self, node);
            }
        }

        let mut visitor = SqlVisitor::new(file);
        visitor.visit_file(&syntax);
        visitor.diagnostics
    }

    fn detect_with_text_fallback(file: &str, source: &str) -> Vec<SqlUsageDiagnostic> {
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
        }
        out
    }

    fn is_qail_raw_call(path: &syn::Path) -> bool {
        let segs: Vec<String> = path.segments.iter().map(|s| s.ident.to_string()).collect();
        for pair in segs.windows(2) {
            if pair[0] == "Qail" && (pair[1] == "raw_sql" || pair[1] == "raw") {
                return true;
            }
        }
        false
    }

    fn is_sqlx_query_call(path: &syn::Path) -> bool {
        let segs: Vec<String> = path.segments.iter().map(|s| s.ident.to_string()).collect();
        if segs.len() < 2 {
            return false;
        }
        segs.first().is_some_and(|s| s == "sqlx")
            && segs
                .last()
                .is_some_and(|name| name.starts_with("query") || name.starts_with("prepare"))
    }

    fn is_sqlx_query_macro(path: &syn::Path) -> bool {
        let segs: Vec<String> = path.segments.iter().map(|s| s.ident.to_string()).collect();
        if segs.len() < 2 {
            return false;
        }
        segs.first().is_some_and(|s| s == "sqlx")
            && segs
                .last()
                .is_some_and(|name| name.starts_with("query") || name.starts_with("prepare"))
    }

    fn looks_like_raw_sql_method(name: &str) -> bool {
        matches!(
            name,
            "simple_query"
                | "batch_execute"
                | "prepare"
                | "prepare_typed"
                | "query"
                | "query_one"
                | "query_opt"
                | "query_raw"
                | "execute"
        )
    }

    fn expr_contains_sql_literal(expr: &syn::Expr) -> bool {
        if let Some(s) = extract_string_from_expr(expr) {
            return looks_like_sql(&s);
        }
        false
    }

    fn extract_string_from_expr(expr: &syn::Expr) -> Option<String> {
        match expr {
            syn::Expr::Lit(l) => {
                if let syn::Lit::Str(s) = &l.lit {
                    Some(s.value())
                } else {
                    None
                }
            }
            syn::Expr::Reference(r) => extract_string_from_expr(&r.expr),
            syn::Expr::Paren(p) => extract_string_from_expr(&p.expr),
            syn::Expr::Group(g) => extract_string_from_expr(&g.expr),
            syn::Expr::MethodCall(m) if m.method == "into" || m.method == "to_string" => {
                extract_string_from_expr(&m.receiver)
            }
            syn::Expr::Call(c) => {
                let syn::Expr::Path(path_expr) = &*c.func else {
                    return None;
                };
                let tail = path_expr.path.segments.last()?.ident.to_string();
                if tail == "from" || tail == "new" || tail == "String" {
                    return c.args.first().and_then(extract_string_from_expr);
                }
                None
            }
            _ => None,
        }
    }

    fn looks_like_sql(value: &str) -> bool {
        let upper = value.to_ascii_uppercase();
        let has_select = upper.contains("SELECT") && upper.contains("FROM");
        let has_insert = upper.contains("INSERT") && upper.contains("INTO");
        let has_update = upper.contains("UPDATE") && upper.contains("SET");
        let has_delete = upper.contains("DELETE") && upper.contains("FROM");
        let has_with = upper.contains("WITH") && upper.contains("SELECT");
        has_select || has_insert || has_update || has_delete || has_with
    }
}

#[cfg(not(feature = "syn-scanner"))]
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
        // Blend mode: union syn-based detections with analyzer literal scans.
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
        #[cfg(feature = "syn-scanner")]
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
