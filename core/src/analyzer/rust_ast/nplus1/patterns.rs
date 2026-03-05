//! Query execution patterns, iterator/wait detection, and path resolution helpers.


use syn::parse::Parser;
use syn::visit::Visit;

// =============================================================================

/// Method names that constitute actual DB execution (network round-trip).
///
/// NOTE: Builder methods like `query()`, `query_as()` are intentionally excluded.
/// Those are sqlx query builders — the actual execution happens when you call
/// `.fetch_one()`, `.fetch_all()`, `.execute()` on them.
pub(super) const QUERY_EXEC_METHODS: &[&str] = &[
    // Qail driver — actual execution
    "fetch_all",
    "fetch_all_uncached",
    "fetch_all_cached",
    "fetch_all_fast",
    "fetch_all_with_format",
    "fetch_all_uncached_with_format",
    "fetch_all_cached_with_format",
    "fetch_all_fast_with_format",
    "fetch_one",
    "fetch_typed",
    "fetch_one_typed",
    "execute",
    "execute_batch",
    "execute_raw",
    "fetch_raw",
    // sqlx execution (the terminal calls, not builders)
    "fetch",
    "fetch_optional",
    // Direct SQL execution helpers in qail-pg
    "simple_query",
    "query_raw_with_params",
    "pipeline_ast",
    "pipeline_fetch",
    "query_pipeline",
];

/// Check if a method name is a query execution call.
pub(super) fn is_query_exec_method(name: &str) -> bool {
    QUERY_EXEC_METHODS.contains(&name)
}

/// Iterator/stream combinators that execute a closure body per-item.
/// Treat these as loop constructs for N+1 analysis.
pub(super) const ITER_LOOP_METHODS: &[&str] = &[
    "for_each",
    "try_for_each",
    "for_each_concurrent",
    "try_for_each_concurrent",
];

pub(super) fn is_iter_loop_method(name: &str) -> bool {
    ITER_LOOP_METHODS.contains(&name)
}

/// Additional closure combinators that behave like per-item iteration when the
/// receiver is iterator/stream-like. We gate these with `expr_is_iter_like`.
pub(super) const ITER_CLOSURE_METHODS: &[&str] = &[
    "map",
    "filter_map",
    "flat_map",
    "then",
    "and_then",
    "try_filter_map",
    "fold",
    "try_fold",
    "scan",
];

pub(super) fn is_iter_closure_method(name: &str) -> bool {
    ITER_CLOSURE_METHODS.contains(&name)
}

pub(super) fn is_iter_source_method(name: &str) -> bool {
    matches!(
        name,
        "iter"
            | "iter_mut"
            | "into_iter"
            | "drain"
            | "chunks"
            | "windows"
            | "split"
            | "lines"
            | "values"
            | "keys"
            | "into_values"
            | "into_keys"
    )
}

/// Small compile-time bounded loops (e.g. hardcoded tier pairs) are often
/// deliberate and not true N+1 anti-patterns.
pub(super) const SMALL_BOUNDED_LOOP_MAX: usize = 8;

pub(super) fn path_tail(expr: &syn::ExprPath) -> Option<String> {
    expr.path.segments.last().map(|seg| seg.ident.to_string())
}

/// Heuristic: identify expressions that are likely iterator/stream producers.
/// This keeps `Option::map(...)` from being treated as a loop while still
/// flagging common iterator/stream chains.
pub(super) fn expr_is_iter_like(expr: &syn::Expr) -> bool {
    match expr {
        syn::Expr::MethodCall(method) => {
            let name = method.method.to_string();
            is_iter_source_method(&name) || expr_is_iter_like(&method.receiver)
        }
        syn::Expr::Call(call) => {
            let syn::Expr::Path(path_expr) = &*call.func else {
                return false;
            };
            // Covers patterns like `std::iter::once(...)` and `stream::iter(...)`.
            matches!(
                path_tail(path_expr).as_deref(),
                Some("iter" | "once" | "repeat")
            ) || {
                path_expr.path.segments.iter().any(|seg| {
                    let n = seg.ident.to_string();
                    n == "iter" || n == "stream"
                })
            }
        }
        syn::Expr::Await(a) => expr_is_iter_like(&a.base),
        syn::Expr::Try(t) => expr_is_iter_like(&t.expr),
        syn::Expr::Paren(p) => expr_is_iter_like(&p.expr),
        syn::Expr::Group(g) => expr_is_iter_like(&g.expr),
        syn::Expr::Reference(r) => expr_is_iter_like(&r.expr),
        _ => false,
    }
}

pub(super) const EVENT_WAIT_METHODS: &[&str] = &[
    "recv",
    "recv_many",
    "accept",
    "next",
    "tick",
    "changed",
    "notified",
];

pub(super) const EVENT_WAIT_FUNCTIONS: &[&str] = &["sleep", "timeout", "timeout_at", "interval_at"];

pub(super) const EVENT_WAIT_MACROS: &[&str] = &["select", "select_biased"];

pub(super) fn path_last_ident(path: &syn::Path) -> Option<String> {
    path.segments.last().map(|seg| seg.ident.to_string())
}

pub(super) fn is_wait_primitive_expr(expr: &syn::Expr) -> bool {
    match expr {
        syn::Expr::MethodCall(method) => {
            EVENT_WAIT_METHODS.contains(&method.method.to_string().as_str())
        }
        syn::Expr::Call(call) => {
            let syn::Expr::Path(path_expr) = &*call.func else {
                return false;
            };
            path_last_ident(&path_expr.path)
                .as_deref()
                .is_some_and(|name| EVENT_WAIT_FUNCTIONS.contains(&name))
        }
        syn::Expr::Macro(expr_macro) => path_last_ident(&expr_macro.mac.path)
            .as_deref()
            .is_some_and(|name| EVENT_WAIT_MACROS.contains(&name)),
        syn::Expr::Paren(p) => is_wait_primitive_expr(&p.expr),
        syn::Expr::Group(g) => is_wait_primitive_expr(&g.expr),
        syn::Expr::Reference(r) => is_wait_primitive_expr(&r.expr),
        syn::Expr::Try(t) => is_wait_primitive_expr(&t.expr),
        _ => false,
    }
}

pub(super) fn expr_contains_wait_point(expr: &syn::Expr) -> bool {
    struct WaitPointFinder {
        found: bool,
    }

    impl<'ast> Visit<'ast> for WaitPointFinder {
        fn visit_expr_await(&mut self, node: &'ast syn::ExprAwait) {
            if is_wait_primitive_expr(&node.base) {
                self.found = true;
                return;
            }
            syn::visit::visit_expr_await(self, node);
        }

        fn visit_expr_macro(&mut self, node: &'ast syn::ExprMacro) {
            if path_last_ident(&node.mac.path)
                .as_deref()
                .is_some_and(|name| EVENT_WAIT_MACROS.contains(&name))
            {
                self.found = true;
                return;
            }
            syn::visit::visit_expr_macro(self, node);
        }

        fn visit_expr(&mut self, node: &'ast syn::Expr) {
            if self.found {
                return;
            }
            syn::visit::visit_expr(self, node);
        }
    }

    let mut finder = WaitPointFinder { found: false };
    finder.visit_expr(expr);
    finder.found
}

pub(super) fn block_contains_wait_point(block: &syn::Block) -> bool {
    block.stmts.iter().any(expr_contains_wait_point_in_stmt)
}

pub(super) fn expr_contains_wait_point_in_stmt(stmt: &syn::Stmt) -> bool {
    match stmt {
        syn::Stmt::Local(local) => local
            .init
            .as_ref()
            .is_some_and(|init| expr_contains_wait_point(&init.expr)),
        syn::Stmt::Expr(expr, _) => expr_contains_wait_point(expr),
        syn::Stmt::Macro(stmt_macro) => path_last_ident(&stmt_macro.mac.path)
            .as_deref()
            .is_some_and(|name| EVENT_WAIT_MACROS.contains(&name)),
        _ => false,
    }
}

pub(super) fn parse_vec_macro_len(expr_macro: &syn::ExprMacro) -> Option<usize> {
    let macro_name = path_last_ident(&expr_macro.mac.path)?;
    if macro_name != "vec" {
        return None;
    }
    let parser = syn::punctuated::Punctuated::<syn::Expr, syn::Token![,]>::parse_terminated;
    parser
        .parse2(expr_macro.mac.tokens.clone())
        .ok()
        .map(|p| p.len())
}

pub(super) fn current_module_path_from_segments(segments: &[String]) -> String {
    if segments.is_empty() {
        "crate".to_string()
    } else {
        format!("crate::{}", segments.join("::"))
    }
}

/// Resolve a called function path expression into an absolute `crate::...` path
/// when possible.
pub(super) fn resolve_called_path(path: &syn::Path, current_module_segments: &[String]) -> Option<String> {
    let segs: Vec<String> = path.segments.iter().map(|s| s.ident.to_string()).collect();
    if segs.is_empty() {
        return None;
    }

    let mut abs: Vec<String> = Vec::new();
    match segs[0].as_str() {
        "crate" => abs.extend(segs),
        "self" => {
            abs.push("crate".to_string());
            abs.extend(current_module_segments.iter().cloned());
            abs.extend(segs.into_iter().skip(1));
        }
        "super" => {
            let up = segs.iter().take_while(|s| s.as_str() == "super").count();
            if up > current_module_segments.len() || up >= segs.len() {
                return None;
            }
            abs.push("crate".to_string());
            abs.extend(
                current_module_segments[..current_module_segments.len() - up]
                    .iter()
                    .cloned(),
            );
            abs.extend(segs.into_iter().skip(up));
        }
        _ => {
            abs.push("crate".to_string());
            abs.extend(current_module_segments.iter().cloned());
            if segs.len() == 1 {
                abs.push(segs[0].clone());
            } else {
                abs.extend(segs);
            }
        }
    }
    Some(abs.join("::"))
}

pub(super) fn path_to_string(path: &syn::Path) -> String {
    path.segments
        .iter()
        .map(|s| s.ident.to_string())
        .collect::<Vec<_>>()
        .join("::")
}

pub(super) fn type_to_string(ty: &syn::Type) -> Option<String> {
    match ty {
        syn::Type::Path(tp) => Some(path_to_string(&tp.path)),
        syn::Type::Reference(r) => type_to_string(&r.elem),
        syn::Type::Paren(p) => type_to_string(&p.elem),
        syn::Type::Group(g) => type_to_string(&g.elem),
        _ => None,
    }
}

pub(super) fn impl_context_key(item_impl: &syn::ItemImpl) -> String {
    let self_ty = type_to_string(&item_impl.self_ty).unwrap_or_else(|| "SelfType".to_string());
    if let Some((_, trait_path, _)) = &item_impl.trait_ {
        format!("<{} as {}>", self_ty, path_to_string(trait_path))
    } else {
        self_ty
    }
}
