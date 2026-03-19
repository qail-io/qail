//! Pass 2: N+1 detection via loop-depth tracking.
//!
//! The main detector that walks the Rust AST, tracking loop nesting depth,
//! loop variables, tainted bindings, and emitting diagnostics.

use std::collections::{HashMap, HashSet};
use syn::visit::Visit;

use super::patterns::*;
use super::types::*;

pub(super) struct NPlusOneDetector {
    file: String,
    module_prefix: Vec<String>,
    module_stack: Vec<String>,
    impl_stack: Vec<String>,
    suppressions: Suppressions,
    query_function_paths: HashSet<String>,
    pub(super) diagnostics: Vec<NPlusOneDiagnostic>,
    /// Current nesting depth of loops (for/while/loop).
    loop_depth: usize,
    /// Logical kind of each currently active loop scope.
    loop_kinds: Vec<LoopKind>,
    /// Identifiers bound by enclosing for-loop patterns.
    loop_variables: Vec<HashSet<String>>,
    /// Local bindings derived from loop variables in the current lexical scopes.
    tainted_bindings: Vec<HashSet<String>>,
    /// Lexical bindings of compile-time bounded collection sizes.
    bounded_bindings: Vec<HashMap<String, usize>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LoopKind {
    Work,
    BoundedWork,
    Event,
}

impl NPlusOneDetector {
    pub(super) fn new(
        file: String,
        module_prefix: Vec<String>,
        suppressions: Suppressions,
        query_function_paths: HashSet<String>,
    ) -> Self {
        Self {
            file,
            module_prefix,
            module_stack: Vec::new(),
            impl_stack: Vec::new(),
            suppressions,
            query_function_paths,
            diagnostics: Vec::new(),
            loop_depth: 0,
            loop_kinds: Vec::new(),
            loop_variables: Vec::new(),
            tainted_bindings: vec![HashSet::new()],
            bounded_bindings: vec![HashMap::new()],
        }
    }

    fn current_module_segments(&self) -> Vec<String> {
        let mut segs = self.module_prefix.clone();
        segs.extend(self.module_stack.iter().cloned());
        segs
    }

    fn is_suppressed(&self, line: usize, code: NPlusOneCode) -> bool {
        self.suppressions.contains(&(line, code))
    }

    fn current_module_path(&self) -> String {
        current_module_path_from_segments(&self.current_module_segments())
    }

    fn method_path_in_current_context(&self, method_name: &str) -> String {
        if let Some(impl_ctx) = self.impl_stack.last() {
            format!(
                "{}::{}::{}",
                self.current_module_path(),
                impl_ctx,
                method_name
            )
        } else {
            format!("{}::{}", self.current_module_path(), method_name)
        }
    }

    fn lookup_bounded_binding_len(&self, name: &str) -> Option<usize> {
        self.bounded_bindings
            .iter()
            .rev()
            .find_map(|scope| scope.get(name).copied())
    }

    fn bounded_collection_len(&self, expr: &syn::Expr) -> Option<usize> {
        match expr {
            syn::Expr::Array(arr) => Some(arr.elems.len()),
            syn::Expr::Macro(expr_macro) => parse_vec_macro_len(expr_macro),
            syn::Expr::Path(path_expr) if path_expr.path.segments.len() == 1 => {
                let name = path_expr.path.segments[0].ident.to_string();
                self.lookup_bounded_binding_len(&name)
            }
            syn::Expr::Reference(r) => self.bounded_collection_len(&r.expr),
            syn::Expr::Paren(p) => self.bounded_collection_len(&p.expr),
            syn::Expr::Group(g) => self.bounded_collection_len(&g.expr),
            syn::Expr::Try(t) => self.bounded_collection_len(&t.expr),
            syn::Expr::Await(a) => self.bounded_collection_len(&a.base),
            syn::Expr::MethodCall(m) if is_iter_source_method(&m.method.to_string()) => {
                self.bounded_collection_len(&m.receiver)
            }
            syn::Expr::Call(call) => {
                // Semantic improvement: recognize domain-specific functions that
                // always return small fixed-size collections (e.g. build_tier_pairs
                // always returns 6 items). This eliminates false-positive N1-002
                // warnings on known-bounded pricing loops.
                if let syn::Expr::Path(p) = &*call.func {
                    if let Some(name) = path_tail(p) {
                        if KNOWN_SMALL_COLLECTION_FNS.contains(&name.as_str()) {
                            return Some(6);
                        }
                    }
                }
                None
            }
            _ => None,
        }
    }

    fn is_batch_chunk_iterable(&self, expr: &syn::Expr) -> bool {
        let syn::Expr::MethodCall(method) = expr else {
            return false;
        };
        matches!(
            method.method.to_string().as_str(),
            "chunks" | "chunks_mut" | "array_chunks" | "rchunks" | "rchunks_mut"
        )
    }

    fn classify_for_loop_kind(&self, iterable: &syn::Expr) -> LoopKind {
        if self.is_batch_chunk_iterable(iterable) {
            return LoopKind::BoundedWork;
        }
        match self.bounded_collection_len(iterable) {
            Some(len) if len <= SMALL_BOUNDED_LOOP_MAX => LoopKind::BoundedWork,
            _ => LoopKind::Work,
        }
    }

    fn emit(
        &mut self,
        span: proc_macro2::Span,
        token_len: usize,
        code: NPlusOneCode,
        message: String,
        hint: Option<String>,
    ) {
        let line = span.start().line;
        let column = span.start().column + 1;
        let end_column = column + token_len;

        let severity = match code {
            NPlusOneCode::N1004 => NPlusOneSeverity::Error,
            _ => NPlusOneSeverity::Warning,
        };

        if self.is_suppressed(line, code) {
            return;
        }

        self.diagnostics.push(NPlusOneDiagnostic {
            code,
            severity,
            file: self.file.clone(),
            line,
            column,
            end_column,
            message,
            hint,
        });
    }

    /// All loop variable names from all enclosing loops.
    fn all_loop_vars(&self) -> HashSet<String> {
        self.loop_variables
            .iter()
            .zip(self.loop_kinds.iter())
            .filter(|(_, kind)| matches!(kind, LoopKind::Work))
            .flat_map(|(vars, _)| vars.iter())
            .cloned()
            .collect()
    }

    fn work_loop_depth(&self) -> usize {
        self.loop_kinds
            .iter()
            .filter(|kind| matches!(kind, LoopKind::Work))
            .count()
    }

    /// All currently tainted local bindings from lexical scopes.
    fn all_tainted_bindings(&self) -> HashSet<String> {
        self.tainted_bindings
            .iter()
            .flat_map(|s| s.iter())
            .cloned()
            .collect()
    }

    /// Check if a loop variable appears in the given expression tree.
    /// Used per-execution-site for precise N1-002 detection.
    fn find_loop_var_in_expr(&self, expr: &syn::Expr) -> Option<String> {
        let targets = self.all_loop_vars();
        if targets.is_empty() {
            return None;
        }
        let mut finder = ExprVarFinder {
            targets: &targets,
            found: None,
        };
        syn::visit::visit_expr(&mut finder, expr);
        finder.found
    }

    /// Check if a loop-derived (tainted) local binding appears in expression.
    fn find_tainted_var_in_expr(&self, expr: &syn::Expr) -> Option<String> {
        let targets = self.all_tainted_bindings();
        if targets.is_empty() {
            return None;
        }
        let mut finder = ExprVarFinder {
            targets: &targets,
            found: None,
        };
        syn::visit::visit_expr(&mut finder, expr);
        finder.found
    }

    /// Emit the right diagnostic for a query execution found inside a loop.
    /// Checks the specific call expression for loop-var references (N1-002)
    /// rather than scanning the entire loop body.
    fn handle_query_in_loop(
        &mut self,
        span: proc_macro2::Span,
        method_name: &str,
        method_len: usize,
        call_expr: &syn::Expr,
    ) {
        let work_depth = self.work_loop_depth();
        if work_depth == 0 {
            return;
        }

        // N1-004: nested loop (depth >= 2) → error
        if work_depth >= 2 {
            self.emit(
                span,
                method_len,
                NPlusOneCode::N1004,
                format!(
                    "Query `{}` inside nested loop (depth {}) — O(n²) or worse",
                    method_name, work_depth
                ),
                Some("Restructure to batch all IDs upfront, then query once with IN/ANY".into()),
            );
            return;
        }

        // N1-002: loop variable referenced in THIS execution expression
        // (receiver chain + args), not the entire loop body
        if let Some(var_name) = self.find_loop_var_in_expr(call_expr) {
            self.emit(
                span,
                method_len,
                NPlusOneCode::N1002,
                format!(
                    "Query `{}` in loop uses loop variable `{}` — N+1 pattern",
                    method_name, var_name
                ),
                Some("Collect IDs first, then batch query with .in_vals() or ANY($1)".into()),
            );
            return;
        }

        // N1-002: query expression references a local binding derived from a loop var
        // (e.g. `let cmd = ...id...; fetch_all(&cmd)`).
        if let Some(binding_name) = self.find_tainted_var_in_expr(call_expr) {
            self.emit(
                span,
                method_len,
                NPlusOneCode::N1002,
                format!(
                    "Query `{}` in loop uses loop-derived binding `{}` — N+1 pattern",
                    method_name, binding_name
                ),
                Some("Collect IDs first, then batch query with .in_vals() or ANY($1)".into()),
            );
            return;
        }

        // N1-001: generic query in loop
        self.emit(
            span,
            method_len,
            NPlusOneCode::N1001,
            format!(
                "Query execution `{}` inside loop can cause N+1",
                method_name
            ),
            Some("Consider batching or moving the query outside the loop".into()),
        );
    }
}

pub(super) fn collect_pat_idents_recursive(pat: &syn::Pat, idents: &mut HashSet<String>) {
    match pat {
        syn::Pat::Ident(pi) => {
            idents.insert(pi.ident.to_string());
        }
        syn::Pat::Tuple(pt) => {
            for elem in &pt.elems {
                collect_pat_idents_recursive(elem, idents);
            }
        }
        syn::Pat::TupleStruct(pts) => {
            for elem in &pts.elems {
                collect_pat_idents_recursive(elem, idents);
            }
        }
        syn::Pat::Struct(ps) => {
            for field in &ps.fields {
                collect_pat_idents_recursive(&field.pat, idents);
            }
        }
        syn::Pat::Reference(pr) => {
            collect_pat_idents_recursive(&pr.pat, idents);
        }
        syn::Pat::Or(po) => {
            for case in &po.cases {
                collect_pat_idents_recursive(case, idents);
            }
        }
        _ => {}
    }
}

// =============================================================================
// syn::Visit Implementation
// =============================================================================

impl<'ast> Visit<'ast> for NPlusOneDetector {
    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        if let Some((_, items)) = &node.content {
            self.module_stack.push(node.ident.to_string());
            for item in items {
                self.visit_item(item);
            }
            self.module_stack.pop();
        } else {
            syn::visit::visit_item_mod(self, node);
        }
    }

    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        self.impl_stack.push(impl_context_key(node));
        syn::visit::visit_item_impl(self, node);
        self.impl_stack.pop();
    }

    fn visit_block(&mut self, node: &'ast syn::Block) {
        self.tainted_bindings.push(HashSet::new());
        self.bounded_bindings.push(HashMap::new());
        syn::visit::visit_block(self, node);
        self.bounded_bindings.pop();
        self.tainted_bindings.pop();
    }

    fn visit_local(&mut self, node: &'ast syn::Local) {
        let bounded_len = node
            .init
            .as_ref()
            .and_then(|init| self.bounded_collection_len(&init.expr));
        if let Some(len) = bounded_len {
            let mut names = HashSet::new();
            collect_pat_idents_recursive(&node.pat, &mut names);
            if let Some(scope) = self.bounded_bindings.last_mut() {
                for name in names {
                    scope.insert(name, len);
                }
            }
        }

        // Always visit init/body first so regular diagnostics still run.
        syn::visit::visit_local(self, node);

        if self.work_loop_depth() == 0 {
            return;
        }

        let Some(init) = &node.init else {
            return;
        };
        let init_expr = &init.expr;
        let tainted = self.find_loop_var_in_expr(init_expr).is_some()
            || self.find_tainted_var_in_expr(init_expr).is_some();
        if !tainted {
            return;
        }

        let mut names = HashSet::new();
        collect_pat_idents_recursive(&node.pat, &mut names);
        if names.is_empty() {
            return;
        }
        if let Some(scope) = self.tainted_bindings.last_mut() {
            scope.extend(names);
        }
    }

    fn visit_expr_for_loop(&mut self, node: &'ast syn::ExprForLoop) {
        self.loop_depth += 1;
        self.loop_kinds
            .push(self.classify_for_loop_kind(&node.expr));
        let mut pat_idents = HashSet::new();
        collect_pat_idents_recursive(&node.pat, &mut pat_idents);
        self.loop_variables.push(pat_idents);

        syn::visit::visit_expr_for_loop(self, node);

        self.loop_variables.pop();
        self.loop_kinds.pop();
        self.loop_depth -= 1;
    }

    fn visit_expr_while(&mut self, node: &'ast syn::ExprWhile) {
        self.loop_depth += 1;
        let loop_kind = if expr_contains_wait_point(&node.cond) {
            LoopKind::Event
        } else {
            LoopKind::Work
        };
        self.loop_kinds.push(loop_kind);
        let mut pat_idents = HashSet::new();
        if let syn::Expr::Let(expr_let) = &*node.cond {
            collect_pat_idents_recursive(&expr_let.pat, &mut pat_idents);
        }
        self.loop_variables.push(pat_idents);

        syn::visit::visit_expr_while(self, node);

        self.loop_variables.pop();
        self.loop_kinds.pop();
        self.loop_depth -= 1;
    }

    fn visit_expr_loop(&mut self, node: &'ast syn::ExprLoop) {
        self.loop_depth += 1;
        let loop_kind = if block_contains_wait_point(&node.body) {
            LoopKind::Event
        } else {
            LoopKind::Work
        };
        self.loop_kinds.push(loop_kind);
        self.loop_variables.push(HashSet::new());

        syn::visit::visit_expr_loop(self, node);

        self.loop_variables.pop();
        self.loop_kinds.pop();
        self.loop_depth -= 1;
    }

    // Detect actual execution: conn.fetch_all(&cmd), conn.execute(&cmd)
    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        let method_name = node.method.to_string();

        // Treat iterator/stream closure combinators as loop constructs.
        let treat_as_loop = is_iter_loop_method(&method_name)
            || (is_iter_closure_method(&method_name) && expr_is_iter_like(&node.receiver));
        if treat_as_loop {
            // Receiver expression still belongs to current scope.
            syn::visit::visit_expr(self, &node.receiver);
            let loop_kind = self.classify_for_loop_kind(&node.receiver);

            for arg in &node.args {
                if let syn::Expr::Closure(closure) = arg {
                    self.loop_depth += 1;
                    self.loop_kinds.push(loop_kind);
                    let mut pat_idents = HashSet::new();
                    for input in &closure.inputs {
                        collect_pat_idents_recursive(input, &mut pat_idents);
                    }
                    self.loop_variables.push(pat_idents);
                    syn::visit::visit_expr(self, &closure.body);
                    self.loop_variables.pop();
                    self.loop_kinds.pop();
                    self.loop_depth -= 1;
                } else {
                    syn::visit::visit_expr(self, arg);
                }
            }
            return;
        }

        if self.work_loop_depth() > 0 && is_query_exec_method(&method_name) {
            // Pass the full method call expression so N1-002 can check
            // if THIS specific call involves a loop variable
            let call_expr = syn::Expr::MethodCall(node.clone());
            self.handle_query_in_loop(
                node.method.span(),
                &method_name,
                method_name.len(),
                &call_expr,
            );
        }

        // Detect N1-003 for `self.some_fn(...)` where `some_fn` is known
        // (from pass 1) to execute queries.
        if self.work_loop_depth() > 0
            && !is_query_exec_method(&method_name)
            && self
                .query_function_paths
                .contains(&self.method_path_in_current_context(&method_name))
        {
            let is_self_receiver =
                matches!(&*node.receiver, syn::Expr::Path(p) if p.path.is_ident("self"));
            if is_self_receiver {
                self.emit(
                    node.method.span(),
                    method_name.len(),
                    NPlusOneCode::N1003,
                    format!(
                        "Method `{}` (which executes queries) called inside loop — indirect N+1",
                        method_name
                    ),
                    Some(
                        "Move the call outside the loop or refactor to accept batched inputs"
                            .into(),
                    ),
                );
            }
        }
        syn::visit::visit_expr_method_call(self, node);
    }

    // Detect N1-003: function calls that resolve to local helpers containing query execution.
    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        if self.work_loop_depth() > 0
            && let syn::Expr::Path(ref path_expr) = *node.func
            && let Some(seg) = path_expr.path.segments.last()
        {
            let fn_name = seg.ident.to_string();
            let resolved_path =
                resolve_called_path(&path_expr.path, &self.current_module_segments());
            let is_query_fn = resolved_path
                .as_ref()
                .is_some_and(|p| self.query_function_paths.contains(p));
            if is_query_fn {
                let span = seg.ident.span();
                self.emit(
                    span,
                    fn_name.len(),
                    NPlusOneCode::N1003,
                    format!(
                        "Function `{}` (which executes queries) called inside loop — indirect N+1",
                        fn_name
                    ),
                    Some(
                        "Move the call outside the loop or refactor to accept batched inputs"
                            .into(),
                    ),
                );
            }
        }
        syn::visit::visit_expr_call(self, node);
    }
}

/// Scan a single expression tree for loop variable references.
/// Used per-execution-site for precise N1-002 detection.
pub(super) struct ExprVarFinder<'a> {
    targets: &'a HashSet<String>,
    found: Option<String>,
}

impl<'a, 'ast> Visit<'ast> for ExprVarFinder<'a> {
    fn visit_ident(&mut self, ident: &'ast proc_macro2::Ident) {
        if self.found.is_none() {
            let name = ident.to_string();
            if self.targets.contains(&name) {
                self.found = Some(name);
            }
        }
    }
}
