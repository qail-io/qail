//! Compile-time N+1 query detection via `syn::Visit` AST traversal.
//!
//! Detects query executions inside loops at compile time, before the code runs.
//!
//! # Rules
//!
//! | Code   | Severity | Description |
//! |--------|----------|-------------|
//! | N1-001 | Warning  | Query execution inside a loop |
//! | N1-002 | Warning  | Loop variable used in query-building chain (suggests IN/ANY) |
//! | N1-003 | Warning  | Function containing query called inside a loop (file-local) |
//! | N1-004 | Error    | Query execution inside nested loops (loop_depth ≥ 2) |
//!
//! # Suppression
//!
//! ```ignore
//! // qail-lint:disable-next-line N1-001
//! for item in items {
//!     conn.fetch_all(&query).await?;
//! }
//! ```
//!
//! # Known Limitations
//!
//! - `detect_n_plus_one_in_dir` builds a module-level symbol index and can
//!   propagate N1-003 across files for common `module::func(...)` patterns.
//!   Dynamic dispatch/import aliasing remains best-effort.
//! - Qail constructor calls (`Qail::get(...)`) alone are NOT flagged — only actual
//!   execution methods (`conn.fetch_all`, `conn.execute`, etc.) are considered
//!   database round-trips.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use syn::visit::Visit;

// =============================================================================
// Public Types
// =============================================================================

/// Diagnostic rule code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NPlusOneCode {
    /// Query execution inside a loop.
    N1001,
    /// Loop variable used in query-building chain — suggests batching.
    N1002,
    /// Function that executes a query called inside a loop (file-local heuristic).
    N1003,
    /// Query execution inside nested loops (loop_depth ≥ 2).
    N1004,
}

impl NPlusOneCode {
    /// Human-readable code string.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::N1001 => "N1-001",
            Self::N1002 => "N1-002",
            Self::N1003 => "N1-003",
            Self::N1004 => "N1-004",
        }
    }
}

impl std::fmt::Display for NPlusOneCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Diagnostic severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NPlusOneSeverity {
    Warning,
    Error,
}

impl std::fmt::Display for NPlusOneSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Warning => f.write_str("warning"),
            Self::Error => f.write_str("error"),
        }
    }
}

/// A single N+1 diagnostic.
#[derive(Debug, Clone)]
pub struct NPlusOneDiagnostic {
    pub code: NPlusOneCode,
    pub severity: NPlusOneSeverity,
    pub file: String,
    pub line: usize,
    pub column: usize,
    /// End column of the highlighted token (for LSP range precision).
    pub end_column: usize,
    pub message: String,
    pub hint: Option<String>,
}

impl std::fmt::Display for NPlusOneDiagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {}:{}:{}: {}",
            self.code, self.file, self.line, self.column, self.message
        )?;
        if let Some(ref hint) = self.hint {
            write!(f, " (hint: {})", hint)?;
        }
        Ok(())
    }
}

// =============================================================================
// Public API
// =============================================================================

/// Detect N+1 patterns in a single Rust source file.
pub fn detect_n_plus_one_in_file(file: &str, source: &str) -> Vec<NPlusOneDiagnostic> {
    let Ok(syntax) = syn::parse_file(source) else {
        return vec![];
    };

    let suppressions = parse_suppressions(source);

    // Pass 1: collect functions that contain actual query *execution* calls
    let mut fn_collector = FunctionQueryCollector::new(Vec::new());
    fn_collector.visit_file(&syntax);
    let query_index = fn_collector.compute_query_index();

    // Pass 2: detect N+1 patterns
    let mut detector = NPlusOneDetector::new(
        file.to_string(),
        Vec::new(),
        suppressions,
        query_index.names,
        query_index.paths,
    );
    detector.visit_file(&syntax);

    detector.diagnostics
}

/// Detect N+1 patterns in all Rust files under a directory.
pub fn detect_n_plus_one_in_dir(dir: &Path) -> Vec<NPlusOneDiagnostic> {
    let files = collect_rust_files(dir);
    if files.is_empty() {
        return Vec::new();
    }

    // Pass 1: build module-level symbol index across files.
    let mut global_infos: HashMap<String, FunctionCallInfo> = HashMap::new();
    let mut file_entries: Vec<(PathBuf, String, Vec<String>)> = Vec::new();
    for path in files {
        let Ok(source) = std::fs::read_to_string(&path) else {
            continue;
        };
        let module_prefix = module_prefix_for_file(dir, &path);
        file_entries.push((path.clone(), source.clone(), module_prefix.clone()));

        let Ok(syntax) = syn::parse_file(&source) else {
            continue;
        };
        let mut collector = FunctionQueryCollector::new(module_prefix);
        collector.visit_file(&syntax);
        collector.merge_into(&mut global_infos);
    }

    let query_index = compute_query_index_from_infos(&global_infos);

    // Pass 2: run detection using global propagated index.
    let mut diagnostics = Vec::new();
    for (path, source, module_prefix) in file_entries {
        let Ok(syntax) = syn::parse_file(&source) else {
            continue;
        };
        let suppressions = parse_suppressions(&source);
        let mut detector = NPlusOneDetector::new(
            path.display().to_string(),
            module_prefix,
            suppressions,
            query_index.names.clone(),
            query_index.paths.clone(),
        );
        detector.visit_file(&syntax);
        diagnostics.extend(detector.diagnostics);
    }
    diagnostics
}

fn collect_rust_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    collect_rust_files_recursive(dir, &mut out);
    out
}

fn collect_rust_files_recursive(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str())
                && (name.starts_with('.') || name == "target")
            {
                continue;
            }
            collect_rust_files_recursive(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

fn module_prefix_for_file(root: &Path, file: &Path) -> Vec<String> {
    let Ok(rel) = file.strip_prefix(root) else {
        return Vec::new();
    };

    let mut segs: Vec<String> = rel
        .parent()
        .into_iter()
        .flat_map(|p| p.components())
        .filter_map(|c| c.as_os_str().to_str())
        .map(ToOwned::to_owned)
        .collect();

    let file_stem = rel.file_stem().and_then(|s| s.to_str()).unwrap_or_default();
    let file_name = rel.file_name().and_then(|s| s.to_str()).unwrap_or_default();

    if file_name != "mod.rs" && file_stem != "lib" && file_stem != "main" && !file_stem.is_empty() {
        segs.push(file_stem.to_string());
    }
    segs
}

// =============================================================================
// Suppression Parsing
// =============================================================================

type Suppressions = HashSet<(usize, NPlusOneCode)>;

fn parse_suppressions(source: &str) -> Suppressions {
    let mut suppressions = HashSet::new();

    for (idx, line) in source.lines().enumerate() {
        let trimmed = line.trim();
        let line_number = idx + 1;

        if let Some(rest) = trimmed.strip_prefix("// qail-lint:disable-next-line") {
            for code in parse_code_list(rest) {
                suppressions.insert((line_number + 1, code));
            }
        }
        if let Some(rest) = trimmed.strip_prefix("// qail-lint:disable-line") {
            for code in parse_code_list(rest) {
                suppressions.insert((line_number, code));
            }
        }
        if let Some(pos) = trimmed.find("// qail-lint:disable-line")
            && pos > 0
        {
            let rest = &trimmed[pos + "// qail-lint:disable-line".len()..];
            for code in parse_code_list(rest) {
                suppressions.insert((line_number, code));
            }
        }
    }

    suppressions
}

fn parse_code_list(s: &str) -> Vec<NPlusOneCode> {
    let mut codes = Vec::new();
    for token in s.split_whitespace() {
        match token.trim_matches(',') {
            "N1-001" | "N1001" => codes.push(NPlusOneCode::N1001),
            "N1-002" | "N1002" => codes.push(NPlusOneCode::N1002),
            "N1-003" | "N1003" => codes.push(NPlusOneCode::N1003),
            "N1-004" | "N1004" => codes.push(NPlusOneCode::N1004),
            _ => {}
        }
    }
    codes
}

// =============================================================================
// Query Execution Detection Patterns
// =============================================================================

/// Method names that constitute actual DB execution (network round-trip).
///
/// NOTE: Builder methods like `query()`, `query_as()` are intentionally excluded.
/// Those are sqlx query builders — the actual execution happens when you call
/// `.fetch_one()`, `.fetch_all()`, `.execute()` on them.
const QUERY_EXEC_METHODS: &[&str] = &[
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
fn is_query_exec_method(name: &str) -> bool {
    QUERY_EXEC_METHODS.contains(&name)
}

/// Iterator/stream combinators that execute a closure body per-item.
/// Treat these as loop constructs for N+1 analysis.
const ITER_LOOP_METHODS: &[&str] = &[
    "for_each",
    "try_for_each",
    "for_each_concurrent",
    "try_for_each_concurrent",
];

fn is_iter_loop_method(name: &str) -> bool {
    ITER_LOOP_METHODS.contains(&name)
}

/// Additional closure combinators that behave like per-item iteration when the
/// receiver is iterator/stream-like. We gate these with `expr_is_iter_like`.
const ITER_CLOSURE_METHODS: &[&str] = &[
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

fn is_iter_closure_method(name: &str) -> bool {
    ITER_CLOSURE_METHODS.contains(&name)
}

fn is_iter_source_method(name: &str) -> bool {
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

fn path_tail(expr: &syn::ExprPath) -> Option<String> {
    expr.path.segments.last().map(|seg| seg.ident.to_string())
}

/// Heuristic: identify expressions that are likely iterator/stream producers.
/// This keeps `Option::map(...)` from being treated as a loop while still
/// flagging common iterator/stream chains.
fn expr_is_iter_like(expr: &syn::Expr) -> bool {
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

fn current_module_path_from_segments(segments: &[String]) -> String {
    if segments.is_empty() {
        "crate".to_string()
    } else {
        format!("crate::{}", segments.join("::"))
    }
}

/// Resolve a called function path expression into an absolute `crate::...` path
/// when possible.
fn resolve_called_path(path: &syn::Path, current_module_segments: &[String]) -> Option<String> {
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
            if segs.len() <= 1 {
                return None;
            }
            abs.push("crate".to_string());
            abs.extend(current_module_segments.iter().cloned());
            abs.extend(segs);
        }
    }
    Some(abs.join("::"))
}

// =============================================================================
// Pass 1: Collect Functions That Execute Queries
// =============================================================================

/// Collects function names that contain actual query execution calls.
/// Only tracks `.fetch_*()` / `.execute*()` method calls — NOT Qail constructors,
/// because constructors alone don't hit the database.
#[derive(Default, Clone)]
struct FunctionCallInfo {
    direct_query_exec: bool,
    calls_by_name: HashSet<String>,
    calls_by_path: HashSet<String>,
}

#[derive(Default, Clone)]
struct QueryFunctionIndex {
    names: HashSet<String>,
    paths: HashSet<String>,
}

fn short_name_from_path(path: &str) -> Option<String> {
    path.rsplit("::").next().map(ToOwned::to_owned)
}

fn compute_query_index_from_infos(infos: &HashMap<String, FunctionCallInfo>) -> QueryFunctionIndex {
    let mut query_paths: HashSet<String> = infos
        .iter()
        .filter_map(|(name, info)| info.direct_query_exec.then_some(name.clone()))
        .collect();
    let mut query_names: HashSet<String> = query_paths
        .iter()
        .filter_map(|p| short_name_from_path(p))
        .collect();

    // Fixed-point closure over graph edges.
    loop {
        let mut changed = false;
        for (name, info) in infos {
            if query_paths.contains(name) {
                continue;
            }
            let calls_query_path = info.calls_by_path.iter().any(|p| query_paths.contains(p));
            let calls_query_name = info.calls_by_name.iter().any(|n| query_names.contains(n));
            if calls_query_path || calls_query_name {
                query_paths.insert(name.clone());
                if let Some(short) = short_name_from_path(name) {
                    query_names.insert(short);
                }
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    QueryFunctionIndex {
        names: query_names,
        paths: query_paths,
    }
}

struct FunctionQueryCollector {
    infos: HashMap<String, FunctionCallInfo>,
    current_function: Option<String>,
    module_prefix: Vec<String>,
    module_stack: Vec<String>,
}

impl FunctionQueryCollector {
    fn new(module_prefix: Vec<String>) -> Self {
        Self {
            infos: HashMap::new(),
            current_function: None,
            module_prefix,
            module_stack: Vec::new(),
        }
    }

    fn current_module_segments(&self) -> Vec<String> {
        let mut segs = self.module_prefix.clone();
        segs.extend(self.module_stack.iter().cloned());
        segs
    }

    fn current_module_path(&self) -> String {
        current_module_path_from_segments(&self.current_module_segments())
    }

    fn function_key(&self, fn_name: &str) -> String {
        format!("{}::{}", self.current_module_path(), fn_name)
    }

    fn ensure_current_entry(&mut self) {
        if let Some(name) = self.current_function.as_ref() {
            self.infos.entry(name.clone()).or_default();
        }
    }

    fn mark_direct_query_exec(&mut self) {
        if let Some(name) = self.current_function.as_ref() {
            self.infos
                .entry(name.clone())
                .or_default()
                .direct_query_exec = true;
        }
    }

    fn add_call_edge(&mut self, callee: String) {
        if let Some(name) = self.current_function.as_ref() {
            self.infos
                .entry(name.clone())
                .or_default()
                .calls_by_name
                .insert(callee);
        }
    }

    fn add_call_edge_path(&mut self, callee_path: String) {
        if let Some(name) = self.current_function.as_ref() {
            self.infos
                .entry(name.clone())
                .or_default()
                .calls_by_path
                .insert(callee_path);
        }
    }

    fn compute_query_index(&self) -> QueryFunctionIndex {
        compute_query_index_from_infos(&self.infos)
    }

    fn merge_into(&self, out: &mut HashMap<String, FunctionCallInfo>) {
        for (name, info) in &self.infos {
            let entry = out.entry(name.clone()).or_default();
            entry.direct_query_exec |= info.direct_query_exec;
            entry
                .calls_by_name
                .extend(info.calls_by_name.iter().cloned());
            entry
                .calls_by_path
                .extend(info.calls_by_path.iter().cloned());
        }
    }
}

impl<'ast> Visit<'ast> for FunctionQueryCollector {
    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        if let Some((_, items)) = &node.content {
            self.module_stack.push(node.ident.to_string());
            for item in items {
                self.visit_item(item);
            }
            self.module_stack.pop();
        }
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        let prev_fn = self.current_function.take();

        self.current_function = Some(self.function_key(&node.sig.ident.to_string()));
        self.ensure_current_entry();

        syn::visit::visit_item_fn(self, node);

        self.current_function = prev_fn;
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        let prev_fn = self.current_function.take();

        self.current_function = Some(self.function_key(&node.sig.ident.to_string()));
        self.ensure_current_entry();

        syn::visit::visit_impl_item_fn(self, node);

        self.current_function = prev_fn;
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        let method_name = node.method.to_string();
        if is_query_exec_method(&method_name) {
            self.mark_direct_query_exec();
        } else if matches!(&*node.receiver, syn::Expr::Path(p) if p.path.is_ident("self")) {
            // Method-to-method edge inside impl blocks, e.g. self.load_user(...)
            self.add_call_edge(method_name);
            self.add_call_edge_path(self.function_key(&node.method.to_string()));
        }
        syn::visit::visit_expr_method_call(self, node);
    }

    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        if let syn::Expr::Path(path_expr) = &*node.func
            && let Some(seg) = path_expr.path.segments.last()
        {
            // Free/qualified function call edge, e.g. helpers::load_user(...)
            self.add_call_edge(seg.ident.to_string());
            if let Some(path) =
                resolve_called_path(&path_expr.path, &self.current_module_segments())
            {
                self.add_call_edge_path(path);
            }
        }
        syn::visit::visit_expr_call(self, node);
    }
}

// =============================================================================
// Pass 2: N+1 Detection via Loop-Depth Tracking
// =============================================================================

struct NPlusOneDetector {
    file: String,
    module_prefix: Vec<String>,
    module_stack: Vec<String>,
    suppressions: Suppressions,
    query_function_names: HashSet<String>,
    query_function_paths: HashSet<String>,
    diagnostics: Vec<NPlusOneDiagnostic>,
    /// Current nesting depth of loops (for/while/loop).
    loop_depth: usize,
    /// Identifiers bound by enclosing for-loop patterns.
    loop_variables: Vec<HashSet<String>>,
    /// Local bindings derived from loop variables in the current lexical scopes.
    tainted_bindings: Vec<HashSet<String>>,
}

impl NPlusOneDetector {
    fn new(
        file: String,
        module_prefix: Vec<String>,
        suppressions: Suppressions,
        query_function_names: HashSet<String>,
        query_function_paths: HashSet<String>,
    ) -> Self {
        Self {
            file,
            module_prefix,
            module_stack: Vec::new(),
            suppressions,
            query_function_names,
            query_function_paths,
            diagnostics: Vec::new(),
            loop_depth: 0,
            loop_variables: Vec::new(),
            tainted_bindings: vec![HashSet::new()],
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
            .flat_map(|s| s.iter())
            .cloned()
            .collect()
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
        if self.loop_depth == 0 {
            return;
        }

        // N1-004: nested loop (depth >= 2) → error
        if self.loop_depth >= 2 {
            self.emit(
                span,
                method_len,
                NPlusOneCode::N1004,
                format!(
                    "Query `{}` inside nested loop (depth {}) — O(n²) or worse",
                    method_name, self.loop_depth
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

fn collect_pat_idents_recursive(pat: &syn::Pat, idents: &mut HashSet<String>) {
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

    fn visit_block(&mut self, node: &'ast syn::Block) {
        self.tainted_bindings.push(HashSet::new());
        syn::visit::visit_block(self, node);
        self.tainted_bindings.pop();
    }

    fn visit_local(&mut self, node: &'ast syn::Local) {
        // Always visit init/body first so regular diagnostics still run.
        syn::visit::visit_local(self, node);

        if self.loop_depth == 0 {
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
        let mut pat_idents = HashSet::new();
        collect_pat_idents_recursive(&node.pat, &mut pat_idents);
        self.loop_variables.push(pat_idents);

        syn::visit::visit_expr_for_loop(self, node);

        self.loop_variables.pop();
        self.loop_depth -= 1;
    }

    fn visit_expr_while(&mut self, node: &'ast syn::ExprWhile) {
        self.loop_depth += 1;
        let mut pat_idents = HashSet::new();
        if let syn::Expr::Let(expr_let) = &*node.cond {
            collect_pat_idents_recursive(&expr_let.pat, &mut pat_idents);
        }
        self.loop_variables.push(pat_idents);

        syn::visit::visit_expr_while(self, node);

        self.loop_variables.pop();
        self.loop_depth -= 1;
    }

    fn visit_expr_loop(&mut self, node: &'ast syn::ExprLoop) {
        self.loop_depth += 1;
        self.loop_variables.push(HashSet::new());

        syn::visit::visit_expr_loop(self, node);

        self.loop_variables.pop();
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

            for arg in &node.args {
                if let syn::Expr::Closure(closure) = arg {
                    self.loop_depth += 1;
                    let mut pat_idents = HashSet::new();
                    for input in &closure.inputs {
                        collect_pat_idents_recursive(input, &mut pat_idents);
                    }
                    self.loop_variables.push(pat_idents);
                    syn::visit::visit_expr(self, &closure.body);
                    self.loop_variables.pop();
                    self.loop_depth -= 1;
                } else {
                    syn::visit::visit_expr(self, arg);
                }
            }
            return;
        }

        if self.loop_depth > 0 && is_query_exec_method(&method_name) {
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
        if self.loop_depth > 0
            && !is_query_exec_method(&method_name)
            && (self.query_function_names.contains(&method_name)
                || self.query_function_paths.contains(&format!(
                    "{}::{}",
                    current_module_path_from_segments(&self.current_module_segments()),
                    method_name
                )))
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
        if self.loop_depth > 0
            && let syn::Expr::Path(ref path_expr) = *node.func
            && let Some(seg) = path_expr.path.segments.last()
        {
            let fn_name = seg.ident.to_string();
            let resolved_path =
                resolve_called_path(&path_expr.path, &self.current_module_segments());
            let is_query_fn = self.query_function_names.contains(&fn_name)
                || resolved_path
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
struct ExprVarFinder<'a> {
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

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn n1001_query_in_single_loop() {
        let source = r#"
fn process(items: Vec<Item>, conn: &mut Conn) {
    for item in &items {
        let rows = conn.fetch_all(&cmd).await.unwrap();
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(!diags.is_empty(), "Expected N1-001 diagnostic");
        assert_eq!(diags[0].code, NPlusOneCode::N1001);
        assert_eq!(diags[0].severity, NPlusOneSeverity::Warning);
    }

    #[test]
    fn n1004_query_in_nested_loop() {
        let source = r#"
fn process(groups: Vec<Vec<Item>>, conn: &mut Conn) {
    for group in &groups {
        for item in group {
            conn.execute(&cmd).await.unwrap();
        }
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(!diags.is_empty(), "Expected N1-004 diagnostic");
        assert_eq!(diags[0].code, NPlusOneCode::N1004);
        assert_eq!(diags[0].severity, NPlusOneSeverity::Error);
    }

    #[test]
    fn n1002_inline_chain_catches_loop_var() {
        // Loop var directly in the execution expression chain → N1-002
        let source = r#"
fn process(ids: Vec<Uuid>, conn: &mut Conn) {
    for id in &ids {
        conn.fetch_all(&Qail::get("users").eq("id", id)).await.unwrap();
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1002 = diags.iter().find(|d| d.code == NPlusOneCode::N1002);
        assert!(
            n1002.is_some(),
            "Inline chain with loop var should trigger N1-002, got: {:?}",
            diags.iter().map(|d| d.code).collect::<Vec<_>>()
        );
    }

    #[test]
    fn n1002_two_statement_binding_is_captured() {
        // Loop var in separate `let` binding should still be N1-002 because
        // the executed binding is derived from loop variable data.
        let source = r#"
fn process(ids: Vec<Uuid>, conn: &mut Conn) {
    for id in &ids {
        let cmd = Qail::get("users").eq("id", id);
        conn.fetch_all(&cmd).await.unwrap();
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(!diags.is_empty());
        assert_eq!(
            diags[0].code,
            NPlusOneCode::N1002,
            "Two-statement loop-derived binding should be N1-002"
        );
    }

    #[test]
    fn n1002_transitive_binding_chain_is_captured() {
        let source = r#"
fn process(ids: Vec<Uuid>, conn: &mut Conn) {
    for id in &ids {
        let uid = *id;
        let cmd = Qail::get("users").eq("id", uid);
        conn.fetch_all(&cmd).await.unwrap();
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1002 = diags.iter().find(|d| d.code == NPlusOneCode::N1002);
        assert!(
            n1002.is_some(),
            "Expected N1-002 for transitive loop-derived binding chain"
        );
    }

    #[test]
    fn unrelated_loop_var_does_not_upgrade() {
        // Loop var used for logging only, query is static → N1-001, not N1-002
        let source = r#"
fn process(items: Vec<Item>, conn: &mut Conn) {
    for item in &items {
        println!("{}", item.name);
        conn.fetch_all(&static_query).await.unwrap();
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(!diags.is_empty());
        assert_eq!(
            diags[0].code,
            NPlusOneCode::N1001,
            "Unrelated loop var should NOT upgrade to N1-002"
        );
    }

    #[test]
    fn n1003_function_with_query_called_in_loop() {
        let source = r#"
async fn load_user(conn: &mut Conn, id: Uuid) -> User {
    let cmd = Qail::get("users").eq("id", id);
    conn.fetch_one(&cmd).await.unwrap()
}

async fn process(ids: Vec<Uuid>, conn: &mut Conn) {
    for id in &ids {
        let user = load_user(conn, *id).await;
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1003 = diags.iter().find(|d| d.code == NPlusOneCode::N1003);
        assert!(
            n1003.is_some(),
            "Expected N1-003 for indirect query call, got: {:?}",
            diags
        );
    }

    #[test]
    fn suppression_disables_diagnostic() {
        let source = r#"
fn process(items: Vec<Item>, conn: &mut Conn) {
    for item in &items {
        // qail-lint:disable-next-line N1-001
        let rows = conn.fetch_all(&cmd).await.unwrap();
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1001 = diags
            .iter()
            .filter(|d| d.code == NPlusOneCode::N1001)
            .count();
        assert_eq!(n1001, 0, "Suppressed N1-001 should not appear");
    }

    #[test]
    fn no_diagnostic_outside_loop() {
        let source = r#"
fn process(conn: &mut Conn) {
    let rows = conn.fetch_all(&cmd).await.unwrap();
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(
            diags.is_empty(),
            "No diagnostic expected outside loop, got: {:?}",
            diags
        );
    }

    #[test]
    fn while_loop_detected() {
        let source = r#"
fn process(conn: &mut Conn) {
    while has_more() {
        conn.fetch_all(&cmd).await.unwrap();
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(!diags.is_empty(), "Expected diagnostic in while loop");
        assert_eq!(diags[0].code, NPlusOneCode::N1001);
    }

    #[test]
    fn while_let_tracks_loop_var_for_n1002() {
        let source = r#"
fn process(mut it: Iter<Item>, conn: &mut Conn) {
    while let Some(item) = it.next() {
        conn.fetch_all(&Qail::get("users").eq("id", item.id)).await.unwrap();
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1002 = diags.iter().find(|d| d.code == NPlusOneCode::N1002);
        assert!(
            n1002.is_some(),
            "Expected N1-002 for while-let loop var usage"
        );
    }

    #[test]
    fn loop_keyword_detected() {
        let source = r#"
fn process(conn: &mut Conn) {
    loop {
        conn.execute(&cmd).await.unwrap();
        break;
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(!diags.is_empty(), "Expected diagnostic in loop keyword");
        assert_eq!(diags[0].code, NPlusOneCode::N1001);
    }

    #[test]
    fn iterator_for_each_detected_as_loop() {
        let source = r#"
fn process(items: Vec<Item>, conn: &mut Conn) {
    items.iter().for_each(|item| {
        let _ = conn.fetch_all(&static_query);
    });
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(
            !diags.is_empty(),
            "Expected diagnostic in iterator for_each"
        );
        assert_eq!(diags[0].code, NPlusOneCode::N1001);
    }

    #[test]
    fn iterator_for_each_loop_var_upgrades_to_n1002() {
        let source = r#"
fn process(ids: Vec<Uuid>, conn: &mut Conn) {
    ids.iter().for_each(|id| {
        let _ = conn.fetch_all(&Qail::get("users").eq("id", id));
    });
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1002 = diags.iter().find(|d| d.code == NPlusOneCode::N1002);
        assert!(n1002.is_some(), "Expected N1-002 in iterator for_each");
    }

    #[test]
    fn nested_for_and_for_each_becomes_n1004() {
        let source = r#"
fn process(groups: Vec<Vec<Item>>, conn: &mut Conn) {
    for group in groups {
        group.iter().for_each(|item| {
            let _ = conn.execute(&cmd);
        });
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1004 = diags.iter().find(|d| d.code == NPlusOneCode::N1004);
        assert!(n1004.is_some(), "Expected N1-004 for nested loop pattern");
    }

    #[test]
    fn n1003_self_method_call_in_loop() {
        let source = r#"
impl Repo {
    async fn load_user(&self, conn: &mut Conn, id: Uuid) -> User {
        conn.fetch_one(&cmd).await.unwrap()
    }

    async fn process(&self, conn: &mut Conn, ids: Vec<Uuid>) {
        for id in ids {
            let _u = self.load_user(conn, id).await;
        }
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1003 = diags.iter().find(|d| d.code == NPlusOneCode::N1003);
        assert!(
            n1003.is_some(),
            "Expected N1-003 for self method call in loop"
        );
    }

    #[test]
    fn n1003_qualified_function_call_in_loop() {
        let source = r#"
mod helpers {
    pub async fn load_user(conn: &mut Conn, id: Uuid) -> User {
        conn.fetch_one(&cmd).await.unwrap()
    }
}

async fn process(conn: &mut Conn, ids: Vec<Uuid>) {
    for id in ids {
        let _u = helpers::load_user(conn, id).await;
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1003 = diags.iter().find(|d| d.code == NPlusOneCode::N1003);
        assert!(
            n1003.is_some(),
            "Expected N1-003 for qualified function call in loop"
        );
    }

    #[test]
    fn inline_suppression_works() {
        let source = r#"
fn process(conn: &mut Conn) {
    for item in &items {
        conn.fetch_all(&cmd).await.unwrap(); // qail-lint:disable-line N1-001
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1001 = diags
            .iter()
            .filter(|d| d.code == NPlusOneCode::N1001)
            .count();
        assert_eq!(n1001, 0, "Inline suppression should work");
    }

    // --- New tests for quality gaps ---

    #[test]
    fn qail_constructor_alone_not_flagged() {
        // Qail::get() is a builder — no DB round-trip without fetch_*/execute
        let source = r#"
fn process(items: Vec<Item>) {
    for item in &items {
        let cmd = Qail::get("users").eq("id", item.id);
        commands.push(cmd);
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(
            diags.is_empty(),
            "Qail::get without execution should NOT be flagged, got: {:?}",
            diags
        );
    }

    #[test]
    fn sqlx_query_builder_not_flagged() {
        // sqlx::query() is a builder, not execution
        let source = r#"
fn process(items: Vec<Item>) {
    for item in &items {
        let q = sqlx::query("SELECT 1");
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(
            diags.is_empty(),
            "sqlx::query() builder should NOT be flagged, got: {:?}",
            diags
        );
    }

    #[test]
    fn end_column_matches_method_name() {
        let source = r#"
fn process(conn: &mut Conn) {
    for item in &items {
        conn.fetch_all(&cmd).await.unwrap();
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(!diags.is_empty());
        let d = &diags[0];
        assert_eq!(
            d.end_column - d.column,
            "fetch_all".len(),
            "end_column should be column + method name length"
        );
    }

    #[test]
    fn n1003_transitive_wrapper_call_in_loop() {
        let source = r#"
async fn load_user_leaf(conn: &mut Conn, id: Uuid) -> User {
    conn.fetch_one(&Qail::get("users").eq("id", id)).await.unwrap()
}

async fn load_user_wrapper(conn: &mut Conn, id: Uuid) -> User {
    load_user_leaf(conn, id).await
}

async fn process(ids: Vec<Uuid>, conn: &mut Conn) {
    for id in &ids {
        let _u = load_user_wrapper(conn, *id).await;
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1003 = diags.iter().find(|d| d.code == NPlusOneCode::N1003);
        assert!(
            n1003.is_some(),
            "Expected N1-003 for transitive wrapper call, got: {:?}",
            diags
        );
    }

    #[test]
    fn iterator_map_collect_detected_as_loop() {
        let source = r#"
fn process(ids: Vec<Uuid>, conn: &mut Conn) {
    let _futs = ids
        .iter()
        .map(|id| conn.fetch_all(&Qail::get("users").eq("id", id)))
        .collect::<Vec<_>>();
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1002 = diags.iter().find(|d| d.code == NPlusOneCode::N1002);
        assert!(n1002.is_some(), "Expected N1-002 for iterator map closure");
    }

    #[test]
    fn option_map_not_treated_as_loop() {
        let source = r#"
fn process(opt_id: Option<Uuid>, conn: &mut Conn) {
    let _ = opt_id.map(|id| conn.fetch_all(&Qail::get("users").eq("id", id)));
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(
            diags.is_empty(),
            "Option::map is not iterator loop semantics, got: {:?}",
            diags
        );
    }

    #[test]
    fn cross_file_n1003_propagates_via_module_index() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "qail_nplus1_cross_file_{}_{}",
            std::process::id(),
            unique
        ));
        std::fs::create_dir_all(&root).unwrap();

        let helpers = r#"
pub async fn load_user(conn: &mut Conn, id: Uuid) -> User {
    conn.fetch_one(&Qail::get("users").eq("id", id)).await.unwrap()
}
"#;
        let main = r#"
mod helpers;

async fn process(ids: Vec<Uuid>, conn: &mut Conn) {
    for id in &ids {
        let _ = helpers::load_user(conn, *id).await;
    }
}
"#;
        std::fs::write(root.join("helpers.rs"), helpers).unwrap();
        std::fs::write(root.join("main.rs"), main).unwrap();

        let diags = detect_n_plus_one_in_dir(&root);
        let _ = std::fs::remove_dir_all(&root);

        let has_n1003 = diags.iter().any(|d| d.code == NPlusOneCode::N1003);
        assert!(
            has_n1003,
            "Expected cross-file N1-003 via module index, got: {:?}",
            diags
        );
    }
}
