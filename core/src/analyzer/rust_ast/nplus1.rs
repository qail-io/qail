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
//! - N1-003 only detects functions defined in the same file. Cross-module calls
//!   require whole-program analysis which is out of scope for a single-file linter.
//! - Qail constructor calls (`Qail::get(...)`) alone are NOT flagged — only actual
//!   execution methods (`conn.fetch_all`, `conn.execute`, etc.) are considered
//!   database round-trips.

use std::collections::HashSet;
use std::path::Path;
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
    let mut fn_collector = FunctionQueryCollector::new();
    fn_collector.visit_file(&syntax);

    // Pass 2: detect N+1 patterns
    let mut detector =
        NPlusOneDetector::new(file.to_string(), suppressions, fn_collector.query_functions);
    detector.visit_file(&syntax);

    detector.diagnostics
}

/// Detect N+1 patterns in all Rust files under a directory.
pub fn detect_n_plus_one_in_dir(dir: &Path) -> Vec<NPlusOneDiagnostic> {
    let mut diagnostics = Vec::new();
    scan_dir_recursive(dir, &mut diagnostics);
    diagnostics
}

fn scan_dir_recursive(dir: &Path, diagnostics: &mut Vec<NPlusOneDiagnostic>) {
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
            scan_dir_recursive(&path, diagnostics);
        } else if path.extension().is_some_and(|e| e == "rs")
            && let Ok(source) = std::fs::read_to_string(&path)
        {
            let file_path = path.display().to_string();
            diagnostics.extend(detect_n_plus_one_in_file(&file_path, &source));
        }
    }
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
];

/// Check if a method name is a query execution call.
fn is_query_exec_method(name: &str) -> bool {
    QUERY_EXEC_METHODS.contains(&name)
}

// =============================================================================
// Pass 1: Collect Functions That Execute Queries
// =============================================================================

/// Collects function names that contain actual query execution calls.
/// Only tracks `.fetch_*()` / `.execute*()` method calls — NOT Qail constructors,
/// because constructors alone don't hit the database.
struct FunctionQueryCollector {
    query_functions: HashSet<String>,
    current_function: Option<String>,
    current_has_query: bool,
}

impl FunctionQueryCollector {
    fn new() -> Self {
        Self {
            query_functions: HashSet::new(),
            current_function: None,
            current_has_query: false,
        }
    }
}

impl<'ast> Visit<'ast> for FunctionQueryCollector {
    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        let prev_fn = self.current_function.take();
        let prev_has = self.current_has_query;

        self.current_function = Some(node.sig.ident.to_string());
        self.current_has_query = false;

        syn::visit::visit_item_fn(self, node);

        if self.current_has_query
            && let Some(ref name) = self.current_function
        {
            self.query_functions.insert(name.clone());
        }

        self.current_function = prev_fn;
        self.current_has_query = prev_has;
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        let prev_fn = self.current_function.take();
        let prev_has = self.current_has_query;

        self.current_function = Some(node.sig.ident.to_string());
        self.current_has_query = false;

        syn::visit::visit_impl_item_fn(self, node);

        if self.current_has_query
            && let Some(ref name) = self.current_function
        {
            self.query_functions.insert(name.clone());
        }

        self.current_function = prev_fn;
        self.current_has_query = prev_has;
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        let method_name = node.method.to_string();
        if is_query_exec_method(&method_name) {
            self.current_has_query = true;
        }
        syn::visit::visit_expr_method_call(self, node);
    }

    // NOTE: No visit_expr_call for Qail::get — constructors are builders, not execution.
}

// =============================================================================
// Pass 2: N+1 Detection via Loop-Depth Tracking
// =============================================================================

struct NPlusOneDetector {
    file: String,
    suppressions: Suppressions,
    query_functions: HashSet<String>,
    diagnostics: Vec<NPlusOneDiagnostic>,
    /// Current nesting depth of loops (for/while/loop).
    loop_depth: usize,
    /// Identifiers bound by enclosing for-loop patterns.
    loop_variables: Vec<HashSet<String>>,
}

impl NPlusOneDetector {
    fn new(file: String, suppressions: Suppressions, query_functions: HashSet<String>) -> Self {
        Self {
            file,
            suppressions,
            query_functions,
            diagnostics: Vec::new(),
            loop_depth: 0,
            loop_variables: Vec::new(),
        }
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
        self.loop_variables.push(HashSet::new());

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
        syn::visit::visit_expr_method_call(self, node);
    }

    // Detect N1-003: bare function calls (not qualified paths like module::func)
    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        if self.loop_depth > 0
            && let syn::Expr::Path(ref path_expr) = *node.func
        {
            // Only match single-segment paths (bare calls like `load_user()`).
            // Qualified paths like `module::func()` or `Type::method()` are
            // unlikely to be the same file-local function from Pass 1.
            if path_expr.path.segments.len() == 1 {
                let fn_name = path_expr.path.segments[0].ident.to_string();
                if self.query_functions.contains(&fn_name) {
                    let span = path_expr.path.segments[0].ident.span();
                    self.emit(
                        span,
                        fn_name.len(),
                        NPlusOneCode::N1003,
                        format!(
                            "Function `{}` (which executes queries) called inside loop — indirect N+1",
                            fn_name
                        ),
                        Some("Move the call outside the loop or refactor to accept batched inputs".into()),
                    );
                }
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
    fn n1002_two_statement_falls_to_n1001() {
        // Loop var in separate `let` binding → N1-001 (not N1-002)
        // because fetch_all(&cmd) doesn't directly contain the loop var
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
            NPlusOneCode::N1001,
            "Two-statement pattern should be N1-001, not N1-002"
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
}
