//! Semantic source scanner for QAIL usage patterns.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

/// Extracted QAIL usage from source code
#[derive(Debug)]
pub struct QailUsage {
    /// Source file path.
    pub file: String,
    /// Line number (1-indexed).
    pub line: usize,
    /// Column number (1-indexed) where `Qail::...` constructor starts.
    pub column: usize,
    /// Table name referenced.
    pub table: String,
    /// True when table name came from a dynamic expression instead of a
    /// compile-time string literal.
    pub is_dynamic_table: bool,
    /// Column names referenced.
    pub columns: Vec<String>,
    /// CRUD action (GET, SET, ADD, DEL, PUT).
    pub action: String,
    /// Additional static tables referenced by the chain.
    ///
    /// This is used for secondary relation slots such as `MERGE USING table`.
    pub related_tables: Vec<String>,
    /// Whether this references a CTE rather than a real table.
    pub is_cte_ref: bool,
    /// Whether this query chain includes `.with_rls(` call
    pub has_rls: bool,
    /// Whether this query chain has explicit tenant scope condition
    /// (e.g. `.eq("tenant_id", ...)` or `.is_null("tenant_id")`).
    pub has_explicit_tenant_scope: bool,
    /// Whether the containing file uses `SuperAdminToken::for_system_process()`.
    /// When true AND the queried table is tenant-scoped, the build emits a
    /// warning: the query may bypass tenant isolation.
    pub file_uses_super_admin: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct LiteralBindings {
    scalars: HashMap<String, Vec<String>>,
    arrays: HashMap<String, Vec<String>>,
    typed_scalars: HashMap<String, Vec<String>>,
    typed_arrays: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Default)]
struct LiteralBindingIndex {
    globals: LiteralBindings,
    locals: Vec<ScopedLiteralBindings>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ScopedLiteralBindings {
    start: usize,
    end: usize,
    bindings: LiteralBindings,
}

#[derive(Debug, Clone)]
struct CteAlias {
    name: String,
    start: usize,
    end: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BindingStatementKind {
    Let,
    Const,
}

#[derive(Debug, Clone, Copy)]
struct BindingStatement<'a> {
    start: usize,
    text: &'a str,
    kind: BindingStatementKind,
}

#[derive(Debug, Clone)]
struct LocalFunction {
    name: String,
    params: Vec<String>,
    body_start: usize,
    body_end: usize,
}

#[derive(Debug, Clone)]
struct LocalFunctionCall {
    name: String,
    args: Vec<String>,
    arg_spans: Vec<(usize, usize)>,
    open_paren: usize,
}

#[derive(Debug, Clone, Default)]
struct ParamSubstitutions {
    values: HashMap<String, String>,
    bindings: LiteralBindings,
}

/// Scan Rust source files for QAIL usage patterns
pub fn scan_source_files(src_dir: &str) -> Vec<QailUsage> {
    let mut usages = Vec::new();
    scan_directory(Path::new(src_dir), &mut usages);
    usages
}

/// Scan a single Rust source text buffer for QAIL usage patterns.
///
/// This is the in-memory counterpart of [`scan_source_files`], used by tools
/// like LSP servers that work on unsaved editor buffers.
///
/// Unlike build-time scanning, this API never emits cargo warnings.
pub fn scan_source_text(file: &str, content: &str) -> Vec<QailUsage> {
    let mut usages = Vec::new();
    scan_file_inner(file, content, &mut usages, false);
    usages
}

fn scan_directory(dir: &Path, usages: &mut Vec<QailUsage>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                scan_directory(&path, usages);
            } else if path.extension().is_some_and(|e| e == "rs")
                && let Ok(content) = fs::read_to_string(&path)
            {
                scan_file(&path.display().to_string(), &content, usages);
            }
        }
    }
}

fn collect_literal_binding_index(
    content: &str,
    local_functions: &[LocalFunction],
) -> LiteralBindingIndex {
    let mut index = LiteralBindingIndex::default();
    let statements = collect_binding_statements(content);

    for _ in 0..statements.len().max(1) {
        let before = index.globals.clone();
        for stmt in &statements {
            if !matches!(stmt.kind, BindingStatementKind::Const)
                || find_enclosing_local_function(stmt.start, local_functions).is_some()
            {
                continue;
            }
            let bindings = collect_const_statement_bindings(stmt.text, &index.globals);
            if !literal_bindings_is_empty(&bindings) {
                merge_literal_bindings(&mut index.globals, &bindings);
            }
        }
        dedupe_literal_bindings(&mut index.globals);
        if index.globals == before {
            break;
        }
    }

    index.locals.extend(collect_local_const_bindings(
        content,
        local_functions,
        &statements,
        &index.globals,
    ));

    for stmt in &statements {
        let enclosing_function = find_enclosing_local_function(stmt.start, local_functions);
        let visible_bindings = literal_bindings_for_offset(&index, stmt.start, enclosing_function);
        match stmt.kind {
            BindingStatementKind::Const => {
                continue;
            }
            BindingStatementKind::Let => {
                let bindings = collect_let_statement_bindings(stmt.text, &visible_bindings);
                if !literal_bindings_is_empty(&bindings) {
                    index.locals.push(ScopedLiteralBindings {
                        start: stmt.start,
                        end: find_innermost_block_end(content, stmt.start).unwrap_or(content.len()),
                        bindings,
                    });
                }
            }
        }
    }

    dedupe_literal_bindings(&mut index.globals);
    index
}

fn collect_local_const_bindings(
    content: &str,
    local_functions: &[LocalFunction],
    statements: &[BindingStatement<'_>],
    globals: &LiteralBindings,
) -> Vec<ScopedLiteralBindings> {
    let const_statements = statements
        .iter()
        .filter_map(|stmt| {
            if !matches!(stmt.kind, BindingStatementKind::Const)
                || find_enclosing_local_function(stmt.start, local_functions).is_none()
            {
                return None;
            }
            find_innermost_block_span(content, stmt.start).map(|(start, end)| (start, end, stmt))
        })
        .collect::<Vec<_>>();
    let mut scopes = Vec::new();
    for _ in 0..const_statements.len().max(1) {
        let before = scopes.clone();
        for (start, end, stmt) in &const_statements {
            let mut visible = globals.clone();
            let mut visible_scopes = scopes
                .iter()
                .filter(|scope: &&ScopedLiteralBindings| {
                    scope.start <= stmt.start && stmt.start < scope.end
                })
                .collect::<Vec<_>>();
            visible_scopes.sort_by(|a, b| a.start.cmp(&b.start).then_with(|| b.end.cmp(&a.end)));
            for scope in visible_scopes {
                merge_shadowing_literal_bindings(&mut visible, &scope.bindings);
            }

            let bindings = collect_const_statement_bindings(stmt.text, &visible);
            if literal_bindings_is_empty(&bindings) {
                continue;
            }
            if let Some(existing) = scopes
                .iter_mut()
                .find(|scope| scope.start == *start && scope.end == *end)
            {
                merge_literal_bindings(&mut existing.bindings, &bindings);
                dedupe_literal_bindings(&mut existing.bindings);
            } else {
                scopes.push(ScopedLiteralBindings {
                    start: *start,
                    end: *end,
                    bindings,
                });
            }
        }

        sort_scoped_literal_bindings(&mut scopes);
        if scopes == before {
            break;
        }
    }

    scopes
}

fn sort_scoped_literal_bindings(bindings: &mut [ScopedLiteralBindings]) {
    bindings.sort_by(|a, b| a.start.cmp(&b.start).then_with(|| b.end.cmp(&a.end)));
}

fn literal_bindings_is_empty(bindings: &LiteralBindings) -> bool {
    bindings.scalars.is_empty()
        && bindings.arrays.is_empty()
        && bindings.typed_scalars.is_empty()
        && bindings.typed_arrays.is_empty()
}

fn literal_bindings_for_offset(
    index: &LiteralBindingIndex,
    offset: usize,
    enclosing_function: Option<&LocalFunction>,
) -> LiteralBindings {
    let mut bindings = index.globals.clone();
    for local in &index.locals {
        if local.start >= offset || offset >= local.end {
            continue;
        }
        let visible = match enclosing_function {
            Some(function) => local.start >= function.body_start && local.start < function.body_end,
            None => true,
        };
        if visible {
            merge_shadowing_literal_bindings(&mut bindings, &local.bindings);
        }
    }
    dedupe_binding_values(&mut bindings.scalars);
    dedupe_binding_values(&mut bindings.arrays);
    bindings
}

fn merge_shadowing_literal_bindings(target: &mut LiteralBindings, source: &LiteralBindings) {
    let shadowed_names = source
        .scalars
        .keys()
        .chain(source.arrays.keys())
        .chain(source.typed_scalars.keys())
        .chain(source.typed_arrays.keys())
        .cloned()
        .collect::<HashSet<_>>();

    for name in shadowed_names {
        target.scalars.remove(&name);
        target.arrays.remove(&name);
        target.typed_scalars.remove(&name);
        target.typed_arrays.remove(&name);
    }

    merge_literal_bindings(target, source);
}

fn merge_literal_bindings(target: &mut LiteralBindings, source: &LiteralBindings) {
    for (name, values) in &source.scalars {
        target
            .scalars
            .entry(name.clone())
            .or_default()
            .extend(values.iter().cloned());
    }
    for (name, values) in &source.arrays {
        target
            .arrays
            .entry(name.clone())
            .or_default()
            .extend(values.iter().cloned());
    }
    for (name, values) in &source.typed_scalars {
        target
            .typed_scalars
            .entry(name.clone())
            .or_default()
            .extend(values.iter().cloned());
    }
    for (name, values) in &source.typed_arrays {
        target
            .typed_arrays
            .entry(name.clone())
            .or_default()
            .extend(values.iter().cloned());
    }
}

fn collect_binding_statements(content: &str) -> Vec<BindingStatement<'_>> {
    let bytes = content.as_bytes();
    let mut statements = Vec::new();
    let mut i = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }
        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        let kind = if starts_with_keyword(content, i, "let")
            && !matches!(
                previous_identifier_before(content, i).as_deref(),
                Some("if" | "while")
            ) {
            Some(BindingStatementKind::Let)
        } else if starts_with_keyword(content, i, "const")
            || starts_with_keyword(content, i, "static")
        {
            Some(BindingStatementKind::Const)
        } else {
            None
        };

        if let Some(kind) = kind {
            let end = find_statement_end(content, i).unwrap_or_else(|| line_end(content, i));
            if let Some(text) = content.get(i..end) {
                statements.push(BindingStatement {
                    start: i,
                    text,
                    kind,
                });
            }
            i = end.max(i + 1);
            continue;
        }

        i += 1;
    }

    statements
}

fn starts_with_keyword(source: &str, idx: usize, keyword: &str) -> bool {
    let bytes = source.as_bytes();
    let kw = keyword.as_bytes();
    if !starts_with_bytes(bytes, idx, kw) {
        return false;
    }
    let before_ok = idx == 0 || !is_ident_byte(bytes[idx - 1]);
    let after = idx + kw.len();
    let after_ok = after >= bytes.len() || !is_ident_byte(bytes[after]);
    before_ok && after_ok
}

fn line_end(source: &str, start: usize) -> usize {
    source
        .get(start..)
        .and_then(|tail| tail.find('\n').map(|idx| start + idx))
        .unwrap_or(source.len())
}

fn collect_let_statement_bindings(
    stmt: &str,
    visible_bindings: &LiteralBindings,
) -> LiteralBindings {
    let mut bindings = LiteralBindings::default();
    let line = stmt.trim();

    if let Some(rest) = line.strip_prefix("let ") {
        let rest = rest.trim();
        if let Some((var, rhs)) = parse_simple_let(rest) {
            let rhs = rhs.trim().trim_end_matches(';').trim();
            let scalar_values = resolve_string_values(rhs, None, visible_bindings);
            if !scalar_values.is_empty() {
                bindings
                    .scalars
                    .entry(var.clone())
                    .or_default()
                    .extend(scalar_values);
            }
            let literals = extract_branch_literals(rhs, visible_bindings);
            if !literals.is_empty() {
                bindings
                    .scalars
                    .entry(var.clone())
                    .or_default()
                    .extend(literals);
            }
            let values = resolve_array_string_values(rhs, None, visible_bindings);
            if !values.is_empty() {
                bindings.arrays.insert(var.clone(), values);
            }
            if let Some(items) = extract_typed_column_collection_items(rhs) {
                bindings.typed_arrays.insert(var.clone(), items);
            } else if direct_typed_column_expr_has_column(rhs) {
                bindings
                    .typed_scalars
                    .entry(var.clone())
                    .or_default()
                    .push(rhs.to_string());
            } else if let Some(key) = binding_lookup_key(rhs) {
                if let Some(items) = visible_bindings.typed_arrays.get(&key) {
                    bindings.typed_arrays.insert(var.clone(), items.clone());
                }
                if let Some(items) = visible_bindings.typed_scalars.get(&key) {
                    bindings
                        .typed_scalars
                        .entry(var.clone())
                        .or_default()
                        .extend(items.iter().cloned());
                }
            }
        }

        if rest.starts_with('(')
            && let Some(result) = parse_destructuring_let(line)
        {
            for (name, values) in result {
                bindings.scalars.entry(name).or_default().extend(values);
            }
        }
    }

    dedupe_binding_values(&mut bindings.scalars);
    dedupe_binding_values(&mut bindings.arrays);
    dedupe_binding_values(&mut bindings.typed_scalars);
    dedupe_binding_values(&mut bindings.typed_arrays);
    bindings
}

fn collect_const_statement_bindings(
    stmt: &str,
    visible_bindings: &LiteralBindings,
) -> LiteralBindings {
    let mut bindings = LiteralBindings::default();
    if let Some((name, rhs)) = parse_const_binding(stmt) {
        let scalar_values = resolve_string_values(rhs, None, visible_bindings);
        if !scalar_values.is_empty() {
            bindings
                .scalars
                .entry(name.clone())
                .or_default()
                .extend(scalar_values);
        }

        let values = resolve_array_string_values(rhs, None, visible_bindings);
        if !values.is_empty() {
            bindings.arrays.insert(name.clone(), values);
        }

        if let Some(items) = extract_typed_column_collection_items(rhs) {
            bindings.typed_arrays.insert(name.clone(), items);
        } else if direct_typed_column_expr_has_column(rhs) {
            bindings
                .typed_scalars
                .entry(name.clone())
                .or_default()
                .push(rhs.to_string());
        } else if let Some(key) = binding_lookup_key(rhs) {
            if let Some(items) = visible_bindings.typed_arrays.get(&key) {
                bindings.typed_arrays.insert(name.clone(), items.clone());
            }
            if let Some(items) = visible_bindings.typed_scalars.get(&key) {
                bindings
                    .typed_scalars
                    .entry(name.clone())
                    .or_default()
                    .extend(items.iter().cloned());
            }
        }
    }
    dedupe_binding_values(&mut bindings.scalars);
    dedupe_binding_values(&mut bindings.arrays);
    dedupe_binding_values(&mut bindings.typed_scalars);
    dedupe_binding_values(&mut bindings.typed_arrays);
    bindings
}

fn parse_const_binding(stmt: &str) -> Option<(String, &str)> {
    let mut rest = stmt.trim();

    for _ in 0..4 {
        let mut advanced = false;
        for prefix in ["pub(crate) ", "pub(super) ", "pub ", "const ", "static "] {
            if let Some(next) = rest.strip_prefix(prefix) {
                rest = next.trim_start();
                advanced = true;
            }
        }
        if !advanced {
            break;
        }
    }

    if let Some(next) = rest.strip_prefix("mut ") {
        rest = next.trim_start();
    }

    let name: String = rest
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    if name.is_empty() {
        return None;
    }

    let rest = rest[name.len()..].trim_start();
    let rest = if rest.starts_with(':') {
        rest.find('=').map(|pos| &rest[pos..])?
    } else {
        rest
    };

    let rhs = rest.strip_prefix('=')?.trim();
    Some((name, rhs.trim_end_matches(';').trim()))
}

fn dedupe_binding_values(bindings: &mut HashMap<String, Vec<String>>) {
    for values in bindings.values_mut() {
        let mut seen = HashSet::new();
        values.retain(|value| seen.insert(value.clone()));
    }
}

fn dedupe_literal_bindings(bindings: &mut LiteralBindings) {
    dedupe_binding_values(&mut bindings.scalars);
    dedupe_binding_values(&mut bindings.arrays);
    dedupe_binding_values(&mut bindings.typed_scalars);
    dedupe_binding_values(&mut bindings.typed_arrays);
}

/// Parse `ident = rest` from a let statement (after stripping `let `).
/// Returns (variable_name, right_hand_side).
fn parse_simple_let(s: &str) -> Option<(String, &str)> {
    // Must start with an ident char, not `(` (that's destructuring) or `mut`
    let s = s.strip_prefix("mut ").unwrap_or(s).trim();
    if s.starts_with('(') {
        return None;
    }

    // Extract identifier
    let ident: String = s
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    if ident.is_empty() {
        return None;
    }

    // Skip optional type annotation  : Type
    let rest = s[ident.len()..].trim_start();
    let rest = if rest.starts_with(':') {
        // Skip past the type, find the `=`
        rest.find('=').map(|pos| &rest[pos..])?
    } else {
        rest
    };

    let rest = rest.strip_prefix('=')?.trim();
    Some((ident, rest))
}

/// Extract string literals from static branch expressions.
/// Handles: `if cond { "a" } else { "b" }` and
/// `match kind { A => "a", _ => "b" }`.
fn extract_branch_literals(expr: &str, visible_bindings: &LiteralBindings) -> Vec<String> {
    let mut literals = Vec::new();

    if expr.trim_start().starts_with("match ") {
        return extract_match_literal_arms(expr, visible_bindings);
    }

    // Find all `{ "literal" }` patterns in the expression
    let mut remaining = expr;
    while let Some(brace_pos) = remaining.find('{') {
        let inside = &remaining[brace_pos + 1..];
        if let Some(close_pos) = inside.find('}') {
            let block = inside[..close_pos].trim();
            literals.extend(extract_branch_scalar_expr(block, visible_bindings));
            remaining = &inside[close_pos + 1..];
        } else {
            break;
        }
    }

    literals
}

fn extract_match_literal_arms(expr: &str, visible_bindings: &LiteralBindings) -> Vec<String> {
    let trimmed = expr.trim_start();
    if !trimmed.starts_with("match ") {
        return Vec::new();
    }

    let Some(open) = find_first_code_byte(trimmed, b'{') else {
        return Vec::new();
    };
    let Some(close) = find_matching_delim(trimmed, open, b'{', b'}') else {
        return Vec::new();
    };
    let Some(body) = trimmed.get(open + 1..close) else {
        return Vec::new();
    };

    let mut out = Vec::new();
    for arm in split_top_level_args(body) {
        let Some(arrow) = find_top_level_match_arrow(arm) else {
            continue;
        };
        let result = arm.get(arrow + 2..).unwrap_or_default().trim();
        out.extend(extract_branch_scalar_expr(result, visible_bindings));
    }
    dedupe_values(&mut out);
    out
}

fn find_first_code_byte(source: &str, needle: u8) -> Option<usize> {
    let bytes = source.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }
        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }
        if bytes[i] == needle {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn find_top_level_match_arrow(source: &str) -> Option<usize> {
    let bytes = source.as_bytes();
    let mut i = 0usize;
    let mut paren = 0usize;
    let mut bracket = 0usize;
    let mut brace = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }
        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        match bytes[i] {
            b'(' => paren += 1,
            b')' => paren = paren.saturating_sub(1),
            b'[' => bracket += 1,
            b']' => bracket = bracket.saturating_sub(1),
            b'{' => brace += 1,
            b'}' => brace = brace.saturating_sub(1),
            b'=' if paren == 0
                && bracket == 0
                && brace == 0
                && bytes.get(i + 1).copied() == Some(b'>') =>
            {
                return Some(i);
            }
            _ => {}
        }
        i += 1;
    }

    None
}

fn extract_branch_scalar_expr(expr: &str, visible_bindings: &LiteralBindings) -> Vec<String> {
    let Some(expr) = unwrap_single_block_expr(expr) else {
        return Vec::new();
    };
    resolve_string_values(expr, None, visible_bindings)
}

fn unwrap_single_block_expr(mut expr: &str) -> Option<&str> {
    expr = expr.trim().trim_end_matches(',').trim();
    while expr.starts_with('{') {
        let close = find_matching_delim(expr, 0, b'{', b'}')?;
        if !expr.get(close + 1..)?.trim().is_empty() {
            break;
        }
        expr = expr.get(1..close)?.trim();
    }
    Some(expr)
}

fn extract_branch_array_literals(expr: &str, bindings: &LiteralBindings) -> Vec<String> {
    let trimmed = expr.trim_start();
    let mut out = if trimmed.starts_with("match ") {
        extract_match_array_arms(trimmed, bindings)
    } else if trimmed.starts_with("if ") {
        extract_if_array_blocks(trimmed, bindings)
    } else {
        Vec::new()
    };
    dedupe_values(&mut out);
    out
}

fn extract_match_array_arms(expr: &str, bindings: &LiteralBindings) -> Vec<String> {
    let Some(open) = find_first_code_byte(expr, b'{') else {
        return Vec::new();
    };
    let Some(close) = find_matching_delim(expr, open, b'{', b'}') else {
        return Vec::new();
    };
    let Some(body) = expr.get(open + 1..close) else {
        return Vec::new();
    };

    let mut out = Vec::new();
    for arm in split_top_level_args(body) {
        let Some(arrow) = find_top_level_match_arrow(arm) else {
            continue;
        };
        let result = arm.get(arrow + 2..).unwrap_or_default().trim();
        out.extend(extract_array_literal_expr(result, bindings));
    }
    out
}

fn extract_if_array_blocks(expr: &str, bindings: &LiteralBindings) -> Vec<String> {
    let mut out = Vec::new();
    let mut cursor = 0usize;

    while cursor < expr.len() {
        let Some(tail) = expr.get(cursor..) else {
            break;
        };
        let Some(open_rel) = find_first_code_byte(tail, b'{') else {
            break;
        };
        let open = cursor + open_rel;
        let Some(close) = find_matching_delim(expr, open, b'{', b'}') else {
            break;
        };
        if let Some(block) = expr.get(open + 1..close) {
            out.extend(extract_array_literal_expr(block, bindings));
        }
        cursor = close + 1;
    }

    out
}

fn extract_array_literal_expr(expr: &str, bindings: &LiteralBindings) -> Vec<String> {
    let mut trimmed = expr.trim();
    while let Some(rest) = trimmed.strip_prefix('&') {
        trimmed = rest.trim_start();
    }
    trimmed = trimmed.trim_end_matches(',').trim();

    if trimmed.starts_with('{') {
        let Some(close) = find_matching_delim(trimmed, 0, b'{', b'}') else {
            return Vec::new();
        };
        if !trimmed
            .get(close + 1..)
            .unwrap_or_default()
            .trim()
            .is_empty()
        {
            return Vec::new();
        }
        return trimmed
            .get(1..close)
            .map(|inner| extract_array_literal_expr(inner, bindings))
            .unwrap_or_default();
    }

    if !trimmed.starts_with('[') {
        return resolve_array_string_values(trimmed, None, bindings);
    }
    let Some(close) = find_matching_delim(trimmed, 0, b'[', b']') else {
        return Vec::new();
    };
    if !trimmed
        .get(close + 1..)
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        return Vec::new();
    }
    trimmed
        .get(1..close)
        .map(collect_string_literals)
        .unwrap_or_default()
}

/// Parse destructuring let: `let (a, b) = ...;`
/// Returns vec of (name, possible_values) for each position.
fn parse_destructuring_let(line: &str) -> Option<Vec<(String, Vec<String>)>> {
    // Find `let (` or `let mut (`
    let rest = line.strip_prefix("let ")?.trim();
    let rest = rest.strip_prefix("mut ").unwrap_or(rest).trim();
    let rest = rest.strip_prefix('(')?;

    // Extract variable names from the tuple pattern
    let close_paren = rest.find(')')?;
    let names_str = &rest[..close_paren];
    let names: Vec<String> = names_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty() && !s.starts_with('_'))
        .collect();

    if names.is_empty() {
        return None;
    }

    // Find the RHS after `=`
    let after_pattern = &rest[close_paren + 1..];
    let eq_pos = after_pattern.find('=')?;
    let rhs = after_pattern[eq_pos + 1..].trim();

    // Case 1: Simple tuple  ("a", "b")
    if rhs.starts_with('(') {
        let values = extract_tuple_literals(rhs);
        if values.len() == names.len() {
            return Some(
                names
                    .into_iter()
                    .zip(values)
                    .map(|(n, v)| (n, vec![v]))
                    .collect(),
            );
        }
    }

    // Case 2: if/else  if cond { ("a", "x") } else { ("b", "y") }
    if rhs.starts_with("if ") {
        let mut all_tuples: Vec<Vec<String>> = Vec::new();

        // Extract tuples from each branch
        let mut remaining = rhs;
        while let Some(brace_pos) = remaining.find('{') {
            let inside = &remaining[brace_pos + 1..];
            if let Some(close_pos) = find_matching_brace(inside) {
                let block = inside[..close_pos].trim();
                // Try to extract a tuple from the block
                if block.starts_with('(') {
                    let values = extract_tuple_literals(block);
                    if values.len() == names.len() {
                        all_tuples.push(values);
                    }
                }
                remaining = &inside[close_pos + 1..];
            } else {
                break;
            }
        }

        if !all_tuples.is_empty() {
            let mut result: Vec<(String, Vec<String>)> =
                names.iter().map(|n| (n.clone(), Vec::new())).collect();

            for tuple in &all_tuples {
                for (i, val) in tuple.iter().enumerate() {
                    if i < result.len() {
                        result[i].1.push(val.clone());
                    }
                }
            }

            return Some(result);
        }
    }

    None
}

/// Extract string literals from a tuple: ("a", "b", "c") → ["a", "b", "c"]
fn extract_tuple_literals(s: &str) -> Vec<String> {
    let mut literals = Vec::new();
    let s = s.trim();
    let s = s.strip_prefix('(').unwrap_or(s);
    // Find the closing paren (handle nested parens)
    let content = if let Some(pos) = s.rfind(')') {
        &s[..pos]
    } else {
        s.trim_end_matches(';').trim_end_matches(')')
    };

    for part in content.split(',') {
        let part = part.trim();
        if let Some(lit) = extract_string_arg(part) {
            literals.push(lit);
        }
    }
    literals
}

/// Find the position of the matching `}` for the first `{`,
/// handling nested braces.
fn find_matching_brace(s: &str) -> Option<usize> {
    let mut depth = 0i32;
    for (i, ch) in s.chars().enumerate() {
        match ch {
            '{' => depth += 1,
            '}' => {
                if depth == 0 {
                    return Some(i);
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    None
}

/// Count net open delimiters in a line: +1 for `(`, `[`, `{`, -1 for `)`, `]`, `}`.
/// Used by the chain scanner to continue joining lines across multi-line arguments.
pub(crate) fn count_net_delimiters(line: &str) -> i32 {
    let mut depth: i32 = 0;
    let mut in_string = false;
    let mut prev = '\0';
    for ch in line.chars() {
        if ch == '"' && prev != '\\' {
            in_string = !in_string;
        } else if !in_string {
            match ch {
                '(' | '[' | '{' => depth += 1,
                ')' | ']' | '}' => depth -= 1,
                _ => {}
            }
        }
        prev = ch;
    }
    depth
}

#[derive(Debug, Clone)]
struct ScannedQailChain {
    start: usize,
    end: usize,
    line: usize,
    column: usize,
    action: &'static str,
    first_arg: String,
    full_chain: String,
    bound_var: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct QailConstructorHit {
    start: usize,
    action: &'static str,
    open_paren: usize,
    close_paren: usize,
    statement_end: usize,
}

#[derive(Debug, Clone, Copy)]
struct MethodCall<'a> {
    name: &'a str,
    args: &'a str,
}

#[derive(Debug, Clone, Copy)]
struct IdentMethodCall<'a> {
    args: &'a str,
    start: usize,
}

fn collect_local_functions(source: &str) -> Vec<LocalFunction> {
    let bytes = source.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }

        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }

        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        if !starts_with_bytes(bytes, i, b"fn")
            || i > 0 && is_ident_byte(bytes[i - 1])
            || bytes.get(i + 2).copied().is_some_and(is_ident_byte)
        {
            i += 1;
            continue;
        }

        let name_start = skip_ws(bytes, i + 2);
        let Some((name, name_end)) = parse_ident_at_bytes(source, name_start) else {
            i += 2;
            continue;
        };
        let Some(open_paren) = parse_fn_params_open(source, name_end) else {
            i += 2;
            continue;
        };
        let Some(close_paren) = find_matching_delim(source, open_paren, b'(', b')') else {
            i += 2;
            continue;
        };
        let Some(body_start) = find_function_body_open(source, close_paren + 1) else {
            i = close_paren + 1;
            continue;
        };
        let Some(body_end) = find_matching_delim(source, body_start, b'{', b'}') else {
            i = body_start + 1;
            continue;
        };

        out.push(LocalFunction {
            name: name.to_string(),
            params: parse_param_names(source.get(open_paren + 1..close_paren).unwrap_or_default()),
            body_start,
            body_end,
        });

        i = close_paren + 1;
    }

    out
}

fn collect_local_function_calls(
    source: &str,
    functions: &[LocalFunction],
) -> Vec<LocalFunctionCall> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    let bytes = source.as_bytes();

    for function in functions {
        let needle = function.name.as_bytes();
        let mut idx = 0usize;
        while idx < bytes.len() {
            if starts_with_bytes(bytes, idx, b"//") {
                idx += 2;
                while idx < bytes.len() && bytes[idx] != b'\n' {
                    idx += 1;
                }
                continue;
            }
            if starts_with_bytes(bytes, idx, b"/*") {
                idx = consume_block_comment(bytes, idx);
                continue;
            }
            if let Some(next) = consume_rust_literal(bytes, idx) {
                idx = next;
                continue;
            }

            if !starts_with_bytes(bytes, idx, needle) {
                idx += 1;
                continue;
            }

            if idx > 0
                && matches!(
                    bytes[idx - 1],
                    b'.' | b':' | b'!' | b'_' | b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z'
                )
            {
                idx += function.name.len();
                continue;
            }
            if bytes
                .get(idx + function.name.len())
                .copied()
                .is_some_and(is_ident_byte)
            {
                idx += function.name.len();
                continue;
            }

            let Some(open_paren) =
                parse_call_open_paren_after_name(source, idx + function.name.len())
            else {
                idx += function.name.len();
                continue;
            };
            if previous_identifier_before(source, idx).as_deref() == Some("fn") {
                idx += function.name.len();
                continue;
            }
            let Some(close_paren) = find_matching_delim(source, open_paren, b'(', b')') else {
                idx = open_paren + 1;
                continue;
            };
            let parsed_args = source
                .get(open_paren + 1..close_paren)
                .map(|args| split_top_level_args_with_spans(args, open_paren + 1))
                .unwrap_or_default();
            let args = parsed_args
                .iter()
                .map(|(arg, _, _)| arg.clone())
                .collect::<Vec<_>>();
            let arg_spans = parsed_args
                .iter()
                .map(|(_, start, end)| (*start, *end))
                .collect::<Vec<_>>();
            let key = format!("{}@{}@{}", function.name, open_paren, close_paren);
            if seen.insert(key) {
                out.push(LocalFunctionCall {
                    name: function.name.clone(),
                    args,
                    arg_spans,
                    open_paren,
                });
            }
            idx = close_paren + 1;
        }
    }

    out
}

fn find_function_body_open(source: &str, start: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let mut i = start;
    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }
        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }
        match bytes[i] {
            b'{' => return Some(i),
            b';' => return None,
            _ => i += 1,
        }
    }
    None
}

fn skip_optional_generics(source: &str, cursor: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let cursor = skip_ws(bytes, cursor);
    if bytes.get(cursor).copied() != Some(b'<') {
        return Some(cursor);
    }
    let end = find_matching_delim(source, cursor, b'<', b'>')?;
    Some(skip_ws(bytes, end + 1))
}

fn parse_fn_params_open(source: &str, name_end: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let cursor = skip_optional_generics(source, name_end)?;
    (bytes.get(cursor).copied() == Some(b'(')).then_some(cursor)
}

fn parse_call_open_paren_after_name(source: &str, name_end: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let mut cursor = skip_ws(bytes, name_end);

    if starts_with_bytes(bytes, cursor, b"::") {
        cursor = skip_ws(bytes, cursor + 2);
        if bytes.get(cursor).copied() != Some(b'<') {
            return None;
        }
        cursor = skip_optional_generics(source, cursor)?;
    }

    (bytes.get(cursor).copied() == Some(b'(')).then_some(cursor)
}

fn parse_param_names(params: &str) -> Vec<String> {
    split_top_level_args(params)
        .into_iter()
        .filter_map(extract_param_name)
        .collect()
}

fn extract_param_name(param: &str) -> Option<String> {
    let lhs = param.split(':').next()?.trim();
    if lhs.is_empty() {
        return None;
    }
    let lhs = lhs.strip_prefix("mut ").unwrap_or(lhs).trim();
    if matches!(lhs, "self" | "&self" | "&mut self" | "mut self") {
        return None;
    }
    extract_last_ident(lhs)
}

fn extract_last_ident(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut end = bytes.len();
    while end > 0 && !is_ident_byte(bytes[end - 1]) {
        end -= 1;
    }
    if end == 0 {
        return None;
    }
    let mut start = end;
    while start > 0 && is_ident_byte(bytes[start - 1]) {
        start -= 1;
    }
    let ident = text.get(start..end)?.trim();
    if ident.is_empty() {
        None
    } else {
        Some(ident.to_string())
    }
}

fn previous_identifier_before(source: &str, start: usize) -> Option<String> {
    let bytes = source.as_bytes();
    let mut end = start;
    while end > 0 && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    if end == 0 {
        return None;
    }

    let mut ident_end = end;
    while ident_end > 0 && !is_ident_byte(bytes[ident_end - 1]) {
        ident_end -= 1;
    }
    if ident_end == 0 {
        return None;
    }

    let mut ident_start = ident_end;
    while ident_start > 0 && is_ident_byte(bytes[ident_start - 1]) {
        ident_start -= 1;
    }
    let ident = source.get(ident_start..ident_end)?.trim();
    if ident.is_empty() {
        None
    } else {
        Some(ident.to_string())
    }
}

fn compute_line_starts(source: &str) -> Vec<usize> {
    let mut starts = Vec::with_capacity(source.lines().count() + 1);
    starts.push(0);
    for (idx, b) in source.bytes().enumerate() {
        if b == b'\n' {
            starts.push(idx + 1);
        }
    }
    starts
}

fn offset_to_line_col(line_starts: &[usize], offset: usize) -> (usize, usize) {
    let idx = line_starts.partition_point(|&start| start <= offset);
    let line_idx = idx.saturating_sub(1);
    let line_start = line_starts.get(line_idx).copied().unwrap_or(0);
    (line_idx + 1, offset.saturating_sub(line_start))
}

fn starts_with_bytes(haystack: &[u8], idx: usize, needle: &[u8]) -> bool {
    haystack
        .get(idx..idx.saturating_add(needle.len()))
        .is_some_and(|s| s == needle)
}

fn skip_ws(bytes: &[u8], mut idx: usize) -> usize {
    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }
    idx
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn parse_ident_at_bytes(text: &str, start: usize) -> Option<(&str, usize)> {
    let bytes = text.as_bytes();
    let mut end = start;
    while end < bytes.len() && is_ident_byte(bytes[end]) {
        end += 1;
    }
    if end == start {
        None
    } else {
        Some((text.get(start..end)?, end))
    }
}

fn consume_block_comment(bytes: &[u8], start: usize) -> usize {
    let mut i = start + 2;
    let mut depth = 1usize;
    while i < bytes.len() && depth > 0 {
        if starts_with_bytes(bytes, i, b"/*") {
            depth += 1;
            i += 2;
        } else if starts_with_bytes(bytes, i, b"*/") {
            depth = depth.saturating_sub(1);
            i += 2;
        } else {
            i += 1;
        }
    }
    i
}

fn raw_string_prefix(bytes: &[u8], idx: usize) -> Option<(usize, usize, usize)> {
    if bytes.get(idx).copied() == Some(b'r') {
        let mut j = idx + 1;
        while bytes.get(j).copied() == Some(b'#') {
            j += 1;
        }
        if bytes.get(j).copied() == Some(b'"') {
            let hashes = j - (idx + 1);
            return Some((idx, j + 1, hashes));
        }
        return None;
    }

    if bytes.get(idx).copied() == Some(b'b') && bytes.get(idx + 1).copied() == Some(b'r') {
        let mut j = idx + 2;
        while bytes.get(j).copied() == Some(b'#') {
            j += 1;
        }
        if bytes.get(j).copied() == Some(b'"') {
            let hashes = j - (idx + 2);
            return Some((idx, j + 1, hashes));
        }
    }

    None
}

fn find_raw_string_end(bytes: &[u8], mut idx: usize, hashes: usize) -> Option<usize> {
    while idx < bytes.len() {
        if bytes[idx] == b'"' {
            let mut ok = true;
            for off in 0..hashes {
                if bytes.get(idx + 1 + off).copied() != Some(b'#') {
                    ok = false;
                    break;
                }
            }
            if ok {
                return Some(idx);
            }
        }
        idx += 1;
    }
    None
}

fn consume_rust_literal(bytes: &[u8], start: usize) -> Option<usize> {
    if let Some((_, content_start, hashes)) = raw_string_prefix(bytes, start) {
        let end_quote = find_raw_string_end(bytes, content_start, hashes)?;
        return Some(end_quote + 1 + hashes);
    }

    if bytes.get(start).copied() == Some(b'"') || starts_with_bytes(bytes, start, b"b\"") {
        let quote_offset = if bytes.get(start).copied() == Some(b'"') {
            start
        } else {
            start + 1
        };
        let mut i = quote_offset + 1;
        while i < bytes.len() {
            if bytes[i] == b'\\' {
                i = (i + 2).min(bytes.len());
                continue;
            }
            if bytes[i] == b'"' {
                return Some(i + 1);
            }
            i += 1;
        }
        return Some(bytes.len());
    }

    if bytes.get(start).copied() == Some(b'\'') {
        let mut i = start + 1;
        while i < bytes.len() {
            if bytes[i] == b'\\' {
                i = (i + 2).min(bytes.len());
                continue;
            }
            if bytes[i] == b'\'' {
                return Some(i + 1);
            }
            i += 1;
        }
        return Some(bytes.len());
    }

    None
}

fn find_matching_delim(source: &str, open_idx: usize, open: u8, close: u8) -> Option<usize> {
    let bytes = source.as_bytes();
    if bytes.get(open_idx).copied() != Some(open) {
        return None;
    }

    let mut depth = 1usize;
    let mut i = open_idx + 1;
    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }
        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        if bytes[i] == open {
            depth += 1;
            i += 1;
            continue;
        }
        if bytes[i] == close {
            depth = depth.saturating_sub(1);
            if depth == 0 {
                return Some(i);
            }
            i += 1;
            continue;
        }
        i += 1;
    }
    None
}

fn find_statement_end(source: &str, start: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let mut i = start;
    let mut paren = 0usize;
    let mut bracket = 0usize;
    let mut brace = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }
        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        if paren == 0 && bracket == 0 && brace == 0 && bytes[i] == b';' {
            return Some(i + 1);
        }

        match bytes[i] {
            b'(' => paren += 1,
            b')' => paren = paren.saturating_sub(1),
            b'[' => bracket += 1,
            b']' => bracket = bracket.saturating_sub(1),
            b'{' => brace += 1,
            b'}' => brace = brace.saturating_sub(1),
            _ => {}
        }
        i += 1;
    }
    None
}

fn find_statement_start(source: &str, end: usize) -> usize {
    let bytes = source.as_bytes();
    let mut i = end.min(bytes.len());
    while i > 0 {
        let prev = i - 1;
        match bytes[prev] {
            b';' | b'{' | b'}' => return i,
            _ => i -= 1,
        }
    }
    0
}

fn find_next_qail_constructor(source: &str, start: usize) -> Option<QailConstructorHit> {
    let bytes = source.as_bytes();
    let mut i = start;
    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }
        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        if !starts_with_bytes(bytes, i, b"Qail::") {
            i += 1;
            continue;
        }
        if i > 0 && is_ident_byte(bytes[i - 1]) {
            i += "Qail::".len();
            continue;
        }

        let name_start = i + "Qail::".len();
        let Some((method, mut cursor)) = parse_ident_at_bytes(source, name_start) else {
            i += "Qail::".len();
            continue;
        };
        let action = match method {
            "get" => "GET",
            "add" => "ADD",
            "set" => "SET",
            "del" => "DEL",
            "put" => "PUT",
            "merge_into" => "MERGE",
            "export" => "EXPORT",
            "truncate" => "TRUNCATE",
            "explain" => "EXPLAIN",
            "explain_analyze" => "EXPLAIN_ANALYZE",
            "lock" => "LOCK",
            "typed" => "TYPED",
            "raw_sql" => "RAW",
            _ => {
                i += "Qail::".len();
                continue;
            }
        };

        cursor = skip_ws(bytes, cursor);
        if bytes.get(cursor).copied() != Some(b'(') {
            i += "Qail::".len();
            continue;
        }

        let Some(close_paren) = find_matching_delim(source, cursor, b'(', b')') else {
            i = cursor + 1;
            continue;
        };
        let statement_end = find_qail_chain_end(source, close_paren);
        return Some(QailConstructorHit {
            start: i,
            action,
            open_paren: cursor,
            close_paren,
            statement_end,
        });
    }
    None
}

fn find_qail_chain_end(source: &str, constructor_close_paren: usize) -> usize {
    let bytes = source.as_bytes();
    let mut cursor = constructor_close_paren + 1;

    loop {
        cursor = skip_ws_and_comments(source, cursor);
        if bytes.get(cursor).copied() == Some(b'?') {
            cursor += 1;
            continue;
        }
        if bytes.get(cursor).copied() != Some(b'.') {
            return cursor;
        }

        let name_start = skip_ws(bytes, cursor + 1);
        let Some((_, mut after_name)) = parse_ident_at_bytes(source, name_start) else {
            return cursor;
        };
        after_name = skip_ws(bytes, after_name);
        if starts_with_bytes(bytes, after_name, b"::") {
            after_name = skip_ws(bytes, after_name + 2);
            if bytes.get(after_name).copied() == Some(b'<') {
                let Some(angle_end) = find_matching_delim(source, after_name, b'<', b'>') else {
                    return cursor;
                };
                after_name = skip_ws(bytes, angle_end + 1);
            }
        }
        if bytes.get(after_name).copied() != Some(b'(') {
            return cursor;
        }
        let Some(close) = find_matching_delim(source, after_name, b'(', b')') else {
            return cursor;
        };
        cursor = close + 1;
    }
}

fn skip_ws_and_comments(source: &str, mut idx: usize) -> usize {
    let bytes = source.as_bytes();
    while idx < bytes.len() {
        if bytes[idx].is_ascii_whitespace() {
            idx += 1;
            continue;
        }
        if starts_with_bytes(bytes, idx, b"//") {
            idx += 2;
            while idx < bytes.len() && bytes[idx] != b'\n' {
                idx += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, idx, b"/*") {
            idx = consume_block_comment(bytes, idx);
            continue;
        }
        break;
    }
    idx
}

fn extract_first_argument(args: &str) -> &str {
    let bytes = args.as_bytes();
    let mut i = 0usize;
    let mut paren = 0usize;
    let mut bracket = 0usize;
    let mut brace = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }
        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        match bytes[i] {
            b'(' => paren += 1,
            b')' => paren = paren.saturating_sub(1),
            b'[' => bracket += 1,
            b']' => bracket = bracket.saturating_sub(1),
            b'{' => brace += 1,
            b'}' => brace = brace.saturating_sub(1),
            b',' if paren == 0 && bracket == 0 && brace == 0 => {
                return args.get(..i).unwrap_or(args).trim();
            }
            _ => {}
        }
        i += 1;
    }
    args.trim()
}

fn split_top_level_args(args: &str) -> Vec<&str> {
    let bytes = args.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    let mut paren = 0usize;
    let mut bracket = 0usize;
    let mut brace = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }
        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        match bytes[i] {
            b'(' => paren += 1,
            b')' => paren = paren.saturating_sub(1),
            b'[' => bracket += 1,
            b']' => bracket = bracket.saturating_sub(1),
            b'{' => brace += 1,
            b'}' => brace = brace.saturating_sub(1),
            b',' if paren == 0 && bracket == 0 && brace == 0 => {
                if let Some(part) = args.get(start..i) {
                    let part = part.trim();
                    if !part.is_empty() {
                        out.push(part);
                    }
                }
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }

    if let Some(part) = args.get(start..) {
        let part = part.trim();
        if !part.is_empty() {
            out.push(part);
        }
    }

    out
}

fn split_top_level_args_with_spans(args: &str, base_offset: usize) -> Vec<(String, usize, usize)> {
    let bytes = args.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    let mut paren = 0usize;
    let mut bracket = 0usize;
    let mut brace = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }
        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        match bytes[i] {
            b'(' => paren += 1,
            b')' => paren = paren.saturating_sub(1),
            b'[' => bracket += 1,
            b']' => bracket = bracket.saturating_sub(1),
            b'{' => brace += 1,
            b'}' => brace = brace.saturating_sub(1),
            b',' if paren == 0 && bracket == 0 && brace == 0 => {
                push_arg_span(args, base_offset, start, i, &mut out);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }

    push_arg_span(args, base_offset, start, args.len(), &mut out);
    out
}

fn push_arg_span(
    args: &str,
    base_offset: usize,
    start: usize,
    end: usize,
    out: &mut Vec<(String, usize, usize)>,
) {
    let Some((trimmed_start, trimmed_end)) = trim_span(args, start, end) else {
        return;
    };
    let Some(arg) = args.get(trimmed_start..trimmed_end) else {
        return;
    };
    out.push((
        arg.to_string(),
        base_offset + trimmed_start,
        base_offset + trimmed_end,
    ));
}

fn trim_span(text: &str, start: usize, end: usize) -> Option<(usize, usize)> {
    if start >= end || end > text.len() {
        return None;
    }
    let bytes = text.as_bytes();
    let mut trimmed_start = start;
    let mut trimmed_end = end;
    while trimmed_start < trimmed_end && bytes[trimmed_start].is_ascii_whitespace() {
        trimmed_start += 1;
    }
    while trimmed_end > trimmed_start && bytes[trimmed_end - 1].is_ascii_whitespace() {
        trimmed_end -= 1;
    }
    if trimmed_start >= trimmed_end {
        None
    } else {
        Some((trimmed_start, trimmed_end))
    }
}

fn parse_string_literal_at(input: &str, start: usize) -> Option<(String, usize)> {
    let bytes = input.as_bytes();
    if start >= bytes.len() {
        return None;
    }

    if let Some((_, content_start, hashes)) = raw_string_prefix(bytes, start) {
        let end_quote = find_raw_string_end(bytes, content_start, hashes)?;
        let lit = input.get(content_start..end_quote)?.to_string();
        return Some((lit, end_quote + 1 + hashes));
    }

    let quote_offset = if bytes.get(start).copied() == Some(b'"') {
        start
    } else if starts_with_bytes(bytes, start, b"b\"") {
        start + 1
    } else {
        return None;
    };

    let mut i = quote_offset + 1;
    while i < bytes.len() {
        if bytes[i] == b'\\' {
            i = (i + 2).min(bytes.len());
            continue;
        }
        if bytes[i] == b'"' {
            let raw = input.get(quote_offset + 1..i)?;
            return Some((unescape_rust_string(raw), i + 1));
        }
        i += 1;
    }

    None
}

fn unescape_rust_string(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('t') => out.push('\t'),
            Some('0') => out.push('\0'),
            Some('\\') => out.push('\\'),
            Some('"') => out.push('"'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}

fn collect_string_literals(input: &str) -> Vec<String> {
    let bytes = input.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }

        if let Some((lit, next)) = parse_string_literal_at(input, i) {
            out.push(lit);
            i = next;
            continue;
        }

        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        i += 1;
    }

    out
}

fn extract_array_string_literals_from_expr(expr: &str) -> Vec<String> {
    let bytes = expr.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }
        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }
        if bytes[i] == b'['
            && let Some(end) = find_matching_delim(expr, i, b'[', b']')
            && let Some(inside) = expr.get(i + 1..end)
        {
            return collect_string_literals(inside);
        }
        i += 1;
    }

    Vec::new()
}

fn scan_chain_method_calls(chain: &str) -> Vec<MethodCall<'_>> {
    let bytes = chain.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    let mut paren = 0usize;
    let mut bracket = 0usize;
    let mut brace = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }
        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        match bytes[i] {
            b'(' => {
                paren += 1;
                i += 1;
                continue;
            }
            b')' => {
                paren = paren.saturating_sub(1);
                i += 1;
                continue;
            }
            b'[' => {
                bracket += 1;
                i += 1;
                continue;
            }
            b']' => {
                bracket = bracket.saturating_sub(1);
                i += 1;
                continue;
            }
            b'{' => {
                brace += 1;
                i += 1;
                continue;
            }
            b'}' => {
                brace = brace.saturating_sub(1);
                i += 1;
                continue;
            }
            b'.' if paren == 0 && bracket == 0 && brace == 0 => {
                let name_start = skip_ws(bytes, i + 1);
                let Some((name, mut cursor)) = parse_ident_at_bytes(chain, name_start) else {
                    i += 1;
                    continue;
                };
                cursor = skip_ws(bytes, cursor);

                if starts_with_bytes(bytes, cursor, b"::") {
                    cursor = skip_ws(bytes, cursor + 2);
                    if bytes.get(cursor).copied() == Some(b'<') {
                        if let Some(angle_end) = find_matching_delim(chain, cursor, b'<', b'>') {
                            cursor = skip_ws(bytes, angle_end + 1);
                        } else {
                            i += 1;
                            continue;
                        }
                    }
                }

                if bytes.get(cursor).copied() != Some(b'(') {
                    i += 1;
                    continue;
                }

                let Some(close_idx) = find_matching_delim(chain, cursor, b'(', b')') else {
                    i += 1;
                    continue;
                };

                if let Some(args) = chain.get(cursor + 1..close_idx) {
                    out.push(MethodCall { name, args });
                }
                i = close_idx + 1;
            }
            _ => i += 1,
        }
    }

    out
}

fn scan_ident_method_calls<'a>(
    source: &'a str,
    ident: &str,
    method: &str,
) -> Vec<IdentMethodCall<'a>> {
    let bytes = source.as_bytes();
    let ident_bytes = ident.as_bytes();
    let mut calls = Vec::new();
    let mut i = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }
        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        if starts_with_bytes(bytes, i, ident_bytes)
            && !(i > 0 && is_ident_byte(bytes[i - 1]))
            && !bytes
                .get(i + ident_bytes.len())
                .copied()
                .is_some_and(is_ident_byte)
        {
            let after_ident = skip_ws(bytes, i + ident_bytes.len());
            if bytes.get(after_ident).copied() != Some(b'.') {
                i += 1;
                continue;
            }
            let method_start = skip_ws(bytes, after_ident + 1);
            if !starts_with_keyword(source, method_start, method) {
                i += 1;
                continue;
            }
            let after_method = skip_ws(bytes, method_start + method.len());
            if bytes.get(after_method).copied() != Some(b'(') {
                i += 1;
                continue;
            }
            let Some(close) = find_matching_delim(source, after_method, b'(', b')') else {
                i = after_method + 1;
                continue;
            };
            let args = source.get(after_method + 1..close).unwrap_or_default();
            calls.push(IdentMethodCall { args, start: i });
            i = close + 1;
            continue;
        }

        i += 1;
    }

    calls
}

fn extract_to_cte_aliases(source: &str, bindings: &LiteralBindings) -> Vec<String> {
    let bytes = source.as_bytes();
    let mut aliases = Vec::new();
    let mut i = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }
        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        if bytes[i] != b'.' {
            i += 1;
            continue;
        }
        let name_start = skip_ws(bytes, i + 1);
        let Some((name, mut cursor)) = parse_ident_at_bytes(source, name_start) else {
            i += 1;
            continue;
        };
        if name != "to_cte" {
            i += 1;
            continue;
        }
        cursor = skip_ws(bytes, cursor);
        if bytes.get(cursor).copied() != Some(b'(') {
            i += 1;
            continue;
        }
        let Some(close) = find_matching_delim(source, cursor, b'(', b')') else {
            i = cursor + 1;
            continue;
        };
        if let Some(args) = source.get(cursor + 1..close) {
            aliases.extend(resolve_string_values(args, None, bindings));
        }
        i = close + 1;
    }

    dedupe_values(&mut aliases);
    aliases
}

fn extract_bound_var_from_prefix(prefix: &str) -> Option<String> {
    let mut s = prefix.trim_start();
    s = s.strip_prefix("let ")?;
    s = s.strip_prefix("mut ").unwrap_or(s).trim_start();
    if s.starts_with('(') {
        return None;
    }

    let ident: String = s
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    if ident.is_empty() {
        return None;
    }

    let rest = s[ident.len()..].trim_start();
    let rest = if rest.starts_with(':') {
        rest.find('=').map(|pos| &rest[pos..])?
    } else {
        rest
    };
    if !rest.trim_start().starts_with('=') {
        return None;
    }

    Some(ident)
}

fn extract_receiver_ident_before_dot(source: &str, dot_idx: usize) -> Option<String> {
    let bytes = source.as_bytes();
    if dot_idx == 0 || dot_idx > bytes.len() || bytes.get(dot_idx).copied() != Some(b'.') {
        return None;
    }

    let mut end = dot_idx;
    while end > 0 && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    let mut start = end;
    while start > 0 && is_ident_byte(bytes[start - 1]) {
        start -= 1;
    }
    if start == end {
        return None;
    }

    let mut before = start;
    while before > 0 && bytes[before - 1].is_ascii_whitespace() {
        before -= 1;
    }
    if before > 0 && matches!(bytes[before - 1], b'.' | b':') {
        return None;
    }

    source.get(start..end).map(str::to_string)
}

fn collect_execution_site_rls_offsets(source: &str) -> HashMap<String, Vec<usize>> {
    let bytes = source.as_bytes();
    let mut out: HashMap<String, Vec<usize>> = HashMap::new();
    let mut i = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }
        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        if starts_with_bytes(bytes, i, b".with_rls") {
            if let Some(var) = extract_receiver_ident_before_dot(source, i) {
                out.entry(var).or_default().push(i);
            }
            i += ".with_rls".len();
            continue;
        }

        i += 1;
    }

    out
}

fn source_has_allow_comment(source: &str, marker: &str) -> bool {
    let bytes = source.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            let start = i + 2;
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            if let Some(comment) = source.get(start..i)
                && comment.contains(marker)
            {
                return true;
            }
            continue;
        }

        if starts_with_bytes(bytes, i, b"/*") {
            let end = consume_block_comment(bytes, i);
            if let Some(comment) = source.get(i..end)
                && comment.contains(marker)
            {
                return true;
            }
            i = end;
            continue;
        }

        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        i += 1;
    }

    false
}

fn source_has_function_call(source: &str, name: &str) -> bool {
    let bytes = source.as_bytes();
    let needle = name.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }
        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        if starts_with_bytes(bytes, i, needle)
            && !(i > 0 && is_ident_byte(bytes[i - 1]))
            && !bytes
                .get(i + needle.len())
                .copied()
                .is_some_and(is_ident_byte)
        {
            let after = skip_ws(bytes, i + needle.len());
            if bytes.get(after).copied() == Some(b'(') {
                return true;
            }
        }

        i += 1;
    }

    false
}

fn collect_qail_chains(source: &str) -> Vec<ScannedQailChain> {
    let line_starts = compute_line_starts(source);
    let mut out = Vec::new();
    let mut cursor = 0usize;

    while let Some(hit) = find_next_qail_constructor(source, cursor) {
        let statement_start = find_statement_start(source, hit.start);
        let args = source
            .get(hit.open_paren + 1..hit.close_paren)
            .unwrap_or_default();
        let first_arg = extract_first_argument(args).to_string();
        let full_chain = source.get(hit.start..hit.statement_end).unwrap_or_default();
        let bound_var = source
            .get(statement_start..hit.start)
            .and_then(extract_bound_var_from_prefix)
            .filter(|_| {
                source
                    .get(statement_start..hit.start)
                    .is_some_and(|prefix| !prefix.contains("Qail::"))
            });
        let (line, column0) = offset_to_line_col(&line_starts, hit.start);
        out.push(ScannedQailChain {
            start: hit.start,
            end: hit.statement_end,
            line,
            column: column0 + 1,
            action: hit.action,
            first_arg,
            full_chain: full_chain.to_string(),
            bound_var,
        });

        let next = hit.start + "Qail::".len();
        if next <= cursor {
            cursor += 1;
        } else {
            cursor = next;
        }
    }

    out
}

fn collect_cte_aliases(
    chains: &[ScannedQailChain],
    source: &str,
    local_functions: &[LocalFunction],
    binding_index: &LiteralBindingIndex,
) -> Vec<CteAlias> {
    let mut aliases = Vec::new();
    let qail_bound_vars = chains
        .iter()
        .filter_map(|chain| chain.bound_var.as_ref().map(|var| (var.as_str(), chain)))
        .collect::<Vec<_>>();

    for chain in chains {
        for call in scan_chain_method_calls(&chain.full_chain) {
            match call.name {
                "to_cte" => {
                    let bindings = literal_bindings_for_offset(
                        binding_index,
                        chain.start,
                        find_enclosing_local_function(chain.start, local_functions),
                    );
                    for name in resolve_string_values(call.args, None, &bindings) {
                        push_cte_alias(&mut aliases, source, chain, name);
                    }
                }
                "with" => {
                    let args = split_top_level_args(call.args);
                    if args.len() < 2 {
                        continue;
                    }
                    if args[1].trim_start().starts_with("Qail::")
                        || cte_arg_is_visible_bound_qail(
                            args[1],
                            chain,
                            &qail_bound_vars,
                            source,
                            local_functions,
                        )
                    {
                        let bindings = literal_bindings_for_offset(
                            binding_index,
                            chain.start,
                            find_enclosing_local_function(chain.start, local_functions),
                        );
                        for alias in resolve_string_values(args[0], None, &bindings) {
                            push_cte_alias(&mut aliases, source, chain, alias);
                        }
                    }
                }
                "with_cte" | "with_ctes" => {
                    let bindings = literal_bindings_for_offset(
                        binding_index,
                        chain.start,
                        find_enclosing_local_function(chain.start, local_functions),
                    );
                    for alias in extract_to_cte_aliases(call.args, &bindings) {
                        push_cte_alias(&mut aliases, source, chain, alias);
                    }
                }
                _ => {}
            }
        }
    }

    for (idx, (var, source_chain)) in qail_bound_vars.iter().enumerate() {
        let scope_end =
            find_innermost_block_end(source, source_chain.start).unwrap_or(source.len());
        let next_same_var_start = qail_bound_vars
            .iter()
            .skip(idx + 1)
            .filter(|(other_var, _)| other_var == var)
            .map(|(_, other_chain)| other_chain.start)
            .next()
            .unwrap_or(scope_end);

        for call in scan_ident_method_calls(source, var, "to_cte") {
            if call.start < source_chain.end
                || call.start >= next_same_var_start
                || call.start >= scope_end
                || !same_enclosing_function(source_chain.start, call.start, local_functions)
            {
                continue;
            }
            let bindings = literal_bindings_for_offset(
                binding_index,
                call.start,
                find_enclosing_local_function(call.start, local_functions),
            );
            for name in resolve_string_values(call.args, None, &bindings) {
                push_cte_alias_at(&mut aliases, source, call.start, name);
            }
        }
    }
    aliases
}

fn push_cte_alias(
    aliases: &mut Vec<CteAlias>,
    source: &str,
    chain: &ScannedQailChain,
    name: String,
) {
    push_cte_alias_at(aliases, source, chain.start, name);
}

fn push_cte_alias_at(aliases: &mut Vec<CteAlias>, source: &str, start: usize, name: String) {
    let end = find_innermost_block_end(source, start).unwrap_or(source.len());
    if aliases
        .iter()
        .any(|alias| alias.name == name && alias.start == start && alias.end == end)
    {
        return;
    }
    aliases.push(CteAlias { name, start, end });
}

fn cte_arg_is_visible_bound_qail(
    arg: &str,
    chain: &ScannedQailChain,
    qail_bound_vars: &[(&str, &ScannedQailChain)],
    source: &str,
    local_functions: &[LocalFunction],
) -> bool {
    let Some(key) = binding_lookup_key(arg) else {
        return false;
    };
    qail_bound_vars.iter().any(|(var, source_chain)| {
        *var == key
            && source_chain.start <= chain.start
            && chain.start
                < find_innermost_block_end(source, source_chain.start).unwrap_or(source.len())
            && same_enclosing_function(source_chain.start, chain.start, local_functions)
    })
}

fn same_enclosing_function(a: usize, b: usize, functions: &[LocalFunction]) -> bool {
    let a_func =
        find_enclosing_local_function(a, functions).map(|func| (func.body_start, func.body_end));
    let b_func =
        find_enclosing_local_function(b, functions).map(|func| (func.body_start, func.body_end));
    a_func == b_func
}

fn visible_cte_alias_names(aliases: &[CteAlias], offset: usize) -> HashSet<String> {
    aliases
        .iter()
        .filter(|alias| alias.start <= offset && offset < alias.end)
        .map(|alias| alias.name.clone())
        .collect()
}

fn find_enclosing_local_function(
    offset: usize,
    functions: &[LocalFunction],
) -> Option<&LocalFunction> {
    functions
        .iter()
        .filter(|func| offset > func.body_start && offset < func.body_end)
        .min_by_key(|func| func.body_end.saturating_sub(func.body_start))
}

fn find_innermost_block_end(source: &str, offset: usize) -> Option<usize> {
    find_innermost_block_span(source, offset).map(|(_, end)| end)
}

fn find_innermost_block_span(source: &str, offset: usize) -> Option<(usize, usize)> {
    let bytes = source.as_bytes();
    let mut stack = Vec::new();
    let mut i = 0usize;
    let limit = offset.min(bytes.len());

    while i < limit {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < limit && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i).min(limit);
            continue;
        }
        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next.min(limit);
            continue;
        }

        match bytes[i] {
            b'{' => stack.push(i),
            b'}' => {
                stack.pop();
            }
            _ => {}
        }
        i += 1;
    }

    let open = *stack.last()?;
    let close = find_matching_delim(source, open, b'{', b'}')?;
    Some((open, close))
}

fn build_param_substitutions(
    function: &LocalFunction,
    calls: &[LocalFunctionCall],
    function_name_counts: &HashMap<String, usize>,
    binding_index: &LiteralBindingIndex,
    local_functions: &[LocalFunction],
) -> Vec<ParamSubstitutions> {
    if function_name_counts
        .get(&function.name)
        .copied()
        .unwrap_or(0)
        != 1
    {
        return Vec::new();
    }

    let mut out = Vec::new();
    for call in calls {
        if call.name != function.name || call.args.len() < function.params.len() {
            continue;
        }
        let values = function
            .params
            .iter()
            .cloned()
            .zip(call.args.iter().cloned())
            .collect::<HashMap<_, _>>();
        if !values.is_empty() {
            let caller_function = find_enclosing_local_function(call.open_paren, local_functions);
            let bindings =
                literal_bindings_for_offset(binding_index, call.open_paren, caller_function);
            out.push(ParamSubstitutions { values, bindings });
        }
    }
    out
}

fn binding_lookup_key(expr: &str) -> Option<String> {
    let mut trimmed = expr.trim();
    while let Some(rest) = trimmed.strip_prefix('&') {
        trimmed = rest.trim_start();
    }
    trimmed = trimmed.trim_matches(|ch: char| matches!(ch, '(' | ')' | '[' | ']'));
    let segment = trimmed.rsplit("::").next().unwrap_or(trimmed);
    let segment = segment.rsplit('.').next().unwrap_or(segment).trim();
    if segment.is_empty() || !segment.chars().all(|c| c.is_alphanumeric() || c == '_') {
        None
    } else {
        Some(segment.to_string())
    }
}

fn resolve_string_values(
    expr: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
) -> Vec<String> {
    let mut out = Vec::new();
    let mut visited = HashSet::new();
    resolve_string_values_inner(expr, substitutions, bindings, &mut visited, &mut out);
    dedupe_values(&mut out);
    out
}

fn resolve_string_values_inner(
    expr: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
    visited: &mut HashSet<String>,
    out: &mut Vec<String>,
) {
    if let Some(value) = extract_string_arg(expr) {
        out.push(value);
        return;
    }

    let Some(key) = binding_lookup_key(expr) else {
        return;
    };
    let marker = format!("s:{key}");
    if !visited.insert(marker.clone()) {
        return;
    }

    if let Some(substitutions) = substitutions
        && let Some(arg_expr) = substitutions.values.get(&key)
    {
        visited.remove(&marker);
        resolve_string_values_inner(arg_expr, None, &substitutions.bindings, visited, out);
        return;
    }
    if let Some(values) = bindings.scalars.get(&key) {
        out.extend(values.iter().cloned());
    }
}

fn resolve_array_string_values(
    expr: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
) -> Vec<String> {
    let mut out = Vec::new();
    let mut visited = HashSet::new();
    resolve_array_string_values_inner(expr, substitutions, bindings, &mut visited, &mut out);
    dedupe_values(&mut out);
    out
}

fn resolve_array_string_values_inner(
    expr: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
    visited: &mut HashSet<String>,
    out: &mut Vec<String>,
) {
    let branch = extract_branch_array_literals(expr, bindings);
    if !branch.is_empty() {
        out.extend(branch);
        return;
    }

    let direct = extract_array_string_literals_from_expr(expr);
    if !direct.is_empty() {
        out.extend(direct);
        return;
    }

    let Some(key) = binding_lookup_key(expr) else {
        return;
    };
    let marker = format!("a:{key}");
    if !visited.insert(marker.clone()) {
        return;
    }

    if let Some(substitutions) = substitutions
        && let Some(arg_expr) = substitutions.values.get(&key)
    {
        visited.remove(&marker);
        resolve_array_string_values_inner(arg_expr, None, &substitutions.bindings, visited, out);
        return;
    }
    if let Some(values) = bindings.arrays.get(&key) {
        out.extend(values.iter().cloned());
    }
}

fn dedupe_values(values: &mut Vec<String>) {
    let mut seen = HashSet::new();
    values.retain(|value| seen.insert(value.clone()));
}

fn collect_helper_rls_param_indices(
    source: &str,
    functions: &[LocalFunction],
) -> HashMap<String, HashSet<usize>> {
    let mut out = HashMap::new();

    for function in functions {
        let body = source
            .get(function.body_start + 1..function.body_end)
            .unwrap_or_default();
        let mut indices = HashSet::new();
        for (idx, param) in function.params.iter().enumerate() {
            if source_contains_ident_method_call(body, param, "with_rls")
                || source_contains_ident_method_call(body, param, "rls")
            {
                indices.insert(idx);
            }
        }
        if !indices.is_empty() {
            out.insert(function.name.clone(), indices);
        }
    }

    out
}

fn source_contains_ident_method_call(source: &str, ident: &str, method: &str) -> bool {
    let needle = format!("{ident}.{method}");
    let bytes = source.as_bytes();
    let mut idx = 0usize;
    while idx < bytes.len() {
        if starts_with_bytes(bytes, idx, b"//") {
            idx += 2;
            while idx < bytes.len() && bytes[idx] != b'\n' {
                idx += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, idx, b"/*") {
            idx = consume_block_comment(bytes, idx);
            continue;
        }
        if let Some(next) = consume_rust_literal(bytes, idx) {
            idx = next;
            continue;
        }
        if !starts_with_bytes(bytes, idx, needle.as_bytes()) {
            idx += 1;
            continue;
        }
        let before_ok = idx == 0 || !is_ident_byte(source.as_bytes()[idx - 1]);
        if !before_ok {
            idx += needle.len();
            continue;
        }
        let mut after = idx + needle.len();
        after = skip_ws(source.as_bytes(), after);
        if source.as_bytes().get(after).copied() == Some(b'(') {
            return true;
        }
        idx += needle.len();
    }
    false
}

fn chain_has_helper_param_rls(
    chain: &ScannedQailChain,
    calls: &[LocalFunctionCall],
    helper_rls_params: &HashMap<String, HashSet<usize>>,
    enclosing_function: Option<&LocalFunction>,
    next_same_var_start: usize,
) -> bool {
    for call in calls {
        let Some(rls_param_indices) = helper_rls_params.get(&call.name) else {
            continue;
        };
        if let Some(function) = enclosing_function
            && !(call.open_paren > function.body_start && call.open_paren < function.body_end)
        {
            continue;
        }

        for (idx, (arg_start, arg_end)) in call.arg_spans.iter().enumerate() {
            if !rls_param_indices.contains(&idx) {
                continue;
            }

            if chain.start >= *arg_start && chain.start < *arg_end {
                return true;
            }

            if let Some(var) = chain.bound_var.as_ref()
                && call.open_paren >= chain.end
                && call.open_paren < next_same_var_start
                && let Some(arg_expr) = call.args.get(idx)
                && binding_lookup_key(arg_expr).as_deref() == Some(var.as_str())
            {
                return true;
            }
        }
    }

    false
}

pub(crate) fn scan_file(file: &str, content: &str, usages: &mut Vec<QailUsage>) {
    scan_file_inner(file, content, usages, true);
}

#[cfg(feature = "analyzer")]
pub(crate) fn scan_file_silent(file: &str, content: &str, usages: &mut Vec<QailUsage>) {
    scan_file_inner(file, content, usages, false);
}

fn scan_file_inner(file: &str, content: &str, usages: &mut Vec<QailUsage>, emit_warnings: bool) {
    // ── File-level flags ─────────────────────────────────────────────
    // Detect SuperAdminToken::for_system_process() usage anywhere in file.
    // Files can opt out with `// qail:allow(super_admin)` comment.
    let file_has_allow_super_admin = source_has_allow_comment(content, "qail:allow(super_admin)");
    let file_uses_super_admin =
        !file_has_allow_super_admin && source_has_function_call(content, "for_system_process");

    let chains = collect_qail_chains(content);
    let execution_site_rls = collect_execution_site_rls_offsets(content);
    let local_functions = collect_local_functions(content);
    let literal_binding_index = collect_literal_binding_index(content, &local_functions);
    let cte_aliases =
        collect_cte_aliases(&chains, content, &local_functions, &literal_binding_index);
    let local_function_calls = collect_local_function_calls(content, &local_functions);
    let helper_rls_params = collect_helper_rls_param_indices(content, &local_functions);
    let mut function_name_counts = HashMap::new();
    for function in &local_functions {
        *function_name_counts
            .entry(function.name.clone())
            .or_insert(0usize) += 1;
    }

    for (idx, chain) in chains.iter().enumerate() {
        let action = chain.action;

        if action == "RAW" {
            if emit_warnings {
                println!(
                    "cargo:warning=QAIL: raw SQL at {}:{} — not schema-validated",
                    file, chain.line
                );
            }
            continue;
        }

        let enclosing_function = find_enclosing_local_function(chain.start, &local_functions);
        let next_same_var_start = chain.bound_var.as_ref().map(|var| {
            chains
                .iter()
                .skip(idx + 1)
                .find(|other| other.bound_var.as_ref() == Some(var))
                .map(|other| other.start)
                .unwrap_or(usize::MAX)
        });
        let has_late_rls = chain.bound_var.as_ref().is_some_and(|var| {
            execution_site_rls
                .get(var)
                .into_iter()
                .flatten()
                .any(|offset| {
                    *offset >= chain.end
                        && *offset < next_same_var_start.unwrap_or(usize::MAX)
                        && match enclosing_function {
                            Some(function) => {
                                *offset > function.body_start && *offset < function.body_end
                            }
                            None => true,
                        }
                })
        });
        let has_helper_param_rls = chain_has_helper_param_rls(
            chain,
            &local_function_calls,
            &helper_rls_params,
            enclosing_function,
            next_same_var_start.unwrap_or(usize::MAX),
        );
        let has_rls = chain_has_rls(&chain.full_chain) || has_late_rls || has_helper_param_rls;
        let literal_bindings =
            literal_bindings_for_offset(&literal_binding_index, chain.start, enclosing_function);
        let substitution_contexts = enclosing_function
            .map(|function| {
                build_param_substitutions(
                    function,
                    &local_function_calls,
                    &function_name_counts,
                    &literal_binding_index,
                    &local_functions,
                )
            })
            .unwrap_or_default();

        let context_iter = if substitution_contexts.is_empty() {
            vec![None]
        } else {
            substitution_contexts.iter().map(Some).collect::<Vec<_>>()
        };
        let mut pushed = false;
        let mut seen_variants = HashSet::new();
        let visible_cte_names = visible_cte_alias_names(&cte_aliases, chain.start);

        for substitutions in context_iter {
            let has_explicit_tenant_scope = chain_has_explicit_tenant_scope(
                action,
                &chain.full_chain,
                substitutions,
                &literal_bindings,
            );
            let resolved_tables = if action == "TYPED" {
                extract_typed_table_arg(&chain.first_arg)
                    .into_iter()
                    .collect::<Vec<_>>()
            } else {
                resolve_string_values(&chain.first_arg, substitutions, &literal_bindings)
            };
            if resolved_tables.is_empty() {
                continue;
            }

            let raw_columns =
                extract_columns_with_bindings(&chain.full_chain, substitutions, &literal_bindings);
            let related_tables = extract_related_tables_with_bindings(
                &chain.full_chain,
                substitutions,
                &literal_bindings,
            )
            .into_iter()
            .filter(|table| !visible_cte_names.contains(table))
            .collect::<Vec<_>>();
            let related_tables_key = related_tables.join("\x1d");

            for table in resolved_tables {
                let alias_map = extract_table_aliases_with_bindings(
                    &chain.full_chain,
                    &table,
                    substitutions,
                    &literal_bindings,
                );
                let columns = normalize_columns_with_aliases(&raw_columns, &alias_map);
                let columns_key = columns.join("\x1f");
                let variant_key = format!("{table}\x1e{columns_key}\x1e{related_tables_key}");
                if !seen_variants.insert(variant_key) {
                    continue;
                }
                let is_cte_ref = visible_cte_names.contains(&table);
                usages.push(QailUsage {
                    file: file.to_string(),
                    line: chain.line,
                    column: chain.column,
                    table,
                    is_dynamic_table: false,
                    columns: columns.clone(),
                    action: action.to_string(),
                    related_tables: related_tables.clone(),
                    is_cte_ref,
                    has_rls,
                    has_explicit_tenant_scope,
                    file_uses_super_admin,
                });
                pushed = true;
            }
        }

        if !pushed && action != "TYPED" && emit_warnings {
            let var_hint = if chain.first_arg.trim().is_empty() {
                "?"
            } else {
                chain.first_arg.trim()
            };
            println!(
                "cargo:warning=Qail: dynamic table name `{}` in {}:{} — cannot validate columns at build time. Consider using string literals.",
                var_hint, file, chain.line
            );
        }
    }
}

pub(crate) fn extract_string_arg(s: &str) -> Option<String> {
    let mut s = s.trim_start();
    while let Some(rest) = s.strip_prefix('&') {
        s = rest.trim_start();
    }
    let (lit, _) = parse_string_literal_at(s, 0)?;
    Some(lit)
}

/// Extract table name from `Qail::typed(module::Table)` patterns.
/// Parses `module::StructName` and returns the last identifier-like segment
/// before the final `::item` as the table name.
///
/// Examples:
///  - `users::table`         → `users`
///  - `users::Users`         → `users`
///  - `schema::users::table` → `users`  (second-to-last segment)
///  - `Orders`               → `orders` (single ident, no ::)
pub(crate) fn extract_typed_table_arg(s: &str) -> Option<String> {
    let s = s.trim();
    // Collect the full path: identifier::Identifier::...
    let ident: String = s
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == ':')
        .collect();

    let segments: Vec<&str> = ident.split("::").filter(|s| !s.is_empty()).collect();

    match segments.len() {
        0 => None,
        1 => {
            // Single ident like `Orders` — use it directly
            let name = segments[0];
            if name.chars().all(|c| c.is_alphanumeric() || c == '_') {
                Some(name.to_lowercase())
            } else {
                None
            }
        }
        _ => {
            // Multiple segments like `users::table` or `schema::users::table`
            // Take the second-to-last segment as the table name
            let table = segments[segments.len() - 2];
            if table.chars().all(|c| c.is_alphanumeric() || c == '_') {
                Some(table.to_lowercase())
            } else {
                None
            }
        }
    }
}

#[cfg(test)]
pub(crate) fn extract_columns(line: &str) -> Vec<String> {
    extract_columns_with_bindings(line, None, &LiteralBindings::default())
}

fn extract_columns_with_bindings(
    line: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
) -> Vec<String> {
    let calls = scan_chain_method_calls(line);
    let mut columns = Vec::new();
    let mut aliases = HashSet::new();

    for call in &calls {
        if call.name == "alias" {
            for name in
                resolve_string_values(extract_first_argument(call.args), substitutions, bindings)
            {
                aliases.insert(name);
            }
        }
    }

    for call in calls {
        match call.name {
            "column" => {
                for col in resolve_string_values(
                    extract_first_argument(call.args),
                    substitutions,
                    bindings,
                ) {
                    columns.push(col);
                }
            }
            "columns" => {
                columns.extend(resolve_array_string_values(
                    extract_first_argument(call.args),
                    substitutions,
                    bindings,
                ));
            }
            "filter"
            | "or_filter"
            | "eq"
            | "ne"
            | "gt"
            | "lt"
            | "gte"
            | "lte"
            | "like"
            | "ilike"
            | "where_eq"
            | "order_by"
            | "order_desc"
            | "order_asc"
            | "in_vals"
            | "is_null"
            | "is_not_null"
            | "array_elem_contained_in_text" => {
                for col in resolve_string_values(
                    extract_first_argument(call.args),
                    substitutions,
                    bindings,
                ) {
                    columns.push(col);
                }
            }
            "typed_column" | "typed_eq" | "typed_ne" | "typed_gt" | "typed_lt" | "typed_gte"
            | "typed_lte" | "typed_filter" => {
                columns.extend(extract_typed_column_arg(
                    call.args,
                    0,
                    substitutions,
                    bindings,
                ));
            }
            "typed_columns" => {
                columns.extend(extract_typed_column_collection_arg(
                    call.args,
                    0,
                    substitutions,
                    bindings,
                ));
            }
            "set_value" | "set_opt" | "set_coalesce" | "set_coalesce_opt" => {
                for col in resolve_string_values(
                    extract_first_argument(call.args),
                    substitutions,
                    bindings,
                ) {
                    columns.push(col);
                }
                if let Some(value_arg) = split_top_level_args(call.args).get(1) {
                    columns.extend(extract_value_reference_columns_with_bindings(
                        value_arg,
                        substitutions,
                        bindings,
                    ));
                }
            }
            "group_by" | "distinct_on" => {
                columns.extend(resolve_array_string_values(
                    extract_first_argument(call.args),
                    substitutions,
                    bindings,
                ));
            }
            "filter_cond" | "having_cond" | "having_conds" | "merge_on_condition" => {
                columns.extend(extract_condition_columns_with_bindings(
                    call.args,
                    substitutions,
                    bindings,
                ));
            }
            "select_expr" => {
                extract_expr_argument_columns_inner(
                    call.args,
                    substitutions,
                    bindings,
                    0,
                    &mut columns,
                );
            }
            "column_expr" | "order_by_expr" => {
                columns.extend(extract_expression_columns_with_bindings(
                    call.args,
                    substitutions,
                    bindings,
                ));
            }
            "columns_expr" | "select_exprs" | "distinct_on_expr" | "group_by_expr" => {
                extract_expr_collection_argument_columns_inner(
                    call.args,
                    substitutions,
                    bindings,
                    0,
                    &mut columns,
                );
                columns.extend(extract_expression_columns_with_bindings(
                    call.args,
                    substitutions,
                    bindings,
                ));
            }
            "returning" | "on_conflict_nothing" => {
                for col in resolve_array_string_values(
                    extract_first_argument(call.args),
                    substitutions,
                    bindings,
                ) {
                    if !col.contains('.') {
                        columns.push(col);
                    }
                }
            }
            "on_conflict_update" => {
                columns.extend(resolve_array_string_arg(
                    call.args,
                    0,
                    substitutions,
                    bindings,
                ));
                columns.extend(resolve_array_string_arg(
                    call.args,
                    1,
                    substitutions,
                    bindings,
                ));
            }
            "merge_on_column" => {
                for col in resolve_string_values(
                    extract_first_argument(call.args),
                    substitutions,
                    bindings,
                ) {
                    columns.push(col);
                }
                columns.extend(resolve_string_arg(call.args, 2, substitutions, bindings));
            }
            "join" => {
                columns.extend(resolve_string_arg(call.args, 2, substitutions, bindings));
                columns.extend(resolve_string_arg(call.args, 3, substitutions, bindings));
            }
            "left_join" | "inner_join" => {
                columns.extend(resolve_string_arg(call.args, 1, substitutions, bindings));
                columns.extend(resolve_string_arg(call.args, 2, substitutions, bindings));
            }
            "left_join_as" | "inner_join_as" => {
                columns.extend(resolve_string_arg(call.args, 2, substitutions, bindings));
                columns.extend(resolve_string_arg(call.args, 3, substitutions, bindings));
            }
            "join_conds" | "left_join_conds" | "inner_join_conds" => {
                columns.extend(extract_condition_columns_with_bindings(
                    call.args,
                    substitutions,
                    bindings,
                ));
            }
            "when_matched_update" | "when_not_matched_by_source_update" => {
                columns.extend(resolve_array_string_arg(
                    call.args,
                    0,
                    substitutions,
                    bindings,
                ));
            }
            "when_matched_update_if" => {
                if let Some(condition_arg) = split_top_level_args(call.args).first() {
                    columns.extend(extract_condition_columns_with_bindings(
                        condition_arg,
                        substitutions,
                        bindings,
                    ));
                }
                columns.extend(resolve_array_string_arg(
                    call.args,
                    1,
                    substitutions,
                    bindings,
                ));
            }
            "when_not_matched_insert" => {
                columns.extend(resolve_array_string_arg(
                    call.args,
                    0,
                    substitutions,
                    bindings,
                ));
            }
            "when_not_matched_insert_if" => {
                if let Some(condition_arg) = split_top_level_args(call.args).first() {
                    columns.extend(extract_condition_columns_with_bindings(
                        condition_arg,
                        substitutions,
                        bindings,
                    ));
                }
                columns.extend(resolve_array_string_arg(
                    call.args,
                    1,
                    substitutions,
                    bindings,
                ));
            }
            _ => {}
        }
    }

    // Clean up extracted columns: strip Postgres ::type casts and AS aliases.
    // e.g. "id::text" → "id", "conn.id::text as connection_id" → "conn.id",
    // "COALESCE(inv.capacity - inv.reserved, 0)::bigint as x" → skipped (expression)
    let columns: Vec<String> = columns
        .into_iter()
        .map(|col| {
            // Strip " as alias" suffix (case-insensitive)
            let col = if let Some(pos) = col.find(" as ").or_else(|| col.find(" AS ")) {
                col[..pos].trim().to_string()
            } else {
                col
            };
            // Strip ::type cast suffix
            if let Some(pos) = col.find("::") {
                col[..pos].to_string()
            } else {
                col
            }
        })
        .filter(|col| {
            // Skip expressions that aren't simple column references
            !col.contains('(') && !col.contains(')') && !col.contains(' ')
        })
        .filter(|col| {
            // Skip computed alias names — these are not schema columns
            !aliases.contains(col.as_str())
        })
        .collect();

    columns
}

fn extract_condition_columns_with_bindings(
    expr: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
) -> Vec<String> {
    let mut columns = Vec::new();
    extract_condition_columns_inner(expr, substitutions, bindings, 0, &mut columns);
    dedupe_values(&mut columns);
    columns
}

fn extract_condition_columns_inner(
    expr: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
    depth: usize,
    columns: &mut Vec<String>,
) {
    if depth > 8 {
        return;
    }
    columns.extend(extract_condition_struct_left_columns(
        expr,
        substitutions,
        bindings,
    ));
    for call in scan_rust_function_calls(expr) {
        if call.name == "cond" {
            if let Some(left_arg) = split_top_level_args(call.args).first() {
                columns.extend(extract_direct_expr_columns(
                    left_arg,
                    substitutions,
                    bindings,
                ));
            }
        } else if call.name == "recent" {
            columns.push("created_at".to_string());
        } else if is_condition_builder_name(call.name) {
            columns.extend(resolve_string_arg(call.args, 0, substitutions, bindings));
        }
        if call.path.ends_with("Value::Column") {
            columns.extend(resolve_string_arg(call.args, 0, substitutions, bindings));
        }
        extract_condition_columns_inner(call.args, substitutions, bindings, depth + 1, columns);
    }
}

fn is_condition_builder_name(name: &str) -> bool {
    matches!(
        name,
        "eq" | "ne"
            | "gt"
            | "gte"
            | "lt"
            | "lte"
            | "is_in"
            | "not_in"
            | "is_null"
            | "is_not_null"
            | "like"
            | "ilike"
            | "not_like"
            | "between"
            | "not_between"
            | "regex"
            | "regex_i"
            | "contains"
            | "overlaps"
            | "similar_to"
            | "key_exists"
            | "recent_col"
            | "in_list"
    )
}

fn extract_condition_struct_left_columns(
    expr: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
) -> Vec<String> {
    let bytes = expr.as_bytes();
    let mut names = Vec::new();
    let mut i = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }
        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        if starts_with_keyword(expr, i, "Condition") {
            let after = skip_ws(bytes, i + "Condition".len());
            if bytes.get(after).copied() == Some(b'{')
                && let Some(close) = find_matching_delim(expr, after, b'{', b'}')
                && let Some(body) = expr.get(after + 1..close)
            {
                names.extend(resolve_struct_expr_column_field(
                    body,
                    "left",
                    substitutions,
                    bindings,
                ));
                i = close + 1;
                continue;
            }
        }

        i += 1;
    }

    names
}

fn resolve_struct_expr_column_field(
    body: &str,
    field: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
) -> Vec<String> {
    let bytes = body.as_bytes();
    let mut values = Vec::new();
    let mut i = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }
        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        if starts_with_keyword(body, i, field) {
            let after_field = skip_ws(bytes, i + field.len());
            if bytes.get(after_field).copied() == Some(b':') {
                let field_expr = body.get(after_field + 1..).unwrap_or_default();
                values.extend(extract_direct_expr_columns(
                    extract_first_argument(field_expr),
                    substitutions,
                    bindings,
                ));
                i = after_field + 1;
                continue;
            }
        }

        i += 1;
    }

    values
}

fn extract_direct_expr_columns(
    expr: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
) -> Vec<String> {
    let mut columns = Vec::new();
    let trimmed = expr.trim();
    if trimmed.starts_with("Expr::Aliased") {
        columns.extend(extract_expr_aliased_names(trimmed, substitutions, bindings));
    }
    columns.extend(resolve_string_values(trimmed, substitutions, bindings));
    for call in scan_rust_function_calls(trimmed) {
        if !trimmed
            .get(..call.start)
            .unwrap_or_default()
            .trim()
            .is_empty()
        {
            continue;
        }
        if call.name == "col"
            || call.path.ends_with("Expr::Named")
            || call.path.ends_with("Value::Column")
        {
            columns.extend(resolve_string_arg(call.args, 0, substitutions, bindings));
        }
    }
    dedupe_values(&mut columns);
    columns
}

fn extract_expression_columns_with_bindings(
    expr: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
) -> Vec<String> {
    let mut columns = Vec::new();
    extract_expression_columns_inner(expr, substitutions, bindings, 0, &mut columns);
    dedupe_values(&mut columns);
    columns
}

fn extract_value_reference_columns_with_bindings(
    expr: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
) -> Vec<String> {
    extract_expression_columns_with_bindings(expr, substitutions, bindings)
}

fn extract_expression_columns_inner(
    expr: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
    depth: usize,
    columns: &mut Vec<String>,
) {
    if depth > 8 {
        return;
    }
    columns.extend(extract_expr_aliased_names(expr, substitutions, bindings));
    columns.extend(extract_string_receiver_expression_columns(
        expr,
        substitutions,
        bindings,
    ));
    extract_expression_method_columns_inner(expr, substitutions, bindings, depth, columns);
    for call in scan_rust_function_calls(expr) {
        let args = split_top_level_args(call.args);
        match call.name {
            "percentage" => {
                columns.extend(resolve_string_arg(call.args, 0, substitutions, bindings));
                columns.extend(resolve_string_arg(call.args, 1, substitutions, bindings));
            }
            "cast" => {
                if let Some(expr_arg) = args.first() {
                    extract_expr_argument_columns_inner(
                        expr_arg,
                        substitutions,
                        bindings,
                        depth + 1,
                        columns,
                    );
                }
            }
            "binary" => {
                for index in [0, 2] {
                    if let Some(expr_arg) = args.get(index) {
                        extract_expr_argument_columns_inner(
                            expr_arg,
                            substitutions,
                            bindings,
                            depth + 1,
                            columns,
                        );
                    }
                }
            }
            "add_expr" | "and_expr" | "or_expr" | "nullif" => {
                for expr_arg in args.iter().take(2) {
                    extract_expr_argument_columns_inner(
                        expr_arg,
                        substitutions,
                        bindings,
                        depth + 1,
                        columns,
                    );
                }
            }
            "replace" => {
                for expr_arg in args.iter().take(3) {
                    extract_expr_argument_columns_inner(
                        expr_arg,
                        substitutions,
                        bindings,
                        depth + 1,
                        columns,
                    );
                }
            }
            "coalesce" | "concat" => {
                if let Some(exprs_arg) = args.first() {
                    extract_expr_collection_argument_columns_inner(
                        exprs_arg,
                        substitutions,
                        bindings,
                        depth + 1,
                        columns,
                    );
                }
            }
            "case_when" => {
                if let Some(condition_arg) = args.first() {
                    columns.extend(extract_condition_columns_with_bindings(
                        condition_arg,
                        substitutions,
                        bindings,
                    ));
                }
                if let Some(then_arg) = args.get(1) {
                    extract_expr_argument_columns_inner(
                        then_arg,
                        substitutions,
                        bindings,
                        depth + 1,
                        columns,
                    );
                }
            }
            _ => {}
        }
        if is_expression_string_arg_builder_name(call.name)
            || is_expression_column_builder_name(call.name)
            || is_condition_builder_name(call.name)
            || call.path.ends_with("Expr::Named")
            || call.path.ends_with("Value::Column")
        {
            columns.extend(resolve_string_arg(call.args, 0, substitutions, bindings));
        }
        extract_expression_columns_inner(call.args, substitutions, bindings, depth + 1, columns);
    }
}

fn extract_expression_method_columns_inner(
    expr: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
    depth: usize,
    columns: &mut Vec<String>,
) {
    if depth > 8 {
        return;
    }
    for call in scan_chain_method_calls(expr) {
        match call.name {
            "when" => {
                let args = split_top_level_args(call.args);
                if let Some(condition_arg) = args.first() {
                    columns.extend(extract_condition_columns_with_bindings(
                        condition_arg,
                        substitutions,
                        bindings,
                    ));
                }
                if let Some(then_arg) = args.get(1) {
                    extract_expr_argument_columns_inner(
                        then_arg,
                        substitutions,
                        bindings,
                        depth + 1,
                        columns,
                    );
                }
            }
            "otherwise" => {
                extract_expr_argument_columns_inner(
                    call.args,
                    substitutions,
                    bindings,
                    depth + 1,
                    columns,
                );
            }
            "filter" => {
                columns.extend(extract_condition_columns_with_bindings(
                    call.args,
                    substitutions,
                    bindings,
                ));
            }
            "or_default" => {
                extract_expr_argument_columns_inner(
                    call.args,
                    substitutions,
                    bindings,
                    depth + 1,
                    columns,
                );
            }
            _ => {}
        }
    }
}

fn extract_string_receiver_expression_columns(
    expr: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
) -> Vec<String> {
    let bytes = expr.as_bytes();
    let mut columns = Vec::new();
    let mut i = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }

        if let Some((lit, next)) = parse_string_literal_at(expr, i) {
            if expression_receiver_method_after(expr, next).is_some() {
                columns.push(lit);
            }
            i = next;
            continue;
        }

        if is_ident_byte(bytes[i])
            && (i == 0 || !is_ident_byte(bytes[i - 1]) && bytes[i - 1] != b'.')
            && let Some((name, name_end)) = parse_ident_at_bytes(expr, i)
        {
            if expression_receiver_method_after(expr, name_end).is_some() {
                columns.extend(resolve_string_values(name, substitutions, bindings));
            }
            i = name_end;
            continue;
        }

        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        i += 1;
    }

    columns
}

fn expression_receiver_method_after(expr: &str, receiver_end: usize) -> Option<&str> {
    let bytes = expr.as_bytes();
    let dot = skip_ws(bytes, receiver_end);
    if bytes.get(dot).copied() != Some(b'.') {
        return None;
    }
    let name_start = skip_ws(bytes, dot + 1);
    let (name, name_end) = parse_ident_at_bytes(expr, name_start)?;
    if !is_expression_receiver_method_name(name) {
        return None;
    }
    let args_start = skip_ws(bytes, name_end);
    (bytes.get(args_start).copied() == Some(b'(')).then_some(name)
}

fn is_expression_receiver_method_name(name: &str) -> bool {
    matches!(
        name,
        "with_alias"
            | "or_default"
            | "json"
            | "path"
            | "cast"
            | "upper"
            | "lower"
            | "trim"
            | "length"
            | "abs"
    )
}

fn extract_expr_argument_columns_inner(
    expr: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
    depth: usize,
    columns: &mut Vec<String>,
) {
    if depth > 8 {
        return;
    }
    columns.extend(resolve_string_values(expr, substitutions, bindings));
    extract_expression_columns_inner(expr, substitutions, bindings, depth + 1, columns);
}

fn extract_expr_collection_argument_columns_inner(
    expr: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
    depth: usize,
    columns: &mut Vec<String>,
) {
    if depth > 8 {
        return;
    }
    let Some(inner) = extract_direct_expr_collection_inner(expr) else {
        extract_expression_columns_inner(expr, substitutions, bindings, depth + 1, columns);
        return;
    };
    for expr_arg in split_top_level_args(inner) {
        extract_expr_argument_columns_inner(expr_arg, substitutions, bindings, depth + 1, columns);
    }
}

fn extract_direct_expr_collection_inner(expr: &str) -> Option<&str> {
    let mut trimmed = expr.trim();
    while let Some(rest) = trimmed.strip_prefix('&') {
        trimmed = rest.trim_start();
    }

    if trimmed.starts_with('[') {
        let close = find_matching_delim(trimmed, 0, b'[', b']')?;
        return trimmed.get(1..close);
    }

    let rest = trimmed.strip_prefix("vec!")?.trim_start();
    if !rest.starts_with('[') {
        return None;
    }
    let close = find_matching_delim(rest, 0, b'[', b']')?;
    rest.get(1..close)
}

fn is_expression_string_arg_builder_name(name: &str) -> bool {
    matches!(
        name,
        "json"
            | "json_path"
            | "json_obj"
            | "string_agg"
            | "substring"
            | "substring_for"
            | "inc"
            | "is_null_expr"
            | "is_not_null_expr"
    )
}

fn is_expression_column_builder_name(name: &str) -> bool {
    matches!(
        name,
        "col"
            | "count_distinct"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "array_agg"
            | "json_agg"
            | "jsonb_agg"
            | "bool_and"
            | "bool_or"
    )
}

fn extract_expr_aliased_names(
    expr: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
) -> Vec<String> {
    let bytes = expr.as_bytes();
    let needle = b"Expr::Aliased";
    let mut names = Vec::new();
    let mut i = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }
        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        if starts_with_bytes(bytes, i, needle) {
            let after = skip_ws(bytes, i + needle.len());
            if bytes.get(after).copied() == Some(b'{')
                && let Some(close) = find_matching_delim(expr, after, b'{', b'}')
                && let Some(body) = expr.get(after + 1..close)
            {
                names.extend(resolve_struct_string_field(
                    body,
                    "name",
                    substitutions,
                    bindings,
                ));
                i = close + 1;
                continue;
            }
        }

        i += 1;
    }

    names
}

fn resolve_struct_string_field(
    body: &str,
    field: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
) -> Vec<String> {
    let bytes = body.as_bytes();
    let mut values = Vec::new();
    let mut i = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }
        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        if starts_with_keyword(body, i, field) {
            let after_field = skip_ws(bytes, i + field.len());
            if bytes.get(after_field).copied() == Some(b':') {
                let field_expr = body.get(after_field + 1..).unwrap_or_default();
                values.extend(resolve_string_values(
                    extract_first_argument(field_expr),
                    substitutions,
                    bindings,
                ));
                i = after_field + 1;
                continue;
            }
        }

        i += 1;
    }

    values
}

#[derive(Debug, Clone, Copy)]
struct RustFunctionCall<'a> {
    path: &'a str,
    name: &'a str,
    args: &'a str,
    start: usize,
}

fn scan_rust_function_calls(source: &str) -> Vec<RustFunctionCall<'_>> {
    let bytes = source.as_bytes();
    let mut calls = Vec::new();
    let mut i = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }
        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        if !is_ident_byte(bytes[i])
            || i > 0 && (is_ident_byte(bytes[i - 1]) || bytes[i - 1] == b':')
        {
            i += 1;
            continue;
        }

        let path_start = i;
        let mut cursor = i;
        let Some((_, ident_end)) = parse_ident_at_bytes(source, cursor) else {
            i += 1;
            continue;
        };
        cursor = ident_end;
        while starts_with_bytes(bytes, cursor, b"::") {
            let next_ident_start = cursor + 2;
            let Some((_, next_ident_end)) = parse_ident_at_bytes(source, next_ident_start) else {
                break;
            };
            cursor = next_ident_end;
        }

        let after_path = skip_ws(bytes, cursor);
        if bytes.get(after_path).copied() != Some(b'(') {
            i = cursor;
            continue;
        }
        let prev = source.get(..path_start).and_then(|prefix| {
            prefix
                .bytes()
                .rev()
                .find(|byte| !byte.is_ascii_whitespace())
        });
        if prev == Some(b'.') {
            i = cursor;
            continue;
        }

        let Some(close) = find_matching_delim(source, after_path, b'(', b')') else {
            i = after_path + 1;
            continue;
        };
        let path = source.get(path_start..cursor).unwrap_or_default();
        let name = path.rsplit("::").next().unwrap_or(path);
        let args = source.get(after_path + 1..close).unwrap_or_default();
        calls.push(RustFunctionCall {
            path,
            name,
            args,
            start: path_start,
        });
        i = close + 1;
    }

    calls
}

fn resolve_array_string_arg(
    args: &str,
    index: usize,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
) -> Vec<String> {
    split_top_level_args(args)
        .get(index)
        .map(|arg| resolve_array_string_values(arg, substitutions, bindings))
        .unwrap_or_default()
}

fn resolve_string_arg(
    args: &str,
    index: usize,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
) -> Vec<String> {
    split_top_level_args(args)
        .get(index)
        .map(|arg| resolve_string_values(arg, substitutions, bindings))
        .unwrap_or_default()
}

fn extract_typed_column_arg(
    args: &str,
    index: usize,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
) -> Vec<String> {
    split_top_level_args(args)
        .get(index)
        .map(|arg| resolve_typed_column_values(arg, substitutions, bindings))
        .unwrap_or_default()
}

fn extract_typed_column_collection_arg(
    args: &str,
    index: usize,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
) -> Vec<String> {
    let args = split_top_level_args(args);
    let Some(arg) = args.get(index) else {
        return Vec::new();
    };
    let Some(inner) = extract_direct_expr_collection_inner(arg) else {
        return resolve_typed_column_values(arg, substitutions, bindings);
    };

    let mut columns = Vec::new();
    for expr in split_top_level_args(inner) {
        columns.extend(resolve_typed_column_values(expr, substitutions, bindings));
    }
    dedupe_values(&mut columns);
    columns
}

fn resolve_typed_column_values(
    expr: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
) -> Vec<String> {
    let mut columns = Vec::new();
    let mut visited = HashSet::new();
    resolve_typed_column_values_inner(expr, substitutions, bindings, &mut visited, &mut columns);
    dedupe_values(&mut columns);
    columns
}

fn resolve_typed_column_values_inner(
    expr: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
    visited: &mut HashSet<String>,
    columns: &mut Vec<String>,
) {
    let trimmed = expr.trim();

    if let Some(inner) = extract_direct_expr_collection_inner(trimmed) {
        for item in split_top_level_args(inner) {
            resolve_typed_column_values_inner(item, substitutions, bindings, visited, columns);
        }
        return;
    }

    let direct = extract_direct_typed_column_expr(trimmed, substitutions, bindings);
    if !direct.is_empty() {
        columns.extend(direct);
        return;
    }

    if !is_simple_binding_reference(trimmed) {
        return;
    }
    let Some(key) = binding_lookup_key(trimmed) else {
        return;
    };
    let marker = format!("t:{key}");
    if !visited.insert(marker.clone()) {
        return;
    }

    if let Some(substitutions) = substitutions
        && let Some(arg_expr) = substitutions.values.get(&key)
    {
        visited.remove(&marker);
        resolve_typed_column_values_inner(
            arg_expr,
            None,
            &substitutions.bindings,
            visited,
            columns,
        );
        return;
    }

    if let Some(exprs) = bindings.typed_scalars.get(&key) {
        for expr in exprs {
            resolve_typed_column_values_inner(expr, None, bindings, visited, columns);
        }
    }
    if let Some(exprs) = bindings.typed_arrays.get(&key) {
        for expr in exprs {
            resolve_typed_column_values_inner(expr, None, bindings, visited, columns);
        }
    }
}

fn direct_typed_column_expr_has_column(expr: &str) -> bool {
    !extract_direct_typed_column_expr(expr, None, &LiteralBindings::default()).is_empty()
}

fn extract_direct_typed_column_expr(
    expr: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
) -> Vec<String> {
    let mut columns = Vec::new();
    if let Some(column) = extract_typed_column_path_expr(expr) {
        columns.push(column);
    }
    for call in scan_rust_function_calls(expr) {
        if call.path.ends_with("TypedColumn::new") {
            columns.extend(resolve_string_arg(call.args, 1, substitutions, bindings));
            continue;
        }
        if !call.args.trim().is_empty() || !call.path.contains("::") {
            continue;
        }
        columns.push(call.name.to_string());
    }
    dedupe_values(&mut columns);
    columns
}

fn extract_typed_column_collection_items(expr: &str) -> Option<Vec<String>> {
    let inner = extract_direct_expr_collection_inner(expr)?;
    let items = split_top_level_args(inner)
        .into_iter()
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
        .collect::<Vec<_>>();
    if items.is_empty() {
        return None;
    }

    if items
        .iter()
        .any(|item| direct_typed_column_expr_has_column(item) || is_simple_binding_reference(item))
    {
        Some(items)
    } else {
        None
    }
}

fn extract_typed_column_path_expr(expr: &str) -> Option<String> {
    let mut trimmed = expr.trim();
    while let Some(rest) = trimmed.strip_prefix('&') {
        trimmed = rest.trim_start();
    }
    while trimmed.starts_with('(') && trimmed.ends_with(')') {
        let close = find_matching_delim(trimmed, 0, b'(', b')')?;
        if close + 1 != trimmed.len() {
            break;
        }
        trimmed = trimmed.get(1..close)?.trim();
    }

    if trimmed.contains(|ch: char| ch.is_whitespace())
        || trimmed.contains(['(', ')', '[', ']', '{', '}', ',', '.'])
    {
        return None;
    }
    if !trimmed.contains("::") {
        return None;
    }

    let raw_segment = trimmed.rsplit("::").next()?.trim();
    let segment = raw_segment.strip_prefix("r#").unwrap_or(raw_segment);
    if segment == "table" {
        return None;
    }
    if segment.is_empty() || !segment.chars().all(|c| c.is_alphanumeric() || c == '_') {
        None
    } else {
        Some(segment.to_string())
    }
}

fn is_simple_binding_reference(expr: &str) -> bool {
    let mut trimmed = expr.trim();
    while let Some(rest) = trimmed.strip_prefix('&') {
        trimmed = rest.trim_start();
    }
    while trimmed.starts_with('(') && trimmed.ends_with(')') {
        let Some(close) = find_matching_delim(trimmed, 0, b'(', b')') else {
            return false;
        };
        if close + 1 != trimmed.len() {
            break;
        }
        trimmed = trimmed.get(1..close).unwrap_or_default().trim();
    }

    !trimmed.contains("::")
        && !trimmed.is_empty()
        && trimmed.chars().all(|c| c.is_alphanumeric() || c == '_')
}

fn extract_related_tables_with_bindings(
    line: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
) -> Vec<String> {
    let mut tables = Vec::new();
    for call in scan_chain_method_calls(line) {
        match call.name {
            "using_table" | "using_table_as" | "left_join" | "inner_join" | "left_join_as"
            | "inner_join_as" | "left_join_conds" | "inner_join_conds" | "join_on"
            | "join_on_optional" => {
                tables.extend(resolve_string_arg(call.args, 0, substitutions, bindings));
            }
            "join" | "join_conds" => {
                tables.extend(resolve_string_arg(call.args, 1, substitutions, bindings));
            }
            "update_from" | "delete_using" => {
                tables.extend(resolve_array_string_values(
                    extract_first_argument(call.args),
                    substitutions,
                    bindings,
                ));
            }
            _ => {}
        }
    }
    tables = tables
        .into_iter()
        .filter_map(|table| normalize_related_table_name(&table))
        .collect();
    dedupe_values(&mut tables);
    tables
}

fn normalize_related_table_name(table: &str) -> Option<String> {
    let table = table.trim();
    if table.is_empty() {
        return None;
    }
    let base = table.split_whitespace().next().unwrap_or(table);
    if base.contains('(') || base.contains(')') {
        None
    } else {
        Some(base.to_string())
    }
}

fn extract_table_aliases_with_bindings(
    line: &str,
    primary_table: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
) -> HashMap<String, String> {
    let mut aliases = HashMap::new();
    for call in scan_chain_method_calls(line) {
        match call.name {
            "table_alias" | "target_alias" => {
                for alias in resolve_string_arg(call.args, 0, substitutions, bindings) {
                    insert_alias(&mut aliases, primary_table, &alias);
                }
            }
            "left_join_as" | "inner_join_as" | "using_table_as" => {
                for table in resolve_string_arg(call.args, 0, substitutions, bindings) {
                    for alias in resolve_string_arg(call.args, 1, substitutions, bindings) {
                        insert_alias(&mut aliases, &table, &alias);
                    }
                }
            }
            "left_join" | "inner_join" | "left_join_conds" | "inner_join_conds" => {
                for table in resolve_string_arg(call.args, 0, substitutions, bindings) {
                    insert_alias_from_table_ref(&mut aliases, &table);
                }
            }
            "join" | "join_conds" => {
                for table in resolve_string_arg(call.args, 1, substitutions, bindings) {
                    insert_alias_from_table_ref(&mut aliases, &table);
                }
            }
            "update_from" | "delete_using" => {
                for table in resolve_array_string_values(
                    extract_first_argument(call.args),
                    substitutions,
                    bindings,
                ) {
                    insert_alias_from_table_ref(&mut aliases, &table);
                }
            }
            _ => {}
        }
    }
    aliases
}

fn insert_alias_from_table_ref(aliases: &mut HashMap<String, String>, table_ref: &str) {
    if let Some((table, alias)) = split_table_alias(table_ref) {
        insert_alias(aliases, &table, &alias);
    }
}

fn insert_alias(aliases: &mut HashMap<String, String>, table: &str, alias: &str) {
    let Some(table) = normalize_related_table_name(table) else {
        return;
    };
    let alias = alias.trim();
    if alias.is_empty()
        || alias.eq_ignore_ascii_case("as")
        || alias == table
        || !alias
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return;
    }
    aliases.insert(alias.to_string(), table);
}

fn split_table_alias(table_ref: &str) -> Option<(String, String)> {
    let parts = table_ref.split_whitespace().collect::<Vec<_>>();
    match parts.as_slice() {
        [table, alias] => Some(((*table).to_string(), (*alias).to_string())),
        [table, as_kw, alias] if as_kw.eq_ignore_ascii_case("as") => {
            Some(((*table).to_string(), (*alias).to_string()))
        }
        _ => None,
    }
}

fn normalize_columns_with_aliases(
    columns: &[String],
    aliases: &HashMap<String, String>,
) -> Vec<String> {
    columns
        .iter()
        .map(|column| normalize_column_with_aliases(column, aliases))
        .collect()
}

fn normalize_column_with_aliases(column: &str, aliases: &HashMap<String, String>) -> String {
    let Some((prefix, suffix)) = column.split_once('.') else {
        return column.to_string();
    };
    if let Some(table) = aliases.get(prefix) {
        format!("{table}.{suffix}")
    } else {
        column.to_string()
    }
}

fn chain_has_rls(chain: &str) -> bool {
    scan_chain_method_calls(chain)
        .into_iter()
        .any(|call| matches!(call.name, "with_rls" | "rls"))
}

fn chain_has_explicit_tenant_scope(
    action: &str,
    chain: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
) -> bool {
    for call in scan_chain_method_calls(chain) {
        let is_filter_scope = matches!(call.name, "eq" | "where_eq" | "is_null");
        let is_typed_filter_scope =
            typed_filter_call_has_tenant_scope(call.name, call.args, substitutions, bindings);
        let is_payload_scope = matches!(
            call.name,
            "set_value" | "set_opt" | "set_coalesce" | "set_coalesce_opt"
        ) && matches!(action, "ADD" | "PUT");
        if is_typed_filter_scope {
            return true;
        }
        if !(is_filter_scope || is_payload_scope) {
            continue;
        }
        if resolve_string_values(extract_first_argument(call.args), substitutions, bindings)
            .into_iter()
            .any(|col| is_tenant_identifier(&col))
        {
            return true;
        }
    }
    false
}

fn typed_filter_call_has_tenant_scope(
    name: &str,
    args: &str,
    substitutions: Option<&ParamSubstitutions>,
    bindings: &LiteralBindings,
) -> bool {
    let columns = extract_typed_column_arg(args, 0, substitutions, bindings);
    if !columns.iter().any(|col| is_tenant_identifier(col)) {
        return false;
    }

    match name {
        "typed_eq" => true,
        "typed_filter" => split_top_level_args(args)
            .get(1)
            .is_some_and(|op| typed_operator_is_tenant_scope(op)),
        _ => false,
    }
}

fn typed_operator_is_tenant_scope(op: &str) -> bool {
    let op = op.trim();
    matches!(op, "Operator::Eq" | "Eq" | "Operator::IsNull" | "IsNull")
}

fn is_tenant_identifier(raw_ident: &str) -> bool {
    let without_cast = raw_ident.split("::").next().unwrap_or(raw_ident).trim();
    let last_segment = without_cast.rsplit('.').next().unwrap_or(without_cast);
    let normalized = last_segment
        .trim_matches('"')
        .trim_matches('`')
        .to_ascii_lowercase();
    normalized == "tenant_id"
}

pub(crate) fn usage_action_to_ast(action: &str) -> Result<crate::ast::Action, String> {
    use crate::ast::Action;

    match action {
        "GET" | "TYPED" => Ok(Action::Get),
        "ADD" => Ok(Action::Add),
        "SET" => Ok(Action::Set),
        "DEL" => Ok(Action::Del),
        "PUT" => Ok(Action::Put),
        "MERGE" => Ok(Action::Merge),
        "EXPORT" => Ok(Action::Export),
        "TRUNCATE" => Ok(Action::Truncate),
        "EXPLAIN" => Ok(Action::Explain),
        "EXPLAIN_ANALYZE" => Ok(Action::ExplainAnalyze),
        "LOCK" => Ok(Action::Lock),
        _ => Err(format!("unknown scanner action '{}'", action)),
    }
}

pub(crate) fn append_scanned_columns(cmd: &mut crate::ast::Qail, columns: &[String]) {
    use crate::ast::Expr;

    for col in columns {
        // Skip SQL function expressions (e.g., count(*), SUM(amount))
        // and wildcard (*) — these are valid SQL, not schema columns
        if col.contains('(') || col == "*" {
            continue;
        }
        let exists = cmd
            .columns
            .iter()
            .any(|e| matches!(e, Expr::Named(existing) if existing == col));
        if !exists {
            cmd.columns.push(Expr::Named(col.clone()));
        }
    }
}
