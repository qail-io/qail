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

#[derive(Debug, Clone, Default)]
struct LiteralBindings {
    scalars: HashMap<String, Vec<String>>,
    arrays: HashMap<String, Vec<String>>,
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

/// Phase 1+2: Collect let-bindings that map variable names to string literal(s).
///
/// Handles:
///   `let table = "foo";`                                    → {"table": ["foo"]}
///   `let (table, col) = ("foo", "bar");`                    → {"table": ["foo"], "col": ["bar"]}
///   `let (table, col) = if cond { ("a", "x") } else { ("b", "y") };`
///                                                           → {"table": ["a", "b"], "col": ["x", "y"]}
///   `let table = if cond { "a" } else { "b" };`             → {"table": ["a", "b"]}
fn collect_let_bindings(content: &str) -> HashMap<String, Vec<String>> {
    let mut bindings: HashMap<String, Vec<String>> = HashMap::new();

    // Join all lines for multi-line let analysis
    let lines: Vec<&str> = content.lines().collect();
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i].trim();

        // Look for: let IDENT = "literal"
        // or:       let (IDENT, IDENT) = ...
        if let Some(rest) = line.strip_prefix("let ") {
            let rest = rest.trim();

            // Phase 1: Simple  let table = "literal";
            if let Some((var, rhs)) = parse_simple_let(rest) {
                if let Some(lit) = extract_string_arg(rhs.trim()) {
                    bindings.entry(var).or_default().push(lit);
                    i += 1;
                    continue;
                }

                // Phase 2: let table = if cond { "a" } else { "b" };
                let rhs = rhs.trim();
                if rhs.starts_with("if ") {
                    // Collect the full if/else expression, possibly spanning multiple lines
                    let mut full_expr = rhs.to_string();
                    let mut j = i + 1;
                    // Keep joining lines until we see the closing `;`
                    while j < lines.len() && !full_expr.contains(';') {
                        // Preserve line boundaries so `//` comments stay line-scoped.
                        full_expr.push('\n');
                        full_expr.push_str(lines[j].trim());
                        j += 1;
                    }
                    let literals = extract_branch_literals(&full_expr);
                    if !literals.is_empty() {
                        bindings.entry(var).or_default().extend(literals);
                    }
                }
            }

            // Phase 2: Destructuring  let (table, col) = if cond { ("a", "x") } else { ("b", "y") };
            //          or             let (table, col) = ("a", "b");
            if rest.starts_with('(') {
                // Collect the full line (may span multiple lines)
                let mut full_line = line.to_string();
                let mut j = i + 1;
                while j < lines.len() && !full_line.contains(';') {
                    // Preserve line boundaries so `//` comments stay line-scoped.
                    full_line.push('\n');
                    full_line.push_str(lines[j].trim());
                    j += 1;
                }

                if let Some(result) = parse_destructuring_let(&full_line) {
                    for (name, values) in result {
                        bindings.entry(name).or_default().extend(values);
                    }
                }
            }
        }

        i += 1;
    }

    bindings
}

fn collect_literal_bindings(content: &str) -> LiteralBindings {
    let mut bindings = LiteralBindings {
        scalars: collect_let_bindings(content),
        arrays: collect_let_array_bindings(content),
    };
    collect_const_literal_bindings(content, &mut bindings);
    dedupe_binding_values(&mut bindings.scalars);
    dedupe_binding_values(&mut bindings.arrays);
    bindings
}

fn collect_let_array_bindings(content: &str) -> HashMap<String, Vec<String>> {
    let mut bindings: HashMap<String, Vec<String>> = HashMap::new();
    let lines: Vec<&str> = content.lines().collect();
    let mut i = 0usize;

    while i < lines.len() {
        let line = lines[i].trim();
        if let Some(rest) = line.strip_prefix("let ")
            && let Some((var, rhs)) = parse_simple_let(rest.trim())
        {
            let mut full_expr = rhs.trim().to_string();
            let mut j = i + 1;
            while j < lines.len() && !full_expr.contains(';') {
                // Preserve line boundaries so `//` comments stay line-scoped.
                full_expr.push('\n');
                full_expr.push_str(lines[j].trim());
                j += 1;
            }
            let values = extract_array_string_literals_from_expr(&full_expr);
            if !values.is_empty() {
                bindings.insert(var, values);
            }
            i = j.max(i + 1);
            continue;
        }
        i += 1;
    }

    bindings
}

fn collect_const_literal_bindings(content: &str, bindings: &mut LiteralBindings) {
    let lines: Vec<&str> = content.lines().collect();
    let mut i = 0usize;

    while i < lines.len() {
        let line = lines[i].trim();
        if !looks_like_const_binding(line) {
            i += 1;
            continue;
        }

        let mut full_stmt = line.to_string();
        let mut j = i + 1;
        while j < lines.len() && !full_stmt.contains(';') {
            // Preserve line boundaries so `//` comments stay line-scoped.
            full_stmt.push('\n');
            full_stmt.push_str(lines[j].trim());
            j += 1;
        }

        if let Some((name, rhs)) = parse_const_binding(&full_stmt) {
            if let Some(value) = extract_string_arg(rhs) {
                bindings
                    .scalars
                    .entry(name.clone())
                    .or_default()
                    .push(value);
            }

            let values = extract_array_string_literals_from_expr(rhs);
            if !values.is_empty() {
                bindings.arrays.insert(name, values);
            }
        }

        i = j.max(i + 1);
    }
}

fn looks_like_const_binding(line: &str) -> bool {
    for prefix in [
        "const ",
        "static ",
        "pub const ",
        "pub static ",
        "pub(crate) const ",
        "pub(crate) static ",
        "pub(super) const ",
        "pub(super) static ",
    ] {
        if line.starts_with(prefix) {
            return true;
        }
    }
    false
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

/// Extract string literals from if/else branches.
/// Handles: `if cond { "a" } else { "b" }` → ["a", "b"]
fn extract_branch_literals(expr: &str) -> Vec<String> {
    let mut literals = Vec::new();

    // Find all `{ "literal" }` patterns in the expression
    let mut remaining = expr;
    while let Some(brace_pos) = remaining.find('{') {
        let inside = &remaining[brace_pos + 1..];
        if let Some(close_pos) = inside.find('}') {
            let block = inside[..close_pos].trim();
            // Check if block content is a simple string literal
            if let Some(lit) = extract_string_arg(block) {
                literals.push(lit);
            }
            remaining = &inside[close_pos + 1..];
        } else {
            break;
        }
    }

    literals
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
        let open_paren = skip_ws(bytes, name_end);
        if bytes.get(open_paren).copied() != Some(b'(') {
            i += 2;
            continue;
        }
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

    for function in functions {
        let needle = format!("{}(", function.name);
        for (idx, _) in source.match_indices(&needle) {
            let open_paren = idx + function.name.len();
            let Some((name, name_start)) = bare_call_name_before_open_paren(source, open_paren)
            else {
                continue;
            };
            if name != function.name {
                continue;
            }
            if previous_identifier_before(source, name_start).as_deref() == Some("fn") {
                continue;
            }
            let Some(close_paren) = find_matching_delim(source, open_paren, b'(', b')') else {
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
            let key = format!("{}@{}@{}", name, open_paren, close_paren);
            if seen.insert(key) {
                out.push(LocalFunctionCall {
                    name,
                    args,
                    arg_spans,
                    open_paren,
                });
            }
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

fn bare_call_name_before_open_paren(
    source: &str,
    open_paren_idx: usize,
) -> Option<(String, usize)> {
    let bytes = source.as_bytes();
    if open_paren_idx == 0 || open_paren_idx > bytes.len() {
        return None;
    }

    let mut end = open_paren_idx;
    while end > 0 && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    if end == 0 {
        return None;
    }

    let mut start = end;
    while start > 0 && is_ident_byte(bytes[start - 1]) {
        start -= 1;
    }
    if start == end {
        return None;
    }

    let prev = source
        .get(..start)
        .and_then(|prefix| prefix.as_bytes().last().copied());
    if matches!(prev, Some(b'.' | b':' | b'!')) {
        return None;
    }

    let name = source.get(start..end)?.trim();
    if name.is_empty() || is_rust_keyword(name) {
        return None;
    }

    Some((name.to_string(), start))
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

fn is_rust_keyword(name: &str) -> bool {
    matches!(
        name,
        "if" | "for"
            | "while"
            | "loop"
            | "match"
            | "return"
            | "let"
            | "fn"
            | "impl"
            | "async"
            | "await"
            | "move"
            | "in"
            | "where"
            | "else"
            | "mod"
            | "struct"
            | "enum"
            | "trait"
            | "use"
            | "pub"
            | "super"
            | "self"
            | "crate"
    )
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
        let statement_end = find_statement_end(source, close_paren + 1).unwrap_or(bytes.len());
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
            .and_then(extract_bound_var_from_prefix);
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

        let next = if hit.statement_end > hit.start {
            hit.statement_end
        } else {
            hit.close_paren + 1
        };
        if next <= cursor {
            cursor += 1;
        } else {
            cursor = next;
        }
    }

    out
}

fn collect_cte_aliases(chains: &[ScannedQailChain]) -> HashSet<String> {
    let mut cte_names = HashSet::new();
    for chain in chains {
        for call in scan_chain_method_calls(&chain.full_chain) {
            match call.name {
                "to_cte" => {
                    if let Some(name) = extract_string_arg(call.args) {
                        cte_names.insert(name);
                    }
                }
                "with" => {
                    let args = split_top_level_args(call.args);
                    if args.len() < 2 {
                        continue;
                    }
                    let Some(alias) = extract_string_arg(args[0]) else {
                        continue;
                    };
                    if args[1].trim_start().starts_with("Qail::") {
                        cte_names.insert(alias);
                    }
                }
                _ => {}
            }
        }
    }
    cte_names
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

fn build_param_substitutions(
    function: &LocalFunction,
    calls: &[LocalFunctionCall],
    function_name_counts: &HashMap<String, usize>,
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
            out.push(ParamSubstitutions { values });
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
    if !visited.insert(format!("s:{key}")) {
        return;
    }

    if let Some(substitutions) = substitutions
        && let Some(arg_expr) = substitutions.values.get(&key)
    {
        resolve_string_values_inner(arg_expr, Some(substitutions), bindings, visited, out);
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
    let direct = extract_array_string_literals_from_expr(expr);
    if !direct.is_empty() {
        out.extend(direct);
        return;
    }

    let Some(key) = binding_lookup_key(expr) else {
        return;
    };
    if !visited.insert(format!("a:{key}")) {
        return;
    }

    if let Some(substitutions) = substitutions
        && let Some(arg_expr) = substitutions.values.get(&key)
    {
        resolve_array_string_values_inner(arg_expr, Some(substitutions), bindings, visited, out);
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
    for (idx, _) in source.match_indices(&needle) {
        let before_ok = idx == 0 || !is_ident_byte(source.as_bytes()[idx - 1]);
        if !before_ok {
            continue;
        }
        let mut after = idx + needle.len();
        after = skip_ws(source.as_bytes(), after);
        if source.as_bytes().get(after).copied() == Some(b'(') {
            return true;
        }
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
    let literal_bindings = collect_literal_bindings(content);

    // ── File-level flags ─────────────────────────────────────────────
    // Detect SuperAdminToken::for_system_process() usage anywhere in file.
    // Files can opt out with `// qail:allow(super_admin)` comment.
    let file_has_allow_super_admin = source_has_allow_comment(content, "qail:allow(super_admin)");
    let file_uses_super_admin =
        !file_has_allow_super_admin && source_has_function_call(content, "for_system_process");

    let chains = collect_qail_chains(content);
    let execution_site_rls = collect_execution_site_rls_offsets(content);
    let file_cte_names = collect_cte_aliases(&chains);
    let local_functions = collect_local_functions(content);
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
        let has_explicit_tenant_scope = chain_has_explicit_tenant_scope(&chain.full_chain);
        let substitution_contexts = enclosing_function
            .map(|function| {
                build_param_substitutions(function, &local_function_calls, &function_name_counts)
            })
            .unwrap_or_default();

        let context_iter = if substitution_contexts.is_empty() {
            vec![None]
        } else {
            substitution_contexts.iter().map(Some).collect::<Vec<_>>()
        };
        let mut pushed = false;
        let mut seen_variants = HashSet::new();

        for substitutions in context_iter {
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

            let columns =
                extract_columns_with_bindings(&chain.full_chain, substitutions, &literal_bindings);
            let columns_key = columns.join("\x1f");

            for table in resolved_tables {
                let variant_key = format!("{table}\x1e{columns_key}");
                if !seen_variants.insert(variant_key) {
                    continue;
                }
                let is_cte_ref = file_cte_names.contains(&table);
                usages.push(QailUsage {
                    file: file.to_string(),
                    line: chain.line,
                    column: chain.column,
                    table,
                    is_dynamic_table: false,
                    columns: columns.clone(),
                    action: action.to_string(),
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

#[cfg_attr(not(test), allow(dead_code))]
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
            "filter" | "eq" | "ne" | "gt" | "lt" | "gte" | "lte" | "like" | "ilike"
            | "where_eq" | "order_by" | "order_desc" | "order_asc" | "in_vals" | "is_null"
            | "is_not_null" | "set_value" | "set_coalesce" | "set_coalesce_opt" => {
                for col in resolve_string_values(
                    extract_first_argument(call.args),
                    substitutions,
                    bindings,
                ) {
                    if !col.contains('.') {
                        columns.push(col);
                    }
                }
            }
            "returning" | "on_conflict_update" | "on_conflict_nothing" => {
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

fn chain_has_rls(chain: &str) -> bool {
    scan_chain_method_calls(chain)
        .into_iter()
        .any(|call| matches!(call.name, "with_rls" | "rls"))
}

fn chain_has_explicit_tenant_scope(chain: &str) -> bool {
    for call in scan_chain_method_calls(chain) {
        if !matches!(
            call.name,
            "eq" | "where_eq" | "is_null" | "set_value" | "set_coalesce" | "set_coalesce_opt"
        ) {
            continue;
        }
        if let Some(col) = extract_string_arg(extract_first_argument(call.args))
            && is_tenant_identifier(&col)
        {
            return true;
        }
    }
    false
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
        _ => Err(format!("unknown scanner action '{}'", action)),
    }
}

pub(crate) fn append_scanned_columns(cmd: &mut crate::ast::Qail, columns: &[String]) {
    use crate::ast::Expr;

    for col in columns {
        // Skip qualified columns (CTE refs like cte.column)
        if col.contains('.') {
            continue;
        }
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
