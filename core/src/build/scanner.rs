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
                        full_expr.push(' ');
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
                    full_line.push(' ');
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
    line: usize,
    column: usize,
    action: &'static str,
    first_arg: String,
    full_chain: String,
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
        let args = source
            .get(hit.open_paren + 1..hit.close_paren)
            .unwrap_or_default();
        let first_arg = extract_first_argument(args).to_string();
        let full_chain = source.get(hit.start..hit.statement_end).unwrap_or_default();
        let (line, column0) = offset_to_line_col(&line_starts, hit.start);
        out.push(ScannedQailChain {
            line,
            column: column0 + 1,
            action: hit.action,
            first_arg,
            full_chain: full_chain.to_string(),
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

pub(crate) fn scan_file(file: &str, content: &str, usages: &mut Vec<QailUsage>) {
    scan_file_inner(file, content, usages, true);
}

#[cfg(feature = "analyzer")]
pub(crate) fn scan_file_silent(file: &str, content: &str, usages: &mut Vec<QailUsage>) {
    scan_file_inner(file, content, usages, false);
}

fn scan_file_inner(file: &str, content: &str, usages: &mut Vec<QailUsage>, emit_warnings: bool) {
    // Phase 1+2: Collect let-bindings that resolve variable → string literal(s)
    let let_bindings = collect_let_bindings(content);

    // ── File-level flags ─────────────────────────────────────────────
    // Detect SuperAdminToken::for_system_process() usage anywhere in file.
    // Files can opt out with `// qail:allow(super_admin)` comment.
    let file_has_allow_super_admin = source_has_allow_comment(content, "qail:allow(super_admin)");
    let file_uses_super_admin =
        !file_has_allow_super_admin && source_has_function_call(content, "for_system_process");

    let chains = collect_qail_chains(content);
    let file_cte_names = collect_cte_aliases(&chains);

    for chain in chains {
        let action = chain.action;
        let table = if action == "TYPED" {
            extract_typed_table_arg(&chain.first_arg)
        } else {
            extract_string_arg(&chain.first_arg)
        };

        if action == "RAW" {
            if emit_warnings {
                println!(
                    "cargo:warning=QAIL: raw SQL at {}:{} — not schema-validated",
                    file, chain.line
                );
            }
            continue;
        }

        let has_rls = chain_has_rls(&chain.full_chain);
        let has_explicit_tenant_scope = chain_has_explicit_tenant_scope(&chain.full_chain);
        let columns = extract_columns(&chain.full_chain);

        if let Some(table) = table {
            let is_cte_ref = file_cte_names.contains(&table);
            usages.push(QailUsage {
                file: file.to_string(),
                line: chain.line,
                column: chain.column,
                table,
                is_dynamic_table: false,
                columns,
                action: action.to_string(),
                is_cte_ref,
                has_rls,
                has_explicit_tenant_scope,
                file_uses_super_admin,
            });
        } else if action != "TYPED" {
            let var_hint = if chain.first_arg.trim().is_empty() {
                "?"
            } else {
                chain.first_arg.trim()
            };
            let lookup_key = var_hint.rsplit('.').next().unwrap_or(var_hint).trim();

            if let Some(resolved_tables) = let_bindings.get(lookup_key) {
                for resolved_table in resolved_tables {
                    let is_cte_ref = file_cte_names.contains(resolved_table);
                    usages.push(QailUsage {
                        file: file.to_string(),
                        line: chain.line,
                        column: chain.column,
                        table: resolved_table.clone(),
                        is_dynamic_table: false,
                        columns: columns.clone(),
                        action: action.to_string(),
                        is_cte_ref,
                        has_rls,
                        has_explicit_tenant_scope,
                        file_uses_super_admin,
                    });
                }
            } else if emit_warnings {
                println!(
                    "cargo:warning=Qail: dynamic table name `{}` in {}:{} — cannot validate columns at build time. Consider using string literals.",
                    var_hint, file, chain.line
                );
            }
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

pub(crate) fn extract_columns(line: &str) -> Vec<String> {
    let calls = scan_chain_method_calls(line);
    let mut columns = Vec::new();
    let mut aliases = HashSet::new();

    for call in &calls {
        if call.name == "alias"
            && let Some(name) = extract_string_arg(extract_first_argument(call.args))
        {
            aliases.insert(name);
        }
    }

    for call in calls {
        match call.name {
            "column" => {
                if let Some(col) = extract_string_arg(extract_first_argument(call.args)) {
                    columns.push(col);
                }
            }
            "columns" => {
                columns.extend(extract_array_string_literals_from_expr(
                    extract_first_argument(call.args),
                ));
            }
            "filter" | "eq" | "ne" | "gt" | "lt" | "gte" | "lte" | "like" | "ilike"
            | "where_eq" | "order_by" | "order_desc" | "order_asc" | "in_vals" | "is_null"
            | "is_not_null" | "set_value" | "set_coalesce" | "set_coalesce_opt" => {
                if let Some(col) = extract_string_arg(extract_first_argument(call.args))
                    && !col.contains('.')
                {
                    columns.push(col);
                }
            }
            "returning" | "on_conflict_update" | "on_conflict_nothing" => {
                for col in
                    extract_array_string_literals_from_expr(extract_first_argument(call.args))
                {
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
        if !matches!(call.name, "eq" | "where_eq" | "is_null") {
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

pub(crate) fn usage_action_to_ast(action: &str) -> crate::ast::Action {
    use crate::ast::Action;

    match action {
        "GET" | "TYPED" => Action::Get,
        "ADD" => Action::Add,
        "SET" => Action::Set,
        "DEL" => Action::Del,
        "PUT" => Action::Put,
        _ => Action::Get,
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
