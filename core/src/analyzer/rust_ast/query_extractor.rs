//! Query pattern extractor using semantic text scanning.
//!
//! Detects database query calls and extracts:
//! - Full span of the entire call chain (for replacement)
//! - SQL string content
//! - Bind parameters with their expressions
//! - Return type (from turbofish)

#![allow(dead_code)] // Module under development, will be used by LSP

/// A detected database query call.
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

struct QueryFnPattern {
    full: &'static str,
    query_fn: &'static str,
    requires_colon_guard: bool,
}

const QUERY_PATTERNS: &[QueryFnPattern] = &[
    QueryFnPattern {
        full: "sqlx::query_scalar",
        query_fn: "query_scalar",
        requires_colon_guard: false,
    },
    QueryFnPattern {
        full: "sqlx::query_as",
        query_fn: "query_as",
        requires_colon_guard: false,
    },
    QueryFnPattern {
        full: "sqlx::query",
        query_fn: "query",
        requires_colon_guard: false,
    },
    QueryFnPattern {
        full: "query_scalar",
        query_fn: "query_scalar",
        requires_colon_guard: true,
    },
    QueryFnPattern {
        full: "query_as",
        query_fn: "query_as",
        requires_colon_guard: true,
    },
    QueryFnPattern {
        full: "query",
        query_fn: "query",
        requires_colon_guard: true,
    },
];

/// Detect database query calls in Rust source code.
pub fn detect_query_calls(source: &str) -> Vec<QueryCall> {
    let bytes = source.as_bytes();
    let line_starts = compute_line_starts(source);
    let mut out = Vec::new();

    let mut i = 0usize;
    while i < bytes.len() {
        let mut matched = false;

        for pat in QUERY_PATTERNS {
            if !starts_with(bytes, i, pat.full.as_bytes()) {
                continue;
            }
            if !is_valid_query_boundary(bytes, i, pat) {
                continue;
            }

            if let Some((call, end_offset)) = parse_query_call(source, &line_starts, i, pat) {
                out.push(call);
                i = end_offset;
                matched = true;
                break;
            }
        }

        if !matched {
            i += 1;
        }
    }

    out
}

fn parse_query_call(
    source: &str,
    line_starts: &[usize],
    start: usize,
    pat: &QueryFnPattern,
) -> Option<(QueryCall, usize)> {
    let bytes = source.as_bytes();

    let mut cursor = start + pat.full.len();
    cursor = skip_ws(bytes, cursor);

    let mut return_type = None;
    if starts_with(bytes, cursor, b"::") {
        let type_start = skip_ws(bytes, cursor + 2);
        if bytes.get(type_start).copied() == Some(b'<') {
            let end_angle = find_matching_delim(source, type_start, b'<', b'>')?;
            if let Some(args) = source.get(type_start + 1..end_angle) {
                return_type = extract_second_turbofish_type(args);
            }
            cursor = end_angle + 1;
            cursor = skip_ws(bytes, cursor);
        }
    }

    if bytes.get(cursor).copied() != Some(b'(') {
        return None;
    }

    let call_end = find_matching_delim(source, cursor, b'(', b')')?;
    let args = source.get(cursor + 1..call_end)?;
    let first_arg = extract_first_argument(args);
    let sql = extract_first_string_literal(first_arg)?;

    let await_pos = find_await_in_chain(source, call_end + 1)?;
    let binds = extract_bind_args(source.get(call_end + 1..await_pos).unwrap_or_default());

    let end_offset = await_pos + ".await".len();
    let (start_line, start_column) = offset_to_line_col(line_starts, start);
    let (end_line, end_column) = offset_to_line_col(line_starts, end_offset);

    Some((
        QueryCall {
            start_line,
            start_column,
            end_line,
            end_column,
            sql,
            binds,
            return_type,
            query_fn: pat.query_fn.to_string(),
        },
        end_offset,
    ))
}

fn is_valid_query_boundary(bytes: &[u8], start: usize, pat: &QueryFnPattern) -> bool {
    let prev = start.checked_sub(1).and_then(|idx| bytes.get(idx).copied());
    let next = bytes.get(start + pat.full.len()).copied();

    if let Some(prev) = prev
        && is_ident_byte(prev)
    {
        return false;
    }

    if pat.requires_colon_guard && prev == Some(b':') {
        return false;
    }

    if let Some(next) = next
        && is_ident_byte(next)
    {
        return false;
    }

    true
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn skip_ws(bytes: &[u8], mut idx: usize) -> usize {
    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }
    idx
}

fn starts_with(haystack: &[u8], idx: usize, needle: &[u8]) -> bool {
    haystack
        .get(idx..idx.saturating_add(needle.len()))
        .is_some_and(|s| s == needle)
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

fn find_matching_delim(source: &str, open_idx: usize, open: u8, close: u8) -> Option<usize> {
    let bytes = source.as_bytes();
    if bytes.get(open_idx).copied() != Some(open) {
        return None;
    }

    let mut depth = 1usize;
    let mut i = open_idx + 1;

    while i < bytes.len() {
        if starts_with(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }

        if starts_with(bytes, i, b"/*") {
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

fn find_await_in_chain(source: &str, start: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let mut i = start;
    let mut paren = 0usize;
    let mut bracket = 0usize;
    let mut brace = 0usize;

    while i < bytes.len() {
        if starts_with(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }

        if starts_with(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }

        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        if paren == 0 && bracket == 0 && brace == 0 {
            if starts_with(bytes, i, b".await") {
                return Some(i);
            }
            if bytes[i] == b';' {
                break;
            }
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

fn consume_block_comment(bytes: &[u8], start: usize) -> usize {
    let mut i = start + 2;
    let mut depth = 1usize;

    while i < bytes.len() && depth > 0 {
        if starts_with(bytes, i, b"/*") {
            depth += 1;
            i += 2;
        } else if starts_with(bytes, i, b"*/") {
            depth = depth.saturating_sub(1);
            i += 2;
        } else {
            i += 1;
        }
    }

    i
}

fn consume_rust_literal(bytes: &[u8], start: usize) -> Option<usize> {
    if let Some((_, _, hashes)) = raw_string_prefix(bytes, start) {
        let content_start = raw_string_prefix(bytes, start).map(|(_, cs, _)| cs)?;
        let end_quote = find_raw_string_end(bytes, content_start, hashes)?;
        return Some(end_quote + 1 + hashes);
    }

    if bytes.get(start).copied() == Some(b'"') || starts_with(bytes, start, b"b\"") {
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

fn extract_first_argument(args: &str) -> &str {
    let bytes = args.as_bytes();
    let mut i = 0usize;
    let mut paren = 0usize;
    let mut bracket = 0usize;
    let mut brace = 0usize;

    while i < bytes.len() {
        if starts_with(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with(bytes, i, b"/*") {
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

fn extract_first_string_literal(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        if starts_with(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if starts_with(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }

        if let Some((_, content_start, hashes)) = raw_string_prefix(bytes, i)
            && let Some(end_quote) = find_raw_string_end(bytes, content_start, hashes)
            && let Some(raw) = input.get(content_start..end_quote)
        {
            return Some(raw.to_string());
        }

        if bytes.get(i).copied() == Some(b'"') || starts_with(bytes, i, b"b\"") {
            let quote_offset = if bytes.get(i).copied() == Some(b'"') {
                i
            } else {
                i + 1
            };

            let mut j = quote_offset + 1;
            while j < bytes.len() {
                if bytes[j] == b'\\' {
                    j = (j + 2).min(bytes.len());
                    continue;
                }
                if bytes[j] == b'"' {
                    if let Some(raw) = input.get(quote_offset + 1..j) {
                        return Some(unescape_rust_string(raw));
                    }
                    return None;
                }
                j += 1;
            }
            return None;
        }

        i += 1;
    }

    None
}

fn extract_bind_args(chain: &str) -> Vec<String> {
    let bytes = chain.as_bytes();
    let mut out = Vec::new();
    let mut cursor = 0usize;

    while let Some(pos) = chain.get(cursor..).and_then(|s| s.find(".bind")) {
        let abs = cursor + pos;
        let after_name = abs + ".bind".len();

        if bytes.get(after_name).is_some_and(|b| is_ident_byte(*b)) {
            cursor = after_name;
            continue;
        }

        let open_idx = skip_ws(bytes, after_name);
        if bytes.get(open_idx).copied() != Some(b'(') {
            cursor = after_name;
            continue;
        }

        let Some(close_idx) = find_matching_delim(chain, open_idx, b'(', b')') else {
            cursor = open_idx + 1;
            continue;
        };

        if let Some(arg) = chain.get(open_idx + 1..close_idx) {
            let arg = arg.trim();
            if !arg.is_empty() {
                out.push(arg.to_string());
            }
        }

        cursor = close_idx + 1;
    }

    out
}

fn extract_second_turbofish_type(args: &str) -> Option<String> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut paren = 0usize;
    let mut bracket = 0usize;
    let mut brace = 0usize;
    let mut angle = 0usize;

    let bytes = args.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        match bytes[i] {
            b'(' => paren += 1,
            b')' => paren = paren.saturating_sub(1),
            b'[' => bracket += 1,
            b']' => bracket = bracket.saturating_sub(1),
            b'{' => brace += 1,
            b'}' => brace = brace.saturating_sub(1),
            b'<' => angle += 1,
            b'>' => angle = angle.saturating_sub(1),
            b',' if paren == 0 && bracket == 0 && brace == 0 && angle == 0 => {
                if let Some(part) = args.get(start..i) {
                    parts.push(part.trim().to_string());
                }
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }

    if let Some(part) = args.get(start..) {
        parts.push(part.trim().to_string());
    }

    parts
        .get(1)
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
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
            Some('"') => out.push('"'),
            Some('\\') => out.push('\\'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }

    out
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
        assert_eq!(calls[0].return_type.as_deref(), Some("User"));
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

    #[test]
    fn ignores_non_awaited_query_chains() {
        let code = r#"
            async fn test() {
                let q = sqlx::query("SELECT 1").bind(id);
                let _ = q;
            }
        "#;

        let calls = detect_query_calls(code);
        assert!(calls.is_empty(), "calls: {calls:?}");
    }
}
