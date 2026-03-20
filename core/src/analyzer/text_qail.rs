//! Shared helpers for recognizing QAIL text queries in source files.

/// Action keywords that can begin a QAIL text query.
pub const QAIL_ACTION_PREFIXES: [&str; 7] = ["get", "set", "add", "del", "with", "make", "mod"];

/// String literal extracted from a text source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextLiteral {
    pub text: String,
    /// 1-based source line where literal content starts (after opening quote).
    pub start_line: usize,
    /// 1-based source column where literal content starts (after opening quote).
    pub start_column: usize,
    /// 1-based source line where literal content ends (position before closing quote).
    pub end_line: usize,
    /// 1-based source column where literal content ends (position before closing quote).
    pub end_column: usize,
}

/// Best-effort classifier for whether text begins with a QAIL action keyword.
pub fn looks_like_qail_query(text: &str) -> bool {
    let head = text
        .trim_start()
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();

    QAIL_ACTION_PREFIXES.contains(&head.as_str())
}

/// Best-effort classifier for whether text begins like a SQL query.
pub fn looks_like_sql_query(text: &str) -> bool {
    let head = text
        .trim_start()
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();

    matches!(
        head.as_str(),
        "select" | "insert" | "update" | "delete" | "with"
    )
}

/// Trim a query payload and remove a trailing semicolon, returning byte bounds.
///
/// Returned bounds are `(start, end)` over the original input string.
pub fn trim_query_bounds(text: &str) -> Option<(usize, usize)> {
    let start = first_non_ws_idx(text)?;
    let mut end = trim_end_ws_idx(text, text.len());
    if end <= start {
        return None;
    }

    if text[..end].ends_with(';') {
        end = trim_end_ws_idx(text, end - 1);
    }

    if end <= start {
        None
    } else {
        Some((start, end))
    }
}

/// Convert an offset inside `literal.text` to absolute (line, column), both 1-based.
pub fn literal_offset_to_line_col(literal: &TextLiteral, offset: usize) -> (usize, usize) {
    let capped = offset.min(literal.text.len());
    let mut rel_line = 0usize;
    let mut rel_col = 0usize;

    for ch in literal.text[..capped].chars() {
        if ch == '\n' {
            rel_line += 1;
            rel_col = 0;
        } else {
            rel_col += 1;
        }
    }

    let line = literal.start_line + rel_line;
    let col = if rel_line == 0 {
        literal.start_column + rel_col
    } else {
        1 + rel_col
    };

    (line, col)
}

/// Extract quoted literals from a source file while ignoring line comments.
///
/// Supports:
/// - Single-quoted strings (`'...'`)
/// - Double-quoted strings (`"..."`)
/// - Backtick strings (`` `...` ``)
/// - Triple-quoted strings (`'''...'''`, `"""..."""`)
pub fn extract_text_literals(content: &str) -> Vec<TextLiteral> {
    let bytes = content.as_bytes();
    let mut out = Vec::new();

    let mut i = 0usize;
    let mut line = 1usize;
    let mut col = 1usize;

    while i < bytes.len() {
        if bytes[i] == b'#'
            || starts_with_bytes(bytes, i, b"//")
            || starts_with_bytes(bytes, i, b"--")
        {
            while i < bytes.len() && bytes[i] != b'\n' {
                advance_byte(bytes[i], &mut line, &mut col);
                i += 1;
            }
            continue;
        }

        let quote = bytes[i];
        if !matches!(quote, b'\'' | b'"' | b'`') {
            advance_byte(quote, &mut line, &mut col);
            i += 1;
            continue;
        }

        let is_triple =
            quote != b'`' && i + 2 < bytes.len() && bytes[i + 1] == quote && bytes[i + 2] == quote;

        if is_triple {
            for _ in 0..3 {
                advance_byte(bytes[i], &mut line, &mut col);
                i += 1;
            }

            let start_line = line;
            let start_column = col;
            let start_idx = i;
            let mut closed = false;

            while i < bytes.len() {
                if i + 2 < bytes.len()
                    && bytes[i] == quote
                    && bytes[i + 1] == quote
                    && bytes[i + 2] == quote
                {
                    let end_line = line;
                    let end_column = col;
                    if let Some(text) = content.get(start_idx..i) {
                        out.push(TextLiteral {
                            text: text.to_string(),
                            start_line,
                            start_column,
                            end_line,
                            end_column,
                        });
                    }

                    for _ in 0..3 {
                        advance_byte(bytes[i], &mut line, &mut col);
                        i += 1;
                    }
                    closed = true;
                    break;
                }

                advance_byte(bytes[i], &mut line, &mut col);
                i += 1;
            }

            if !closed {
                break;
            }

            continue;
        }

        advance_byte(bytes[i], &mut line, &mut col);
        i += 1;

        let start_line = line;
        let start_column = col;
        let start_idx = i;
        let mut closed = false;
        let mut escaped = false;

        while i < bytes.len() {
            let b = bytes[i];
            if escaped {
                escaped = false;
                advance_byte(b, &mut line, &mut col);
                i += 1;
                continue;
            }

            if b == b'\\' {
                escaped = true;
                advance_byte(b, &mut line, &mut col);
                i += 1;
                continue;
            }

            if b == quote {
                let end_line = line;
                let end_column = col;
                if let Some(text) = content.get(start_idx..i) {
                    out.push(TextLiteral {
                        text: text.to_string(),
                        start_line,
                        start_column,
                        end_line,
                        end_column,
                    });
                }

                advance_byte(b, &mut line, &mut col);
                i += 1;
                closed = true;
                break;
            }

            advance_byte(b, &mut line, &mut col);
            i += 1;
        }

        if !closed {
            break;
        }
    }

    out
}

/// Extract a probable QAIL candidate from a single source line.
///
/// Returns the start column and extracted query text.
pub fn extract_qail_candidate_from_line(line: &str) -> Option<(usize, String)> {
    let start = find_qail_start(line)?;
    let rest = line.get(start..)?;

    if start > 0
        && let Some(prev) = line[..start].chars().next_back()
        && matches!(prev, '"' | '\'' | '`')
        && let Some(end) = find_quote_terminator(rest, prev)
    {
        return Some((start, rest[..end].trim().to_string()));
    }

    Some((start, rest.trim().trim_end_matches(';').to_string()))
}

fn first_non_ws_idx(text: &str) -> Option<usize> {
    text.char_indices()
        .find_map(|(idx, ch)| (!ch.is_whitespace()).then_some(idx))
}

fn trim_end_ws_idx(text: &str, mut end: usize) -> usize {
    while end > 0 {
        let ch = text[..end].chars().next_back().unwrap_or('\0');
        if !ch.is_whitespace() {
            break;
        }
        end -= ch.len_utf8();
    }
    end
}

fn advance_byte(b: u8, line: &mut usize, col: &mut usize) {
    if b == b'\n' {
        *line += 1;
        *col = 1;
    } else {
        *col += 1;
    }
}

fn find_qail_start(line: &str) -> Option<usize> {
    for (idx, _) in line.char_indices() {
        let before_ok = if idx == 0 {
            true
        } else {
            let ch = line[..idx].chars().next_back().unwrap_or(' ');
            !is_ident_char(ch)
        };
        if !before_ok {
            continue;
        }

        for action in QAIL_ACTION_PREFIXES {
            let Some(tail) = line.get(idx..) else {
                continue;
            };
            if !tail.starts_with(action) {
                continue;
            }

            let after = idx + action.len();
            let Some(next) = line[after..].chars().next() else {
                continue;
            };
            if next.is_whitespace() {
                return Some(idx);
            }
        }
    }

    None
}

fn is_ident_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn find_quote_terminator(input: &str, quote: char) -> Option<usize> {
    let mut escaped = false;
    for (idx, ch) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }

        if ch == '\\' {
            escaped = true;
            continue;
        }

        if ch == quote {
            return Some(idx);
        }
    }
    None
}

fn starts_with_bytes(haystack: &[u8], idx: usize, needle: &[u8]) -> bool {
    haystack
        .get(idx..idx.saturating_add(needle.len()))
        .is_some_and(|s| s == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_candidate_in_quotes() {
        let line = r#"const q = "get users fields id";"#;
        let (col, query) = extract_qail_candidate_from_line(line).expect("candidate");
        assert_eq!(col, 11);
        assert_eq!(query, "get users fields id");
    }

    #[test]
    fn classifier_matches_qail_actions_only() {
        assert!(looks_like_qail_query("get users fields id"));
        assert!(looks_like_qail_query(
            "  with a as (get users fields id) get a fields id"
        ));
        assert!(!looks_like_qail_query("SELECT id FROM users"));
    }

    #[test]
    fn classifier_matches_sql_heads() {
        assert!(looks_like_sql_query("SELECT id FROM users"));
        assert!(looks_like_sql_query("with x as (select 1) select * from x"));
        assert!(!looks_like_sql_query("get users fields id"));
    }

    #[test]
    fn trim_bounds_drops_ws_and_semicolon() {
        let text = " \n  get users fields id ; \n";
        let (start, end) = trim_query_bounds(text).expect("trim bounds");
        assert_eq!(&text[start..end], "get users fields id");
    }

    #[test]
    fn extract_literals_ignores_comments_and_supports_multiline() {
        let src = r#"
// "get users fields id" should not be extracted
const q = `
  get users
  fields id, email
`;
const sql = "SELECT id FROM users";
"#;

        let literals = extract_text_literals(src);
        assert_eq!(literals.len(), 2);
        assert!(literals[0].text.contains("get users"));
        assert!(literals[1].text.contains("SELECT id FROM users"));
    }

    #[test]
    fn literal_offset_maps_to_absolute_line_col() {
        let src = "const q = `get\nusers`;\n";
        let literals = extract_text_literals(src);
        let lit = literals.first().expect("literal");
        let offset = lit.text.find("users").expect("users offset");
        let (line, col) = literal_offset_to_line_col(lit, offset);
        assert_eq!(line, 2);
        assert_eq!(col, 1);
    }
}
