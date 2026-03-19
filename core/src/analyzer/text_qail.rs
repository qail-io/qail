//! Shared helpers for recognizing QAIL text queries in source files.

/// Action keywords that can begin a QAIL text query.
pub const QAIL_ACTION_PREFIXES: [&str; 7] = ["get", "set", "add", "del", "with", "make", "mod"];

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

/// Extract a probable QAIL candidate from a source line.
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

/// Strip trailing line comments while respecting quoted strings.
pub fn strip_text_line_comment(line: &str) -> &str {
    let bytes = line.as_bytes();
    let mut i = 0usize;
    let mut in_quote: Option<u8> = None;
    let mut escaped = false;

    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = in_quote {
            if escaped {
                escaped = false;
                i += 1;
                continue;
            }
            if b == b'\\' {
                escaped = true;
                i += 1;
                continue;
            }
            if b == q {
                in_quote = None;
            }
            i += 1;
            continue;
        }

        if matches!(b, b'"' | b'\'' | b'`') {
            in_quote = Some(b);
            i += 1;
            continue;
        }

        if b == b'#' || starts_with_bytes(bytes, i, b"//") || starts_with_bytes(bytes, i, b"--") {
            return line.get(..i).unwrap_or(line);
        }

        i += 1;
    }

    line
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
    fn strip_comments_ignores_markers_inside_quotes() {
        let line = r#"const q = "get users fields id // literal"; // comment"#;
        let stripped = strip_text_line_comment(line);
        assert_eq!(stripped, r#"const q = "get users fields id // literal"; "#);
    }

    #[test]
    fn classifier_matches_qail_actions_only() {
        assert!(looks_like_qail_query("get users fields id"));
        assert!(looks_like_qail_query(
            "  with a as (get users fields id) get a fields id"
        ));
        assert!(!looks_like_qail_query("SELECT id FROM users"));
    }
}
