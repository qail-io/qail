//! Shared Rust lexical masking helpers used by build-time analyzers.

pub(super) fn starts_with_bytes(haystack: &[u8], idx: usize, needle: &[u8]) -> bool {
    haystack
        .get(idx..idx.saturating_add(needle.len()))
        .is_some_and(|s| s == needle)
}

pub(super) fn consume_block_comment(bytes: &[u8], start: usize) -> usize {
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

pub(super) fn consume_rust_literal(bytes: &[u8], start: usize) -> Option<usize> {
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

pub(super) fn mask_non_code(source: &str) -> String {
    let bytes = source.as_bytes();
    let mut out = bytes.to_vec();
    let mut i = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            let start = i;
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            for b in &mut out[start..i] {
                *b = b' ';
            }
            continue;
        }

        if starts_with_bytes(bytes, i, b"/*") {
            let start = i;
            i = consume_block_comment(bytes, i);
            for b in &mut out[start..i] {
                if *b != b'\n' {
                    *b = b' ';
                }
            }
            continue;
        }

        if let Some(next) = consume_rust_literal(bytes, i) {
            for b in &mut out[i..next] {
                if *b != b'\n' {
                    *b = b' ';
                }
            }
            i = next;
            continue;
        }

        i += 1;
    }

    String::from_utf8(out).unwrap_or_else(|_| source.to_string())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks_comment_and_literal_content() {
        let src = r#"
let _ = "query(SELECT 1)";
// comment with query_as!
/* block Qail::raw_sql */
let ok = 1;
"#;
        let masked = mask_non_code(src);
        assert!(!masked.contains("query(SELECT 1)"));
        assert!(!masked.contains("query_as!"));
        assert!(!masked.contains("Qail::raw_sql"));
        assert!(masked.contains("let ok = 1;"));
    }
}
