//! QAIL Codegen — Rust AST → Zig type definition generator.
//!
//! Reads Rust source files from `core/src/ast/` and generates
//! equivalent Zig struct/enum definitions in `qail-zig/src/ast/generated/`.

use std::fmt::Write;
use std::path::Path;

/// Rust → Zig type mapping.
fn map_type(ty: &str) -> String {
    // Strip outer whitespace
    let ty = ty.trim();

    // Handle Option<Box<T>> → ?*const T (not ??*const T)
    if let Some(inner) = strip_angle("Option", ty) {
        if let Some(box_inner) = strip_angle("Box", inner) {
            let mapped = map_type(box_inner);
            return format!("?*const {}", mapped);
        }
        let mapped = map_type(inner);
        // Don't double-wrap optionals
        if mapped.starts_with('?') {
            return mapped;
        }
        return format!("?{}", mapped);
    }

    // Handle Vec<T>
    if let Some(inner) = strip_angle("Vec", ty) {
        let mapped = map_type(inner);
        return format!("[]const {}", mapped);
    }

    // Handle Box<T>
    if let Some(inner) = strip_angle("Box", ty) {
        let mapped = map_type(inner);
        return format!("?*const {}", mapped);
    }

    // Handle tuples (A, B)
    if ty.starts_with('(') && ty.ends_with(')') {
        let inner = &ty[1..ty.len() - 1];
        let parts: Vec<&str> = split_top_level(inner);
        let fields: Vec<String> = parts
            .iter()
            .enumerate()
            .map(|(i, p)| format!("    f{}: {},", i, map_type(p.trim())))
            .collect();
        return format!("struct {{\n{}\n}}", fields.join("\n"));
    }

    // Strip crate:: paths AFTER generic unwrapping
    // (e.g. "crate :: ast :: Expr" → "Expr")
    if ty.contains("::") {
        let cleaned: String = ty.replace(' ', "");
        if let Some(last) = cleaned.rsplit("::").next() {
            return map_type(last);
        }
    }

    // Primitives
    match ty {
        "String" => "[]const u8".to_string(),
        "bool" => "bool".to_string(),
        "i32" => "i32".to_string(),
        "i64" => "i64".to_string(),
        "u64" => "u64".to_string(),
        "usize" => "usize".to_string(),
        "f32" => "f32".to_string(),
        "f64" => "f64".to_string(),
        "u8" => "u8".to_string(),
        "Uuid" => "[16]u8".to_string(),
        // Self-referential types
        "Qail" => "QailCmd".to_string(),
        // Known AST types — keep as-is (Zig names match)
        _ => ty.to_string(),
    }
}

/// Strip `Wrapper<inner>` and return inner.
fn strip_angle<'a>(prefix: &str, ty: &'a str) -> Option<&'a str> {
    if ty.starts_with(prefix) && ty.ends_with('>') {
        let start = prefix.len() + 1; // skip "Prefix<"
        let inner = &ty[start..ty.len() - 1];
        Some(inner.trim())
    } else {
        None
    }
}

/// Split on commas, respecting nested angle brackets.
fn split_top_level(s: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (i, ch) in s.char_indices() {
        match ch {
            '<' | '(' => depth += 1,
            '>' | ')' => depth -= 1,
            ',' if depth == 0 => {
                result.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    result.push(&s[start..]);
    result
}

/// Convert Rust PascalCase to Zig snake_case.
fn to_snake_case(name: &str) -> String {
    let mut result = String::new();
    for (i, ch) in name.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(ch.to_lowercase().next().unwrap());
        } else {
            result.push(ch);
        }
    }
    result
}

/// Get the default value for a Zig type.
fn zig_default(zig_type: &str, field_name: &str) -> Option<String> {
    if zig_type.starts_with("?") {
        Some("null".to_string())
    } else if zig_type == "[]const u8" {
        // String fields: default to empty string
        // Exception: "table" and "name" are required identifiers, no default
        if field_name == "table" || field_name == "name" {
            None
        } else {
            Some("\"\"".to_string())
        }
    } else if zig_type.starts_with("[]const ") {
        Some("&.{}".to_string())
    } else if zig_type == "bool" {
        Some("false".to_string())
    } else {
        None
    }
}

// ============================================================================
// Parsing (custom, syn-free)
// ============================================================================

/// A parsed Rust enum.
struct RustEnum {
    name: String,
    doc: String,
    variants: Vec<EnumVariant>,
}

struct EnumVariant {
    name: String,
    doc: String,
    fields: Vec<StructField>,
}

/// A parsed Rust struct.
struct RustStruct {
    name: String,
    doc: String,
    fields: Vec<StructField>,
}

struct StructField {
    name: String,
    doc: String,
    ty: String,
}

/// Parse a single Rust source file and extract enums & structs.
fn parse_file(path: &Path) -> (Vec<RustEnum>, Vec<RustStruct>) {
    let source = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("Cannot read {}: {}", path.display(), e));
    let mut enums = Vec::new();
    let mut structs = Vec::new();

    let mut pos = 0usize;
    let mut brace_depth = 0i32;
    let mut pending_doc: Vec<String> = Vec::new();

    while pos < source.len() {
        let line_end = source[pos..]
            .find('\n')
            .map(|off| pos + off)
            .unwrap_or(source.len());
        let line = &source[pos..line_end];
        let trimmed = line.trim_start();

        if brace_depth == 0 {
            if let Some(doc_line) = trimmed.strip_prefix("///") {
                pending_doc.push(doc_line.trim().to_string());
                pos = next_line_start(&source, line_end);
                continue;
            }

            if trimmed.starts_with("#[") {
                pos = next_line_start(&source, line_end);
                continue;
            }

            if trimmed.is_empty() {
                pending_doc.clear();
                pos = next_line_start(&source, line_end);
                continue;
            }

            let kind = if trimmed.starts_with("pub enum ") {
                Some(ItemKind::Enum)
            } else if trimmed.starts_with("pub struct ") {
                Some(ItemKind::Struct)
            } else {
                None
            };

            if let Some(kind) = kind {
                let item_start = pos + (line.len() - trimmed.len());
                let doc = join_doc_lines(&pending_doc);
                pending_doc.clear();
                let parsed = parse_pub_item(&source, item_start, kind, doc).unwrap_or_else(|| {
                    panic!("Cannot parse item at {}:{}", path.display(), item_start)
                });
                let parsed_end = parsed.end_idx;

                match parsed.kind {
                    ItemKind::Enum => enums.push(parse_enum_item(parsed)),
                    ItemKind::Struct => structs.push(parse_struct_item(parsed)),
                }

                pos = next_line_start(&source, parsed_end);
                brace_depth = 0;
                continue;
            }

            pending_doc.clear();
        }

        brace_depth += brace_delta(line);
        pos = next_line_start(&source, line_end);
    }

    (enums, structs)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ItemKind {
    Enum,
    Struct,
}

#[derive(Debug, Clone)]
struct ParsedItem {
    kind: ItemKind,
    name: String,
    doc: String,
    body: String,
    end_idx: usize,
}

fn parse_pub_item(
    source: &str,
    item_start: usize,
    kind: ItemKind,
    doc: String,
) -> Option<ParsedItem> {
    let prefix = match kind {
        ItemKind::Enum => "pub enum",
        ItemKind::Struct => "pub struct",
    };
    let after_prefix = source.get(item_start..)?.strip_prefix(prefix)?;
    let mut cursor = item_start + (source.get(item_start..)?.len() - after_prefix.len());
    cursor = skip_ascii_ws(source, cursor);
    let name = parse_ident_from(source, &mut cursor)?;
    let open_brace = find_char_outside_comments(source, cursor, b'{')?;
    let close_brace = find_matching_brace(source, open_brace)?;
    let body = source.get(open_brace + 1..close_brace)?.to_string();

    let mut end_idx = close_brace + 1;
    while end_idx < source.len() {
        let b = source.as_bytes()[end_idx];
        if b.is_ascii_whitespace() {
            end_idx += 1;
            continue;
        }
        if b == b';' {
            end_idx += 1;
        }
        break;
    }

    Some(ParsedItem {
        kind,
        name,
        doc,
        body,
        end_idx,
    })
}

fn parse_enum_item(parsed: ParsedItem) -> RustEnum {
    let variants = split_top_level_entries(&parsed.body, ',')
        .into_iter()
        .filter_map(parse_enum_variant)
        .collect();

    RustEnum {
        name: parsed.name,
        doc: parsed.doc,
        variants,
    }
}

fn parse_struct_item(parsed: ParsedItem) -> RustStruct {
    RustStruct {
        name: parsed.name,
        doc: parsed.doc,
        fields: parse_named_fields(&parsed.body),
    }
}

fn parse_enum_variant(entry: &str) -> Option<EnumVariant> {
    let (doc, core) = split_doc_prefix(entry);
    if core.is_empty() {
        return None;
    }

    let mut cursor = 0usize;
    let name = parse_ident_from(&core, &mut cursor)?;
    let rest = core[cursor..].trim_start();

    let fields = if rest.starts_with('{') {
        let (inside, _) = extract_enclosed_content(rest, '{', '}')?;
        parse_named_fields(inside)
    } else if rest.starts_with('(') {
        let (inside, _) = extract_enclosed_content(rest, '(', ')')?;
        split_top_level_entries(inside, ',')
            .into_iter()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .enumerate()
            .map(|(idx, ty)| StructField {
                name: format!("f{}", idx),
                doc: String::new(),
                ty: normalize_type(ty),
            })
            .collect()
    } else {
        Vec::new()
    };

    Some(EnumVariant { name, doc, fields })
}

fn parse_named_fields(body: &str) -> Vec<StructField> {
    split_top_level_entries(body, ',')
        .into_iter()
        .filter_map(|entry| {
            let (doc, core) = split_doc_prefix(entry);
            if core.is_empty() {
                return None;
            }

            let core = strip_visibility_prefix(core.trim());
            let colon_idx = find_top_level_char(core, ':')?;
            let name = core[..colon_idx].trim();
            let ty = normalize_type(core[colon_idx + 1..].trim());

            if name.is_empty() || ty.is_empty() {
                None
            } else {
                Some(StructField {
                    name: name.to_string(),
                    doc,
                    ty,
                })
            }
        })
        .collect()
}

fn split_doc_prefix(entry: &str) -> (String, String) {
    let mut docs = Vec::new();
    let mut body_lines = Vec::new();
    let mut in_prefix = true;

    for line in entry.lines() {
        let trimmed = line.trim_start();
        if in_prefix && trimmed.starts_with("///") {
            docs.push(trimmed.trim_start_matches("///").trim().to_string());
            continue;
        }
        if in_prefix && (trimmed.starts_with("#[") || trimmed.is_empty()) {
            continue;
        }
        in_prefix = false;
        body_lines.push(line);
    }

    (
        join_doc_lines(&docs),
        body_lines.join("\n").trim().to_string(),
    )
}

fn strip_visibility_prefix(mut s: &str) -> &str {
    s = s.trim_start();
    if let Some(rest) = s.strip_prefix("pub ") {
        return rest.trim_start();
    }
    if let Some(rest) = s.strip_prefix("pub(")
        && let Some(close) = rest.find(')')
    {
        return rest[close + 1..].trim_start();
    }
    s
}

fn normalize_type(raw: &str) -> String {
    raw.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .replace(" < ", "<")
        .replace("< ", "<")
        .replace(" <", "<")
        .replace(" > ", ">")
        .replace("> ", ">")
        .replace(" >", ">")
        .replace(" :: ", "::")
        .replace(":: ", "::")
        .replace(" ::", "::")
}

fn split_top_level_entries(text: &str, delim: char) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut paren = 0i32;
    let mut bracket = 0i32;
    let mut brace = 0i32;
    let mut angle = 0i32;
    let mut in_string = false;
    let mut in_line_comment = false;
    let mut block_comment_depth = 0usize;
    let bytes = text.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        if in_line_comment {
            if bytes[i] == b'\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }

        if block_comment_depth > 0 {
            if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
                block_comment_depth += 1;
                i += 2;
                continue;
            }
            if i + 1 < bytes.len() && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                block_comment_depth = block_comment_depth.saturating_sub(1);
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }

        if in_string {
            if bytes[i] == b'\\' {
                i = (i + 2).min(bytes.len());
                continue;
            }
            if bytes[i] == b'"' {
                in_string = false;
            }
            i += 1;
            continue;
        }

        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'/' {
            in_line_comment = true;
            i += 2;
            continue;
        }

        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            block_comment_depth = 1;
            i += 2;
            continue;
        }

        match bytes[i] {
            b'"' => {
                in_string = true;
            }
            b'(' => paren += 1,
            b')' => paren -= 1,
            b'[' => bracket += 1,
            b']' => bracket -= 1,
            b'{' => brace += 1,
            b'}' => brace -= 1,
            b'<' => angle += 1,
            b'>' => angle -= 1,
            _ => {}
        }

        if text.as_bytes()[i] == delim as u8
            && paren == 0
            && bracket == 0
            && brace == 0
            && angle == 0
        {
            out.push(&text[start..i]);
            start = i + delim.len_utf8();
        }

        i += 1;
    }

    out.push(&text[start..]);
    out
}

fn find_top_level_char(text: &str, target: char) -> Option<usize> {
    let mut paren = 0i32;
    let mut bracket = 0i32;
    let mut brace = 0i32;
    let mut angle = 0i32;
    let mut in_string = false;
    let bytes = text.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        if in_string {
            if bytes[i] == b'\\' {
                i = (i + 2).min(bytes.len());
                continue;
            }
            if bytes[i] == b'"' {
                in_string = false;
            }
            i += 1;
            continue;
        }

        match bytes[i] {
            b'"' => in_string = true,
            b'(' => paren += 1,
            b')' => paren -= 1,
            b'[' => bracket += 1,
            b']' => bracket -= 1,
            b'{' => brace += 1,
            b'}' => brace -= 1,
            b'<' => angle += 1,
            b'>' => angle -= 1,
            _ => {}
        }

        if bytes[i] == target as u8 && paren == 0 && bracket == 0 && brace == 0 && angle == 0 {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn extract_enclosed_content(text: &str, open: char, close: char) -> Option<(&str, usize)> {
    let text = text.trim_start();
    if !text.starts_with(open) {
        return None;
    }

    let mut depth = 0i32;
    let mut in_string = false;
    let mut in_line_comment = false;
    let mut block_comment_depth = 0usize;
    let bytes = text.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        if in_line_comment {
            if bytes[i] == b'\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }

        if block_comment_depth > 0 {
            if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
                block_comment_depth += 1;
                i += 2;
                continue;
            }
            if i + 1 < bytes.len() && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                block_comment_depth = block_comment_depth.saturating_sub(1);
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }

        if in_string {
            if bytes[i] == b'\\' {
                i = (i + 2).min(bytes.len());
                continue;
            }
            if bytes[i] == b'"' {
                in_string = false;
            }
            i += 1;
            continue;
        }

        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'/' {
            in_line_comment = true;
            i += 2;
            continue;
        }

        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            block_comment_depth = 1;
            i += 2;
            continue;
        }

        if bytes[i] == b'"' {
            in_string = true;
            i += 1;
            continue;
        }

        if bytes[i] == open as u8 {
            depth += 1;
        } else if bytes[i] == close as u8 {
            depth -= 1;
            if depth == 0 {
                return Some((&text[1..i], i + 1));
            }
        }
        i += 1;
    }

    None
}

fn find_char_outside_comments(source: &str, start: usize, target: u8) -> Option<usize> {
    let bytes = source.as_bytes();
    let mut in_string = false;
    let mut in_line_comment = false;
    let mut block_comment_depth = 0usize;
    let mut i = start;

    while i < bytes.len() {
        if in_line_comment {
            if bytes[i] == b'\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }

        if block_comment_depth > 0 {
            if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
                block_comment_depth += 1;
                i += 2;
                continue;
            }
            if i + 1 < bytes.len() && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                block_comment_depth = block_comment_depth.saturating_sub(1);
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }

        if in_string {
            if bytes[i] == b'\\' {
                i = (i + 2).min(bytes.len());
                continue;
            }
            if bytes[i] == b'"' {
                in_string = false;
            }
            i += 1;
            continue;
        }

        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'/' {
            in_line_comment = true;
            i += 2;
            continue;
        }

        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            block_comment_depth = 1;
            i += 2;
            continue;
        }

        if bytes[i] == b'"' {
            in_string = true;
            i += 1;
            continue;
        }

        if bytes[i] == target {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn find_matching_brace(source: &str, open_brace: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    if bytes.get(open_brace).copied() != Some(b'{') {
        return None;
    }

    let mut depth = 0i32;
    let mut in_string = false;
    let mut in_line_comment = false;
    let mut block_comment_depth = 0usize;
    let mut i = open_brace;

    while i < bytes.len() {
        if in_line_comment {
            if bytes[i] == b'\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }

        if block_comment_depth > 0 {
            if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
                block_comment_depth += 1;
                i += 2;
                continue;
            }
            if i + 1 < bytes.len() && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                block_comment_depth = block_comment_depth.saturating_sub(1);
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }

        if in_string {
            if bytes[i] == b'\\' {
                i = (i + 2).min(bytes.len());
                continue;
            }
            if bytes[i] == b'"' {
                in_string = false;
            }
            i += 1;
            continue;
        }

        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'/' {
            in_line_comment = true;
            i += 2;
            continue;
        }

        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            block_comment_depth = 1;
            i += 2;
            continue;
        }

        if bytes[i] == b'"' {
            in_string = true;
            i += 1;
            continue;
        }

        if bytes[i] == b'{' {
            depth += 1;
        } else if bytes[i] == b'}' {
            depth -= 1;
            if depth == 0 {
                return Some(i);
            }
        }

        i += 1;
    }
    None
}

fn parse_ident_from(source: &str, cursor: &mut usize) -> Option<String> {
    *cursor = skip_ascii_ws(source, *cursor);
    let start = *cursor;
    while *cursor < source.len()
        && (source.as_bytes()[*cursor].is_ascii_alphanumeric()
            || source.as_bytes()[*cursor] == b'_')
    {
        *cursor += 1;
    }
    if *cursor == start {
        None
    } else {
        source.get(start..*cursor).map(ToOwned::to_owned)
    }
}

fn skip_ascii_ws(source: &str, mut idx: usize) -> usize {
    while idx < source.len() && source.as_bytes()[idx].is_ascii_whitespace() {
        idx += 1;
    }
    idx
}

fn next_line_start(source: &str, line_end: usize) -> usize {
    if line_end < source.len() {
        line_end + 1
    } else {
        source.len()
    }
}

fn join_doc_lines(lines: &[String]) -> String {
    lines.join("\n")
}

fn strip_line_comment(line: &str) -> &str {
    let mut in_string = false;
    let mut prev = '\0';
    for (idx, ch) in line.char_indices() {
        if ch == '"' && prev != '\\' {
            in_string = !in_string;
        }
        if !in_string && ch == '/' && prev == '/' {
            return &line[..idx - 1];
        }
        prev = ch;
    }
    line
}

fn brace_delta(line: &str) -> i32 {
    let line = strip_line_comment(line);
    let mut in_string = false;
    let mut prev = '\0';
    let mut depth = 0i32;
    for ch in line.chars() {
        if ch == '"' && prev != '\\' {
            in_string = !in_string;
        } else if !in_string {
            match ch {
                '{' => depth += 1,
                '}' => depth -= 1,
                _ => {}
            }
        }
        prev = ch;
    }
    depth
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_public_enum_with_tuple_and_named_variants() {
        let source = r#"
/// Value docs
pub enum Value {
    /// Boolean docs
    Bool(bool),
    /// Interval docs
    Interval {
        /// Amount docs
        amount: i64,
        unit: IntervalUnit,
    },
    Unit,
}
"#;

        let start = source.find("pub enum").expect("enum start");
        let parsed = parse_pub_item(source, start, ItemKind::Enum, "Value docs".to_string())
            .expect("parse enum");
        let parsed_enum = parse_enum_item(parsed);

        assert_eq!(parsed_enum.name, "Value");
        assert_eq!(parsed_enum.doc, "Value docs");
        assert_eq!(parsed_enum.variants.len(), 3);
        assert_eq!(parsed_enum.variants[0].name, "Bool");
        assert_eq!(parsed_enum.variants[0].fields[0].ty, "bool");
        assert_eq!(parsed_enum.variants[1].name, "Interval");
        assert_eq!(parsed_enum.variants[1].fields[0].name, "amount");
        assert_eq!(parsed_enum.variants[1].fields[0].doc, "Amount docs");
    }

    #[test]
    fn parses_public_struct_and_normalizes_field_types() {
        let source = r#"
/// Demo docs
pub struct Demo {
    pub name: String,
    pub(crate) payload: Option < Box < Qail > >,
}
"#;

        let start = source.find("pub struct").expect("struct start");
        let parsed = parse_pub_item(source, start, ItemKind::Struct, "Demo docs".to_string())
            .expect("parse struct");
        let parsed_struct = parse_struct_item(parsed);

        assert_eq!(parsed_struct.name, "Demo");
        assert_eq!(parsed_struct.doc, "Demo docs");
        assert_eq!(parsed_struct.fields.len(), 2);
        assert_eq!(parsed_struct.fields[0].name, "name");
        assert_eq!(parsed_struct.fields[0].ty, "String");
        assert_eq!(parsed_struct.fields[1].name, "payload");
        assert_eq!(parsed_struct.fields[1].ty, "Option<Box<Qail>>");
    }
}

// ============================================================================
// Zig code generation
// ============================================================================

/// Generate Zig source for a simple enum (all unit variants).
fn gen_zig_enum(e: &RustEnum) -> String {
    let mut out = String::new();
    if !e.doc.is_empty() {
        for line in e.doc.lines() {
            let _ = writeln!(out, "/// {}", line);
        }
    }
    let _ = writeln!(out, "pub const {} = enum {{", e.name);
    for v in &e.variants {
        if !v.doc.is_empty() {
            for line in v.doc.lines() {
                let _ = writeln!(out, "    /// {}", line);
            }
        }
        let zig_name = to_snake_case(&v.name);
        // Zig keywords need @"" quoting
        let zig_name = match zig_name.as_str() {
            "and" | "or" | "union" | "mod" | "error" => format!("@\"{}\"", zig_name),
            _ => zig_name,
        };
        let _ = writeln!(out, "    {},", zig_name);
    }
    let _ = writeln!(out, "}};");
    out
}

/// Generate Zig source for a tagged union (enum with data variants).
fn gen_zig_tagged_union(e: &RustEnum) -> String {
    let mut out = String::new();
    if !e.doc.is_empty() {
        for line in e.doc.lines() {
            let _ = writeln!(out, "/// {}", line);
        }
    }
    let _ = writeln!(out, "pub const {} = union(enum) {{", e.name);
    for v in &e.variants {
        if !v.doc.is_empty() {
            for line in v.doc.lines() {
                let _ = writeln!(out, "    /// {}", line);
            }
        }
        let zig_name = to_snake_case(&v.name);
        if v.fields.is_empty() {
            let _ = writeln!(out, "    {},", zig_name);
        } else if v.fields.len() == 1 && v.fields[0].name.starts_with('f') {
            // Unnamed tuple variant with single field
            let zig_ty = map_type(&v.fields[0].ty);
            let _ = writeln!(out, "    {}: {},", zig_name, zig_ty);
        } else {
            // Named fields → inline struct
            let _ = writeln!(out, "    {}: struct {{", zig_name);
            for field in &v.fields {
                let zig_ty = map_type(&field.ty);
                let _ = writeln!(out, "        {}: {},", field.name, zig_ty);
            }
            let _ = writeln!(out, "    }},");
        }
    }
    let _ = writeln!(out, "}};");
    out
}

/// Generate Zig source for a struct.
fn gen_zig_struct(s: &RustStruct, name_override: Option<&str>) -> String {
    let mut out = String::new();
    let name = name_override.unwrap_or(&s.name);
    if !s.doc.is_empty() {
        for line in s.doc.lines() {
            let _ = writeln!(out, "/// {}", line);
        }
    }
    let _ = writeln!(out, "pub const {} = struct {{", name);
    for field in &s.fields {
        if !field.doc.is_empty() {
            for line in field.doc.lines() {
                let _ = writeln!(out, "    /// {}", line);
            }
        }
        let zig_ty = map_type(&field.ty);
        if let Some(default) = zig_default(&zig_ty, &field.name) {
            let _ = writeln!(out, "    {}: {} = {},", field.name, zig_ty, default);
        } else {
            let _ = writeln!(out, "    {}: {},", field.name, zig_ty);
        }
    }
    let _ = writeln!(out, "}};");
    out
}

/// Check if an enum has any non-unit variants (needs tagged union).
fn is_simple_enum(e: &RustEnum) -> bool {
    e.variants.iter().all(|v| v.fields.is_empty())
}

// ============================================================================
// Main
// ============================================================================

fn main() {
    let rust_ast_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../core/src/ast");
    let zig_output_dir =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../qail-zig/src/ast/generated");

    // Create output directory
    std::fs::create_dir_all(&zig_output_dir).expect("Failed to create output directory");

    // Files to process
    let files = [
        ("operators.rs", "operators.gen.zig"),
        ("values.rs", "values.gen.zig"),
        ("conditions.rs", "conditions.gen.zig"),
        ("cages.rs", "cages.gen.zig"),
        ("joins.rs", "joins.gen.zig"),
        ("cmd/mod.rs", "cmd.gen.zig"),
        ("expr.rs", "expr.gen.zig"),
    ];

    let mut total_enums = 0;
    let mut total_structs = 0;

    for (rust_file, zig_file) in &files {
        let rust_path = rust_ast_dir.join(rust_file);
        if !rust_path.exists() {
            eprintln!("⚠️  Skipping {} (not found)", rust_path.display());
            continue;
        }

        let (enums, structs) = parse_file(&rust_path);

        let mut output = String::new();
        let _ = writeln!(output, "// Auto-generated by qail-codegen — DO NOT EDIT");
        let _ = writeln!(output, "// Source: core/src/ast/{}", rust_file);
        let _ = writeln!(output, "// Generated: {}", chrono_like_now());
        let _ = writeln!(output);

        for e in &enums {
            if is_simple_enum(e) {
                output.push_str(&gen_zig_enum(e));
            } else {
                output.push_str(&gen_zig_tagged_union(e));
            }
            output.push('\n');
            total_enums += 1;
        }

        for s in &structs {
            // Rename Qail → QailCmd for Zig convention
            let name = if s.name == "Qail" {
                Some("QailCmd")
            } else {
                None
            };
            output.push_str(&gen_zig_struct(s, name));
            output.push('\n');
            total_structs += 1;
        }

        let zig_path = zig_output_dir.join(zig_file);
        std::fs::write(&zig_path, &output)
            .unwrap_or_else(|e| panic!("Cannot write {}: {}", zig_path.display(), e));

        println!(
            "✅ {} → {} ({} enums, {} structs)",
            rust_file,
            zig_file,
            enums.len(),
            structs.len()
        );
    }

    println!(
        "\n🎯 Total: {} enums, {} structs generated",
        total_enums, total_structs
    );
    println!("📁 Output: {}", zig_output_dir.display());
}

/// Simple timestamp without chrono dependency.
fn chrono_like_now() -> String {
    use std::process::Command;
    Command::new("date")
        .arg("+%Y-%m-%dT%H:%M:%S")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .unwrap_or_else(|| "unknown".to_string())
        .trim()
        .to_string()
}
