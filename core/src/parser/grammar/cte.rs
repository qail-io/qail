use super::base::parse_identifier;
use crate::ast::*;
use nom::{
    IResult, Parser,
    bytes::complete::tag_no_case,
    character::complete::{char, multispace0, multispace1},
    combinator::{map, opt},
    multi::separated_list1,
    sequence::{delimited, preceded},
};

/// Parse WITH clause including optional RECURSIVE keyword.
///
/// Grammar contract:
/// ```text
/// WITH [RECURSIVE] name[(cols)] AS ( <body> ) [, ...] <main-query>
/// ```
///
/// All CTE bodies are parsed strictly as QAIL DSL (no raw-SQL fallback).
///
/// When RECURSIVE is present, each CTE body must additionally:
/// - Must contain exactly one top-level `UNION ALL`
/// - Both halves must parse as valid QAIL (no raw-SQL fallback)
/// - Base must NOT reference the CTE name, recursive part MUST reference it
pub fn parse_with_clause(input: &str) -> IResult<&str, (Vec<CTEDef>, bool)> {
    let (input, _) = tag_no_case("with").parse(input)?;
    let (input, _) = multispace1(input)?;

    let (input, recursive) = opt(preceded(tag_no_case("recursive"), multispace1)).parse(input)?;
    let is_recursive = recursive.is_some();

    let (input, ctes) = separated_list1((multispace0, char(','), multispace0), |i| {
        parse_cte_definition(i, is_recursive)
    })
    .parse(input)?;

    Ok((input, (ctes, is_recursive)))
}

/// Parse a single CTE definition: name [(columns)] AS (subquery)
fn parse_cte_definition(input: &str, is_recursive: bool) -> IResult<&str, CTEDef> {
    // CTE name
    let (input, name) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;

    // Optional column list: (col1, col2, ...)
    let (input, columns) = opt(delimited(
        char('('),
        separated_list1(
            (multispace0, char(','), multispace0),
            map(parse_identifier, |s| s.to_string()),
        ),
        char(')'),
    ))
    .parse(input)?;
    let (input, _) = multispace0(input)?;

    // AS keyword
    let (input, _) = tag_no_case("as").parse(input)?;
    let (input, _) = multispace0(input)?;

    // Subquery in parentheses - extract content
    let (input, cte_body) =
        delimited(char('('), take_until_matching_paren, char(')')).parse(input)?;

    let cte_body = cte_body.trim();

    if is_recursive {
        // Strict recursive path: split on UNION ALL, parse both halves, validate invariants
        parse_recursive_cte_strict(input, name, columns.unwrap_or_default(), cte_body)
    } else {
        // Non-recursive strict path: must be valid QAIL with full consumption.
        let base_query = parse_qail_strict(cte_body).map_err(|_| {
            nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
        })?;

        Ok((
            input,
            CTEDef {
                name: name.to_string(),
                recursive: false,
                columns: columns.unwrap_or_default(),
                base_query: Box::new(base_query),
                recursive_query: None,
                source_table: None,
            },
        ))
    }
}

/// Strict recursive CTE parser pipeline (steps 2-6 from QA spec).
///
/// 1. Split body on top-level `UNION ALL`
/// 2. Parse both halves strictly (no raw-SQL fallback)
/// 3. Validate: base must NOT reference CTE name, recursive MUST reference it
/// 4. Build CTEDef structurally with both base_query and recursive_query
fn parse_recursive_cte_strict<'a>(
    remaining_input: &'a str,
    name: &str,
    columns: Vec<String>,
    body: &str,
) -> IResult<&'a str, CTEDef> {
    // Step 3: Split on top-level UNION ALL
    let (base_str, recursive_str) = split_top_level_union_all(body).map_err(|_| {
        nom::Err::Failure(nom::error::Error::new(
            remaining_input,
            nom::error::ErrorKind::Verify,
        ))
    })?;

    // Step 4: Parse both halves strictly
    let base_query = parse_qail_strict(base_str.trim()).map_err(|_| {
        nom::Err::Failure(nom::error::Error::new(
            remaining_input,
            nom::error::ErrorKind::Verify,
        ))
    })?;

    let recursive_query = parse_qail_strict(recursive_str.trim()).map_err(|_| {
        nom::Err::Failure(nom::error::Error::new(
            remaining_input,
            nom::error::ErrorKind::Verify,
        ))
    })?;

    // Step 5: Validate recursive invariants
    // Base must NOT reference CTE name
    if contains_ident_outside_quotes_comments(base_str, name) {
        return Err(nom::Err::Failure(nom::error::Error::new(
            remaining_input,
            nom::error::ErrorKind::Verify,
        )));
    }

    // Recursive part MUST reference CTE name
    if !contains_ident_outside_quotes_comments(recursive_str, name) {
        return Err(nom::Err::Failure(nom::error::Error::new(
            remaining_input,
            nom::error::ErrorKind::Verify,
        )));
    }

    // Step 6: Build CTEDef structurally
    Ok((
        remaining_input,
        CTEDef {
            name: name.to_string(),
            recursive: true,
            columns,
            base_query: Box::new(base_query),
            recursive_query: Some(Box::new(recursive_query)),
            source_table: None,
        },
    ))
}

/// Split a CTE body on exactly one top-level `UNION ALL`.
///
/// Uses the same quote/comment/paren-aware state machine as `take_until_matching_paren`.
/// Only detects `UNION ALL` at paren depth 0 and outside all quoted/comment contexts.
///
/// Returns `Err` if:
/// - No top-level `UNION ALL` found
/// - More than one top-level `UNION ALL` found
/// - A bare `UNION` (without ALL) is found at top level (rejected)
pub fn split_top_level_union_all(body: &str) -> Result<(&str, &str), &'static str> {
    let bytes = body.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    let mut depth: usize = 0;
    let mut union_pos: Option<usize> = None; // byte offset of 'U' in matched UNION ALL
    let mut union_end: Option<usize> = None; // byte offset after "UNION ALL"

    while i < len {
        match bytes[i] {
            // Skip quoted strings/identifiers/comments (same as take_until_matching_paren)
            b'\'' => {
                i += 1;
                while i < len {
                    if bytes[i] == b'\'' {
                        i += 1;
                        if i < len && bytes[i] == b'\'' {
                            i += 1;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'"' => {
                i += 1;
                while i < len {
                    if bytes[i] == b'"' {
                        i += 1;
                        if i < len && bytes[i] == b'"' {
                            i += 1;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'$' if i + 1 < len && bytes[i + 1] == b'$' => {
                i += 2;
                while i + 1 < len {
                    if bytes[i] == b'$' && bytes[i + 1] == b'$' {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            b'-' if i + 1 < len && bytes[i + 1] == b'-' => {
                i += 2;
                while i < len && bytes[i] != b'\n' {
                    i += 1;
                }
                if i < len {
                    i += 1;
                }
            }
            b'/' if i + 1 < len && bytes[i + 1] == b'*' => {
                i += 2;
                while i + 1 < len {
                    if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            b'(' => {
                depth += 1;
                i += 1;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                i += 1;
            }
            // At depth 0, check for UNION ALL / UNION (case-insensitive)
            b'U' | b'u' if depth == 0 => {
                // Check identifier boundary before: must be start of string or non-ident char
                if i > 0 && is_ident_char(bytes[i - 1]) {
                    i += 1;
                    continue;
                }

                // Try to match "UNION ALL" (9 chars + boundaries)
                if i + 9 <= len
                    && body[i..i + 5].eq_ignore_ascii_case("UNION")
                    && !is_ident_char(bytes[i + 5])
                {
                    // Skip whitespace between UNION and ALL
                    let mut j = i + 5;
                    while j < len && bytes[j].is_ascii_whitespace() {
                        j += 1;
                    }

                    if j + 3 <= len && body[j..j + 3].eq_ignore_ascii_case("ALL") {
                        // Check boundary after ALL
                        let after_all = j + 3;
                        if after_all >= len || !is_ident_char(bytes[after_all]) {
                            // Found "UNION ALL" at top level
                            if union_pos.is_some() {
                                return Err("multiple top-level UNION ALL found");
                            }
                            union_pos = Some(i);
                            union_end = Some(after_all);
                            i = after_all;
                            continue;
                        }
                    }

                    // Bare UNION (without ALL) — reject
                    let after_union = i + 5;
                    if after_union >= len || !is_ident_char(bytes[after_union]) {
                        return Err(
                            "bare UNION (without ALL) found; only UNION ALL is supported in recursive CTEs",
                        );
                    }
                }
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }

    match (union_pos, union_end) {
        (Some(pos), Some(end)) => Ok((&body[..pos], &body[end..])),
        _ => Err("no top-level UNION ALL found"),
    }
}

/// Parse a QAIL query strictly: no raw-SQL fallback, must consume all input.
pub fn parse_qail_strict(sql: &str) -> Result<Qail, &'static str> {
    match super::parse_root(sql) {
        Ok((remaining, cmd)) => {
            if !remaining.trim().is_empty() {
                return Err("partial parse — trailing input");
            }
            if cmd.is_raw_sql() {
                return Err("raw SQL not allowed in strict mode");
            }
            Ok(cmd)
        }
        Err(_) => Err("QAIL parse failed"),
    }
}

/// Check if `ident` appears as a bare identifier (case-insensitive) in `input`,
/// outside of quotes, comments, and dollar-quoted blocks.
///
/// Uses identifier boundaries: the character before and after the match must
/// not be `[a-zA-Z0-9_]`.
pub fn contains_ident_outside_quotes_comments(input: &str, ident: &str) -> bool {
    let bytes = input.as_bytes();
    let len = bytes.len();
    let ident_len = ident.len();
    let mut i = 0;

    while i < len {
        match bytes[i] {
            // Skip quoted/comment contexts
            b'\'' => {
                i += 1;
                while i < len {
                    if bytes[i] == b'\'' {
                        i += 1;
                        if i < len && bytes[i] == b'\'' {
                            i += 1;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'"' => {
                i += 1;
                while i < len {
                    if bytes[i] == b'"' {
                        i += 1;
                        if i < len && bytes[i] == b'"' {
                            i += 1;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'$' if i + 1 < len && bytes[i + 1] == b'$' => {
                i += 2;
                while i + 1 < len {
                    if bytes[i] == b'$' && bytes[i + 1] == b'$' {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            b'-' if i + 1 < len && bytes[i + 1] == b'-' => {
                i += 2;
                while i < len && bytes[i] != b'\n' {
                    i += 1;
                }
                if i < len {
                    i += 1;
                }
            }
            b'/' if i + 1 < len && bytes[i + 1] == b'*' => {
                i += 2;
                while i + 1 < len {
                    if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            _ => {
                // Check for identifier match at this position
                if i + ident_len <= len
                    && input[i..i + ident_len].eq_ignore_ascii_case(ident)
                    && (i == 0 || !is_ident_char(bytes[i - 1]))
                    && (i + ident_len >= len || !is_ident_char(bytes[i + ident_len]))
                {
                    return true;
                }
                i += 1;
            }
        }
    }

    false
}

/// Returns true if byte is a valid identifier character: [a-zA-Z0-9_]
fn is_ident_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Helper to take content until matching closing paren (handles nested parens).
///
/// This scanner is quote and comment-aware: parentheses inside single-quoted
/// strings ('...'), double-quoted identifiers ("..."), dollar-quoted blocks
/// ($$...$$), line comments (-- ...), and block comments (/* ... */) are
/// ignored. Only bare `(`/`)` affect the depth counter.
fn take_until_matching_paren(input: &str) -> IResult<&str, &str> {
    let bytes = input.as_bytes();
    let len = bytes.len();
    let mut depth: usize = 1;
    let mut i = 0;

    while i < len {
        match bytes[i] {
            b'\'' => {
                i += 1;
                while i < len {
                    if bytes[i] == b'\'' {
                        i += 1;
                        if i < len && bytes[i] == b'\'' {
                            i += 1;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'"' => {
                i += 1;
                while i < len {
                    if bytes[i] == b'"' {
                        i += 1;
                        if i < len && bytes[i] == b'"' {
                            i += 1;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'$' if i + 1 < len && bytes[i + 1] == b'$' => {
                i += 2;
                while i + 1 < len {
                    if bytes[i] == b'$' && bytes[i + 1] == b'$' {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            b'-' if i + 1 < len && bytes[i + 1] == b'-' => {
                i += 2;
                while i < len && bytes[i] != b'\n' {
                    i += 1;
                }
                if i < len {
                    i += 1;
                }
            }
            b'/' if i + 1 < len && bytes[i + 1] == b'*' => {
                i += 2;
                while i + 1 < len {
                    if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            b'(' => {
                depth += 1;
                i += 1;
            }
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Ok((&input[i..], &input[..i]));
                }
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }

    Err(nom::Err::Error(nom::error::Error::new(
        input,
        nom::error::ErrorKind::TakeUntil,
    )))
}
