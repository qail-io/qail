use crate::ast::values::IntervalUnit;
use crate::ast::*;
use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::complete::{char, digit1, multispace0, multispace1},
    combinator::{map, opt, recognize, value},
    sequence::{delimited, preceded},
};

/// Parse checking identifier (table name, column name, or qualified name like table.column)
pub fn parse_identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == '.').parse(input)
}

/// Parse interval shorthand: 24h, 7d, 1w, 30m, 6mo, 1y
pub fn parse_interval(input: &str) -> IResult<&str, Value> {
    let (input, num_str) = digit1(input)?;
    let amount: i64 = num_str.parse().unwrap_or(0);

    let (input, unit) = alt((
        value(IntervalUnit::Second, tag_no_case("s")),
        value(IntervalUnit::Minute, tag_no_case("m")),
        value(IntervalUnit::Hour, tag_no_case("h")),
        value(IntervalUnit::Day, tag_no_case("d")),
        value(IntervalUnit::Week, tag_no_case("w")),
        value(IntervalUnit::Month, tag_no_case("mo")),
        value(IntervalUnit::Year, tag_no_case("y")),
    ))
    .parse(input)?;

    Ok((input, Value::Interval { amount, unit }))
}

/// Parse value: string, number, bool, null, $param, :named_param, interval, JSON
pub fn parse_value(input: &str) -> IResult<&str, Value> {
    alt((
        // Parameter: $1, $2
        map(preceded(char('$'), digit1), |d: &str| {
            Value::Param(d.parse().unwrap_or(0))
        }),
        // Named parameter: :name, :id, :user_id
        map(
            preceded(
                char(':'),
                take_while1(|c: char| c.is_alphanumeric() || c == '_'),
            ),
            |name: &str| Value::NamedParam(name.to_string()),
        ),
        // Boolean
        value(Value::Bool(true), tag_no_case("true")),
        value(Value::Bool(false), tag_no_case("false")),
        // Null
        value(Value::Null, tag_no_case("null")),
        // Triple-quoted multi-line string (must come before single/double quotes)
        parse_triple_quoted_string,
        // JSON object literal: { ... } or array: [ ... ]
        parse_json_literal,
        // String (double quoted) - allow empty strings
        map(
            delimited(
                char('"'),
                nom::bytes::complete::take_while(|c| c != '"'),
                char('"'),
            ),
            |s: &str| Value::String(s.to_string()),
        ),
        // String (single quoted) - allow empty strings
        map(
            delimited(
                char('\''),
                nom::bytes::complete::take_while(|c| c != '\''),
                char('\''),
            ),
            |s: &str| Value::String(s.to_string()),
        ),
        // Float (must check before int)
        map(
            recognize((opt(char('-')), digit1, char('.'), digit1)),
            |s: &str| Value::Float(s.parse().unwrap_or(0.0)),
        ),
        // Interval shorthand before plain integers: 24h, 7d, 1w
        parse_interval,
        // Integer (last, after interval)
        map(recognize((opt(char('-')), digit1)), |s: &str| {
            Value::Int(s.parse().unwrap_or(0))
        }),
    ))
    .parse(input)
}

/// Parse triple-quoted multi-line string: '''content''' or """content"""
fn parse_triple_quoted_string(input: &str) -> IResult<&str, Value> {
    alt((
        // Triple single quotes
        map(
            delimited(
                tag("'''"),
                nom::bytes::complete::take_until("'''"),
                tag("'''"),
            ),
            |s: &str| Value::String(s.to_string()),
        ),
        // Triple double quotes
        map(
            delimited(
                tag("\"\"\""),
                nom::bytes::complete::take_until("\"\"\""),
                tag("\"\"\""),
            ),
            |s: &str| Value::String(s.to_string()),
        ),
    ))
    .parse(input)
}

/// Parse JSON object literal: { key: value, ... } or array: [...]
/// This captures the entire JSON structure as a string for Value::Json
fn parse_json_literal(input: &str) -> IResult<&str, Value> {
    // Determine if it's an object or array
    let trimmed = input.trim_start();
    if trimmed.is_empty() {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        )));
    }

    let (open_char, close_char) = match trimmed.chars().next() {
        Some('{') => ('{', '}'),
        Some('[') => ('[', ']'),
        _ => {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }
    };

    // Count brackets to find matching close
    let mut depth = 0;
    let mut in_string = false;
    let mut escape_next = false;
    let mut end_pos = 0;

    for (i, c) in trimmed.char_indices() {
        if escape_next {
            escape_next = false;
            continue;
        }

        if c == '\\' && in_string {
            escape_next = true;
            continue;
        }

        if c == '"' {
            in_string = !in_string;
            continue;
        }

        if !in_string {
            if c == open_char {
                depth += 1;
            } else if c == close_char {
                depth -= 1;
                if depth == 0 {
                    end_pos = i + 1;
                    break;
                }
            }
        }
    }

    if depth != 0 || end_pos == 0 {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Eof,
        )));
    }

    let json_str = &trimmed[..end_pos];
    let _remaining = &trimmed[end_pos..];

    // Calculate how much of original input we consumed (account for leading whitespace)
    let consumed = input.len() - trimmed.len() + end_pos;
    let remaining_original = &input[consumed..];

    Ok((remaining_original, Value::Json(json_str.to_string())))
}

/// Parse comparison operator
pub fn parse_operator(input: &str) -> IResult<&str, Operator> {
    alt((
        // Multi-char keyword operators first
        alt((
            value(Operator::NotBetween, tag_no_case("not between")),
            value(Operator::Between, tag_no_case("between")),
            value(Operator::IsNotNull, tag_no_case("is not null")),
            value(Operator::IsNull, tag_no_case("is null")),
            value(Operator::NotIn, tag_no_case("not in")),
            value(Operator::NotILike, tag_no_case("not ilike")),
            value(Operator::NotLike, tag_no_case("not like")),
            value(Operator::SimilarTo, tag_no_case("similar to")),
            value(Operator::JsonExists, tag_no_case("json_exists")),
            value(Operator::JsonQuery, tag_no_case("json_query")),
            value(Operator::JsonValue, tag_no_case("json_value")),
            value(Operator::Regex, tag_no_case("regex")),
            value(Operator::ILike, tag_no_case("ilike")),
            value(Operator::Like, tag_no_case("like")),
            value(Operator::In, tag_no_case("in")),
        )),
        // Multi-char symbol operators (before shorter prefixes)
        alt((
            value(Operator::RegexI, tag("~*")),
            value(Operator::JsonPathText, tag("#>>")),
            value(Operator::JsonPath, tag("#>")),
            value(Operator::TextSearch, tag("@@")),
            value(Operator::KeyExistsAny, tag("?|")),
            value(Operator::KeyExistsAll, tag("?&")),
            value(Operator::Contains, tag("@>")),
            value(Operator::ContainedBy, tag("<@")),
            value(Operator::Overlaps, tag("&&")),
            value(Operator::Gte, tag(">=")),
            value(Operator::Lte, tag("<=")),
            value(Operator::Ne, tag("!=")),
            value(Operator::Ne, tag("<>")),
        )),
        // Single char operators
        alt((
            value(Operator::Eq, tag("=")),
            value(Operator::Gt, tag(">")),
            value(Operator::Lt, tag("<")),
            value(Operator::KeyExists, tag("?")),
            value(Operator::Fuzzy, tag("~")),
        )),
    ))
    .parse(input)
}

/// Parse action keyword: get, export, set, del, add, make, cnt
pub fn parse_action(input: &str) -> IResult<&str, (Action, bool)> {
    alt((
        // get distinct
        map(
            (tag_no_case("get"), multispace1, tag_no_case("distinct")),
            |_| (Action::Get, true),
        ),
        // get
        value((Action::Get, false), tag_no_case("get")),
        // export
        value((Action::Export, false), tag_no_case("export")),
        // cnt / count (must come before general keywords)
        alt((
            value((Action::Cnt, false), tag_no_case("count")),
            value((Action::Cnt, false), tag_no_case("cnt")),
        )),
        // set
        value((Action::Set, false), tag_no_case("set")),
        // del / delete
        alt((
            value((Action::Del, false), tag_no_case("delete")),
            value((Action::Del, false), tag_no_case("del")),
        )),
        // add / insert
        alt((
            value((Action::Add, false), tag_no_case("insert")),
            value((Action::Add, false), tag_no_case("add")),
        )),
        // make / create
        alt((
            value((Action::Make, false), tag_no_case("create")),
            value((Action::Make, false), tag_no_case("make")),
        )),
    ))
    .parse(input)
}

/// Parse transaction commands: begin, commit, rollback
pub fn parse_txn_command(input: &str) -> IResult<&str, Qail> {
    let (input, action) = alt((
        value(Action::TxnStart, tag_no_case("begin")),
        value(Action::TxnCommit, tag_no_case("commit")),
        value(Action::TxnRollback, tag_no_case("rollback")),
    ))
    .parse(input)?;

    Ok((
        input,
        Qail {
            action,
            table: String::new(),
            columns: vec![],
            joins: vec![],
            cages: vec![],
            distinct: false,
            distinct_on: vec![],
            index_def: None,
            table_constraints: vec![],
            set_ops: vec![],
            having: vec![],
            group_by_mode: GroupByMode::default(),
            ctes: vec![],
            returning: None,
            on_conflict: None,
            source_query: None,
            channel: None,
            payload: None,
            savepoint_name: None,
            from_tables: vec![],
            using_tables: vec![],
            lock_mode: None,
            fetch: None,
            default_values: false,
            overriding: None,
            sample: None,
            only_table: false,
            vector: None,
            score_threshold: None,
            vector_name: None,
            with_vector: false,
            vector_size: None,
            distance: None,
            on_disk: None,
            function_def: None,
            trigger_def: None,
        },
    ))
}

/// Parse procedural/session commands that don't match the regular `action table ...` flow.
///
/// Supported forms:
/// - `call procedure_name(args...)`
/// - `do $$ ... $$ [language <lang>]`
/// - `session set <key> = <value>`
/// - `session show <key>`
/// - `session reset <key>`
pub fn parse_procedural_command(input: &str) -> IResult<&str, Qail> {
    alt((parse_call_command, parse_do_command, parse_session_command)).parse(input)
}

fn parse_call_command(input: &str) -> IResult<&str, Qail> {
    let (input, _) = tag_no_case("call").parse(input)?;
    let (input, _) = multispace1(input)?;

    let procedure = input.trim().trim_end_matches(';').trim();
    if procedure.is_empty() {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Eof,
        )));
    }

    Ok((
        "",
        Qail {
            action: Action::Call,
            table: procedure.to_string(),
            ..Default::default()
        },
    ))
}

fn parse_do_command(input: &str) -> IResult<&str, Qail> {
    let (input, _) = tag_no_case("do").parse(input)?;
    let (input, _) = multispace1(input)?;

    let rest = input.trim().trim_end_matches(';').trim();
    if rest.is_empty() {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Eof,
        )));
    }

    // Preferred syntax: do $$...$$ [language <lang>]
    let (body, language) = if let Some(after_open) = rest.strip_prefix("$$") {
        if let Some(close_idx) = after_open.find("$$") {
            let body = after_open[..close_idx].to_string();
            let trailing = after_open[close_idx + 2..].trim();
            let lang = if trailing.to_ascii_lowercase().starts_with("language ") {
                trailing[9..].trim().to_string()
            } else {
                "plpgsql".to_string()
            };
            (body, lang)
        } else {
            (rest.to_string(), "plpgsql".to_string())
        }
    } else {
        (rest.to_string(), "plpgsql".to_string())
    };

    Ok((
        "",
        Qail {
            action: Action::Do,
            table: language,
            payload: Some(body),
            ..Default::default()
        },
    ))
}

fn parse_session_command(input: &str) -> IResult<&str, Qail> {
    let (input, _) = tag_no_case("session").parse(input)?;
    let (input, _) = multispace1(input)?;

    // session set <key> = <value>
    if let Ok((input, _)) = tag_no_case::<_, _, nom::error::Error<&str>>("set").parse(input) {
        let (input, _) = multispace1(input)?;
        let (input, key) = parse_identifier(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = opt(char('=')).parse(input)?;
        let value = input.trim().trim_end_matches(';').trim();
        if value.is_empty() {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Eof,
            )));
        }
        let value = strip_matching_quotes(value);
        return Ok((
            "",
            Qail {
                action: Action::SessionSet,
                table: key.to_string(),
                payload: Some(value.to_string()),
                ..Default::default()
            },
        ));
    }

    // session show <key>
    if let Ok((input, _)) = tag_no_case::<_, _, nom::error::Error<&str>>("show").parse(input) {
        let (input, _) = multispace1(input)?;
        let (input, key) = parse_identifier(input)?;
        let trailing = input.trim().trim_end_matches(';').trim();
        if !trailing.is_empty() {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }
        return Ok((
            "",
            Qail {
                action: Action::SessionShow,
                table: key.to_string(),
                ..Default::default()
            },
        ));
    }

    // session reset <key>
    let (input, _) = tag_no_case("reset").parse(input)?;
    let (input, _) = multispace1(input)?;
    let (input, key) = parse_identifier(input)?;
    let trailing = input.trim().trim_end_matches(';').trim();
    if !trailing.is_empty() {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        )));
    }
    Ok((
        "",
        Qail {
            action: Action::SessionReset,
            table: key.to_string(),
            ..Default::default()
        },
    ))
}

fn strip_matching_quotes(s: &str) -> &str {
    let bytes = s.as_bytes();
    if bytes.len() >= 2 {
        let first = bytes[0];
        let last = bytes[bytes.len() - 1];
        if (first == b'\'' && last == b'\'') || (first == b'"' && last == b'"') {
            return &s[1..s.len() - 1];
        }
    }
    s
}
