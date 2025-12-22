use nom::{
    branch::alt,
    bytes::complete::{tag, take_while, take_while1},
    character::complete::{char, digit1, multispace1, not_line_ending},
    combinator::{map, opt, recognize, value},
    multi::{many0, separated_list1},
    sequence::{pair, preceded, tuple},
    IResult,
};

use crate::ast::*;

/// Parse whitespace or comments.
pub fn ws_or_comment(input: &str) -> IResult<&str, ()> {
    value((), many0(alt((
        value((), multispace1),
        parse_comment,
    ))))(input)
}

/// Parse a single comment line (// ... or -- ...).
pub fn parse_comment(input: &str) -> IResult<&str, ()> {
    value((), pair(alt((tag("//"), tag("--"))), not_line_ending))(input)
}

/// Parse the action (get, set, del, add, gen, make, mod, over, with).
pub fn parse_action(input: &str) -> IResult<&str, Action> {
    alt((
        value(Action::Get, tag("get")),
        value(Action::Set, tag("set")),
        value(Action::Del, tag("del")),
        value(Action::Add, tag("add")),
        value(Action::Gen, tag("gen")),
        value(Action::Make, tag("make")),
        value(Action::Mod, tag("mod")),
        value(Action::Over, tag("over")),
        value(Action::With, tag("with")),
        value(Action::With, tag("with")),
        value(Action::Index, tag("index")),
        // Transactions
        value(Action::TxnStart, tag("txn")), //specific variant in commands.rs
        value(Action::Put, tag("put")),
        value(Action::DropCol, tag("drop")),
        value(Action::RenameCol, tag("rename")),
        // Additional clauses
        value(Action::JsonTable, tag("jtable")),
    ))(input)
}

/// Parse an identifier (table name, column name).
/// Parse an identifier (table name, column name).
pub fn parse_identifier(input: &str) -> IResult<&str, &str> {
    alt((
        // Standard identifier
        take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == '.'),
        // LSP Snippet (e.g. ${1:table})
        recognize(tuple((
            tag("${"),
            take_while(|c: char| c != '}'),
            char('}'),
        ))),
    ))(input)
}

pub fn parse_joins(input: &str) -> IResult<&str, Vec<Join>> {
    many0(parse_single_join)(input)
}

/// Parse a single join: `->` (INNER), `<-` (LEFT), `->>` (RIGHT)
fn parse_single_join(input: &str) -> IResult<&str, Join> {
    let (input, _) = ws_or_comment(input)?;
    
    // Try RIGHT JOIN first (->>)
    if let Ok((remaining, _)) = tag::<_, _, nom::error::Error<&str>>("->>") (input) {
        let (remaining, _) = ws_or_comment(remaining)?;
        let (remaining, table) = parse_identifier(remaining)?;
        return Ok((remaining, Join {
            table: table.to_string(),
            kind: JoinKind::Right,
        }));
    }
    
    // FULL OUTER JOIN (<->)
    if let Ok((remaining, _)) = tag::<_, _, nom::error::Error<&str>>("<->") (input) {
        let (remaining, _) = ws_or_comment(remaining)?;
        let (remaining, table) = parse_identifier(remaining)?;
        return Ok((remaining, Join {
            table: table.to_string(),
            kind: JoinKind::Full,
        }));
    }

    // CROSS JOIN (><)
    if let Ok((remaining, _)) = tag::<_, _, nom::error::Error<&str>>("><") (input) {
        let (remaining, _) = ws_or_comment(remaining)?;
        let (remaining, table) = parse_identifier(remaining)?;
        return Ok((remaining, Join {
            table: table.to_string(),
            kind: JoinKind::Cross,
        }));
    }

    // LATERAL JOIN (->^)
    if let Ok((remaining, _)) = tag::<_, _, nom::error::Error<&str>>("->^") (input) {
        let (remaining, _) = ws_or_comment(remaining)?;
        let (remaining, table) = parse_identifier(remaining)?;
        return Ok((remaining, Join {
            table: table.to_string(),
            kind: JoinKind::Lateral,
        }));
    }

    // Try LEFT JOIN (<-)
    if let Ok((remaining, _)) = tag::<_, _, nom::error::Error<&str>>("<-") (input) {
        let (remaining, _) = ws_or_comment(remaining)?;
        let (remaining, table) = parse_identifier(remaining)?;
        return Ok((remaining, Join {
            table: table.to_string(),
            kind: JoinKind::Left,
        }));
    }
    
    // Default: INNER JOIN (->)
    let (input, _) = tag("->")(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, table) = parse_identifier(input)?;
    Ok((input, Join {
        table: table.to_string(),
        kind: JoinKind::Inner,
    }))
}

/// Parse operator and value together.
pub fn parse_operator_and_value(input: &str) -> IResult<&str, (Operator, Value)> {
    alt((
        // Fuzzy match: ~value
        map(preceded(char('~'), preceded(ws_or_comment, parse_value)), |v| (Operator::Fuzzy, v)),
        // Contains: @>value (JSON/Array Contains)
        map(preceded(tag("@>"), preceded(ws_or_comment, parse_value)), |v| (Operator::Contains, v)),
        // KeyExists: ?value (JSON Key Exists)
        map(preceded(char('?'), preceded(ws_or_comment, parse_value)), |v| (Operator::KeyExists, v)),
        // Equal: ==value (try before >=)
        map(preceded(tag("=="), preceded(ws_or_comment, parse_value)), |v| (Operator::Eq, v)),
        // Greater than or equal: >=value
        map(preceded(tag(">="), preceded(ws_or_comment, parse_value)), |v| (Operator::Gte, v)),
        // Less than or equal: <=value
        map(preceded(tag("<="), preceded(ws_or_comment, parse_value)), |v| (Operator::Lte, v)),
        // Not equal: !=value
        map(preceded(tag("!="), preceded(ws_or_comment, parse_value)), |v| (Operator::Ne, v)),
        // Greater than: >value
        map(preceded(char('>'), preceded(ws_or_comment, parse_value)), |v| (Operator::Gt, v)),
        // Less than: <value
        map(preceded(char('<'), preceded(ws_or_comment, parse_value)), |v| (Operator::Lt, v)),
        // Equal (Assignment/Comparison)
        map(preceded(char('='), preceded(ws_or_comment, parse_value)), |v| (Operator::Eq, v)),
    ))(input)
}

/// Parse a value.
pub fn parse_value(input: &str) -> IResult<&str, Value> {
    let (input, _) = ws_or_comment(input)?;
    
    alt((
        // Parameter: $1, $2, etc.
        map(preceded(char('$'), digit1), |n: &str| {
            Value::Param(n.parse().unwrap_or(1))
        }),
        // Boolean: true/false
        value(Value::Bool(true), tag("true")),
        value(Value::Bool(false), tag("false")),
        // Function call: name(args)
        parse_function_call,
        // Function without parens: now, etc. (keyword-like)
        map(tag("now"), |_| Value::Function("now".to_string())),
        // Number (float or int)
        parse_number,
        // Double-quoted string
        parse_double_quoted_string,
        // Single-quoted string
        parse_quoted_string,
        // Array literal: ['a', 'b']
        parse_array_literal,
        // Bare identifier (treated as string)
        map(parse_identifier, |s| Value::String(s.to_string())),
    ))(input)
}

/// Parse array literal: [val1, val2]
fn parse_array_literal(input: &str) -> IResult<&str, Value> {
    let (input, _) = char('[')(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, values) = separated_list1(
        tuple((ws_or_comment, char(','), ws_or_comment)),
        parse_value
    )(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, _) = char(']')(input)?;
    
    Ok((input, Value::Array(values)))
}

/// Parse function call: name(arg1, arg2)
fn parse_function_call(input: &str) -> IResult<&str, Value> {
    let (input, name) = parse_identifier(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, args) = opt(tuple((
        parse_value,
        many0(preceded(
            tuple((ws_or_comment, char(','), ws_or_comment)),
            parse_value
        ))
    )))(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, _) = char(')')(input)?;

    let params = match args {
        Some((first, mut rest)) => {
            let mut v = vec![first];
            v.append(&mut rest);
            v
        },
        None => vec![],
    };

    Ok((input, Value::Function(format!("{}({})", name, params.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", ")))))
}

/// Parse a number (integer or float).
fn parse_number(input: &str) -> IResult<&str, Value> {
    let (input, num_str) = recognize(tuple((
        opt(char('-')),
        digit1,
        opt(pair(char('.'), digit1)),
    )))(input)?;
    
    if num_str.contains('.') {
        Ok((input, Value::Float(num_str.parse().unwrap_or(0.0))))
    } else {
        Ok((input, Value::Int(num_str.parse().unwrap_or(0))))
    }
}

/// Parse a single-quoted string.
fn parse_quoted_string(input: &str) -> IResult<&str, Value> {
    let (input, _) = char('\'')(input)?;
    let (input, content) = take_while(|c| c != '\'')(input)?;
    let (input, _) = char('\'')(input)?;
    
    Ok((input, Value::String(content.to_string())))
}

/// Parse a double-quoted string.
fn parse_double_quoted_string(input: &str) -> IResult<&str, Value> {
    let (input, _) = char('"')(input)?;
    let (input, content) = take_while(|c| c != '"')(input)?;
    let (input, _) = char('"')(input)?;
    
    Ok((input, Value::String(content.to_string())))
}
