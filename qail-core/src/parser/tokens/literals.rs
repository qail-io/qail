use nom::{
    branch::alt,
    bytes::complete::{tag, take_while},
    character::complete::{char, digit1},
    combinator::{map, opt, recognize, value},
    multi::{many0, separated_list1},
    sequence::{pair, preceded, tuple},
    IResult,
};

use crate::ast::*;
use super::identifiers::{ws_or_comment, parse_identifier};
use super::utils::parse_balanced_block;

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
        // Raw block: { ... } - keep as String "{...}"
        map(parse_balanced_block, |s: &str| Value::String(format!("{{{}}}", s))),
        // Bare identifier (treated as Column for SQL standard behavior)
        map(parse_identifier, |s| Value::Column(s.to_string())),
    ))(input)
}

/// Parse value excluding bare identifiers (to resolve ambiguity in Joins)
pub fn parse_value_no_bare_id(input: &str) -> IResult<&str, Value> {
    alt((
        map(preceded(char('$'), digit1), |n: &str| {
            Value::Param(n.parse().unwrap_or(1))
        }),
        value(Value::Bool(true), tag("true")),
        value(Value::Bool(false), tag("false")),
        // parse_function_call, // might conflict if function names are identifiers? yes.
        // For simplicity allow numbers and strings
        parse_number,
        parse_double_quoted_string,
        parse_quoted_string,
    ))(input)
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
pub fn parse_number(input: &str) -> IResult<&str, Value> {
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
pub fn parse_quoted_string(input: &str) -> IResult<&str, Value> {
    let (input, _) = char('\'')(input)?;
    let (input, content) = take_while(|c| c != '\'')(input)?;
    let (input, _) = char('\'')(input)?;
    
    Ok((input, Value::String(content.to_string())))
}

/// Parse a double-quoted string.
pub fn parse_double_quoted_string(input: &str) -> IResult<&str, Value> {
    let (input, _) = char('"')(input)?;
    let (input, content) = take_while(|c| c != '"')(input)?;
    let (input, _) = char('"')(input)?;
    
    Ok((input, Value::String(content.to_string())))
}
