use nom::{
    character::complete::{char, multispace0},
    combinator::opt,
    multi::separated_list1,
    sequence::{preceded, tuple},
    IResult,
};
use crate::ast::*;
use crate::parser::tokens::*;

/// Parse a column function call: name(arg1, arg2)
pub fn parse_call_suffix<'a>(input: &'a str, name: &str) -> IResult<&'a str, Column> {
    let (input, _) = char('(')(input)?;
    
    // Parse args
    let (input, args) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_arg_value
    )(input)?;
    
    let (input, _) = char(')')(input)?;
    
    // Optional alias
    let (input, alias) = opt(preceded(char('@'), parse_identifier))(input)?;
    
    Ok((input, Column::FunctionCall {
        name: name.to_string(),
        args: args.into_iter().map(|s| s.to_string()).collect(),
        alias: alias.map(|s| s.to_string()),
    }))
}

pub fn parse_function_column(input: &str) -> IResult<&str, Column> {
    // Look ahead to ensure it's a function call `name(`
    let (input, name) = parse_identifier(input)?;
    let (input, _) = char('(')(input)?;
    
    // Parse args
    let (input, args) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_arg_value // Need a parser that accepts identifiers, strings, numbers
    )(input)?;
    
    let (input, _) = char(')')(input)?;
    
    // Optional alias
    let (input, alias) = opt(preceded(char('@'), parse_identifier))(input)?;
    
    Ok((input, Column::FunctionCall {
        name: name.to_string(),
        args: args.into_iter().map(|s| s.to_string()).collect(),
        alias: alias.map(|s| s.to_string()),
    }))
}

/// Helper to parse function arguments by consuming valid SQL expressions until a delimiter
pub fn parse_arg_value(input: &str) -> IResult<&str, String> {
    let mut chars = input.chars().peekable();
    let mut depth = 0;
    let mut quote: Option<char> = None;
    let mut len = 0;

    while let Some(&c) = chars.peek() {
        if let Some(q) = quote {
            // Inside quote, consume until closing quote
            if c == q {
                quote = None;
            }
        } else {
            match c {
                '\'' | '"' => quote = Some(c),
                '(' => depth += 1,
                ')' => {
                    if depth == 0 {
                        // End of argument list
                        break;
                    }
                    depth -= 1;
                }
                ',' => {
                    if depth == 0 {
                        // End of argument
                        break;
                    }
                }
                _ => {}
            }
        }
        
        len += c.len_utf8();
        chars.next();
    }

    if len == 0 {
         return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::NonEmpty)));
    }

    let (captured, remaining) = input.split_at(len);
    let captured = captured.trim();

    // Heuristics:
    // 1. If it starts/ends with {}, it's already a raw block.
    // 2. If it's a simple identifier (alphanumeric + _ + . for table.col), treat as Column (quoted).
    // 3. Otherwise (spaces, operators), wrap in {} to treat as raw.
    
    // Check if simple identifier
    let is_simple = captured.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.');
    // Check if integer (simple digits)
    let is_int = captured.chars().all(|c| c.is_ascii_digit());
    // Check if raw block
    let is_raw = captured.starts_with('{') && captured.ends_with('}');
    // Check if literal
    let is_literal = captured.starts_with('\'');

    let result = if is_raw {
        captured.to_string()
    } else if is_literal {
        // String literal -> must be raw to avoid quoting quotes: "'foo'" -> "{'foo'}"
         format!("{{{}}}", captured)
    } else if is_simple && !is_int {
        // Simple identifier -> let transpiler quote it
        captured.to_string()
    } else {
        // Complex expression (func calls, math, operators, spaces, numbers) -> Raw
        format!("{{{}}}", captured)
    };

    Ok((remaining, result))
}
