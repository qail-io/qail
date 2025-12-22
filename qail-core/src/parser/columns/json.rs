use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::char,
    combinator::{map, opt, value},
    sequence::preceded,
    IResult,
};
use crate::ast::*;
use crate::parser::tokens::*;

/// Parse a column with JSON path access: name->'key' or name->>'key'
pub fn parse_json_suffix<'a>(input: &'a str, name: &str) -> IResult<&'a str, Column> {
    let (input, as_text) = alt((
        value(true, tag("->>")),
        value(false, tag("->")),
    ))(input)?;
    
    // Parse path key
    let (input, path) = alt((
        parse_quoted_string,
        map(parse_identifier, |s: &str| s.to_string()),
    ))(input)?;
    
    // Optional alias
    let (input, alias) = opt(preceded(char('@'), parse_identifier))(input)?;
    
    Ok((input, Column::JsonAccess {
        column: name.to_string(),
        path,
        as_text,
        alias: alias.map(|s| s.to_string()),
    }))
}

fn parse_quoted_string(input: &str) -> IResult<&str, String> {
    use nom::bytes::complete::take_until;
    use nom::sequence::delimited;
    
    let (input, content) = delimited(char('\''), take_until("'"), char('\''))(input)?;
    Ok((input, content.to_string()))
}
