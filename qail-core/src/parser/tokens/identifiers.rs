use nom::{
    branch::alt,
    bytes::complete::{tag, take_while, take_while1},
    character::complete::{char, multispace1, not_line_ending},
    combinator::{recognize, value},
    multi::many0,
    sequence::{pair, tuple},
    IResult,
};

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
