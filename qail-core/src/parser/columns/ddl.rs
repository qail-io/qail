use nom::{
    branch::alt,
    bytes::complete::{tag, take_until, take_while1},
    character::complete::{char, multispace0},
    combinator::{map, value},
    multi::{many0, separated_list1},
    sequence::{delimited, pair, preceded, tuple},
    IResult,
};
use crate::ast::*;
use crate::parser::tokens::*;
use crate::ast::WindowFrame;

pub fn parse_constraints(input: &str) -> IResult<&str, Vec<Constraint>> {
    many0(alt((
        // ^pk without parentheses (column-level PK)
        map(
            tuple((tag("^pk"), nom::combinator::not(char('(')))),
            |_| Constraint::PrimaryKey
        ),
        // ^uniq without following 'ue(' (to avoid matching ^unique())
        map(
            tuple((tag("^uniq"), nom::combinator::not(tag("ue(")))),
            |_| Constraint::Unique
        ),
        value(Constraint::Nullable, char('?')),
        parse_default_constraint,
        parse_check_constraint,
        parse_comment_constraint,
    )))(input)
}

/// Parse DEFAULT value constraint: `= value` or `= func()`
fn parse_default_constraint(input: &str) -> IResult<&str, Constraint> {
    let (input, _) = preceded(multispace0, char('='))(input)?;
    let (input, _) = multispace0(input)?;
    
    // Parse function call like uuid(), now(), or literal values
    let (input, value) = alt((
        // Function call: name()
        map(
            pair(
                take_while1(|c: char| c.is_alphanumeric() || c == '_'),
                pair(char('('), char(')'))
            ),
            |(name, _parens)| format!("{}()", name)
        ),
        // Numeric literal
        map(
            take_while1(|c: char| c.is_numeric() || c == '.' || c == '-'),
            |s: &str| s.to_string()
        ),
        // Quoted string
        map(
            delimited(char('"'), take_until("\""), char('"')),
            |s: &str| format!("'{}'", s)
        ),
    ))(input)?;
    
    Ok((input, Constraint::Default(value)))
}

/// Parse CHECK constraint: `^check("a","b","c")`
fn parse_check_constraint(input: &str) -> IResult<&str, Constraint> {
    let (input, _) = tag("^check(")(input)?;
    let (input, values) = separated_list1(
        char(','),
        delimited(
            multispace0,
            delimited(char('"'), take_until("\""), char('"')),
            multispace0
        )
    )(input)?;
    let (input, _) = char(')')(input)?;
    
    Ok((input, Constraint::Check(values.into_iter().map(|s| s.to_string()).collect())))
}

/// Parse COMMENT constraint: `^comment("description")`
fn parse_comment_constraint(input: &str) -> IResult<&str, Constraint> {
    let (input, _) = tag("^comment(\"")(input)?;
    let (input, text) = take_until("\"")(input)?;
    let (input, _) = tag("\")")(input)?;
    Ok((input, Constraint::Comment(text.to_string())))
}

/// Parse index columns: 'col1-col2-col3
pub fn parse_index_columns(input: &str) -> IResult<&str, Vec<String>> {
    let (input, _) = char('\'')(input)?;
    let (input, first) = parse_identifier(input)?;
    let (input, rest) = many0(preceded(char('-'), parse_identifier))(input)?;
    
    let mut cols = vec![first.to_string()];
    cols.extend(rest.iter().map(|s| s.to_string()));
    Ok((input, cols))
}

/// Parse table-level constraints: ^unique(col1, col2) or ^pk(col1, col2)
pub fn parse_table_constraints(input: &str) -> IResult<&str, Vec<TableConstraint>> {
    many0(alt((
        parse_table_unique,
        parse_table_pk,
    )))(input)
}

/// Parse ^unique(col1, col2)
fn parse_table_unique(input: &str) -> IResult<&str, TableConstraint> {
    let (input, _) = tag("^unique(")(input)?;
    let (input, (cols, _)) = parse_constraint_columns(input)?;
    let (input, _) = char(')')(input)?;
    Ok((input, TableConstraint::Unique(cols)))
}

/// Parse ^pk(col1, col2)
fn parse_table_pk(input: &str) -> IResult<&str, TableConstraint> {
    let (input, _) = tag("^pk(")(input)?;
    let (input, (cols, _)) = parse_constraint_columns(input)?;
    let (input, _) = char(')')(input)?;
    Ok((input, TableConstraint::PrimaryKey(cols)))
}

/// Parse comma-separated column names: col1, col2, col3
/// Returns (columns, optional_window_frame) because it handles window partition block {Part=...}
pub fn parse_constraint_columns(input: &str) -> IResult<&str, (Vec<String>, Option<WindowFrame>)> {
    let (input, _) = multispace0(input)?;
    let (input, first) = parse_identifier(input)?;
    let (input, rest) = many0(preceded(
        tuple((multispace0, char(','), multispace0)),
        parse_identifier
    ))(input)?;
    let (input, _) = multispace0(input)?;
    
    let mut cols = vec![first.to_string()];
    cols.extend(rest.iter().map(|s| s.to_string()));
    Ok((input, (cols, None)))
}
