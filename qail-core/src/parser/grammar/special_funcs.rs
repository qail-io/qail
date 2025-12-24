//! Special SQL functions with keyword syntax.
//! 
//! Handles parsing of:
//! - SUBSTRING(expr FROM pos [FOR len])
//! - EXTRACT(field FROM date_expr)
//! - TRIM(LEADING/TRAILING/BOTH 'x' FROM str) (future)

use nom::{
    bytes::complete::tag_no_case,
    character::complete::{char, multispace0, multispace1},
    combinator::opt,
    sequence::{preceded},
    Parser,
    IResult,
};
use crate::ast::*;
use super::base::parse_identifier;
use super::expressions::parse_expression;

/// Parse special SQL functions with keyword syntax
/// e.g., SUBSTRING(expr FROM pos [FOR len]), EXTRACT(YEAR FROM date), TRIM(LEADING 'x' FROM str)
pub fn parse_special_function(input: &str) -> IResult<&str, Expr> {
    // Try SUBSTRING first
    if let Ok(result) = parse_substring(input) {
        return Ok(result);
    }
    // Try EXTRACT
    if let Ok(result) = parse_extract(input) {
        return Ok(result);
    }
    // Not a special function
    Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)))
}

/// Parse SUBSTRING(expr FROM pos [FOR len])
pub fn parse_substring(input: &str) -> IResult<&str, Expr> {
    let (input, _) = tag_no_case("substring").parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, _) = multispace0(input)?;
    
    // First argument: the string expression
    let (input, string_expr) = parse_expression(input)?;
    let (input, _) = multispace1(input)?;
    
    // FROM keyword
    let (input, _) = tag_no_case("from").parse(input)?;
    let (input, _) = multispace1(input)?;
    
    // Position expression
    let (input, from_expr) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    
    // Optional FOR length
    let (input, for_expr) = opt(preceded(
        (tag_no_case("for"), multispace1),
        parse_expression
    )).parse(input)?;
    
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')').parse(input)?;
    
    let mut args = vec![
        (None, Box::new(string_expr)),
        (Some("FROM".to_string()), Box::new(from_expr)),
    ];
    if let Some(len_expr) = for_expr {
        args.push((Some("FOR".to_string()), Box::new(len_expr)));
    }
    
    Ok((input, Expr::SpecialFunction {
        name: "SUBSTRING".to_string(),
        args,
        alias: None,
    }))
}

/// Parse EXTRACT(field FROM date_expr)
pub fn parse_extract(input: &str) -> IResult<&str, Expr> {
    let (input, _) = tag_no_case("extract").parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, _) = multispace0(input)?;
    
    // Field name (YEAR, MONTH, DAY, etc.)
    let (input, field) = parse_identifier(input)?;
    let (input, _) = multispace1(input)?;
    
    // FROM keyword
    let (input, _) = tag_no_case("from").parse(input)?;
    let (input, _) = multispace1(input)?;
    
    // Date expression
    let (input, date_expr) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')').parse(input)?;
    
    Ok((input, Expr::SpecialFunction {
        name: "EXTRACT".to_string(),
        args: vec![
            (None, Box::new(Expr::Named(field.to_string()))),
            (Some("FROM".to_string()), Box::new(date_expr)),
        ],
        alias: None,
    }))
}
