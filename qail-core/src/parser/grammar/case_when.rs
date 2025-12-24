//! CASE WHEN ... THEN ... ELSE ... END expression parsing.

use nom::{
    bytes::complete::tag_no_case,
    character::complete::{multispace0, multispace1},
    combinator::opt,
    multi::separated_list0,
    sequence::{preceded, tuple},
    IResult,
};
use crate::ast::*;
use super::base::{parse_operator, parse_value};
use super::expressions::{parse_expression, parse_multiplicative_expr};

/// Parse CASE WHEN ... END
pub fn parse_case(input: &str) -> IResult<&str, Expr> {
    let (input, _) = tag_no_case("case")(input)?;
    let (input, _) = multispace1(input)?;
    
    // Parse when clauses
    let (input, when_clauses) = separated_list0(
        multispace1,
        parse_when
    )(input)?;
    
    let (input, _) = multispace0(input)?;
    
    // Else - parse as expression (not just value)
    let (input, else_value) = opt(preceded(
        tuple((tag_no_case("else"), multispace1)),
        parse_expression
    ))(input)?;
    
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("end")(input)?;
    
    Ok((input, Expr::Case {
        when_clauses,
        else_value: else_value.map(Box::new),
        alias: None,
    }))
}

/// Parse a single WHEN condition THEN expression clause
pub fn parse_when(input: &str) -> IResult<&str, (Condition, Box<Expr>)> {
    let (input, _) = tag_no_case("when")(input)?;
    let (input, _) = multispace1(input)?;
    
    // Parse condition LHS as full expression (supports JSON access, functions, etc.)
    let (input, left_expr) = parse_multiplicative_expr(input)?; // Use lower-level to avoid consuming || as OR
    let (input, _) = multispace0(input)?;
    
    // Operator
    let (input, op) = parse_operator(input)?;
    let (input, _) = multispace0(input)?;
    
    // For IS NULL / IS NOT NULL, there's no value to parse
    let (input, val) = if matches!(op, Operator::IsNull | Operator::IsNotNull) {
        (input, Value::Null)
    } else {
        parse_value(input)?
    };
    
    // Use multispace0 since IS NULL already consumed trailing space
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("then")(input)?;
    let (input, _) = multispace1(input)?;
    
    // Parse THEN as expression (not just value) - allows SUBSTRING, functions, etc.
    let (input, then_expr) = parse_expression(input)?;
    
    Ok((input, (
        Condition {
            left: left_expr,
            op,
            value: val,
            is_array_unnest: false,
        },
        Box::new(then_expr)
    )))
}
