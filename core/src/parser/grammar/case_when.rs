//! CASE WHEN ... THEN ... ELSE ... END expression parsing.

use super::base::{parse_operator, parse_value};
use super::expressions::{parse_expression, parse_multiplicative_expr};
use crate::ast::*;
use nom::{
    IResult, Parser,
    bytes::complete::tag_no_case,
    character::complete::{multispace0, multispace1},
    combinator::opt,
    multi::{separated_list0, separated_list1},
    sequence::preceded,
};

/// Parse a `CASE WHEN ... THEN ... [ELSE ...] END` expression into an AST `Expr::Case`.
///
/// Simple CASE (`CASE operand WHEN value THEN ...`) becomes searched CASE
/// with `operand = value` per arm. The operand is repeated, so it is limited
/// to column references, which evaluate the same each time.
pub fn parse_case(input: &str) -> IResult<&str, Expr> {
    let (input, _) = tag_no_case("case").parse(input)?;
    let (input, _) = multispace1(input)?;

    if tag_no_case::<_, _, nom::error::Error<&str>>("when")
        .parse(input)
        .is_err()
    {
        return parse_simple_case(input);
    }

    let (input, when_clauses) = separated_list0(multispace1, parse_when).parse(input)?;
    if when_clauses.is_empty() {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }

    let (input, _) = multispace0(input)?;

    // Else - parse as expression (not just value)
    let (input, else_value) = opt(preceded(
        (tag_no_case("else"), multispace1),
        parse_expression,
    ))
    .parse(input)?;

    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("end").parse(input)?;

    Ok((
        input,
        Expr::Case {
            when_clauses,
            else_value: else_value.map(Box::new),
            alias: None,
        },
    ))
}

fn parse_simple_case(input: &str) -> IResult<&str, Expr> {
    let operand_start = input;
    let (input, operand) = parse_multiplicative_expr(input)?;
    if !matches!(operand, Expr::Named(_) | Expr::JsonAccess { .. }) {
        return Err(nom::Err::Failure(nom::error::Error::new(
            operand_start,
            nom::error::ErrorKind::Verify,
        )));
    }
    let (input, _) = multispace1(input)?;

    let simple_when = |input| -> IResult<&str, (Condition, Box<Expr>)> {
        let (input, _) = tag_no_case("when").parse(input)?;
        let (input, _) = multispace1(input)?;
        let (input, value) = parse_value(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("then").parse(input)?;
        let (input, _) = multispace1(input)?;
        let (input, then_expr) = parse_expression(input)?;
        Ok((
            input,
            (
                Condition {
                    left: operand.clone(),
                    op: Operator::Eq,
                    value,
                    is_array_unnest: false,
                },
                Box::new(then_expr),
            ),
        ))
    };
    let (input, when_clauses) = separated_list1(multispace1, simple_when).parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, else_value) = opt(preceded(
        (tag_no_case("else"), multispace1),
        parse_expression,
    ))
    .parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("end").parse(input)?;

    Ok((
        input,
        Expr::Case {
            when_clauses,
            else_value: else_value.map(Box::new),
            alias: None,
        },
    ))
}

/// Parse a single WHEN condition THEN expression clause
pub fn parse_when(input: &str) -> IResult<&str, (Condition, Box<Expr>)> {
    let (input, _) = tag_no_case("when").parse(input)?;
    let (input, _) = multispace1(input)?;

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
    let (input, _) = tag_no_case("then").parse(input)?;
    let (input, _) = multispace1(input)?;

    let (input, then_expr) = parse_expression(input)?;

    Ok((
        input,
        (
            Condition {
                left: left_expr,
                op,
                value: val,
                is_array_unnest: false,
            },
            Box::new(then_expr),
        ),
    ))
}
