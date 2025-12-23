use nom::{
    branch::alt,
    bytes::complete::{tag_no_case},
    character::complete::{char, multispace0, multispace1},
    combinator::{opt, map},
    sequence::{tuple, preceded},
    IResult,
};
use crate::ast::*;
use super::base::parse_identifier;
use super::expressions::parse_expression;

/// Parse join clause: [left|right|inner] join table [on col = col]
pub fn parse_join_clause(input: &str) -> IResult<&str, Join> {
    // Parse join kind (optional, defaults to LEFT)
    let (input, kind) = alt((
        map(
            tuple((tag_no_case("left"), multispace1, tag_no_case("join"))),
            |_| JoinKind::Left
        ),
        map(
            tuple((tag_no_case("right"), multispace1, tag_no_case("join"))),
            |_| JoinKind::Right
        ),
        map(
            tuple((tag_no_case("inner"), multispace1, tag_no_case("join"))),
            |_| JoinKind::Inner
        ),
        // Default: just "join" = LEFT join
        map(tag_no_case("join"), |_| JoinKind::Left),
    ))(input)?;
    
    let (input, _) = multispace1(input)?;
    let (input, table) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    
    // Optional ON clause: parse as a single condition (left.col = right.col)
    let (input, on_clause) = opt(preceded(
        tuple((tag_no_case("on"), multispace1)),
        parse_join_condition
    ))(input)?;
    
    Ok((input, Join {
        table: table.to_string(),
        kind,
        on: on_clause,
    }))
}

/// Parse join condition: table.col = table.col
pub fn parse_join_condition(input: &str) -> IResult<&str, Vec<Condition>> {
    let (input, left_expr) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('=')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, right_col) = parse_identifier(input)?;
    
    Ok((input, vec![Condition {
        left: left_expr, // Use parsed expression
        op: Operator::Eq,
        value: Value::Column(right_col.to_string()),
        is_array_unnest: false,
    }]))
}
