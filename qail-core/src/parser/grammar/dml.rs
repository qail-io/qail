use nom::{
    bytes::complete::{tag_no_case},
    character::complete::{char, multispace0, multispace1},
    multi::separated_list1,
    sequence::tuple,
    IResult,
};
use crate::ast::*;
use super::base::{parse_identifier, parse_value};

/// Parse: values col = val, col2 = val2 (for SET/UPDATE)
pub fn parse_values_clause(input: &str) -> IResult<&str, Cage> {
    let (input, _) = tag_no_case("values")(input)?;
    let (input, _) = multispace1(input)?;
    
    let (input, conditions) = parse_set_assignments(input)?;
    
    Ok((input, Cage {
        kind: CageKind::Payload,
        conditions,
        logical_op: LogicalOp::And,
    }))
}

/// Parse comma-separated assignments: col = val, col2 = val2
pub fn parse_set_assignments(input: &str) -> IResult<&str, Vec<Condition>> {
    separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_assignment
    )(input)
}

/// Parse single assignment: column = value
pub fn parse_assignment(input: &str) -> IResult<&str, Condition> {
    let (input, column) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('=')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, value) = parse_value(input)?;
    
    Ok((input, Condition {
        left: Expr::Named(column.to_string()),
        op: Operator::Eq,
        value,
        is_array_unnest: false,
    }))
}
