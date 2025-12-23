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

/// Parse: values :val1, :val2 (for INSERT/ADD) - just list of values without column names
pub fn parse_insert_values(input: &str) -> IResult<&str, Cage> {
    let (input, _) = tag_no_case("values")(input)?;
    let (input, _) = multispace1(input)?;
    
    // Parse comma-separated values
    let (input, values) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_value
    )(input)?;
    
    // Create conditions with placeholder column names (positional)
    let conditions: Vec<Condition> = values.into_iter().enumerate().map(|(i, val)| {
        Condition {
            left: Expr::Named(format!("${}", i + 1)), // Use positional placeholder for column
            op: Operator::Eq,
            value: val,
            is_array_unnest: false,
        }
    }).collect();
    
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

/// Parse ON CONFLICT clause: conflict (col1, col2) update col = val OR conflict (col) nothing
/// 
/// Syntax:
/// - `conflict (col1, col2) nothing` -> ON CONFLICT (col1, col2) DO NOTHING
/// - `conflict (col1) update col2 = val` -> ON CONFLICT (col1) DO UPDATE SET col2 = val
pub fn parse_on_conflict(input: &str) -> IResult<&str, OnConflict> {
    use nom::branch::alt;
    
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("conflict")(input)?;
    let (input, _) = multispace0(input)?;
    
    // Parse conflict columns: (col1, col2)
    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, columns) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_identifier
    )(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;
    let (input, _) = multispace0(input)?;
    
    // Parse action: "nothing" or "update col = val, col2 = val2"
    let (input, action) = alt((
        parse_conflict_nothing,
        parse_conflict_update,
    ))(input)?;
    
    Ok((input, OnConflict {
        columns: columns.iter().map(|s| s.to_string()).collect(),
        action,
    }))
}

/// Parse: nothing
fn parse_conflict_nothing(input: &str) -> IResult<&str, ConflictAction> {
    use nom::combinator::value;
    value(ConflictAction::DoNothing, tag_no_case("nothing"))(input)
}

/// Parse: update col = val, col2 = val2
fn parse_conflict_update(input: &str) -> IResult<&str, ConflictAction> {
    let (input, _) = tag_no_case("update")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, assignments) = parse_conflict_assignments(input)?;
    
    Ok((input, ConflictAction::DoUpdate { assignments }))
}

/// Parse assignments for ON CONFLICT UPDATE: col = val, col2 = excluded.col2
fn parse_conflict_assignments(input: &str) -> IResult<&str, Vec<(String, Expr)>> {
    separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_conflict_assignment
    )(input)
}

/// Parse single conflict assignment: column = expression
fn parse_conflict_assignment(input: &str) -> IResult<&str, (String, Expr)> {
    use super::expressions::parse_expression;
    
    let (input, column) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('=')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, expr) = parse_expression(input)?;
    
    Ok((input, (column.to_string(), expr)))
}
