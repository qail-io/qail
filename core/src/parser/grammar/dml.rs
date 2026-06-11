use super::base::{parse_bare_identifier, parse_value};
use crate::ast::*;
use nom::{
    IResult, Parser,
    bytes::complete::tag_no_case,
    character::complete::{char, multispace0, multispace1},
    multi::separated_list1,
};
use std::collections::HashSet;

/// Parse: values col = val, col2 = val2 (for SET/UPDATE)
pub fn parse_values_clause(input: &str) -> IResult<&str, Cage> {
    let (input, _) = tag_no_case("values").parse(input)?;
    let (input, _) = multispace1(input)?;

    let (input, conditions) = parse_set_assignments(input)?;

    Ok((
        input,
        Cage {
            kind: CageKind::Payload,
            conditions,
            logical_op: LogicalOp::And,
        },
    ))
}

/// Parse: values :val1, :val2 (for INSERT/ADD) - just list of values without column names
pub fn parse_insert_values(input: &str) -> IResult<&str, Cage> {
    let (input, _) = tag_no_case("values").parse(input)?;
    let (input, _) = multispace1(input)?;

    let (input, values) =
        separated_list1((multispace0, char(','), multispace0), parse_value).parse(input)?;

    let conditions: Vec<Condition> = values
        .into_iter()
        .enumerate()
        .map(|(i, val)| {
            Condition {
                left: Expr::Named(format!("${}", i + 1)), // Use positional placeholder for column
                op: Operator::Eq,
                value: val,
                is_array_unnest: false,
            }
        })
        .collect();

    Ok((
        input,
        Cage {
            kind: CageKind::Payload,
            conditions,
            logical_op: LogicalOp::And,
        },
    ))
}

/// Parse comma-separated assignments: col = val, col2 = val2
pub fn parse_set_assignments(input: &str) -> IResult<&str, Vec<Condition>> {
    let (remaining, conditions) =
        separated_list1((multispace0, char(','), multispace0), parse_assignment).parse(input)?;
    if !condition_targets_are_unique(&conditions) {
        return Err(duplicate_target_error(input));
    }
    Ok((remaining, conditions))
}

/// Parse single assignment: column = value or column = expression (supports functions and subqueries)
pub fn parse_assignment(input: &str) -> IResult<&str, Condition> {
    use super::expressions::parse_expression;
    use nom::branch::alt;

    let (input, column) = parse_bare_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('=').parse(input)?;
    let (input, _) = multispace0(input)?;

    // Try simple value first (booleans, strings, numbers, params), then subquery, then expression
    let (input, value) = alt((
        // Try simple value parsing first (handles booleans, strings, numbers, params)
        parse_value,
        // Try parenthesized subquery: (get ...)
        parse_subquery_value,
        // Fall back to expression and convert to Value::Function
        nom::combinator::map(parse_expression, |expr| Value::Function(expr.to_string())),
    ))
    .parse(input)?;

    Ok((
        input,
        Condition {
            left: Expr::Named(column.to_string()),
            op: Operator::Eq,
            value,
            is_array_unnest: false,
        },
    ))
}

/// Parse a subquery value: (get ...) -> Value::Subquery
fn parse_subquery_value(input: &str) -> IResult<&str, Value> {
    let (input, _) = char('(').parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, subquery) = super::parse_root(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')').parse(input)?;
    Ok((input, Value::Subquery(Box::new(subquery))))
}

/// Parse ON CONFLICT clause: conflict (col1, col2) update col = val OR conflict (col) nothing
/// Syntax:
/// - `conflict (col1, col2) nothing` -> ON CONFLICT (col1, col2) DO NOTHING
/// - `conflict (col1) update col2 = val` -> ON CONFLICT (col1) DO UPDATE SET col2 = val
pub fn parse_on_conflict(input: &str) -> IResult<&str, OnConflict> {
    use nom::branch::alt;

    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("conflict").parse(input)?;
    let (input, _) = multispace0(input)?;

    let (input, _) = char('(').parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, columns) =
        separated_list1((multispace0, char(','), multispace0), parse_bare_identifier)
            .parse(input)?;
    if !identifier_targets_are_unique(&columns) {
        return Err(duplicate_target_error(input));
    }
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')').parse(input)?;
    let (input, _) = multispace0(input)?;

    let (input, action) = alt((parse_conflict_nothing, parse_conflict_update)).parse(input)?;

    Ok((
        input,
        OnConflict {
            columns: columns.iter().map(|s| s.to_string()).collect(),
            action,
        },
    ))
}

/// Parse: nothing
fn parse_conflict_nothing(input: &str) -> IResult<&str, ConflictAction> {
    use nom::combinator::value;
    value(ConflictAction::DoNothing, tag_no_case("nothing")).parse(input)
}

/// Parse: update col = val, col2 = val2
fn parse_conflict_update(input: &str) -> IResult<&str, ConflictAction> {
    let (input, _) = tag_no_case("update").parse(input)?;
    let (input, _) = multispace1(input)?;
    let (input, assignments) = parse_conflict_assignments(input)?;

    Ok((input, ConflictAction::DoUpdate { assignments }))
}

/// Parse assignments for ON CONFLICT UPDATE: col = val, col2 = excluded.col2
fn parse_conflict_assignments(input: &str) -> IResult<&str, Vec<(String, Expr)>> {
    let (remaining, assignments) = separated_list1(
        (multispace0, char(','), multispace0),
        parse_conflict_assignment,
    )
    .parse(input)?;
    if !conflict_assignment_targets_are_unique(&assignments) {
        return Err(duplicate_target_error(input));
    }
    Ok((remaining, assignments))
}

/// Parse single conflict assignment: column = expression (supports :named_params)
fn parse_conflict_assignment(input: &str) -> IResult<&str, (String, Expr)> {
    use super::expressions::parse_expression;
    use nom::branch::alt;

    let (input, column) = parse_bare_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('=').parse(input)?;
    let (input, _) = multispace0(input)?;

    // Try to parse a value first (handles :named_params, literals, etc.)
    // Then fall back to full expression parsing
    let (input, expr) = alt((
        nom::combinator::map(parse_value, super::expressions::value_to_expr),
        // Fall back to full expression parsing
        parse_expression,
    ))
    .parse(input)?;

    Ok((input, (column.to_string(), expr)))
}

fn condition_targets_are_unique(conditions: &[Condition]) -> bool {
    let mut seen = HashSet::new();
    conditions.iter().all(|condition| match &condition.left {
        Expr::Named(name) => seen.insert(name.as_str()),
        _ => true,
    })
}

fn identifier_targets_are_unique(columns: &[&str]) -> bool {
    let mut seen = HashSet::new();
    columns.iter().all(|column| seen.insert(*column))
}

fn conflict_assignment_targets_are_unique(assignments: &[(String, Expr)]) -> bool {
    let mut seen = HashSet::new();
    assignments
        .iter()
        .all(|(column, _)| seen.insert(column.as_str()))
}

fn duplicate_target_error(input: &str) -> nom::Err<nom::error::Error<&str>> {
    nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
}

/// Parse: from (get ...) - source query for INSERT...SELECT
/// Syntax: `from (get table fields col1, col2 where ...)`
pub fn parse_source_query(input: &str) -> IResult<&str, Box<crate::ast::Qail>> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("from").parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, subquery) = super::parse_root(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')').parse(input)?;
    Ok((input, Box::new(subquery)))
}
