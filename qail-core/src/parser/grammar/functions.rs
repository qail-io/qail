//! Function and aggregate parsing.
//! 
//! Handles parsing of:
//! - Function calls: name(arg1, arg2)
//! - Aggregates: COUNT, SUM, AVG, MIN, MAX
//! - FILTER (WHERE ...) clause for aggregates
//! - COUNT(DISTINCT col) syntax

use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{char, multispace0, multispace1},
    combinator::{map, opt, peek},
    multi::separated_list0,
    sequence::{preceded, tuple},
    IResult,
};
use crate::ast::*;
use super::base::{parse_identifier, parse_operator, parse_value};
use super::expressions::parse_expression;

/// Parse function call or aggregate: name(arg1, arg2)
pub fn parse_function_or_aggregate(input: &str) -> IResult<&str, Expr> {
    // Identifier followed by (
    let (input, name) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;
    
    // Check for DISTINCT keyword (for COUNT(DISTINCT col), etc.)
    let (input, has_distinct) = opt(tuple((
        tag_no_case("distinct"),
        multispace1
    )))(input)?;
    let distinct = has_distinct.is_some();
    
    // Parse arguments as full expressions (supports nesting)
    let (input, args) = separated_list0(
        tuple((multispace0, char(','), multispace0)),
        parse_function_arg
    )(input)?;
    
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;
    let (input, _) = multispace0(input)?;
    
    // Check for FILTER (WHERE ...) clause - PostgreSQL aggregate extension
    let (input, filter_clause) = opt(parse_filter_clause)(input)?;
    
    // Optional alias: AS alias_name or just alias_name (after space)
    let (input, alias) = opt(preceded(
        tuple((multispace0, tag_no_case("as"), multispace1)),
        parse_identifier
    ))(input)?;
    let alias = alias.map(|s| s.to_string());
    
    let name_lower = name.to_lowercase();
    match name_lower.as_str() {
        "count" | "sum" | "avg" | "min" | "max" => {
            // For aggregates, convert first arg to string representation
            let col = args.first()
                .map(|e| e.to_string())
                .unwrap_or_else(|| "*".to_string());
            let func = match name_lower.as_str() {
                "count" => AggregateFunc::Count,
                "sum" => AggregateFunc::Sum,
                "avg" => AggregateFunc::Avg,
                "min" => AggregateFunc::Min,
                "max" => AggregateFunc::Max,
                _ => AggregateFunc::Count, // unreachable
            };
            Ok((input, Expr::Aggregate { col, func, distinct, filter: filter_clause, alias }))
        },
        _ => {
            Ok((input, Expr::FunctionCall {
                name: name.to_string(),
                args,
                alias,
            }))
        }
    }
}

/// Parse a single function argument (supports expressions or star)
pub fn parse_function_arg(input: &str) -> IResult<&str, Expr> {
    alt((
        map(tag("*"), |_| Expr::Star),
        parse_expression,
    ))(input)
}

/// Parse FILTER (WHERE condition) clause for aggregates
fn parse_filter_clause(input: &str) -> IResult<&str, Vec<Condition>> {
    let (input, _) = tag_no_case("filter")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("where")(input)?;
    let (input, _) = multispace1(input)?;
    
    // Parse conditions (simple version - single or AND-joined conditions)
    let (input, conditions) = parse_filter_conditions(input)?;
    
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;
    
    Ok((input, conditions))
}

/// Parse conditions inside FILTER clause
fn parse_filter_conditions(input: &str) -> IResult<&str, Vec<Condition>> {
    let mut conditions = Vec::new();
    let mut current_input = input;
    
    loop {
        // Parse: column op value/expression
        let (input, _) = multispace0(current_input)?;
        let (input, col) = parse_identifier(input)?;
        let (input, _) = multispace0(input)?;
        let (input, op) = parse_operator(input)?;
        let (input, _) = multispace0(input)?;
        
        // For IS NULL / IS NOT NULL, no value needed
        let (input, value) = if matches!(op, Operator::IsNull | Operator::IsNotNull) {
            (input, Value::Null)
        } else if matches!(op, Operator::In | Operator::NotIn) {
            // Parse IN ('val1', 'val2', ...)
            let (input, _) = char('(')(input)?;
            let (input, _) = multispace0(input)?;
            let (input, values) = separated_list0(
                tuple((multispace0, char(','), multispace0)),
                parse_value
            )(input)?;
            let (input, _) = multispace0(input)?;
            let (input, _) = char(')')(input)?;
            (input, Value::Array(values))
        } else {
            // Try parsing as expression first (for now() - 24h type syntax)
            parse_filter_value(input)?
        };
        
        conditions.push(Condition {
            left: Expr::Named(col.to_string()),
            op,
            value,
            is_array_unnest: false,
        });
        
        current_input = input;
        
        // Check for AND (use multispace0 since parse_filter_value may consume trailing space)
        let and_result: IResult<&str, _> = preceded(
            tuple((multispace0, tag_no_case("and"), multispace1)),
            peek(parse_identifier)
        )(current_input);
        
        if let Ok((_next_input, _)) = and_result {
            // Skip the AND keyword and trailing whitespace
            let (next_input, _) = multispace0(current_input)?;
            let (next_input, _) = tag_no_case("and")(next_input)?;
            let (next_input, _) = multispace1(next_input)?;
            current_input = next_input;
        } else {
            break;
        }
    }
    
    Ok((current_input, conditions))
}

/// Parse a value in FILTER condition that can be either a simple value or an expression
/// like `now() - 24h`. Converts expressions to Value::Function with SQL representation.
fn parse_filter_value(input: &str) -> IResult<&str, Value> {
    // First try simple value (but NOT interval - we want full expression parsing for that)
    // Try parsing just string, int, float, bool, null, params first
    if let Ok((remaining, val)) = parse_value(input) {
        // If it's an interval, we still want the proper SQL format
        if let Value::Interval { amount, unit } = val {
            return Ok((remaining, Value::Function(format!("INTERVAL '{} {}'", amount, unit))));
        }
        
        // If it's a function or other complex value, we need expression parsing
        if !matches!(val, Value::Function(_)) {
            return Ok((remaining, val));
        }
    }
    
    // Try parsing as a full expression (handles now() - 24h correctly)
    // We parse as expression and then convert to SQL string representation
    // Need to parse until we hit a boundary (AND, ))
    let mut end_pos = 0;
    let mut paren_depth = 0;
    
    for (i, c) in input.char_indices() {
        match c {
            '(' => paren_depth += 1,
            ')' => {
                if paren_depth == 0 {
                    end_pos = i;
                    break;
                }
                paren_depth -= 1;
            }
            _ => {}
        }
        
        // Check for AND keyword (case insensitive)
        if paren_depth == 0 && i > 0 {
            let remaining = &input[i..];
            if remaining.len() >= 4 {
                let potential_and = &remaining[..4].to_lowercase();
                if potential_and.starts_with("and ") || potential_and.starts_with("and\t") || potential_and.starts_with("and\n") {
                    end_pos = i;
                    break;
                }
            }
        }
    }
    
    if end_pos == 0 {
        end_pos = input.len();
    }
    
    let expr_str = input[..end_pos].trim();
    if expr_str.is_empty() {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::TakeWhile1)));
    }
    
    // Try to parse as expression and get proper SQL
    if let Ok((_, expr)) = parse_expression(expr_str) {
        // Convert expression to SQL string (this handles INTERVAL properly)
        return Ok((&input[end_pos..], Value::Function(expr.to_string())));
    }
    
    // Fallback: return as-is (shouldn't happen often)
    Ok((&input[end_pos..], Value::Function(expr_str.to_string())))
}
