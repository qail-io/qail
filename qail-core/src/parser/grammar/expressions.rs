//! Expression parsing coordinator.
//!
//! This module provides the main entry point for parsing SQL expressions
//! and coordinates parsing by importing from specialized submodules:
//! - `binary_ops`: Binary operator chains (+, -, *, /, ||)
//! - `functions`: Function calls and aggregates (COUNT, SUM, FILTER)
//! - `case_when`: CASE WHEN expressions
//! - `special_funcs`: SUBSTRING, EXTRACT, TRIM with keyword syntax

use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::multispace0,
    combinator::{map, opt},
    sequence::preceded,
    IResult,
};
use crate::ast::*;
use super::base::{parse_identifier, parse_value};

// Re-export from submodules for internal use
pub use super::binary_ops::{parse_concat_expr, parse_additive_expr, parse_multiplicative_expr};
pub use super::functions::{parse_function_or_aggregate, parse_function_arg};
pub use super::case_when::parse_case;
pub use super::special_funcs::parse_special_function;

/// Parse a general expression.
/// Handles binary operators with precedence:
/// - Low: || (concat)  
/// - Medium: + - 
/// - High: * / %
pub fn parse_expression(input: &str) -> IResult<&str, Expr> {
    parse_concat_expr(input)
}

/// Parse an expression with optional AS alias
/// e.g., `column`, `CASE...END AS name`, `func(...) AS alias`
pub fn parse_expression_with_alias(input: &str) -> IResult<&str, Expr> {
    let (input, mut expr) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    
    // Check for optional AS alias
    if let Ok((remaining, _)) = tag_no_case::<_, _, nom::error::Error<&str>>("as")(input) {
        let (remaining, _) = nom::character::complete::multispace1(remaining)?;
        let (remaining, alias) = parse_identifier(remaining)?;
        expr = set_expr_alias(expr, alias.to_string());
        return Ok((remaining, expr));
    }
    
    Ok((input, expr))
}

/// Set alias on any Expr variant that supports it
fn set_expr_alias(expr: Expr, alias: String) -> Expr {
    match expr {
        Expr::Named(name) => Expr::Aliased { name, alias },
        Expr::Case { when_clauses, else_value, .. } => Expr::Case { when_clauses, else_value, alias: Some(alias) },
        Expr::FunctionCall { name, args, .. } => Expr::FunctionCall { name, args, alias: Some(alias) },
        Expr::SpecialFunction { name, args, .. } => Expr::SpecialFunction { name, args, alias: Some(alias) },
        Expr::Binary { left, op, right, .. } => Expr::Binary { left, op, right, alias: Some(alias) },
        Expr::JsonAccess { column, path, as_text, .. } => Expr::JsonAccess { column, path, as_text, alias: Some(alias) },
        other => other, // Star, Aliased already have alias
    }
}

/// Parse identifier or JSON access or type cast.
/// JSON access: col->'key' or col->>'key'
/// Type cast: expr::type
pub fn parse_json_or_ident(input: &str) -> IResult<&str, Expr> {
    let (input, mut expr) = parse_atom(input)?;
    
    // Check for trailing JSON operators
    let (input, json_op) = opt(alt((
        tag("->>"),
        tag("->"),
    )))(input)?;
    
    let input = if let Some(op) = json_op {
        if let Expr::Named(col_name) = expr {
             let (input, _) = multispace0(input)?;
             let (input, key_val) = parse_value(input)?;
             
             let path = match key_val {
                 Value::String(s) => s,
                 _ => key_val.to_string(),
             };
             
             expr = Expr::JsonAccess {
                 column: col_name,
                 path,
                 as_text: op == "->>",
                 alias: None,
             };
             input
        } else {
            input
        }
    } else {
        input
    };
    
    // Check for type cast suffix: ::type (PostgreSQL syntax)
    let (input, cast_type) = opt(preceded(
        tag("::"),
        parse_identifier
    ))(input)?;
    
    if let Some(target_type) = cast_type {
        expr = Expr::Cast {
            expr: Box::new(expr),
            target_type: target_type.to_string(),
            alias: None,
        };
    }
    
    Ok((input, expr))
}

/// Parse atomic expressions (functions, case, literals, identifiers, wildcards)
fn parse_atom(input: &str) -> IResult<&str, Expr> {
    alt((
        parse_case,
        parse_special_function,
        parse_function_or_aggregate,
        parse_star,
        parse_literal,
        parse_simple_ident,
    ))(input)
}

fn parse_star(input: &str) -> IResult<&str, Expr> {
    map(tag("*"), |_| Expr::Star)(input)
}

/// Parse literal values (strings, numbers, named params) as expressions
fn parse_literal(input: &str) -> IResult<&str, Expr> {
    map(parse_value, |v| match v {
        Value::NamedParam(name) => Expr::Named(format!(":{}", name)),
        Value::Param(n) => Expr::Named(format!("${}", n)),
        Value::String(s) => Expr::Named(format!("'{}'", s)),
        Value::Int(n) => Expr::Named(n.to_string()),
        Value::Float(f) => {
            // Ensure float always has decimal point (100.0 not 100)
            let s = f.to_string();
            if s.contains('.') { Expr::Named(s) } else { Expr::Named(format!("{}.0", s)) }
        },
        Value::Bool(b) => Expr::Named(if b { "TRUE".to_string() } else { "FALSE".to_string() }),
        Value::Null => Expr::Named("NULL".to_string()),
        _ => Expr::Named("VALUE".to_string()),
    })(input)
}

fn parse_simple_ident(input: &str) -> IResult<&str, Expr> {
    map(parse_identifier, |s| Expr::Named(s.to_string()))(input)
}
