//! Binary operator expression parsing.
//! 
//! Handles parsing of binary operator chains with precedence:
//! - Low: || (concat)
//! - Medium: + - 
//! - High: * / %

use nom::{
    bytes::complete::tag,
    character::complete::multispace0,
    IResult,
    Parser,
};
use crate::ast::*;
use super::expressions::parse_json_or_ident;

/// Parse concatenation (lowest precedence): expr || expr
pub fn parse_concat_expr(input: &str) -> IResult<&str, Expr> {
    let (input, left) = parse_additive_expr(input)?;
    parse_binary_chain(input, left, parse_additive_expr, &[("||", BinaryOp::Concat)])
}

/// Parse additive: expr + expr, expr - expr
pub fn parse_additive_expr(input: &str) -> IResult<&str, Expr> {
    let (input, left) = parse_multiplicative_expr(input)?;
    parse_binary_chain(input, left, parse_multiplicative_expr, &[
        ("+", BinaryOp::Add),
        ("-", BinaryOp::Sub),
    ])
}

/// Parse multiplicative: expr * expr, expr / expr, expr % expr
pub fn parse_multiplicative_expr(input: &str) -> IResult<&str, Expr> {
    let (input, left) = parse_json_or_ident(input)?;
    parse_binary_chain(input, left, parse_json_or_ident, &[
        ("*", BinaryOp::Mul),
        ("/", BinaryOp::Div),
        ("%", BinaryOp::Rem),
    ])
}

/// Generic left-associative binary chain parser
pub fn parse_binary_chain<'a, F>(
    mut input: &'a str,
    mut left: Expr,
    parse_operand: F,
    operators: &[(&str, BinaryOp)],
) -> IResult<&'a str, Expr>
where
    F: Fn(&'a str) -> IResult<&'a str, Expr>,
{
    loop {
        let (remaining, _) = multispace0(input)?;
        
        // Try each operator
        let mut matched = None;
        for (op_str, op_enum) in operators {
            if let Ok((after_op, _)) = tag::<_, _, nom::error::Error<&str>>(*op_str).parse(remaining) {
                matched = Some((after_op, *op_enum));
                break;
            }
        }
        
        if let Some((after_op, op)) = matched {
            let (after_ws, _) = multispace0(after_op)?;
            let (after_right, right) = parse_operand(after_ws)?;
            left = Expr::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
                alias: None,
            };
            input = after_right;
        } else {
            break;
        }
    }
    
    Ok((input, left))
}
