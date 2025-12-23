use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{char, multispace0, multispace1},
    combinator::{map, opt},
    multi::separated_list0,
    sequence::{preceded, tuple},
    IResult,
};
use crate::ast::*;
use super::base::{parse_identifier, parse_value, parse_operator};

/// Parse a general expression.
/// Handles binary operators with precedence:
/// - Low: || (concat)  
/// - Medium: + - 
/// - High: * / %
pub fn parse_expression(input: &str) -> IResult<&str, Expr> {
    parse_concat_expr(input)
}

/// Parse concatenation (lowest precedence): expr || expr
fn parse_concat_expr(input: &str) -> IResult<&str, Expr> {
    let (input, left) = parse_additive_expr(input)?;
    parse_binary_chain(input, left, parse_additive_expr, &[("||", BinaryOp::Concat)])
}

/// Parse additive: expr + expr, expr - expr
fn parse_additive_expr(input: &str) -> IResult<&str, Expr> {
    let (input, left) = parse_multiplicative_expr(input)?;
    parse_binary_chain(input, left, parse_multiplicative_expr, &[
        ("+", BinaryOp::Add),
        ("-", BinaryOp::Sub),
    ])
}

/// Parse multiplicative: expr * expr, expr / expr, expr % expr
fn parse_multiplicative_expr(input: &str) -> IResult<&str, Expr> {
    let (input, left) = parse_json_or_ident(input)?;
    parse_binary_chain(input, left, parse_json_or_ident, &[
        ("*", BinaryOp::Mul),
        ("/", BinaryOp::Div),
        ("%", BinaryOp::Rem),
    ])
}

/// Generic left-associative binary chain parser
fn parse_binary_chain<'a, F>(
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
            if let Ok((after_op, _)) = tag::<_, _, nom::error::Error<&str>>(*op_str)(remaining) {
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

/// Parse identifier or JSON access.
/// JSON access: col->'key' or col->>'key'
fn parse_json_or_ident(input: &str) -> IResult<&str, Expr> {
    let (input, mut expr) = parse_atom(input)?;
    
    // Check for trailing JSON operators
    // Loop because logicaly you could chain, though Expr::JsonAccess isn't recursive yet in struct.
    // Expr::JsonAccess has { column: String, path: String ... }
    // So distinct limits chaining unless we restructure Expr to be recursive `base: Box<Expr>`.
    // Current definition: column is String. So only one level supported for now in AST.
    // We will parse ONE level if present.
    
    // Attempt to match -> or ->>
    let (input, json_op) = opt(alt((
        tag("->>"),
        tag("->"),
    )))(input)?;
    
    if let Some(op) = json_op {
        if let Expr::Named(col_name) = expr {
             // Parse key: 'key' (string value)
             let (input, _) = multispace0(input)?;
             let (input, key_val) = parse_value(input)?; // Expect string
             
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
             return Ok((input, expr));
        }
    }
    
    Ok((input, expr))
}

/// Parse atomic expressions (functions, case, identifiers, wildcards)
fn parse_atom(input: &str) -> IResult<&str, Expr> {
    alt((
        parse_case,
        parse_special_function, // Try special functions first (SUBSTRING, EXTRACT, TRIM)
        parse_function_or_aggregate,
        parse_star,
        parse_simple_ident,
    ))(input)
}

fn parse_star(input: &str) -> IResult<&str, Expr> {
    map(tag("*"), |_| Expr::Star)(input)
}

fn parse_simple_ident(input: &str) -> IResult<&str, Expr> {
    map(parse_identifier, |s| Expr::Named(s.to_string()))(input)
}

/// Parse function call or aggregate: name(arg1, arg2)
fn parse_function_or_aggregate(input: &str) -> IResult<&str, Expr> {
    // Identifier followed by (
    let (input, name) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;
    
    // Parse arguments as full expressions (supports nesting)
    let (input, args) = separated_list0(
        tuple((multispace0, char(','), multispace0)),
        parse_function_arg
    )(input)?;
    
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;
    
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
            Ok((input, Expr::Aggregate { col, func }))
        },
        _ => {
            Ok((input, Expr::FunctionCall {
                name: name.to_string(),
                args,
                alias: None,
            }))
        }
    }
}

/// Parse a single function argument (supports expressions or star)
fn parse_function_arg(input: &str) -> IResult<&str, Expr> {
    alt((
        map(tag("*"), |_| Expr::Star),
        parse_expression,
    ))(input)
}

/// Parse CASE WHEN ... END
fn parse_case(input: &str) -> IResult<&str, Expr> {
    let (input, _) = tag_no_case("case")(input)?;
    let (input, _) = multispace1(input)?;
    
    // Parse when clauses
    let (input, when_clauses) = separated_list0(
        multispace1,
        parse_when
    )(input)?;
    
    if when_clauses.is_empty() {
        // error?
    }
    
    let (input, _) = multispace0(input)?;
    
    // Else
    let (input, else_value) = opt(preceded(
        tuple((tag_no_case("else"), multispace1)),
        parse_value
    ))(input)?;
    
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("end")(input)?;
    
    Ok((input, Expr::Case {
        when_clauses,
        else_value: else_value.map(Box::new),
        alias: None,
    }))
}

fn parse_when(input: &str) -> IResult<&str, (Condition, Value)> {
    let (input, _) = tag_no_case("when")(input)?;
    let (input, _) = multispace1(input)?;
    
    // Parse condition LHS as full expression (supports JSON access, functions, etc.)
    let (input, left_expr) = parse_multiplicative_expr(input)?; // Use lower-level to avoid consuming || as OR
    let (input, _) = multispace0(input)?;
    
    // Operator
    let (input, op) = parse_operator(input)?;
    let (input, _) = multispace0(input)?;
    
    let (input, val) = parse_value(input)?;
    
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("then")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, then_val) = parse_value(input)?;
    
    Ok((input, (
        Condition {
            left: left_expr,
            op,
            value: val,
            is_array_unnest: false,
        },
        then_val
    )))
}

/// Parse special SQL functions with keyword syntax
/// e.g., SUBSTRING(expr FROM pos [FOR len]), EXTRACT(YEAR FROM date), TRIM(LEADING 'x' FROM str)
fn parse_special_function(input: &str) -> IResult<&str, Expr> {
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
fn parse_substring(input: &str) -> IResult<&str, Expr> {
    let (input, _) = tag_no_case("substring")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;
    
    // First argument: the string expression
    let (input, string_expr) = parse_expression(input)?;
    let (input, _) = multispace1(input)?;
    
    // FROM keyword
    let (input, _) = tag_no_case("from")(input)?;
    let (input, _) = multispace1(input)?;
    
    // Position expression
    let (input, from_expr) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    
    // Optional FOR length
    let (input, for_expr) = opt(preceded(
        tuple((tag_no_case("for"), multispace1)),
        parse_expression
    ))(input)?;
    
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;
    
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
fn parse_extract(input: &str) -> IResult<&str, Expr> {
    let (input, _) = tag_no_case("extract")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;
    
    // Field name (YEAR, MONTH, DAY, etc.)
    let (input, field) = parse_identifier(input)?;
    let (input, _) = multispace1(input)?;
    
    // FROM keyword
    let (input, _) = tag_no_case("from")(input)?;
    let (input, _) = multispace1(input)?;
    
    // Date expression
    let (input, date_expr) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;
    
    Ok((input, Expr::SpecialFunction {
        name: "EXTRACT".to_string(),
        args: vec![
            (None, Box::new(Expr::Named(field.to_string()))),
            (Some("FROM".to_string()), Box::new(date_expr)),
        ],
        alias: None,
    }))
}
