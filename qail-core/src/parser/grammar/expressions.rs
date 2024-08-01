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
use super::base::{parse_identifier, parse_value, parse_operator};

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
        let (remaining, _) = multispace1(remaining)?;
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

/// Parse identifier or JSON access or type cast.
/// JSON access: col->'key' or col->>'key'
/// Type cast: expr::type
fn parse_json_or_ident(input: &str) -> IResult<&str, Expr> {
    let (input, mut expr) = parse_atom(input)?;
    
    // Check for trailing JSON operators
    // Loop because logically you could chain, though Expr::JsonAccess isn't recursive yet.
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
        parse_special_function, // Try special functions first (SUBSTRING, EXTRACT, TRIM)
        parse_function_or_aggregate,
        parse_star,
        parse_literal, // String literals, numbers, named params
        parse_simple_ident,
    ))(input)
}

fn parse_star(input: &str) -> IResult<&str, Expr> {
    map(tag("*"), |_| Expr::Star)(input)
}

/// Parse literal values (strings, numbers, named params) as expressions
fn parse_literal(input: &str) -> IResult<&str, Expr> {
    use super::base::parse_value;
    use crate::ast::Value;
    
    map(parse_value, |v| match v {
        Value::NamedParam(name) => Expr::Named(format!(":{}", name)),
        Value::Param(n) => Expr::Named(format!("${}", n)),
        Value::String(s) => Expr::Named(format!("'{}'", s)),
        Value::Int(n) => Expr::Named(n.to_string()),
        Value::Float(f) => Expr::Named(f.to_string()),
        Value::Bool(b) => Expr::Named(if b { "TRUE".to_string() } else { "FALSE".to_string() }),
        Value::Null => Expr::Named("NULL".to_string()),
        _ => Expr::Named("VALUE".to_string()),
    })(input)
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
    use crate::parser::grammar::base::{parse_operator, parse_value};
    
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
            // then convert to Value::Function with the expression string
            parse_filter_value(input)?
        };
        
        conditions.push(Condition {
            left: Expr::Named(col.to_string()),
            op,
            value,
            is_array_unnest: false,
        });
        
        current_input = input;
        
        // Check for AND
        let and_result: IResult<&str, _> = preceded(
            tuple((multispace1, tag_no_case("and"), multispace1)),
            peek(parse_identifier)
        )(current_input);
        
        if let Ok((_next_input, _)) = and_result {
            // Skip the AND keyword
            let (next_input, _) = multispace1(current_input)?;
            let (next_input, _) = tag_no_case("and")(next_input)?;
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
    use crate::parser::grammar::base::parse_value;
    
    // First try simple value
    if let Ok((remaining, val)) = parse_value(input) {
        return Ok((remaining, val));
    }
    
    // Try parsing as a function call expression (e.g., now(), now() - 24h)
    // We need to parse until we hit a boundary (AND, ), whitespace before AND)
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
    
    // Return as Value::Function with the expression string (will be output as-is)
    Ok((&input[end_pos..], Value::Function(expr_str.to_string())))
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

fn parse_when(input: &str) -> IResult<&str, (Condition, Box<Expr>)> {
    let (input, _) = tag_no_case("when")(input)?;
    let (input, _) = multispace1(input)?;
    
    // Parse condition LHS as full expression (supports JSON access, functions, etc.)
    let (input, left_expr) = parse_multiplicative_expr(input)?; // Use lower-level to avoid consuming || as OR
    let (input, _) = multispace0(input)?;
    
    // Operator
    let (input, op) = parse_operator(input)?;
    let (input, _) = multispace0(input)?;
    
    // For IS NULL / IS NOT NULL, there's no value to parse
    let (input, val) = if matches!(op, crate::ast::Operator::IsNull | crate::ast::Operator::IsNotNull) {
        (input, crate::ast::Value::Null)
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

#[cfg(test)]
mod case_when_tests {
    use super::*;
    
    #[test]
    fn test_parse_when_is_null() {
        let input = "when name is null then email";
        let result = parse_when(input);
        println!("Result: {:?}", result);
        assert!(result.is_ok(), "Failed to parse WHEN with IS NULL: {:?}", result);
    }
    
    #[test]
    fn test_parse_case_is_null() {
        let input = "case when name is null then email else name end";
        let result = parse_case(input);
        println!("Result: {:?}", result);
        assert!(result.is_ok(), "Failed to parse CASE with IS NULL: {:?}", result);
    }
}
