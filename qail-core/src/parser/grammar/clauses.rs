use nom::{
    branch::alt,
    bytes::complete::{tag_no_case},
    character::complete::{char, multispace0, multispace1, digit1},
    combinator::{opt, map, value},
    multi::{separated_list0, separated_list1, many0},
    sequence::{tuple, preceded, delimited},
    IResult,
};
use crate::ast::*;
use super::base::{parse_identifier, parse_value, parse_operator};
use super::expressions::parse_expression;

/// Parse: fields id, name, email  OR  fields *
pub fn parse_fields_clause(input: &str) -> IResult<&str, Vec<Expr>> {
    let (input, _) = tag_no_case("fields")(input)?;
    let (input, _) = multispace1(input)?;
    
    alt((
        // Wildcard: *
        map(char('*'), |_| vec![Expr::Star]),
        // Column list: id, name, email
        parse_column_list,
    ))(input)
}

/// Parse comma-separated column list, respecting parenthesis depth
/// (commas inside parens don't split columns)
pub fn parse_column_list(input: &str) -> IResult<&str, Vec<Expr>> {
    let mut columns = Vec::new();
    let mut current_input = input;
    
    loop {
        // Parse a single column
        let (remaining, col) = parse_single_column(current_input)?;
        columns.push(col);
        current_input = remaining;
        
        // Skip whitespace
        let (remaining, _) = multispace0(current_input)?;
        
        // Check for comma separator at depth 0
        if remaining.starts_with(',') {
            // Consume comma and whitespace
            let (remaining, _) = char(',')(remaining)?;
            let (remaining, _) = multispace0(remaining)?;
            current_input = remaining;
        } else {
            // No more columns
            current_input = remaining;
            break;
        }
    }
    
    if columns.is_empty() {
        Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::SeparatedList)))
    } else {
        Ok((current_input, columns))
    }
}

/// Parse a single column with optional alias: name as display_name
pub fn parse_single_column(input: &str) -> IResult<&str, Expr> {
    let (input, mut expr) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    
    // Check for alias
    let (input, alias) = opt(preceded(
        tuple((tag_no_case("as"), multispace1)),
        parse_identifier
    ))(input)?;
    
    if let Some(a) = alias {
        // Wrap whatever expr we found in Aliased?
        // Wait, Expr::Aliased has { name: String, alias: String }.
        // This suggests only named columns can be aliased?
        // AST needs update if we want aliased complex expressions.
        // Actually Expr definition:
        // Aliased { name: String, alias: String }
        // Case { ..., alias: Option<String> }
        // JsonAccess { ..., alias: Option<String> }
        // FunctionCall { ..., alias: Option<String> }
        
        // We should move Alias into global Expr wrapper or update Expr structure.
        // For now, let's map what we can.
        expr = match expr {
            Expr::Named(n) => Expr::Aliased { name: n, alias: a.to_string() },
            Expr::FunctionCall { name, args, .. } => Expr::FunctionCall { name, args, alias: Some(a.to_string()) },
            Expr::JsonAccess { column, path, as_text, .. } => Expr::JsonAccess { column, path, as_text, alias: Some(a.to_string()) },
            Expr::Case { when_clauses, else_value, .. } => Expr::Case { when_clauses, else_value, alias: Some(a.to_string()) },
            Expr::Aggregate { col, func, distinct, filter, .. } => Expr::Aggregate { col, func, distinct, filter, alias: Some(a.to_string()) },
            Expr::Cast { expr: inner, target_type, .. } => Expr::Cast { expr: inner, target_type, alias: Some(a.to_string()) },
            _ => expr,
        };
    }
    
    Ok((input, expr))
}

/// Parse: where col = value and col2 = value2
pub fn parse_where_clause(input: &str) -> IResult<&str, Vec<Cage>> {
    let (input, _) = tag_no_case("where")(input)?;
    let (input, _) = multispace1(input)?;
    
    let (input, conditions) = parse_conditions(input)?;
    
    Ok((input, vec![Cage {
        kind: CageKind::Filter,
        conditions,
        logical_op: LogicalOp::And, // Default, actual logic handled in conditions
    }]))
}

/// Parse: having condition and condition2
/// HAVING is for filtering on aggregates after GROUP BY
pub fn parse_having_clause(input: &str) -> IResult<&str, Vec<Condition>> {
    let (input, _) = tag_no_case("having")(input)?;
    let (input, _) = multispace1(input)?;
    
    let (input, conditions) = parse_conditions(input)?;
    
    Ok((input, conditions))
}

/// Parse conditions with and/or
pub fn parse_conditions(input: &str) -> IResult<&str, Vec<Condition>> {
    let (input, first) = parse_condition(input)?;
    // Use multispace0 before and/or to handle IS NULL case (trailing space already consumed)
    let (input, rest) = many0(preceded(
        tuple((multispace0, alt((tag_no_case("and"), tag_no_case("or"))), multispace1)),
        parse_condition
    ))(input)?;
    
    let mut conditions = vec![first];
    conditions.extend(rest);
    Ok((input, conditions))
}

/// Parse single condition: column op value OR exists (subquery) OR not exists (subquery)
pub fn parse_condition(input: &str) -> IResult<&str, Condition> {
    // Special case: EXISTS (subquery) and NOT EXISTS (subquery) - unary operators
    if let Ok((input, _)) = tag_no_case::<_, _, nom::error::Error<&str>>("not exists")(input) {
        let (input, _) = multispace0(input)?;
        let (input, _) = char('(')(input)?;
        let (input, _) = multispace0(input)?;
        let (input, subquery) = super::parse_root(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = char(')')(input)?;
        return Ok((input, Condition {
            left: Expr::Named("".to_string()),
            op: Operator::NotExists,
            value: Value::Subquery(Box::new(subquery)),
            is_array_unnest: false,
        }));
    }
    if let Ok((input, _)) = tag_no_case::<_, _, nom::error::Error<&str>>("exists")(input) {
        let (input, _) = multispace0(input)?;
        let (input, _) = char('(')(input)?;
        let (input, _) = multispace0(input)?;
        let (input, subquery) = super::parse_root(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = char(')')(input)?;
        return Ok((input, Condition {
            left: Expr::Named("".to_string()),
            op: Operator::Exists,
            value: Value::Subquery(Box::new(subquery)),
            is_array_unnest: false,
        }));
    }
    
    // Normal case: column op value
    let (input, left_expr) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    
    // Parse operator
    let (input, op) = parse_operator(input)?;
    let (input, _) = multispace0(input)?;
    
    // Parse value (handle special operators)
    let (input, value) = if matches!(op, Operator::IsNull | Operator::IsNotNull) {
        (input, Value::Null)
    } else if matches!(op, Operator::Between | Operator::NotBetween) {
        // Parse BETWEEN min AND max
        let (input, min_val) = parse_value(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("and")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, max_val) = parse_value(input)?;
        // Store as array with 2 elements [min, max]
        (input, Value::Array(vec![min_val, max_val]))
    } else if matches!(op, Operator::In | Operator::NotIn) {
        // Parse IN (val1, val2, ...)
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
        if let Ok((i, val)) = parse_value(input) {
            (i, val)
        } else {
            // Fallback: try parsing as identifier -> Value::Column
            let (i, col_name) = parse_identifier(input)?;
            (i, Value::Column(col_name.to_string()))
        }
    };
    
    Ok((input, Condition {
        left: left_expr,
        op,
        value,
        is_array_unnest: false,
    }))
}

/// Parse: order by col [asc|desc], col2 [asc|desc]
pub fn parse_order_by_clause(input: &str) -> IResult<&str, Vec<Cage>> {
    let (input, _) = tag_no_case("order")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("by")(input)?;
    let (input, _) = multispace1(input)?;
    
    let (input, sorts) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_sort_column
    )(input)?;
    
    Ok((input, sorts))
}

/// Parse single sort column: col [asc|desc]
pub fn parse_sort_column(input: &str) -> IResult<&str, Cage> {
    let (input, expr) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    
    let (input, order) = opt(alt((
        value(SortOrder::Desc, tag_no_case("desc")),
        value(SortOrder::Asc, tag_no_case("asc")),
    )))(input)?;
    
    Ok((input, Cage {
        kind: CageKind::Sort(order.unwrap_or(SortOrder::Asc)),
        conditions: vec![Condition {
            left: expr,
            op: Operator::Eq,
            value: Value::Null,
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    }))
}

/// Parse: limit N
pub fn parse_limit_clause(input: &str) -> IResult<&str, Cage> {
    let (input, _) = tag_no_case("limit")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, n) = digit1(input)?;
    
    Ok((input, Cage {
        kind: CageKind::Limit(n.parse().unwrap_or(0)),
        conditions: vec![],
        logical_op: LogicalOp::And,
    }))
}

/// Parse: offset N
pub fn parse_offset_clause(input: &str) -> IResult<&str, Cage> {
    let (input, _) = tag_no_case("offset")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, n) = digit1(input)?;
    
    Ok((input, Cage {
        kind: CageKind::Offset(n.parse().unwrap_or(0)),
        conditions: vec![],
        logical_op: LogicalOp::And,
    }))
}

/// Parse: DISTINCT ON (col1, col2, ...)
/// Returns list of column names for DISTINCT ON
pub fn parse_distinct_on(input: &str) -> IResult<&str, Vec<String>> {
    let (input, _) = tag_no_case("distinct")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("on")(input)?;
    let (input, _) = multispace0(input)?;
    
    // Parse column list in parentheses
    let (input, cols) = delimited(
        char('('),
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            map(parse_identifier, |s| s.to_string())
        ),
        char(')')
    )(input)?;
    
    Ok((input, cols))
}
