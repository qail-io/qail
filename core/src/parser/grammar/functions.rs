//! Function and aggregate parsing.
//!
//! Handles parsing of:
//! - Function calls: name(arg1, arg2)
//! - Aggregates: COUNT, SUM, AVG, MIN, MAX
//! - FILTER (WHERE ...) clause for aggregates
//! - COUNT(DISTINCT col) syntax

use super::base::{parse_identifier, parse_operator, parse_value};
use super::expressions::parse_expression;
use crate::ast::*;
use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{char, multispace0, multispace1},
    combinator::{map, opt, peek},
    multi::separated_list0,
    sequence::preceded,
};

/// Parse function call or aggregate: name(arg1, arg2)
pub fn parse_function_or_aggregate(input: &str) -> IResult<&str, Expr> {
    // Identifier followed by (
    let (input, name) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, _) = multispace0(input)?;

    let (input, has_distinct) = opt((tag_no_case("distinct"), multispace1)).parse(input)?;
    let distinct = has_distinct.is_some();

    let (input, args) =
        separated_list0((multispace0, char(','), multispace0), parse_function_arg).parse(input)?;

    let (input, _) = multispace0(input)?;
    let (input, _) = char(')').parse(input)?;
    let (input, _) = multispace0(input)?;

    let (input, filter_clause) = opt(parse_filter_clause).parse(input)?;

    let (input, _) = multispace0(input)?;
    if let Ok((remaining, _)) = tag_no_case::<_, _, nom::error::Error<&str>>("over").parse(input) {
        let (remaining, _) = multispace0(remaining)?;
        let (remaining, _) = char('(').parse(remaining)?;
        let (remaining, _) = multispace0(remaining)?;

        let (remaining, partition) = opt(parse_partition_by).parse(remaining)?;
        let partition = partition.unwrap_or_default();
        let (remaining, _) = multispace0(remaining)?;

        let (remaining, order) = opt(parse_window_order_by).parse(remaining)?;
        let order = order.unwrap_or_default();
        let (remaining, _) = multispace0(remaining)?;

        // Parse optional window frame: ROWS/RANGE BETWEEN ... AND ...
        let (remaining, frame) = opt(parse_window_frame).parse(remaining)?;
        let (remaining, _) = multispace0(remaining)?;

        // Close the OVER clause
        let (remaining, _) = char(')').parse(remaining)?;
        let (remaining, _) = multispace0(remaining)?;

        // Optional alias for window function
        let (remaining, alias) = opt(preceded(
            (multispace0, tag_no_case("as"), multispace1),
            parse_identifier,
        ))
        .parse(remaining)?;
        let alias_str = alias
            .map(|s| s.to_string())
            .unwrap_or_else(|| name.to_string());

        let params: Vec<Value> = args
            .iter()
            .map(|e| Value::Function(e.to_string()))
            .collect();

        return Ok((
            remaining,
            Expr::Window {
                name: alias_str,
                func: name.to_string(),
                params,
                partition,
                order,
                frame,
            },
        ));
    }

    // Optional alias: AS alias_name or just alias_name (after space)
    let (input, alias) = opt(preceded(
        (multispace0, tag_no_case("as"), multispace1),
        parse_identifier,
    ))
    .parse(input)?;
    let alias = alias.map(|s| s.to_string());

    let name_lower = name.to_lowercase();
    match name_lower.as_str() {
        "count" | "sum" | "avg" | "min" | "max" => {
            // For aggregates, convert first arg to string representation
            let col = args
                .first()
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
            Ok((
                input,
                Expr::Aggregate {
                    col,
                    func,
                    distinct,
                    filter: filter_clause,
                    alias,
                },
            ))
        }
        _ => Ok((
            input,
            Expr::FunctionCall {
                name: name.to_string(),
                args,
                alias,
            },
        )),
    }
}

/// Parse a single function argument (supports expressions or star)
pub fn parse_function_arg(input: &str) -> IResult<&str, Expr> {
    alt((map(tag("*"), |_| Expr::Star), parse_expression)).parse(input)
}

/// Parse FILTER (WHERE condition) clause for aggregates
fn parse_filter_clause(input: &str) -> IResult<&str, Vec<Condition>> {
    let (input, _) = tag_no_case("filter").parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("where").parse(input)?;
    let (input, _) = multispace1(input)?;

    let (input, conditions) = parse_filter_conditions(input)?;

    let (input, _) = multispace0(input)?;
    let (input, _) = char(')').parse(input)?;

    Ok((input, conditions))
}

/// Parse conditions inside FILTER clause
fn parse_filter_conditions(input: &str) -> IResult<&str, Vec<Condition>> {
    let mut conditions = Vec::new();
    let mut current_input = input;

    loop {
        let (input, _) = multispace0(current_input)?;
        let (input, col) = parse_identifier(input)?;
        let (input, _) = multispace0(input)?;
        let (input, op) = parse_operator(input)?;
        let (input, _) = multispace0(input)?;

        // For IS NULL / IS NOT NULL, no value needed
        let (input, value) = if matches!(op, Operator::IsNull | Operator::IsNotNull) {
            (input, Value::Null)
        } else if matches!(op, Operator::In | Operator::NotIn) {
            let (input, _) = char('(').parse(input)?;
            let (input, _) = multispace0(input)?;
            let (input, values) =
                separated_list0((multispace0, char(','), multispace0), parse_value).parse(input)?;
            let (input, _) = multispace0(input)?;
            let (input, _) = char(')').parse(input)?;
            (input, Value::Array(values))
        } else if matches!(op, Operator::Between | Operator::NotBetween) {
            let (input, min_val) = parse_value(input)?;
            let (input, _) = multispace1(input)?;
            let (input, _) = tag_no_case("and").parse(input)?;
            let (input, _) = multispace1(input)?;
            let (input, max_val) = parse_value(input)?;
            // Store as array with 2 elements [min, max]
            (input, Value::Array(vec![min_val, max_val]))
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

        let and_result: IResult<&str, _> = preceded(
            (multispace0, tag_no_case("and"), multispace1),
            peek(parse_identifier),
        )
        .parse(current_input);

        if let Ok((_next_input, _)) = and_result {
            // Skip the AND keyword and trailing whitespace
            let (next_input, _) = multispace0(current_input)?;
            let (next_input, _) = tag_no_case("and").parse(next_input)?;
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
            return Ok((
                remaining,
                Value::Function(format!("INTERVAL '{} {}'", amount, unit)),
            ));
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

        if paren_depth == 0 && i > 0 {
            let remaining = &input[i..];
            if remaining.len() >= 4 {
                let potential_and = &remaining[..4].to_lowercase();
                if potential_and.starts_with("and ")
                    || potential_and.starts_with("and\t")
                    || potential_and.starts_with("and\n")
                {
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
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TakeWhile1,
        )));
    }

    // Try to parse as expression and get proper SQL
    if let Ok((_, expr)) = parse_expression(expr_str) {
        return Ok((&input[end_pos..], Value::Function(expr.to_string())));
    }

    Ok((&input[end_pos..], Value::Function(expr_str.to_string())))
}

/// Parse PARTITION BY col1, col2 clause for window functions
fn parse_partition_by(input: &str) -> IResult<&str, Vec<String>> {
    let (input, _) = tag_no_case("partition").parse(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("by").parse(input)?;
    let (input, _) = multispace1(input)?;

    let (input, cols) =
        separated_list0((multispace0, char(','), multispace0), parse_identifier).parse(input)?;

    Ok((input, cols.into_iter().map(|s| s.to_string()).collect()))
}

/// Parse ORDER BY col1 [asc|desc], col2 clause for window functions
fn parse_window_order_by(input: &str) -> IResult<&str, Vec<Cage>> {
    let (input, _) = tag_no_case("order").parse(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("by").parse(input)?;
    let (input, _) = multispace1(input)?;

    let (input, order_parts) = separated_list0(
        (multispace0, char(','), multispace0),
        parse_window_sort_item,
    )
    .parse(input)?;

    Ok((input, order_parts))
}

/// Parse a single order by item: col [asc|desc]
fn parse_window_sort_item(input: &str) -> IResult<&str, Cage> {
    use nom::combinator::value;

    let (input, col) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;

    let (input, order) = opt(alt((
        value(SortOrder::Desc, tag_no_case("desc")),
        value(SortOrder::Asc, tag_no_case("asc")),
    )))
    .parse(input)?;

    Ok((
        input,
        Cage {
            kind: CageKind::Sort(order.unwrap_or(SortOrder::Asc)),
            conditions: vec![Condition {
                left: Expr::Named(col.to_string()),
                op: Operator::Eq,
                value: Value::Null,
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        },
    ))
}

/// Parse window frame: ROWS/RANGE BETWEEN start AND end
fn parse_window_frame(input: &str) -> IResult<&str, WindowFrame> {
    use nom::combinator::value;
    
    // Parse ROWS or RANGE
    let (input, is_rows) = alt((
        value(true, tag_no_case("rows")),
        value(false, tag_no_case("range")),
    )).parse(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("between").parse(input)?;
    let (input, _) = multispace1(input)?;
    
    // Parse start bound
    let (input, start) = parse_frame_bound(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("and").parse(input)?;
    let (input, _) = multispace1(input)?;
    
    // Parse end bound
    let (input, end) = parse_frame_bound(input)?;
    
    let frame = if is_rows {
        WindowFrame::Rows { start, end }
    } else {
        WindowFrame::Range { start, end }
    };
    
    Ok((input, frame))
}

/// Parse frame bound: UNBOUNDED PRECEDING, N PRECEDING, CURRENT ROW, N FOLLOWING, UNBOUNDED FOLLOWING
fn parse_frame_bound(input: &str) -> IResult<&str, FrameBound> {
    use nom::combinator::value;
    use nom::character::complete::i32 as parse_i32;
    
    alt((
        // UNBOUNDED PRECEDING
        value(FrameBound::UnboundedPreceding, 
            (tag_no_case("unbounded"), multispace1, tag_no_case("preceding"))),
        // UNBOUNDED FOLLOWING
        value(FrameBound::UnboundedFollowing,
            (tag_no_case("unbounded"), multispace1, tag_no_case("following"))),
        // CURRENT ROW
        value(FrameBound::CurrentRow,
            (tag_no_case("current"), multispace1, tag_no_case("row"))),
        // N PRECEDING
        map((parse_i32, multispace1, tag_no_case("preceding")),
            |(n, _, _)| FrameBound::Preceding(n)),
        // N FOLLOWING
        map((parse_i32, multispace1, tag_no_case("following")),
            |(n, _, _)| FrameBound::Following(n)),
    )).parse(input)
}
