use nom::{
    character::complete::char,
    combinator::opt,
    IResult,
};
use crate::ast::*;
use crate::parser::tokens::{ws_or_comment, parse_identifier, parse_operator_and_value};

use super::blocks::BlockItem;

/// Parse a filter item: 'col == value or col == value
pub fn parse_filter_item(input: &str) -> IResult<&str, BlockItem> {
    // Optional leading ' for column
    let (input, _) = opt(char('\''))(input)?;
    let (input, column) = parse_identifier(input)?;
    
    // Check for array unnest syntax: column[*]
    let (input, is_array_unnest) = if input.starts_with("[*]") {
        (&input[3..], true)
    } else {
        (input, false)
    };
    
    let (input, _) = ws_or_comment(input)?;
    let (input, (op, val)) = parse_operator_and_value(input)?;
    
    Ok((input, BlockItem::Filter(
        Condition {
            column: column.to_string(),
            op,
            value: val,
            is_array_unnest,
        },
        LogicalOp::And, // Default, could be enhanced for | and &
    )))
}
