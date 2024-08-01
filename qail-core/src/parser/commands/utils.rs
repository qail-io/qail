use nom::{
    bytes::complete::tag,
    character::complete::char,
    IResult,
};

use crate::ast::*;
use crate::parser::tokens::*;
use crate::parser::columns::ddl::parse_constraint_columns;

/// Parse inline !on(columns) for DISTINCT ON
pub fn parse_inline_distinct_on(input: &str) -> IResult<&str, Vec<String>> {
    if let Ok((input, _)) = tag::<_, _, nom::error::Error<&str>>("!on(")(input) {
        let (input, (cols, _)) = parse_constraint_columns(input)?;
        let (input, _) = char(')')(input)?;
        Ok((input, cols))
    } else {
        Ok((input, vec![]))
    }
}

/// Parse inline !sort(columns) for ordering
pub fn parse_inline_sort(input: &str) -> IResult<&str, Vec<Cage>> {
    if let Ok((input, _)) = tag::<_, _, nom::error::Error<&str>>("!sort(")(input) {
        let (input, sort_content) = take_until_balanced('(', ')')(input)?;
        let (input, _) = char(')')(input)?;
        let (input, _) = ws_or_comment(input)?;
        
        // Parse sort columns: col1, -col2, col3
        let mut sort_cages = Vec::new();
        for part in sort_content.split(',') {
            let part = part.trim();
            if part.is_empty() { continue; }
            let (order, col_name) = if part.starts_with('-') {
                (SortOrder::Desc, part[1..].trim())
            } else if part.starts_with('+') {
                (SortOrder::Asc, part[1..].trim())
            } else {
                (SortOrder::Asc, part)
            };
            // CageKind::Sort only takes SortOrder, column goes in conditions
            sort_cages.push(Cage {
                kind: CageKind::Sort(order),
                conditions: vec![Condition {
                    column: col_name.to_string(),
                    op: Operator::Eq,
                    value: Value::Null,
                    is_array_unnest: false,
                }],
                logical_op: LogicalOp::And,
            });
        }
        Ok((input, sort_cages))
    } else {
        Ok((input, vec![]))
    }
}
