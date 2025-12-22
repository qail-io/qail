use nom::{
    branch::alt,
    bytes::complete::{tag},
    character::complete::{char, digit1},
    combinator::{map, opt},
    multi::{many0},
    sequence::{preceded},
    IResult,
};

use crate::ast::*;
use super::tokens::*;

/// Parse unified constraint blocks [...].
/// Syntax: [ 'active == true, -created_at, 0..10 ]
pub fn parse_unified_blocks(input: &str) -> IResult<&str, Vec<Cage>> {
    many0(preceded(ws_or_comment, parse_unified_block))(input)
}

/// Parse a unified constraint block [...].
/// Contains comma-separated items: filters, sorts, ranges.
fn parse_unified_block(input: &str) -> IResult<&str, Cage> {
    let (input, _) = char('[')(input)?;
    let (input, _) = ws_or_comment(input)?;
    
    // Parse all items in the block (comma-separated)
    let (input, items) = parse_block_items(input)?;
    
    let (input, _) = ws_or_comment(input)?;
    let (input, _) = char(']')(input)?;
    
    // Convert items into appropriate cages
    // For now, combine into a single cage or return the first meaningful one
    items_to_cage(items, input)
}

/// Represents a parsed item within a unified block.
#[derive(Debug)]
enum BlockItem {
    Filter(Condition, LogicalOp),
    Sort(String, SortOrder),
    Range(usize, Option<usize>), // start, end
    NamedLimit(usize),
    NamedOffset(usize),
}

/// Parse comma-separated items within a block.
/// Also handles | (OR) and & (AND) operators for filter conditions.
fn parse_block_items(input: &str) -> IResult<&str, Vec<BlockItem>> {
    let (input, first) = opt(parse_block_item)(input)?;
    
    match first {
        None => Ok((input, vec![])),
        Some(mut item) => {
            let mut items = vec![];
            let mut remaining = input;
            
            loop {
                let (input, _) = ws_or_comment(remaining)?;
                
                // Check for various separators: comma, pipe (OR), ampersand (AND)
                if let Ok((input, _)) = char::<_, nom::error::Error<&str>>(',')(input) {
                    // Comma separator - add current item and parse next
                    items.push(item);
                    let (input, _) = ws_or_comment(input)?;
                    let (input, next_item) = parse_block_item(input)?;
                    item = next_item;
                    remaining = input;
                } else if let Ok((new_input, _)) = char::<_, nom::error::Error<&str>>('|')(input) {
                    // OR separator - update item's logical op and parse next filter
                    if let BlockItem::Filter(cond, _) = item {
                        items.push(BlockItem::Filter(cond, LogicalOp::Or));
                    } else {
                        items.push(item);
                    }
                    let (new_input, _) = ws_or_comment(new_input)?;
                    let (new_input, next_item) = parse_filter_item(new_input)?;
                    // Mark the next item as part of an OR chain
                    if let BlockItem::Filter(cond, _) = next_item {
                        item = BlockItem::Filter(cond, LogicalOp::Or);
                    } else {
                        item = next_item;
                    }
                    remaining = new_input;
                } else if let Ok((new_input, _)) = char::<_, nom::error::Error<&str>>('&')(input) {
                    // AND separator
                    items.push(item);
                    let (new_input, _) = ws_or_comment(new_input)?;
                    let (new_input, next_item) = parse_filter_item(new_input)?;
                    item = next_item;
                    remaining = new_input;
                } else {
                    items.push(item);
                    remaining = input;
                    break;
                }
            }
            
            Ok((remaining, items))
        }
    }
}

/// Parse a single item in a unified block.
fn parse_block_item(input: &str) -> IResult<&str, BlockItem> {
    alt((
        // Range: N..M or N.. (must try before other number parsing)
        parse_range_item,
        // Sort: +col (asc) or -col (desc)
        parse_sort_item,
        // Named limit: lim=N
        parse_named_limit_item,
        // Named offset: off=N
        parse_named_offset_item,
        // Caret sort: ^col or ^!col
        parse_caret_sort_item,
        // Filter: 'col == value
        parse_filter_item,
    ))(input)
}

/// Parse a range item: N..M or N..
fn parse_range_item(input: &str) -> IResult<&str, BlockItem> {
    let (input, start) = digit1(input)?;
    let (input, _) = tag("..")(input)?;
    let (input, end) = opt(digit1)(input)?;
    
    let start_num: usize = start.parse().unwrap_or(0);
    let end_num = end.map(|e| e.parse().unwrap_or(0));
    
    Ok((input, BlockItem::Range(start_num, end_num)))
}

/// Parse a sort item: +col (asc) or -col (desc).
fn parse_sort_item(input: &str) -> IResult<&str, BlockItem> {
    alt((
        map(preceded(char('+'), parse_identifier), |col| {
            BlockItem::Sort(col.to_string(), SortOrder::Asc)
        }),
        map(preceded(char('-'), parse_identifier), |col| {
            BlockItem::Sort(col.to_string(), SortOrder::Desc)
        }),
    ))(input)
}

/// Parse named limit: lim=N
fn parse_named_limit_item(input: &str) -> IResult<&str, BlockItem> {
    let (input, _) = tag("lim")(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, _) = char('=')(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, n) = digit1(input)?;
    Ok((input, BlockItem::NamedLimit(n.parse().unwrap_or(10))))
}

/// Parse named offset: off=N
fn parse_named_offset_item(input: &str) -> IResult<&str, BlockItem> {
    let (input, _) = tag("off")(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, _) = char('=')(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, n) = digit1(input)?;
    Ok((input, BlockItem::NamedOffset(n.parse().unwrap_or(0))))
}

/// Parse caret sort: ^col or ^!col
fn parse_caret_sort_item(input: &str) -> IResult<&str, BlockItem> {
    let (input, _) = char('^')(input)?;
    let (input, desc) = opt(char('!'))(input)?;
    let (input, col) = parse_identifier(input)?;
    
    let order = if desc.is_some() {
        SortOrder::Desc
    } else {
        SortOrder::Asc
    };
    
    Ok((input, BlockItem::Sort(col.to_string(), order)))
}

/// Parse a filter item: 'col == value or col == value
fn parse_filter_item(input: &str) -> IResult<&str, BlockItem> {
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

/// Convert parsed block items into a Cage.
fn items_to_cage(items: Vec<BlockItem>, input: &str) -> IResult<&str, Cage> {
    // Default: return a filter cage if we have filters
    let mut conditions = Vec::new();
    let mut logical_op = LogicalOp::And;
    
    // Check for special single-item cases
    for item in &items {
        match item {
            BlockItem::Range(start, end) => {
                // Range: start..end means OFFSET start, LIMIT (end - start)
                // If end is None, it's just OFFSET start
                // Semantics: 0..10 = LIMIT 10 OFFSET 0
                //                 20..30 = LIMIT 10 OFFSET 20
                if let Some(e) = end {
                    let limit = e - start;
                    let offset = *start;
                    // We need to return multiple cages, but our current structure
                    // returns one. For now, prioritize LIMIT if offset is 0,
                    // otherwise use OFFSET.
                    if offset == 0 {
                        return Ok((input, Cage {
                            kind: CageKind::Limit(limit),
                            conditions: vec![],
                            logical_op: LogicalOp::And,
                        }));
                    } else {
                        return Ok((input, Cage {
                            kind: CageKind::Limit(limit),
                            conditions: vec![Condition {
                                column: "__offset__".to_string(),
                                op: Operator::Eq,
                                value: Value::Int(offset as i64),
                                is_array_unnest: false,
                            }],
                            logical_op: LogicalOp::And,
                        }));
                    }
                } else {
                    // Just offset
                    return Ok((input, Cage {
                        kind: CageKind::Offset(*start),
                        conditions: vec![],
                        logical_op: LogicalOp::And,
                    }));
                }
            }
            BlockItem::Sort(col, order) => {
                return Ok((input, Cage {
                    kind: CageKind::Sort(*order),
                    conditions: vec![Condition {
                        column: col.clone(),
                        op: Operator::Eq,
                        value: Value::Null,
                        is_array_unnest: false,
                    }],
                    logical_op: LogicalOp::And,
                }));
            }
            BlockItem::NamedLimit(n) => {
                return Ok((input, Cage {
                    kind: CageKind::Limit(*n),
                    conditions: vec![],
                    logical_op: LogicalOp::And,
                }));
            }
            BlockItem::NamedOffset(n) => {
                return Ok((input, Cage {
                    kind: CageKind::Offset(*n),
                    conditions: vec![],
                    logical_op: LogicalOp::And,
                }));
            }
            BlockItem::Filter(cond, op) => {
                conditions.push(cond.clone());
                logical_op = *op;
            }
        }
    }
    
    // If we have conditions, return a filter cage
    if !conditions.is_empty() {
        Ok((input, Cage {
            kind: CageKind::Filter,
            conditions,
            logical_op,
        }))
    } else {
        // Empty block - return empty filter
        Ok((input, Cage {
            kind: CageKind::Filter,
            conditions: vec![],
            logical_op: LogicalOp::And,
        }))
    }
}
