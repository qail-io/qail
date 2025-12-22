use nom::{
    branch::alt,
    character::complete::char,
    combinator::{opt, map},
    IResult,
};

use crate::ast::*;
use crate::parser::tokens::ws_or_comment;
use crate::parser::tokens::parse_value; // Assuming parse_value is public there

use super::filter::*;
use super::range::*;
use super::sort::*;

/// Represents a parsed item within a unified block.
#[derive(Debug)]
pub enum BlockItem {
    Filter(Condition, LogicalOp),
    Sort(String, SortOrder),
    Range(usize, Option<usize>), // start, end
    NamedLimit(usize),
    NamedOffset(usize),
    Value(Value),
}

/// Parse comma-separated items within a block.
/// Also handles | (OR) and & (AND) operators for filter conditions.
pub fn parse_block_items(input: &str) -> IResult<&str, Vec<BlockItem>> {
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
pub fn parse_block_item(input: &str) -> IResult<&str, BlockItem> {
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
        // Raw Value (for INSERTs): '$1', 123
        map(parse_value, BlockItem::Value),
    ))(input)
}

/// Convert parsed block items into a Cage.
pub fn items_to_cage(items: Vec<BlockItem>, input: &str) -> IResult<&str, Cage> {
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
                logical_op = op.clone();
            }
            BlockItem::Value(val) => {
                // If value is a raw string "{...}", we treat it as a raw condition
                // by putting it in 'column' and using a dummy OP.
                // Logic in conditions.rs/to_sql must handle this.
                if let Value::String(s) = &val {
                    if s.starts_with('{') && s.ends_with('}') {
                        conditions.push(Condition {
                            column: s.clone(), // Pass raw string as column
                            op: Operator::Eq, // Dummy
                            value: Value::Null, // Dummy
                            is_array_unnest: false,
                        });
                        continue;
                    }
                }
                
                conditions.push(Condition {
                    column: "".to_string(), // Dummy for INSERT values
                    op: Operator::Eq, 
                    value: val.clone(),
                    is_array_unnest: false,
                });
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
