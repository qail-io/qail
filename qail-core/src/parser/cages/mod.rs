pub mod blocks;
pub mod filter;
pub mod range;
pub mod sort;

use nom::{
    character::complete::char,
    multi::many0,
    sequence::preceded,
    IResult,
};

use crate::ast::*;
use crate::parser::tokens::ws_or_comment;

pub use self::blocks::{parse_block_items, items_to_cage, BlockItem};
pub use self::filter::parse_filter_item;
pub use self::sort::{parse_sort_item, parse_caret_sort_item as parse_window_sort};
pub use self::range::{parse_range_item, parse_named_limit_item, parse_named_offset_item};

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
