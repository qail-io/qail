use nom::{
    branch::alt,
    character::complete::char,
    combinator::{map, opt},
    sequence::preceded,
    IResult,
};
use crate::ast::*;
use crate::parser::tokens::parse_identifier;

use super::blocks::BlockItem;

/// Parse a sort item: +col (asc) or -col (desc).
pub fn parse_sort_item(input: &str) -> IResult<&str, BlockItem> {
    alt((
        map(preceded(char('+'), parse_identifier), |col| {
            BlockItem::Sort(col.to_string(), SortOrder::Asc)
        }),
        map(preceded(char('-'), parse_identifier), |col| {
            BlockItem::Sort(col.to_string(), SortOrder::Desc)
        }),
    ))(input)
}

/// Parse caret sort: ^col or ^!col
pub fn parse_caret_sort_item(input: &str) -> IResult<&str, BlockItem> {
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
