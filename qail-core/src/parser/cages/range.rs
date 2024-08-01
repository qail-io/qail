use nom::{
    bytes::complete::tag,
    character::complete::{char, digit1},
    combinator::{opt},
    IResult,
};
use crate::parser::tokens::ws_or_comment;
use super::blocks::BlockItem;

/// Parse a range item: N..M or N..
pub fn parse_range_item(input: &str) -> IResult<&str, BlockItem> {
    let (input, start) = digit1(input)?;
    let (input, _) = tag("..")(input)?;
    let (input, end) = opt(digit1)(input)?;
    
    let start_num: usize = start.parse().unwrap_or(0);
    let end_num = end.map(|e| e.parse().unwrap_or(0));
    
    Ok((input, BlockItem::Range(start_num, end_num)))
}

/// Parse named limit: lim=N
pub fn parse_named_limit_item(input: &str) -> IResult<&str, BlockItem> {
    let (input, _) = tag("lim")(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, _) = char('=')(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, n) = digit1(input)?;
    Ok((input, BlockItem::NamedLimit(n.parse().unwrap_or(10))))
}

/// Parse named offset: off=N
pub fn parse_named_offset_item(input: &str) -> IResult<&str, BlockItem> {
    let (input, _) = tag("off")(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, _) = char('=')(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, n) = digit1(input)?;
    Ok((input, BlockItem::NamedOffset(n.parse().unwrap_or(0))))
}
