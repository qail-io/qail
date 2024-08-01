use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::{char, digit1},
    combinator::{map, opt, value},
    multi::many0,
    sequence::{preceded, tuple},
    IResult,
};
use crate::ast::*;
use crate::parser::tokens::*;

pub fn parse_partition_block(input: &str) -> IResult<&str, (Vec<String>, Option<WindowFrame>)> {
    let (input, _) = char('{')(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, _) = tag("Part")(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, _) = char('=')(input)?;
    let (input, _) = ws_or_comment(input)?;
    
    let (input, first) = parse_identifier(input)?;
    let (input, rest) = many0(preceded(
        tuple((ws_or_comment, char(','), ws_or_comment)),
        parse_identifier
    ))(input)?;
    
    let (input, frame) = opt(preceded(
        tuple((ws_or_comment, char(','), ws_or_comment)),
        parse_window_frame
    ))(input)?;
    
    let (input, _) = ws_or_comment(input)?;
    let (input, _) = char('}')(input)?;
    
    let mut cols = vec![first.to_string()];
    cols.extend(rest.iter().map(|s| s.to_string()));
    Ok((input, (cols, frame)))
}

pub fn parse_window_frame(input: &str) -> IResult<&str, WindowFrame> {
    let (input, kind) = alt((
        value("rows", tag("rows")),
        value("range", tag("range")),
    ))(input)?;
    
    let (input, _) = char(':')(input)?;
    // between(start, end)
    let (input, _) = tag("between(")(input)?;
    let (input, start) = parse_frame_bound(input)?;
    let (input, _) = char(',')(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, end) = parse_frame_bound(input)?;
    let (input, _) = char(')')(input)?;
    
    match kind {
        "rows" => Ok((input, WindowFrame::Rows { start, end })),
        "range" => Ok((input, WindowFrame::Range { start, end })),
        _ => unreachable!(),
    }
}

fn parse_frame_bound(input: &str) -> IResult<&str, FrameBound> {
    alt((
        value(FrameBound::UnboundedPreceding, tag("unbounded_preceding")),
        value(FrameBound::UnboundedPreceding, tag("unbounded")), // alias
        value(FrameBound::UnboundedFollowing, tag("unbounded_following")),
        value(FrameBound::CurrentRow, tag("current_row")),
        value(FrameBound::CurrentRow, tag("current")), // alias
        map(tuple((digit1, ws_or_comment, tag("preceding"))), |(n, _, _): (&str, _, _)| {
            FrameBound::Preceding(n.parse().unwrap_or(1))
        }),
        map(tuple((digit1, ws_or_comment, tag("following"))), |(n, _, _): (&str, _, _)| {
            FrameBound::Following(n.parse().unwrap_or(1))
        }),
    ))(input)
}

/// Parse sort cage [^col] or [^!col] for window functions.
pub fn parse_window_sort(input: &str) -> IResult<&str, Cage> {
    let (input, _) = char('^')(input)?;
    let (input, desc) = opt(char('!'))(input)?;
    let (input, col) = parse_identifier(input)?;
    
    // Check for nulls directive
    let (input, nulls) = opt(alt((
        tag("!nulls_first"),
        tag("!first"),
        tag("!nulls_last"),
        tag("!last"),
        tag("!null"), // Default to last for !null
    )))(input)?;
    
    let order = match (desc.is_some(), nulls) {
        (false, None) => SortOrder::Asc,
        (true, None) => SortOrder::Desc,
        (false, Some("!first") | Some("!nulls_first")) => SortOrder::AscNullsFirst,
        (false, Some(_)) => SortOrder::AscNullsLast, 
        (true, Some("!first") | Some("!nulls_first")) => SortOrder::DescNullsFirst,
        (true, Some(_)) => SortOrder::DescNullsLast,
    };
    
    Ok((
        input,
        Cage {
            kind: CageKind::Sort(order),
            conditions: vec![Condition {
                column: col.to_string(),
                op: Operator::Eq,
                value: Value::Null,
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        },
    ))
}
