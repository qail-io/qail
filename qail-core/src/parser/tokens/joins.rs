use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::char,
    combinator::{map, value},
    multi::{many0, separated_list1},
    sequence::tuple,
    IResult,
};

use crate::ast::*;
use super::identifiers::{ws_or_comment, parse_identifier};
use super::literals::parse_value_no_bare_id;

pub fn parse_joins(input: &str) -> IResult<&str, Vec<Join>> {
    many0(parse_single_join)(input)
}

/// Parse a single join: `->` (INNER), `<-` (LEFT), `->>` (RIGHT)
fn parse_single_join(input: &str) -> IResult<&str, Join> {
    // Helper to finalize join
    fn finish_join<'a>(input: &'a str, table: &'a str, kind: JoinKind) -> IResult<&'a str, Join> {
         // Check for optional "ON" condition: (a=b, c=d)
         let (input, on) = if let Ok((input, _)) = ws_or_comment(input) {
             if let Ok((input, _)) = char::<_, nom::error::Error<&str>>('(')(input) {
                 let (input, conds) = separated_list1(
                     tuple((ws_or_comment, char(','), ws_or_comment)),
                     parse_join_condition
                 )(input)?;
                 let (input, _) = char(')')(input)?;
                 (input, Some(conds))
             } else {
                 (input, None)
             }
         } else {
             (input, None)
         };

         Ok((input, Join {
             table: table.to_string(),
             kind,
             on,
         }))
    }

    let (input, _) = ws_or_comment(input)?;
    
    // Try RIGHT JOIN first (->>)
    if let Ok((remaining, _)) = tag::<_, _, nom::error::Error<&str>>("->>") (input) {
        let (remaining, _) = ws_or_comment(remaining)?;
        let (remaining, table) = parse_identifier(remaining)?;
        return finish_join(remaining, table, JoinKind::Right);
    }
    
    // FULL OUTER JOIN (<->)
    if let Ok((remaining, _)) = tag::<_, _, nom::error::Error<&str>>("<->") (input) {
        let (remaining, _) = ws_or_comment(remaining)?;
        let (remaining, table) = parse_identifier(remaining)?;
        return finish_join(remaining, table, JoinKind::Full);
    }

    // CROSS JOIN (><)
    if let Ok((remaining, _)) = tag::<_, _, nom::error::Error<&str>>("><") (input) {
        let (remaining, _) = ws_or_comment(remaining)?;
        let (remaining, table) = parse_identifier(remaining)?;
        return finish_join(remaining, table, JoinKind::Cross);
    }

    // LATERAL JOIN (->^)
    if let Ok((remaining, _)) = tag::<_, _, nom::error::Error<&str>>("->^") (input) {
        let (remaining, _) = ws_or_comment(remaining)?;
        let (remaining, table) = parse_identifier(remaining)?;
        return finish_join(remaining, table, JoinKind::Lateral);
    }

    // Try LEFT JOIN (<-)
    if let Ok((remaining, _)) = tag::<_, _, nom::error::Error<&str>>("<-") (input) {
        let (remaining, _) = ws_or_comment(remaining)?;
        let (remaining, table) = parse_identifier(remaining)?;
        return finish_join(remaining, table, JoinKind::Left);
    }
    
    // Default: INNER JOIN (->)
    let (input, _) = tag("->")(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, table) = parse_identifier(input)?;
    finish_join(input, table, JoinKind::Inner)
}

/// Parse a join condition: col = col (where RHS identifier is Value::Column)
fn parse_join_condition(input: &str) -> IResult<&str, Condition> {
    let (input, column) = parse_identifier(input)?;
    let (input, _) = ws_or_comment(input)?;
    
    // Parse operator
    let (input, op) = alt((
        value(Operator::Eq, tag("=")),
        value(Operator::Eq, tag("==")),
        value(Operator::Ne, tag("!=")),
        value(Operator::Gt, tag(">")),
        value(Operator::Lt, tag("<")),
        value(Operator::Gte, tag(">=")),
        value(Operator::Lte, tag("<=")),
    ))(input)?;
    
    let (input, _) = ws_or_comment(input)?;
    
    // Parse RHS value
    let (input, value) = alt((
        // Identifiers as Columns
        map(parse_identifier, |s| Value::Column(s.to_string())),
        // Fallback to standard value types (numbers, quoted strings)
        parse_value_no_bare_id
    ))(input)?;

    Ok((input, Condition {
        column: column.to_string(),
        op,
        value,
        is_array_unnest: false,
    }))
}
