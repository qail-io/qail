use nom::{
    bytes::complete::{tag_no_case},
    character::complete::{char, multispace0, multispace1},
    combinator::{opt, map},
    multi::separated_list1,
    sequence::{tuple, preceded, delimited},
    IResult,
};
use crate::ast::*;
use super::base::parse_identifier;

/// Parse WITH clause including optional RECURSIVE keyword
/// Syntax: WITH [RECURSIVE] cte_name [(col1, col2)] AS (subquery) [, ...] main_query
pub fn parse_with_clause(input: &str) -> IResult<&str, (Vec<CTEDef>, bool)> {
    let (input, _) = tag_no_case("with")(input)?;
    let (input, _) = multispace1(input)?;
    
    // Check for RECURSIVE keyword
    let (input, recursive) = opt(preceded(
        tag_no_case("recursive"),
        multispace1
    ))(input)?;
    let is_recursive = recursive.is_some();
    
    // Parse CTE definitions (comma-separated)
    let (input, ctes) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        |i| parse_cte_definition(i, is_recursive)
    )(input)?;
    
    Ok((input, (ctes, is_recursive)))
}

/// Parse a single CTE definition: name [(columns)] AS (subquery)
fn parse_cte_definition(input: &str, is_recursive: bool) -> IResult<&str, CTEDef> {
    // CTE name
    let (input, name) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    
    // Optional column list: (col1, col2, ...)
    let (input, columns) = opt(delimited(
        char('('),
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            map(parse_identifier, |s| s.to_string())
        ),
        char(')')
    ))(input)?;
    let (input, _) = multispace0(input)?;
    
    // AS keyword
    let (input, _) = tag_no_case("as")(input)?;
    let (input, _) = multispace0(input)?;
    
    // Subquery in parentheses - extract content and parse recursively
    let (input, cte_body) = delimited(
        char('('),
        take_until_matching_paren,
        char(')')
    )(input)?;
    
    // Try to parse as QAIL query first, fallback to raw SQL
    let base_query = parse_cte_body(cte_body.trim());
    
    Ok((input, CTEDef {
        name: name.to_string(),
        recursive: is_recursive,
        columns: columns.unwrap_or_default(),
        base_query: Box::new(base_query),
        recursive_query: None,
        source_table: None,
    }))
}

/// Parse CTE body content as a QAIL query
/// Falls back to raw SQL storage if parsing fails
fn parse_cte_body(content: &str) -> QailCmd {
    // Try to parse as QAIL query using parent module's parse_root
    match super::parse_root(content) {
        Ok((remaining, cmd)) => {
            // Check if we consumed all or most of the input
            if remaining.trim().is_empty() {
                cmd
            } else {
                // Partial parse - fall back to raw SQL
                QailCmd::raw_sql(content)
            }
        }
        Err(_) => {
            // Parse failed - store as raw SQL for passthrough
            QailCmd::raw_sql(content)
        }
    }
}

/// Helper to take content until matching closing paren (handles nested parens)
fn take_until_matching_paren(input: &str) -> IResult<&str, &str> {
    let mut depth = 1;
    let mut end = 0;
    
    for (i, c) in input.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    end = i;
                    break;
                }
            }
            _ => {}
        }
    }
    
    if depth == 0 {
        Ok((&input[end..], &input[..end]))
    } else {
        Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::TakeUntil)))
    }
}
