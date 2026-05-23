//! PostgreSQL MERGE parsing.

use super::base::parse_identifier;
use super::clauses::parse_condition;
use super::expressions::parse_expression;
use crate::ast::*;
use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::tag_no_case,
    character::complete::{char, multispace0, multispace1},
    combinator::{map, opt, value},
    multi::{many0, many1, separated_list1},
    sequence::delimited,
};

/// Parse the body after `merge <target>`.
pub fn parse_merge_after_target<'a>(
    input: &'a str,
    table: &'a str,
    ctes: Vec<CTEDef>,
) -> IResult<&'a str, Qail> {
    let (input, target_alias) = parse_optional_alias(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("using").parse(input)?;
    let (input, _) = multispace1(input)?;
    let (input, source) = parse_merge_source(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("on").parse(input)?;
    let (input, _) = multispace1(input)?;
    let (input, on) = parse_and_conditions(input)?;
    let (input, _) = multispace0(input)?;
    let (input, clauses) = many1(parse_merge_clause).parse(input)?;
    let (input, _) = multispace0(input)?;

    Ok((
        input,
        Qail {
            action: Action::Merge,
            table: table.to_string(),
            columns: vec![],
            ctes,
            merge: Some(Merge {
                target_alias,
                source,
                on,
                clauses,
            }),
            ..Default::default()
        },
    ))
}

fn parse_merge_source(input: &str) -> IResult<&str, MergeSource> {
    let (input, source) = alt((parse_query_source, parse_table_source)).parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, alias) = parse_optional_alias(input)?;

    Ok((
        input,
        match source {
            MergeSource::Table { name, .. } => MergeSource::Table { name, alias },
            MergeSource::Query { query, .. } => MergeSource::Query { query, alias },
        },
    ))
}

fn parse_table_source(input: &str) -> IResult<&str, MergeSource> {
    map(parse_identifier, |name: &str| MergeSource::Table {
        name: name.to_string(),
        alias: None,
    })
    .parse(input)
}

fn parse_query_source(input: &str) -> IResult<&str, MergeSource> {
    let (input, query) = delimited(
        (char('('), multispace0),
        super::parse_root,
        (multispace0, char(')')),
    )
    .parse(input)?;

    Ok((
        input,
        MergeSource::Query {
            query: Box::new(query),
            alias: None,
        },
    ))
}

fn parse_optional_alias(input: &str) -> IResult<&str, Option<String>> {
    let (input, _) = multispace0(input)?;
    if let Ok((remaining, _)) = tag_no_case::<_, _, nom::error::Error<&str>>("as").parse(input) {
        let (remaining, _) = multispace1(remaining)?;
        let (remaining, alias) = parse_identifier(remaining)?;
        return Ok((remaining, Some(alias.to_string())));
    }

    let trimmed = input.trim_start();
    let consumed_ws = input.len() - trimmed.len();
    let Some(first_word) = trimmed.split_whitespace().next() else {
        return Ok((input, None));
    };
    let lower = first_word.to_ascii_lowercase();
    if matches!(
        lower.as_str(),
        "using" | "on" | "when" | "then" | "matched" | "not" | "by"
    ) {
        return Ok((input, None));
    }

    let (remaining, alias) = parse_identifier(trimmed)?;
    Ok((
        &input[consumed_ws + (trimmed.len() - remaining.len())..],
        Some(alias.to_string()),
    ))
}

fn parse_merge_clause(input: &str) -> IResult<&str, MergeClause> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("when").parse(input)?;
    let (input, _) = multispace1(input)?;
    let (input, match_kind) = parse_match_kind(input)?;
    let (input, condition) = parse_optional_when_condition(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("then").parse(input)?;
    let (input, _) = multispace1(input)?;
    let (input, action) = parse_merge_action(input)?;

    Ok((
        input,
        MergeClause {
            match_kind,
            condition,
            action,
        },
    ))
}

fn parse_match_kind(input: &str) -> IResult<&str, MergeMatchKind> {
    alt((
        value(MergeMatchKind::Matched, tag_no_case("matched")),
        parse_not_matched,
    ))
    .parse(input)
}

fn parse_not_matched(input: &str) -> IResult<&str, MergeMatchKind> {
    let (input, _) = tag_no_case("not").parse(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("matched").parse(input)?;
    let (input, _) = multispace0(input)?;

    if let Ok((remaining, _)) = tag_no_case::<_, _, nom::error::Error<&str>>("by").parse(input) {
        let (remaining, _) = multispace1(remaining)?;
        let (remaining, kind) = alt((
            value(MergeMatchKind::NotMatchedBySource, tag_no_case("source")),
            value(MergeMatchKind::NotMatchedByTarget, tag_no_case("target")),
        ))
        .parse(remaining)?;
        return Ok((remaining, kind));
    }

    Ok((input, MergeMatchKind::NotMatchedByTarget))
}

fn parse_optional_when_condition(input: &str) -> IResult<&str, Vec<Condition>> {
    let (input, _) = multispace0(input)?;
    if let Ok((remaining, _)) = tag_no_case::<_, _, nom::error::Error<&str>>("and").parse(input) {
        let (remaining, _) = multispace1(remaining)?;
        parse_and_conditions(remaining)
    } else {
        Ok((input, Vec::new()))
    }
}

fn parse_and_conditions(input: &str) -> IResult<&str, Vec<Condition>> {
    let (input, first) = parse_condition(input)?;
    let (input, rest) = many0((
        multispace0,
        tag_no_case("and"),
        multispace1,
        parse_condition,
    ))
    .parse(input)?;

    let mut conditions = Vec::with_capacity(1 + rest.len());
    conditions.push(first);
    conditions.extend(rest.into_iter().map(|(_, _, _, condition)| condition));
    Ok((input, conditions))
}

fn parse_merge_action(input: &str) -> IResult<&str, MergeAction> {
    alt((
        parse_update_action,
        parse_insert_action,
        value(MergeAction::Delete, tag_no_case("delete")),
        value(
            MergeAction::DoNothing,
            (tag_no_case("do"), multispace1, tag_no_case("nothing")),
        ),
        value(MergeAction::DoNothing, tag_no_case("nothing")),
    ))
    .parse(input)
}

fn parse_update_action(input: &str) -> IResult<&str, MergeAction> {
    let (input, _) = tag_no_case("update").parse(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("set").parse(input)?;
    let (input, _) = multispace1(input)?;
    let (input, assignments) = separated_list1(
        (multispace0, char(','), multispace0),
        parse_merge_assignment,
    )
    .parse(input)?;
    Ok((input, MergeAction::Update { assignments }))
}

fn parse_merge_assignment(input: &str) -> IResult<&str, (String, Expr)> {
    let (input, column) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('=').parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, expr) = parse_expression(input)?;
    Ok((input, (column.to_string(), expr)))
}

fn parse_insert_action(input: &str) -> IResult<&str, MergeAction> {
    let (input, _) = tag_no_case("insert").parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, columns) = opt(delimited(
        (char('('), multispace0),
        separated_list1((multispace0, char(','), multispace0), parse_identifier),
        (multispace0, char(')')),
    ))
    .parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("values").parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, values) = delimited(
        (char('('), multispace0),
        separated_list1((multispace0, char(','), multispace0), parse_expression),
        (multispace0, char(')')),
    )
    .parse(input)?;

    Ok((
        input,
        MergeAction::Insert {
            columns: columns
                .unwrap_or_default()
                .into_iter()
                .map(str::to_string)
                .collect(),
            values,
        },
    ))
}
