use super::base::{parse_bare_identifier, parse_identifier};
use crate::ast::*;
use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag_no_case, take_while1},
    character::complete::{char, multispace0, multispace1},
    combinator::{map, opt, recognize, value},
    multi::{many0, separated_list1},
    sequence::{delimited, preceded},
};

/// Parse CREATE TABLE: make users id:uuid:pk, name:varchar, bio:text:nullable
pub fn parse_create_table<'a>(input: &'a str, table: &str) -> IResult<&'a str, Qail> {
    let (input, columns) = separated_list1(
        (multispace0, char(','), multispace0),
        parse_column_definition,
    )
    .parse(input)?;

    let (input, _) = multispace0(input)?;
    let (input, table_constraints) = many0(parse_table_constraint).parse(input)?;

    Ok((
        input,
        Qail {
            action: Action::Make,
            table: table.to_string(),
            columns,
            joins: vec![],
            cages: vec![],
            distinct: false,
            distinct_on: vec![],
            index_def: None,
            table_constraints,
            set_ops: vec![],
            having: vec![],
            group_by_mode: GroupByMode::default(),
            ctes: vec![],
            returning: None,
            on_conflict: None,
            merge: None,
            source_query: None,
            channel: None,
            payload: None,
            savepoint_name: None,
            from_tables: vec![],
            using_tables: vec![],
            lock_mode: None,
            skip_locked: false,
            fetch: None,
            default_values: false,
            overriding: None,
            sample: None,
            only_table: false,
            vector: None,
            score_threshold: None,
            vector_name: None,
            with_vector: false,
            vector_size: None,
            distance: None,
            on_disk: None,
            function_def: None,
            trigger_def: None,
            policy_def: None,
        },
    ))
}

/// Parse table constraint: primary key (col1, col2) or unique (col1, col2)
pub fn parse_table_constraint(input: &str) -> IResult<&str, TableConstraint> {
    let (input, _) = multispace0(input)?;

    alt((
        // primary key (col1, col2)
        map(
            (
                tag_no_case("primary"),
                multispace1,
                tag_no_case("key"),
                multispace0,
                delimited(
                    char('('),
                    separated_list1((multispace0, char(','), multispace0), parse_identifier),
                    char(')'),
                ),
            ),
            |(_, _, _, _, cols): (_, _, _, _, Vec<&str>)| {
                TableConstraint::PrimaryKey(cols.iter().map(|s| s.to_string()).collect())
            },
        ),
        // unique (col1, col2)
        map(
            (
                tag_no_case("unique"),
                multispace0,
                delimited(
                    char('('),
                    separated_list1((multispace0, char(','), multispace0), parse_identifier),
                    char(')'),
                ),
            ),
            |(_, _, cols): (_, _, Vec<&str>)| {
                TableConstraint::Unique(cols.iter().map(|s| s.to_string()).collect())
            },
        ),
    ))
    .parse(input)
}

/// Parse column definition: `name:type[:constraint1[:constraint2]]`
pub fn parse_column_definition(input: &str) -> IResult<&str, Expr> {
    let (input, name) = parse_bare_identifier(input)?;
    let (input, _) = char(':').parse(input)?;

    let (input, data_type) = parse_data_type(input)?;

    let (input, constraints) = many0(preceded(char(':'), parse_constraint)).parse(input)?;
    validate_column_constraints(input, &constraints)?;

    Ok((
        input,
        Expr::Def {
            name: name.to_string(),
            data_type: data_type.to_string(),
            constraints,
        },
    ))
}

fn parse_data_type(input: &str) -> IResult<&str, String> {
    let (input, base_type) = parse_bare_identifier(input)?;
    let (input, params) = opt(delimited(
        char('('),
        take_while1(|c: char| c != ')'),
        char(')'),
    ))
    .parse(input)?;

    if let Some(params) = params {
        validate_type_params(input, params)?;
        Ok((input, format!("{base_type}({params})")))
    } else {
        Ok((input, base_type.to_string()))
    }
}

fn validate_type_params<'a>(error_input: &'a str, params: &str) -> IResult<&'a str, ()> {
    let mut has_part = false;
    for part in params.split(',') {
        let part = part.trim();
        if part.is_empty() || !part.chars().all(|c| c.is_ascii_digit()) {
            return Err(column_definition_error(error_input));
        }
        has_part = true;
    }

    if !has_part {
        return Err(column_definition_error(error_input));
    }

    Ok((error_input, ()))
}

fn validate_column_constraints<'a>(
    error_input: &'a str,
    constraints: &[Constraint],
) -> IResult<&'a str, ()> {
    let mut primary_key = false;
    let mut unique = false;
    let mut nullable = false;
    let mut default = false;
    let mut check = false;

    for constraint in constraints {
        match constraint {
            Constraint::PrimaryKey => {
                if primary_key {
                    return Err(column_definition_error(error_input));
                }
                primary_key = true;
            }
            Constraint::Unique => {
                if unique {
                    return Err(column_definition_error(error_input));
                }
                unique = true;
            }
            Constraint::Nullable => {
                if nullable {
                    return Err(column_definition_error(error_input));
                }
                nullable = true;
            }
            Constraint::Default(_) => {
                if default {
                    return Err(column_definition_error(error_input));
                }
                default = true;
            }
            Constraint::Check(_) => {
                if check {
                    return Err(column_definition_error(error_input));
                }
                check = true;
            }
            _ => {}
        }
    }

    if primary_key && nullable {
        return Err(column_definition_error(error_input));
    }

    Ok((error_input, ()))
}

fn column_definition_error(input: &str) -> nom::Err<nom::error::Error<&str>> {
    nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
}

/// Parse column constraint: pk, unique, nullable, default=value, check=expr
pub fn parse_constraint(input: &str) -> IResult<&str, Constraint> {
    alt((
        // Primary key
        value(Constraint::PrimaryKey, tag_no_case("pk")),
        value(Constraint::PrimaryKey, tag_no_case("primarykey")),
        // Unique
        value(Constraint::Unique, tag_no_case("unique")),
        value(Constraint::Unique, tag_no_case("uniq")),
        // Nullable (column allows NULL; without this, columns default to NOT NULL)
        value(Constraint::Nullable, tag_no_case("nullable")),
        value(Constraint::Nullable, tag_no_case("null")),
        // Default value: default=uuid() or default=0
        map(
            preceded(
                alt((tag_no_case("default="), tag_no_case("def="))),
                recognize(take_while1(|c: char| c != ',' && c != ':' && c != ' ')),
            ),
            |val: &str| Constraint::Default(val.to_string()),
        ),
        map(
            preceded(
                tag_no_case("check="),
                recognize(take_while1(|c: char| c != ',' && c != ':' && c != ' ')),
            ),
            |expr: &str| Constraint::Check(vec![expr.to_string()]),
        ),
    ))
    .parse(input)
}

/// Parse CREATE INDEX: `index idx_name on table_name col1, col2 [unique]`
pub fn parse_create_index(input: &str) -> IResult<&str, Qail> {
    let (input, _) = tag_no_case("index").parse(input)?;
    let (input, _) = multispace1(input)?;

    let (input, index_name) = parse_identifier(input)?;
    let (input, _) = multispace1(input)?;

    let (input, _) = tag_no_case("on").parse(input)?;
    let (input, _) = multispace1(input)?;

    let (input, table_name) = parse_identifier(input)?;
    let (input, _) = multispace1(input)?;

    let (input, columns) =
        separated_list1((multispace0, char(','), multispace0), parse_identifier).parse(input)?;
    let (input, _) = multispace0(input)?;

    let (input, unique) = opt(tag_no_case("unique")).parse(input)?;

    Ok((
        input,
        Qail {
            action: Action::Index,
            table: String::new(),
            columns: vec![],
            joins: vec![],
            cages: vec![],
            distinct: false,
            distinct_on: vec![],
            index_def: Some(IndexDef {
                name: index_name.to_string(),
                table: table_name.to_string(),
                columns: columns.iter().map(|s| s.to_string()).collect(),
                unique: unique.is_some(),
                index_type: None,
                include: vec![],
                concurrently: false,
                where_clause: None,
            }),
            table_constraints: vec![],
            set_ops: vec![],
            having: vec![],
            group_by_mode: GroupByMode::default(),
            ctes: vec![],
            returning: None,
            on_conflict: None,
            merge: None,
            source_query: None,
            channel: None,
            payload: None,
            savepoint_name: None,
            from_tables: vec![],
            using_tables: vec![],
            lock_mode: None,
            skip_locked: false,
            fetch: None,
            default_values: false,
            overriding: None,
            sample: None,
            only_table: false,
            vector: None,
            score_threshold: None,
            vector_name: None,
            with_vector: false,
            vector_size: None,
            distance: None,
            on_disk: None,
            function_def: None,
            trigger_def: None,
            policy_def: None,
        },
    ))
}
