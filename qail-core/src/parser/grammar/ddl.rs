use nom::{
    branch::alt,
    bytes::complete::{tag_no_case, take_while1},
    character::complete::{char, multispace0, multispace1},
    combinator::{opt, map, value, recognize},
    multi::{separated_list1, many0},
    sequence::{preceded, delimited},
    Parser,
    IResult,
};
use crate::ast::*;
use super::base::parse_identifier;

/// Parse CREATE TABLE: make users id:uuid:pk, name:varchar:notnull
pub fn parse_create_table<'a>(input: &'a str, table: &str) -> IResult<&'a str, QailCmd> {
    // Parse column definitions
    let (input, columns) = separated_list1(
        (multispace0, char(','), multispace0),
        parse_column_definition
    ).parse(input)?;
    
    // Parse optional table constraints: primary key (col1, col2), unique (col1, col2)
    let (input, _) = multispace0(input)?;
    let (input, table_constraints) = many0(parse_table_constraint).parse(input)?;
    
    Ok((input, QailCmd {
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
            source_query: None,
    }))
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
                    separated_list1(
                        (multispace0, char(','), multispace0),
                        parse_identifier
                    ),
                    char(')')
                )
            ),
            |(_, _, _, _, cols): (_, _, _, _, Vec<&str>)| {
                TableConstraint::PrimaryKey(cols.iter().map(|s| s.to_string()).collect())
            }
        ),
        // unique (col1, col2)
        map(
            (
                tag_no_case("unique"),
                multispace0,
                delimited(
                    char('('),
                    separated_list1(
                        (multispace0, char(','), multispace0),
                        parse_identifier
                    ),
                    char(')')
                )
            ),
            |(_, _, cols): (_, _, Vec<&str>)| {
                TableConstraint::Unique(cols.iter().map(|s| s.to_string()).collect())
            }
        ),
    )).parse(input)
}

/// Parse column definition: name:type[:constraint1[:constraint2]]
pub fn parse_column_definition(input: &str) -> IResult<&str, Expr> {
    // Parse column name
    let (input, name) = take_while1(|c: char| c.is_alphanumeric() || c == '_').parse(input)?;
    let (input, _) = char(':').parse(input)?;
    
    // Parse data type
    let (input, data_type) = take_while1(|c: char| c.is_alphanumeric() || c == '_').parse(input)?;
    
    // Parse optional constraints (separated by :)
    let (input, constraints) = many0(preceded(
        char(':'),
        parse_constraint
    )).parse(input)?;
    
    Ok((input, Expr::Def {
        name: name.to_string(),
        data_type: data_type.to_string(),
        constraints,
    }))
}

/// Parse column constraint: pk, unique, notnull, default=value, check=expr
pub fn parse_constraint(input: &str) -> IResult<&str, Constraint> {
    alt((
        // Primary key
        value(Constraint::PrimaryKey, tag_no_case("pk")),
        value(Constraint::PrimaryKey, tag_no_case("primarykey")),
        // Unique
        value(Constraint::Unique, tag_no_case("unique")),
        value(Constraint::Unique, tag_no_case("uniq")),
        // Not null (opposite of nullable)
        value(Constraint::Nullable, tag_no_case("notnull")),
        value(Constraint::Nullable, tag_no_case("nn")),
        // Default value: default=uuid() or default=0
        map(
            preceded(
                alt((tag_no_case("default="), tag_no_case("def="))),
                recognize(take_while1(|c: char| c != ',' && c != ':' && c != ' '))
            ),
            |val: &str| Constraint::Default(val.to_string())
        ),
        // Check constraint: check=expr
        map(
            preceded(
                tag_no_case("check="),
                recognize(take_while1(|c: char| c != ',' && c != ':' && c != ' '))
            ),
            |expr: &str| Constraint::Check(vec![expr.to_string()])
        ),
    )).parse(input)
}

/// Parse CREATE INDEX: index idx_name on table_name col1, col2 [unique]
pub fn parse_create_index(input: &str) -> IResult<&str, QailCmd> {
    // Parse "index"
    let (input, _) = tag_no_case("index").parse(input)?;
    let (input, _) = multispace1(input)?;
    
    // Parse index name
    let (input, index_name) = parse_identifier(input)?;
    let (input, _) = multispace1(input)?;
    
    // Parse "on"
    let (input, _) = tag_no_case("on").parse(input)?;
    let (input, _) = multispace1(input)?;
    
    // Parse table name
    let (input, table_name) = parse_identifier(input)?;
    let (input, _) = multispace1(input)?;
    
    // Parse column list
    let (input, columns) = separated_list1(
        (multispace0, char(','), multispace0),
        parse_identifier
    ).parse(input)?;
    let (input, _) = multispace0(input)?;
    
    // Parse optional "unique"
    let (input, unique) = opt(tag_no_case("unique")).parse(input)?;
    
    Ok((input, QailCmd {
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
        }),
        table_constraints: vec![],
        set_ops: vec![],
        having: vec![],
        group_by_mode: GroupByMode::default(),
        ctes: vec![],
        returning: None,
        on_conflict: None,
            source_query: None,
    }))
}
