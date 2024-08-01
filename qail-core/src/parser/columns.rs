use nom::{
    branch::alt,
    bytes::complete::{tag, take_until, take_while1},
    character::complete::{char, multispace0},
    combinator::{map, opt, value},
    multi::{many0, separated_list1},
    sequence::{delimited, pair, preceded, tuple},
    IResult,
};

use crate::ast::*;
use super::tokens::*;

/// Parse columns using the label syntax ('col).
pub fn parse_columns(input: &str) -> IResult<&str, Vec<Column>> {
    many0(preceded(ws_or_comment, parse_any_column))(input)
}

fn parse_any_column(input: &str) -> IResult<&str, Column> {
    alt((
        // Label: 'col...
        preceded(char('\''), parse_label_column),
        // v0.8.0: Allow bare identifiers (e.g. drop::users:password)
        parse_column_full_def_or_named,
    ))(input)
}

/// Parse a column with the label syntax ('col).
fn parse_label_column(input: &str) -> IResult<&str, Column> {
    alt((
        // Wildcard: '_ for all columns
        value(Column::Star, char('_')),
        // Named or complex column
        parse_column_full_def_or_named,
    ))(input)
}

fn parse_column_full_def_or_named(input: &str) -> IResult<&str, Column> {
    // 1. Parse Name
    let (input, name) = parse_identifier(input)?;
    
    // 2. Opt: Aggregates (#func)
    if let Ok((input, Some(func))) = opt(preceded(char('#'), parse_agg_func))(input) {
        return Ok((input, Column::Aggregate {
             col: name.to_string(),
             func
        }));
    }
    
    // 3. Opt: check for colon (type definition)
    if let Ok((input, _)) = char::<_, nom::error::Error<&str>>(':')(input) {
        // We have a type OR a window function.
        let (input, type_or_func) = parse_identifier(input)?;
        
        let (input, _) = ws_or_comment(input)?;
        
        // Peek/Check for open paren `(` for window function
        if let Ok((input, _)) = char::<_, nom::error::Error<&str>>('(')(input) {
            // It IS a function call -> Window Column
            let (input, _) = ws_or_comment(input)?;
            let (input, args) = opt(tuple((
                parse_value,
                many0(preceded(
                    tuple((ws_or_comment, char(','), ws_or_comment)),
                    parse_value
                ))
            )))(input)?;
            let (input, _) = ws_or_comment(input)?;
            let (input, _) = char(')')(input)?;
            
            let params = match args {
                Some((first, mut rest)) => {
                    let mut v = vec![first];
                    v.append(&mut rest);
                    v
                },
                None => vec![],
            };

            // Parse Order Cages (e.g. ^!amount)
            let (input, sorts) = many0(parse_window_sort)(input)?;
            
            // Parse Partition: {Part=...}
            let (input, partitions) = opt(parse_partition_block)(input)?;
            let partition = partitions.unwrap_or_default();

            return Ok((input, Column::Window {
                name: name.to_string(),
                func: type_or_func.to_string(),
                params,
                partition,
                order: sorts,
            }));
        } else {
            // It is just a Type Definition
            let (input, constraints) = parse_constraints(input)?;
            
            return Ok((input, Column::Def { 
                name: name.to_string(), 
                data_type: type_or_func.to_string(), 
                constraints 
            }));
        }
    }
    
    // No colon, check for constraints (inferred type Def)
    let (input, constraints) = parse_constraints(input)?;
    if !constraints.is_empty() {
         Ok((input, Column::Def { 
            name: name.to_string(), 
            data_type: "str".to_string(), 
            constraints 
        }))
    } else {
        // Just a named column
        Ok((input, Column::Named(name.to_string())))
    }
}

fn parse_constraints(input: &str) -> IResult<&str, Vec<Constraint>> {
    many0(alt((
        // ^pk without parentheses (column-level PK)
        map(
            tuple((tag("^pk"), nom::combinator::not(char('(')))),
            |_| Constraint::PrimaryKey
        ),
        // ^uniq without following 'ue(' (to avoid matching ^unique())
        map(
            tuple((tag("^uniq"), nom::combinator::not(tag("ue(")))),
            |_| Constraint::Unique
        ),
        value(Constraint::Nullable, char('?')),
        parse_default_constraint,
        parse_check_constraint,
        parse_comment_constraint,
    )))(input)
}

/// Parse DEFAULT value constraint: `= value` or `= func()`
fn parse_default_constraint(input: &str) -> IResult<&str, Constraint> {
    let (input, _) = preceded(multispace0, char('='))(input)?;
    let (input, _) = multispace0(input)?;
    
    // Parse function call like uuid(), now(), or literal values
    let (input, value) = alt((
        // Function call: name()
        map(
            pair(
                take_while1(|c: char| c.is_alphanumeric() || c == '_'),
                tag("()")
            ),
            |(name, parens): (&str, &str)| format!("{}{}", name, parens)
        ),
        // Numeric literal
        map(
            take_while1(|c: char| c.is_numeric() || c == '.' || c == '-'),
            |s: &str| s.to_string()
        ),
        // Quoted string
        map(
            delimited(char('"'), take_until("\""), char('"')),
            |s: &str| format!("'{}'", s)
        ),
    ))(input)?;
    
    Ok((input, Constraint::Default(value)))
}

/// Parse CHECK constraint: `^check("a","b","c")`
fn parse_check_constraint(input: &str) -> IResult<&str, Constraint> {
    let (input, _) = tag("^check(")(input)?;
    let (input, values) = separated_list1(
        char(','),
        delimited(
            multispace0,
            delimited(char('"'), take_until("\""), char('"')),
            multispace0
        )
    )(input)?;
    let (input, _) = char(')')(input)?;
    
    Ok((input, Constraint::Check(values.into_iter().map(|s| s.to_string()).collect())))
}

/// Parse COMMENT constraint: `^comment("description")`
fn parse_comment_constraint(input: &str) -> IResult<&str, Constraint> {
    let (input, _) = tag("^comment(\"")(input)?;
    let (input, text) = take_until("\"")(input)?;
    let (input, _) = tag("\")")(input)?;
    Ok((input, Constraint::Comment(text.to_string())))
}

/// Parse index columns: 'col1-col2-col3
pub fn parse_index_columns(input: &str) -> IResult<&str, Vec<String>> {
    let (input, _) = char('\'')(input)?;
    let (input, first) = parse_identifier(input)?;
    let (input, rest) = many0(preceded(char('-'), parse_identifier))(input)?;
    
    let mut cols = vec![first.to_string()];
    cols.extend(rest.iter().map(|s| s.to_string()));
    Ok((input, cols))
}

/// Parse table-level constraints: ^unique(col1, col2) or ^pk(col1, col2)
pub fn parse_table_constraints(input: &str) -> IResult<&str, Vec<TableConstraint>> {
    many0(alt((
        parse_table_unique,
        parse_table_pk,
    )))(input)
}

/// Parse ^unique(col1, col2)
fn parse_table_unique(input: &str) -> IResult<&str, TableConstraint> {
    let (input, _) = tag("^unique(")(input)?;
    let (input, cols) = parse_constraint_columns(input)?;
    let (input, _) = char(')')(input)?;
    Ok((input, TableConstraint::Unique(cols)))
}

/// Parse ^pk(col1, col2)
fn parse_table_pk(input: &str) -> IResult<&str, TableConstraint> {
    let (input, _) = tag("^pk(")(input)?;
    let (input, cols) = parse_constraint_columns(input)?;
    let (input, _) = char(')')(input)?;
    Ok((input, TableConstraint::PrimaryKey(cols)))
}

/// Parse comma-separated column names: col1, col2, col3
fn parse_constraint_columns(input: &str) -> IResult<&str, Vec<String>> {
    let (input, _) = multispace0(input)?;
    let (input, first) = parse_identifier(input)?;
    let (input, rest) = many0(preceded(
        tuple((multispace0, char(','), multispace0)),
        parse_identifier
    ))(input)?;
    let (input, _) = multispace0(input)?;
    
    let mut cols = vec![first.to_string()];
    cols.extend(rest.iter().map(|s| s.to_string()));
    Ok((input, cols))
}

fn parse_agg_func(input: &str) -> IResult<&str, AggregateFunc> {
    alt((
        value(AggregateFunc::Count, tag("count")),
        value(AggregateFunc::Sum, tag("sum")),
        value(AggregateFunc::Avg, tag("avg")),
        value(AggregateFunc::Min, tag("min")),
        value(AggregateFunc::Max, tag("max")),
    ))(input)
}

fn parse_partition_block(input: &str) -> IResult<&str, Vec<String>> {
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
    
    let (input, _) = ws_or_comment(input)?;
    let (input, _) = char('}')(input)?;
    
    let mut cols = vec![first.to_string()];
    cols.append(&mut rest.iter().map(|s| s.to_string()).collect());
    Ok((input, cols))
}

/// Parse sort cage [^col] or [^!col] for window functions.
fn parse_window_sort(input: &str) -> IResult<&str, Cage> {
    let (input, _) = char('^')(input)?;
    let (input, desc) = opt(char('!'))(input)?;
    let (input, col) = parse_identifier(input)?;
    
    let order = if desc.is_some() {
        SortOrder::Desc
    } else {
        SortOrder::Asc
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
