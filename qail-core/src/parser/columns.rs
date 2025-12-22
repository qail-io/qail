use nom::{
    branch::alt,
    bytes::complete::{tag, take_until, take_while1},
    character::complete::{char, digit1, multispace0},
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
        // JSON access: 'col->key
        parse_json_column,
        // Function call: 'func(args)
        parse_function_column,
        // Named or complex column
        parse_column_full_def_or_named,
    ))(input)
}

fn parse_json_column(input: &str) -> IResult<&str, Column> {
    let (input, col) = parse_identifier(input)?;
    let (input, as_text) = alt((
        value(true, tag("->>")),
        value(false, tag("->")),
    ))(input)?;
    // Parse path key (could be 'key' or just key)
    // Simplify: take identifier or string
    let (input, path) = alt((
        parse_quoted_string,
        map(parse_identifier, |s: &str| s.to_string()),
    ))(input)?;
    
    // Optional alias
    let (input, alias) = opt(preceded(char('@'), parse_identifier))(input)?;

    Ok((input, Column::JsonAccess {
        column: col.to_string(),
        path: path.to_string(),
        as_text,
        alias: alias.map(|s| s.to_string()),
    }))
}

fn parse_function_column(input: &str) -> IResult<&str, Column> {
    // Look ahead to ensure it's a function call `name(`
    let (input, name) = parse_identifier(input)?;
    let (input, _) = char('(')(input)?;
    
    // Parse args
    let (input, args) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_arg_value // Need a parser that accepts identifiers, strings, numbers
    )(input)?;
    
    let (input, _) = char(')')(input)?;
    
    // Optional alias
    let (input, alias) = opt(preceded(char('@'), parse_identifier))(input)?;
    
    Ok((input, Column::FunctionCall {
        name: name.to_string(),
        args: args.into_iter().map(|s| s.to_string()).collect(),
        alias: alias.map(|s| s.to_string()),
    }))
}

/// Helper to parse function arguments (simple strings/identifiers/numbers for now)
fn parse_arg_value(input: &str) -> IResult<&str, String> {
    alt((
        parse_quoted_string,
        map(parse_identifier, |s: &str| s.to_string()),
        // numeric
        map(
             take_while1(|c: char| c.is_numeric() || c == '.'),
             |s: &str| s.to_string()
        )
    ))(input)
}

fn parse_quoted_string(input: &str) -> IResult<&str, String> {
    let (input, _) = char('\'')(input)?;
    let (input, content) = take_until("'")(input)?;
    let (input, _) = char('\'')(input)?;
    Ok((input, content.to_string()))
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
            let (input, part_block) = opt(parse_partition_block)(input)?;
            let (partition, frame) = part_block.unwrap_or((vec![], None));

            return Ok((input, Column::Window {
                name: name.to_string(),
                func: type_or_func.to_string(),
                params,
                partition,
                order: sorts,
                frame,
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
                pair(char('('), char(')'))
            ),
            |(name, _parens)| format!("{}()", name)
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
    let (input, (cols, _)) = parse_constraint_columns(input)?;
    let (input, _) = char(')')(input)?;
    Ok((input, TableConstraint::Unique(cols)))
}

/// Parse ^pk(col1, col2)
fn parse_table_pk(input: &str) -> IResult<&str, TableConstraint> {
    let (input, _) = tag("^pk(")(input)?;
    let (input, (cols, _)) = parse_constraint_columns(input)?;
    let (input, _) = char(')')(input)?;
    Ok((input, TableConstraint::PrimaryKey(cols)))
}

/// Parse comma-separated column names: col1, col2, col3
/// Returns (columns, optional_window_frame) because it handles window partition block {Part=...}
pub fn parse_constraint_columns(input: &str) -> IResult<&str, (Vec<String>, Option<WindowFrame>)> {
    let (input, _) = multispace0(input)?;
    let (input, first) = parse_identifier(input)?;
    let (input, rest) = many0(preceded(
        tuple((multispace0, char(','), multispace0)),
        parse_identifier
    ))(input)?;
    let (input, _) = multispace0(input)?;
    
    let mut cols = vec![first.to_string()];
    cols.extend(rest.iter().map(|s| s.to_string()));
    Ok((input, (cols, None)))
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

fn parse_partition_block(input: &str) -> IResult<&str, (Vec<String>, Option<WindowFrame>)> {
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

fn parse_window_frame(input: &str) -> IResult<&str, WindowFrame> {
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
        // Simple integers implies preceding? No, explicit keywords needed.
    ))(input)
}

/// Parse sort cage [^col] or [^!col] for window functions.
fn parse_window_sort(input: &str) -> IResult<&str, Cage> {
    let (input, _) = char('^')(input)?;
    let (input, desc) = opt(char('!'))(input)?;
    let (input, col) = parse_identifier(input)?;
    
    // Check for nulls directive
    // !null or !nulls_last or !last -> AscNullsLast/DescNullsLast
    // !first -> AscNullsFirst/DescNullsFirst
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
        (false, Some(_)) => SortOrder::AscNullsLast, // !last, !null, !nulls_last
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
