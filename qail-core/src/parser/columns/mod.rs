pub mod json;
pub mod functions;
pub mod window;
pub mod ddl;
pub mod aggregates;

use nom::{
    branch::alt,
    character::complete::{char},
    combinator::{opt},
    multi::{many0},
    sequence::{tuple, preceded},
    IResult,
};

use crate::ast::*;
use crate::parser::tokens::*;

pub use self::json::parse_json_suffix;
pub use self::functions::{parse_call_suffix, parse_arg_value, parse_function_column};
pub use self::window::{parse_partition_block, parse_window_sort};
pub use self::ddl::{parse_constraints, parse_index_columns, parse_table_constraints, parse_constraint_columns};
pub use self::aggregates::parse_agg_func;

/// Parse columns using the label syntax ('col).
pub fn parse_columns(input: &str) -> IResult<&str, Vec<Column>> {
    many0(preceded(ws_or_comment, parse_any_column))(input)
}

pub fn parse_any_column(input: &str) -> IResult<&str, Column> {
    // Consume optional ':' before columns (used as separator in `:'col1:'col2` syntax)
    let (input, _) = opt(char(':'))(input)?;
    let (input, _) = ws_or_comment(input)?;
    
    alt((
        // Label: 'col...
        preceded(char('\''), parse_label_column),
        // Bare syntax: use same pattern-based approach as parse_label_column
        parse_bare_column,
    ))(input)
}

/// Parse a bare (non-label) column using pattern-based detection
fn parse_bare_column(input: &str) -> IResult<&str, Column> {
    let (input, name) = parse_identifier(input)?;
    
    alt((
        |i| parse_json_suffix(i, name),
        |i| parse_call_suffix(i, name),
        |i| parse_column_def_rest(i, name),
    ))(input)
}

/// Parse a column with the label syntax ('col).
fn parse_label_column(input: &str) -> IResult<&str, Column> {
    // First check for wildcard
    if let Ok((remaining, _)) = char::<_, nom::error::Error<&str>>('_')(input) {
        return Ok((remaining, Column::Star));
    }
    
    let (input, name) = parse_identifier(input)?;

    alt((
        |i| parse_json_suffix(i, name),
        |i| parse_call_suffix(i, name),
        |i| parse_column_def_rest(i, name),
    ))(input)
}

/// Parse the rest of a full column definition (after name)
/// Handles: #agg, :type, constraints
fn parse_column_def_rest<'a>(input: &'a str, name: &str) -> IResult<&'a str, Column> {
    // 0. Check for alias: @alias
    if let Ok((input, _)) = char::<_, nom::error::Error<&str>>('@')(input) {
        let (input, alias) = parse_identifier(input)?;
        return Ok((input, Column::Aliased {
            name: name.to_string(),
            alias: alias.to_string(),
        }));
    }

    // 1. Opt: Aggregates (#func)
    if let Ok((input, Some(func))) = opt(preceded(char('#'), parse_agg_func))(input) {
        return Ok((input, Column::Aggregate {
             col: name.to_string(),
             func
        }));
    }
    
    // 2. Opt: check for colon (type definition)
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
