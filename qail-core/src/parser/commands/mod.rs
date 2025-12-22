pub mod cte;
pub mod index;
pub mod utils;

use nom::{
    bytes::complete::{tag},
    character::complete::{char},
    combinator::{opt},
    IResult,
};

use crate::ast::*;
use crate::parser::tokens::*;
use crate::parser::columns::*;
use crate::parser::cages::*;

use self::cte::parse_with_command;
use self::index::parse_index_command;
use self::utils::{parse_inline_distinct_on, parse_inline_sort};

/// Parse the complete QAIL command.
pub fn parse_qail_cmd(input: &str) -> IResult<&str, QailCmd> {
    let (input, action) = parse_action(input)?;
    
    // Check for distinct_on before :: 
    // e.g. get!on(a,b)::table
    let (input, distinct_on) = parse_inline_distinct_on(input)?;

    let (input, distinct) = if distinct_on.is_empty() {
        let (input, distinct_marker) = opt(char('!'))(input)?;
        (input, distinct_marker.is_some())
    } else {
        (input, false) // distinct_on implies distinct, but we store it separately
    };

    let (input, _) = tag("::")(input)?;
    
    // Special handling for INDEX action
    if action == Action::Index {
        return parse_index_command(input);
    }
    
    // Special handling for WITH (CTE) action
    if action == Action::With {
        return parse_with_command(input);
    }
    
    let (input, table) = parse_identifier(input)?;
    let (input, _) = ws_or_comment(input)?;
    
    // Parse inline !on(columns) for DISTINCT ON after table name
    // e.g. get::table !on(a,b)
    let (input, inline_distinct_on) = parse_inline_distinct_on(input)?;
    let (input, _) = ws_or_comment(input)?;
    
    // Merge distinct_on from both positions
    let final_distinct_on = if distinct_on.is_empty() { inline_distinct_on } else { distinct_on };
    
    // Parse inline !sort(columns)
    let (input, inline_sort_cages) = parse_inline_sort(input)?;
    
    let (input, joins) = parse_joins(input)?;
    let (input, _) = ws_or_comment(input)?;
    // Link character ':' is optional - connects table to columns
    let (input, _) = opt(char(':'))(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, columns) = parse_columns(input)?;
    let (input, _) = ws_or_comment(input)?;
    
    // Parse table-level constraints for Make action
    let (input, table_constraints) = if action == Action::Make {
        crate::parser::columns::ddl::parse_table_constraints(input)?
    } else {
        (input, vec![])
    };
    
    let (input, _) = ws_or_comment(input)?;
    let (input, mut cages) = parse_unified_blocks(input)?;
    
    // Append inline sort cages (if !sort() came before cages)
    cages.extend(inline_sort_cages);
    
    // Also check for !sort() AFTER cages (alternative position)
    let (input, _) = ws_or_comment(input)?;
    let (input, trailing_sort_cages) = parse_inline_sort(input)?;
    cages.extend(trailing_sort_cages);
 
    // Refine Transaction Actions
    let final_action = if action == Action::TxnStart {
        match table {
            "start" | "begin" => Action::TxnStart,
            "commit" => Action::TxnCommit,
            "rollback" => Action::TxnRollback,
            _ => Action::TxnStart, // Default or Error?
        }
    } else {
        action
    };

    Ok((
        input,
        QailCmd {
            action: final_action,
            table: table.to_string(),
            joins,
            columns,
            cages,
            distinct,
            index_def: None,
            table_constraints,
            set_ops: vec![],
            having: vec![],
            group_by_mode: GroupByMode::default(),
            ctes: vec![],
            distinct_on: final_distinct_on,
        },
    ))
}
