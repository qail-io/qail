use nom::{
    bytes::complete::{tag},
    character::complete::{char},
    combinator::{opt},
    IResult,
};

use crate::ast::*;
use super::tokens::*;
use super::columns::*;
use super::cages::*;

/// Parse the complete QAIL command.
pub fn parse_qail_cmd(input: &str) -> IResult<&str, QailCmd> {
    let (input, action) = parse_action(input)?;
    // Check for ! after action for DISTINCT (e.g., get!::users)
    let (input, distinct_marker) = opt(char('!'))(input)?;
    let distinct = distinct_marker.is_some();
    let (input, _) = tag("::")(input)?;
    
    // Special handling for INDEX action
    if action == Action::Index {
        return parse_index_command(input);
    }
    
    let (input, table) = parse_identifier(input)?;
    let (input, joins) = parse_joins(input)?;
    let (input, _) = ws_or_comment(input)?;
    // Link character ':' is optional - connects table to columns
    let (input, _) = opt(char(':'))(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, columns) = parse_columns(input)?;
    let (input, _) = ws_or_comment(input)?;
    
    // Parse table-level constraints for Make action
    let (input, table_constraints) = if action == Action::Make {
        parse_table_constraints(input)?
    } else {
        (input, vec![])
    };
    
    let (input, _) = ws_or_comment(input)?;
    let (input, cages) = parse_unified_blocks(input)?;

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
            cte: None,
        },
    ))
}

/// Parse INDEX command: `index::idx_name^on(table:'col1-col2)^unique`
/// Returns a QailCmd with action=Index and populated index_def
fn parse_index_command(input: &str) -> IResult<&str, QailCmd> {
    // Parse index name
    let (input, name) = parse_identifier(input)?;
    
    // Parse ^on(table:'columns)
    let (input, _) = tag("^on(")(input)?;
    let (input, table) = parse_identifier(input)?;
    let (input, _) = char(':')(input)?;
    let (input, columns) = parse_index_columns(input)?;
    let (input, _) = char(')')(input)?;
    
    // Parse optional ^unique
    let (input, unique) = opt(tag("^unique"))(input)?;
    
    Ok((input, QailCmd {
        action: Action::Index,
        table: table.to_string(),
        columns: vec![],
        joins: vec![],
        cages: vec![],
        distinct: false,
        index_def: Some(IndexDef {
            name: name.to_string(),
            table: table.to_string(),
            columns,
            unique: unique.is_some(),
        }),
        table_constraints: vec![],
        set_ops: vec![],
        having: vec![],
        group_by_mode: GroupByMode::default(),
        cte: None,
    }))
}
