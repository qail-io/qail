use nom::{
    bytes::complete::{tag, take_until},
    character::complete::{char},
    combinator::{opt},
    sequence::{delimited},
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
    
    // Special handling for WITH (CTE) action
    if action == Action::With {
        return parse_with_command(input);
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
            ctes: vec![],
        },
    ))
}

/// Parse WITH (CTE) command.
/// 
/// Syntax: `with::cte_name { base_query } ~> { recursive_query } -> final_query`
/// 
/// Examples:
/// - Simple CTE: `with::recent { get::orders:'_[created_at > now() - 7d] } -> get::recent:'_`
/// - Recursive: `with::emp_tree { get::employees:'_[manager_id IS NULL] } ~> { get::employees:'_->emp_tree } -> get::emp_tree:'_`
fn parse_with_command(input: &str) -> IResult<&str, QailCmd> {
    let (input, _) = ws_or_comment(input)?;
    
    // Parse CTE name
    let (input, cte_name) = parse_identifier(input)?;
    let (input, _) = ws_or_comment(input)?;
    
    // Parse base query in braces: { get::... }
    let (input, base_str) = delimited(
        char('{'),
        take_until("}"),
        char('}')
    )(input)?;
    
    // Recursively parse the base query
    let (_, base_query) = parse_qail_cmd(base_str.trim())?;
    
    let (input, _) = ws_or_comment(input)?;
    
    // Check for recursive part: ~> { ... }
    let (input, recursive_query) = if input.starts_with("~>") {
        let (input, _) = tag("~>")(input)?;
        let (input, _) = ws_or_comment(input)?;
        let (input, rec_str) = delimited(
            char('{'),
            take_until("}"),
            char('}')
        )(input)?;
        let (_, rec_query) = parse_qail_cmd(rec_str.trim())?;
        (input, Some(rec_query))
    } else {
        (input, None)
    };
    
    let (input, _) = ws_or_comment(input)?;
    
    // Parse final query: -> get::cte_name:'_
    let (input, _) = tag("->")(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (_input, final_query) = parse_qail_cmd(input)?;
    
    // Extract column names from base query
    let columns: Vec<String> = base_query.columns.iter().filter_map(|c| {
        match c {
            Column::Named(n) => Some(n.clone()),
            Column::Aliased { alias, .. } => Some(alias.clone()),
            _ => None,
        }
    }).collect();
    
    Ok((
        "",  // Consume all input
        QailCmd {
            action: Action::With,
            table: final_query.table.clone(),
            columns: final_query.columns.clone(),
            joins: final_query.joins.clone(),
            cages: final_query.cages.clone(),
            distinct: final_query.distinct,
            index_def: None,
            table_constraints: vec![],
            set_ops: vec![],
            having: vec![],
            group_by_mode: GroupByMode::default(),
            ctes: vec![CTEDef {
                name: cte_name.to_string(),
                recursive: recursive_query.is_some(),
                columns,
                base_query: Box::new(base_query),
                recursive_query: recursive_query.map(Box::new),
                source_table: Some(cte_name.to_string()),
            }],
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
        ctes: vec![],
    }))
}
