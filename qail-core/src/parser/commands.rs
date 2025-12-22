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
    
    // Check for DISTINCT variants:
    // 1. !on(col,col) -> DISTINCT ON
    // 2. ! -> DISTINCT
    
    let (input, distinct_on) = if let Ok((input, _)) = tag::<_, _, nom::error::Error<&str>>("!on(")(input) {
        let (input, (cols, _)) = parse_constraint_columns(input)?;
        let (input, _) = char(')')(input)?;
        (input, cols)
    } else {
        (input, vec![])
    };

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
            distinct_on: vec![],
        },
    ))
}

/// Parse WITH (CTE) command.
/// 
/// Syntax: `with::cte1 { ... } ~> { ... }; cte2 { ... } -> final_query`
/// 
/// Supports multiple CTEs separated by semicolons.
/// Recursive CTEs use `~> { ... }` syntax for the recursive part.
/// All CTEs are followed by `->` pointing to the final query.
fn parse_with_command(input: &str) -> IResult<&str, QailCmd> {
    let (input, _) = ws_or_comment(input)?;
    
    let mut ctes = Vec::new();
    let mut remaining = input;
    
    loop {
        // Parse CTE name
        let (input, cte_name) = parse_identifier(remaining)?;
        let (input, _) = ws_or_comment(input)?;
        
        // Parse base query: { ... }
        let (input, base_str) = delimited(
            char('{'),
            take_until("}"),
            char('}')
        )(input)?;
        
        let (_, base_query) = parse_qail_cmd(base_str.trim())?;
        
        let (input, _) = ws_or_comment(input)?;
        
        // Parse recursive part: ~> { ... }
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
        
        // Extract columns from base query for the CTE definition
        let columns: Vec<String> = base_query.columns.iter().filter_map(|c| {
            match c {
                Column::Named(n) => Some(n.clone()),
                Column::Aliased { alias, .. } => Some(alias.clone()),
                _ => None,
            }
        }).collect();
        
        ctes.push(CTEDef {
            name: cte_name.to_string(),
            recursive: recursive_query.is_some(),
            columns,
            base_query: Box::new(base_query),
            recursive_query: recursive_query.map(Box::new),
            source_table: Some(cte_name.to_string()),
        });
        
        let (input, _) = ws_or_comment(input)?;
        
        // Check separator or end
        if input.starts_with("->") {
            remaining = input;
            break;
        } else if input.starts_with(';') {
            let (input, _) = char(';')(input)?;
            let (input, _) = ws_or_comment(input)?;
            remaining = input;
        } else {
            remaining = input;
        }
    }
    
    // Parse final query: -> get::...
    let (remaining, _) = tag("->")(remaining)?;
    let (remaining, _) = ws_or_comment(remaining)?;
    let (_input, final_query) = parse_qail_cmd(remaining)?;
    
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
            distinct_on: vec![],
            ctes,
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
            distinct_on: vec![],
    }))
}
