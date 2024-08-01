use nom::{
    bytes::complete::{tag},
    character::complete::{char},
    IResult,
};

use crate::ast::*;
use crate::parser::tokens::*;
use crate::parser::commands::parse_qail_cmd; // Recursive dependency

/// Parse WITH (CTE) command.
/// 
/// Syntax: `with::cte1 { ... } ~> { ... }; cte2 { ... } -> final_query`
/// 
/// Supports multiple CTEs separated by semicolons.
/// Recursive CTEs use `~> { ... }` syntax for the recursive part.
/// All CTEs are followed by `->` pointing to the final query.
pub fn parse_with_command(input: &str) -> IResult<&str, QailCmd> {
    let (input, _) = ws_or_comment(input)?;
    
    let mut ctes = Vec::new();
    let mut remaining = input;
    
    loop {
        // Parse CTE name
        let (input, cte_name) = parse_identifier(remaining)?;
        let (input, _) = ws_or_comment(input)?;
        
        // Parse base query: { ... }
        let (input, base_str) = parse_balanced_block(input)?;
        
        // Use full parser for the inner query
        let (_, base_query) = parse_qail_cmd(base_str.trim())?;
        
        let (input, _) = ws_or_comment(input)?;
        
        // Parse recursive part: ~> { ... }
        let (input, recursive_query) = if input.starts_with("~>") {
            let (input, _) = tag("~>")(input)?;
            let (input, _) = ws_or_comment(input)?;
            
            // Custom parser for balanced braces { ... }
            let (input, rec_str) = parse_balanced_block(input)?;
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
