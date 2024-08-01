use nom::{
    bytes::complete::{tag},
    character::complete::{char},
    combinator::{opt},
    IResult,
};

use crate::ast::*;
use crate::parser::tokens::*;
use crate::parser::columns::ddl::parse_index_columns;

/// Parse INDEX command: `index::idx_name^on(table:'col1-col2)^unique`
/// Returns a QailCmd with action=Index and populated index_def
pub fn parse_index_command(input: &str) -> IResult<&str, QailCmd> {
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
