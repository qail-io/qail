//! DML (Data Manipulation Language) SQL generation.
//!
//! This module contains functions for generating SELECT, INSERT, UPDATE, DELETE,
//! and other DML statements.

pub mod cte;
pub mod delete;
pub mod insert;
pub mod json_table;
pub mod merge;
pub mod select;
pub mod update;
pub mod upsert;
pub mod window;

/// `WITH (OLD AS x, NEW AS y) ` to follow `RETURNING `, or empty.
pub(crate) fn returning_aliases_sql(
    cmd: &crate::ast::Qail,
    generator: &dyn crate::transpiler::SqlGenerator,
) -> String {
    let Some(parts) = cmd.returning_aliases.as_ref().and_then(|a| a.sql_parts()) else {
        return String::new();
    };
    let parts = parts
        .iter()
        .map(|(keyword, alias)| format!("{keyword} AS {}", generator.quote_identifier(alias)))
        .collect::<Vec<_>>();
    format!("WITH ({}) ", parts.join(", "))
}
