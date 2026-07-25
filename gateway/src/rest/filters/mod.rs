//! Filter parsing and query parameter helpers.
//!
//! Parses PostgREST-style filter operators from query strings and applies them
//! to Qail AST commands.

#[cfg(test)]
use qail_core::ast::{Operator, Value as QailValue};

mod apply;
mod convert;
mod parse;

pub(crate) use apply::{
    MAX_SORT_COLUMNS, apply_filters, apply_filters_owned, apply_returning, apply_sorting,
    qualify_base_filter_columns_for_join,
};
pub(crate) use convert::{json_into_qail_value, json_to_qail_value};
#[cfg(test)]
pub(crate) use parse::parse_filters;
#[cfg(test)]
pub(crate) use parse::parse_scalar_value;
pub(crate) use parse::{
    is_safe_identifier, parse_cursor_value, parse_expand_relations, parse_filters_checked,
    parse_identifier_csv, parse_select_columns,
};

#[cfg(test)]
mod tests;
