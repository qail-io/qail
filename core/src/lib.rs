//! Type-safe SQL query builder with AST-native design.
//!
//! Build queries as typed AST, not strings. Zero SQL injection risk.
//!
//! ```ignore
//! use qail_core::ast::{Qail, Operator};
//! let cmd = Qail::get("users").column("name").filter("active", Operator::Eq, true);
//! ```

pub mod analyzer;
pub mod ast;
pub mod error;
pub mod fmt;
pub mod migrate;
pub mod parser;
pub mod schema;
pub mod transformer;
pub mod transpiler;
pub mod validator;

pub use parser::parse;

/// Ergonomic alias for Qail - the primary query builder type.
pub type Qail = ast::Qail;

pub mod prelude {
    pub use crate::ast::*;
    pub use crate::ast::builders::{
        // Column builders
        col, param, star,
        // Aggregate builders
        count, count_distinct, count_filter, count_where, count_where_all,
        sum, avg, max, min,
        // Condition builders  
        eq, ne, gt, gte, lt, lte, is_null, is_not_null, is_in, not_in, like, ilike,
        cond,
        // Literal builders
        text, int, float, boolean, null, bind,
        // Expression builders
        cast, now, now_minus, now_plus, interval, binary, case_when,
        // Function builders
        coalesce, func, replace, nullif, concat,
        // JSON builders
        json, json_path, json_obj,
        // Shortcut helpers
        recent, recent_col, in_list, percentage, all, and, and3,
        // Extension traits
        ExprExt,
    };

    pub use crate::error::*;
    pub use crate::parser::parse;
    pub use crate::transpiler::ToSql;
    pub use crate::Qail;
}
