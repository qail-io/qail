//! Type-safe SQL query builder with AST-native design.
//!
//! Build queries as typed AST, not strings. Zero SQL injection risk.
//!
//! ```ignore
//! use qail_core::ast::{Qail, Operator};
//! let cmd = Qail::get("users").column("name").filter("active", Operator::Eq, true);
//! ```

#[cfg(feature = "analyzer")]
pub mod analyzer;
/// Abstract syntax tree types.
pub mod ast;
/// Schema branching.
pub mod branch;
/// Build-time schema validation.
pub mod build;
/// Code generation for typed schema modules.
pub mod codegen;
/// Configuration types.
pub mod config;
/// Error types.
pub mod error;
/// SQL formatter.
pub mod fmt;
/// Database migration types.
pub mod migrate;
/// Query normalization and optimizer support.
pub mod optimizer;
/// QAIL query parser.
pub mod parser;
/// Row-level security context.
pub mod rls;
/// AST structural sanitization for untrusted binary input.
pub mod sanitize;
/// Schema definitions for validation.
pub mod schema;
/// Filesystem schema source loader (`schema.qail` or modular `schema/`).
pub mod schema_source;
#[cfg(feature = "analyzer")]
pub mod transformer;
/// SQL transpiler (AST to SQL).
pub mod transpiler;
/// Typed column and table traits.
pub mod typed;
/// Schema validator.
pub mod validator;

#[cfg(test)]
mod proptest;

pub use parser::parse;

/// Ergonomic alias for Qail - the primary query builder type.
pub type Qail = ast::Qail;

/// Common re-exports for convenient wildcard imports.
pub mod prelude {
    pub use crate::ast::builders::{
        // Extension traits
        ExprExt,
        add_expr,
        all,
        and,
        and3,
        avg,
        binary,
        bind,
        boolean,
        case_when,
        // Expression builders
        cast,
        // Function builders
        coalesce,
        // Column builders
        col,
        concat,
        cond,
        // Aggregate builders
        count,
        count_distinct,
        count_filter,
        count_where,
        count_where_all,
        // Condition builders
        eq,
        float,
        func,
        gt,
        gte,
        ilike,
        in_list,
        inc,
        int,
        interval,
        is_in,
        is_not_null,
        is_null,
        // JSON builders
        json,
        json_obj,
        json_path,
        like,
        lt,
        lte,
        max,
        min,
        ne,
        not_in,
        now,
        now_minus,
        now_plus,
        null,
        nullif,
        param,
        percentage,
        // Shortcut helpers
        recent,
        recent_col,
        replace,
        star,
        sum,
        // Literal builders
        text,
    };
    pub use crate::ast::*;

    pub use crate::Qail;
    pub use crate::error::*;
    pub use crate::parser::parse;
    pub use crate::transpiler::ToSql;
}
