//! QAIL AST Kernel.
//!
//! Build database intent as typed AST, not application-assembled SQL strings.
//! The AST can then be validated, policy-checked, formatted, encoded for a
//! driver, or inspected by tooling.
//!
//! Main concepts:
//!
//! - `Qail`: the primary command builder for `get`, `add`, `set`, `del`,
//!   `put`, `merge_into`, CTEs, filters, projections, and returning clauses.
//! - `access`: native table/operation/column policy checks.
//! - `rls`: tenant/user/super-admin execution context witnesses.
//! - `migrate`: `schema.qail` parsing, diffing, and migration model types.
//! - `build`: source scanner helpers for stale schema references and N+1
//!   diagnostics.
//!
//! ```ignore
//! use qail_core::prelude::*;
//!
//! let ctx = RlsContext::tenant("018f6a60-4d5f-7a9d-9f4c-7dd8c338f1d2");
//! let cmd = Qail::get("users")
//!     .columns(["id", "email"])
//!     .eq("active", true)
//!     .with_rls(&ctx)?;
//! ```

/// Native vertical access policy checks for QAIL commands.
pub mod access;
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
/// SQL transpiler (AST to SQL).
pub mod transpiler;
/// Typed column and table traits.
pub mod typed;
/// Schema validator.
pub mod validator;
/// Versioned AST wire codecs (text + QWB2 binary).
pub mod wire;

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
    pub use crate::access::*;
    pub use crate::error::*;
    pub use crate::parser::parse;
    pub use crate::transpiler::ToSql;
}
