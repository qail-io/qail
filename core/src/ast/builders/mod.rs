//! Ergonomic builder functions for QAIL AST expressions.
//!
//! This module provides convenient helper functions to construct AST nodes
//! without the verbosity of creating structs directly.
//!
//! # Modules
//!
//! - `columns` - Column references and parameters
//! - `aggregates` - Aggregate functions (COUNT, SUM, AVG, etc.)
//! - `json` - JSON/JSONB access operators
//! - `functions` - Function calls (COALESCE, REPLACE, etc.)
//! - `literals` - Literal values (text, int, float, boolean)
//! - `conditions` - WHERE clause conditions (eq, gt, like, etc.)
//! - `time` - Time functions (NOW, INTERVAL)
//! - `case_when` - CASE WHEN expressions
//! - `cast` - Type casting
//! - `binary` - Binary operations (+, -, ||)
//! - `ext` - Extension traits for Expr
//!
//! # Example
//! ```ignore
//! use qail_core::ast::builders::*;
//!
//! let query = Qail::get("orders")
//!     .column_expr(col("id"))
//!     .column_expr(json("contact_info", "phone").alias("phone"))
//!     .column_expr(coalesce([col("name"), text("Unknown")]).alias("name"))
//!     .filter_cond(cond(json("data", "status"), Operator::Eq, "active"))
//!     .order_desc("created_at")
//!     .limit(10);
//! ```

pub mod aggregates;
pub mod binary;
pub mod case_when;
pub mod cast;
pub mod columns;
pub mod conditions;
pub mod ext;
pub mod functions;
pub mod json;
pub mod literals;
pub mod shortcuts;
pub mod time;

// Re-export everything for convenient `use qail_core::ast::builders::*;`

pub use columns::{col, param, star};

// Aggregates
pub use aggregates::{
    AggregateBuilder, array_agg, avg, bool_and, bool_or, count, count_distinct, count_filter,
    json_agg, jsonb_agg, max, min, sum,
};

// JSON
pub use json::{JsonBuilder, json, json_obj, json_path};

// Functions
pub use functions::{
    ConcatBuilder, FunctionBuilder, coalesce, concat, func, nullif, replace, string_agg,
    substring, substring_for,
};

// Literals
pub use literals::{bind, boolean, float, int, null, text};

// Conditions
pub use conditions::{
    between, cond, contains, eq, gt, gte, ilike, is_in, is_not_null, is_null, key_exists, like, lt,
    lte, ne, not_between, not_in, not_like, overlaps, regex, regex_i, similar_to,
};

// Time
pub use time::{interval, now, now_minus, now_plus};

// CASE WHEN
pub use case_when::{CaseBuilder, case_when};

// Cast
pub use cast::{CastBuilder, cast};

// Binary
pub use binary::{BinaryBuilder, binary};

// Extension traits
pub use ext::ExprExt;

// Shortcuts (ergonomic helpers)
pub use shortcuts::{all, and, and3, count_where, count_where_all, in_list, percentage, recent, recent_col};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::BinaryOp;

    #[test]
    fn test_count_filter() {
        let expr = count_filter(vec![eq("direction", "outbound")]).alias("sent_count");

        assert!(
            matches!(expr, crate::ast::Expr::Aggregate { alias: Some(a), .. } if a == "sent_count")
        );
    }

    #[test]
    fn test_now_minus() {
        let expr = now_minus("24 hours");
        assert!(matches!(
            expr,
            crate::ast::Expr::Binary {
                op: BinaryOp::Sub,
                ..
            }
        ));
    }

    #[test]
    fn test_case_when() {
        let expr = case_when(gt("x", 0), int(1))
            .otherwise(int(0))
            .alias("result");

        assert!(matches!(expr, crate::ast::Expr::Case { alias: Some(a), .. } if a == "result"));
    }

    #[test]
    fn test_cast() {
        let expr = cast(col("value"), "float8").alias("value_f");
        assert!(
            matches!(expr, crate::ast::Expr::Cast { target_type, .. } if target_type == "float8")
        );
    }

    #[test]
    fn test_json_access() {
        let expr = json("contact_info", "phone").alias("phone");
        assert!(
            matches!(expr, crate::ast::Expr::JsonAccess { alias: Some(a), .. } if a == "phone")
        );
    }

    #[test]
    fn test_concat() {
        let expr: crate::ast::Expr = concat([col("a"), text(" "), col("b")]).into();
        assert!(matches!(
            expr,
            crate::ast::Expr::Binary {
                op: BinaryOp::Concat,
                ..
            }
        ));
    }
}
