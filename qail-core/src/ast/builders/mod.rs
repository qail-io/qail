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
//! let query = QailCmd::get("orders")
//!     .column_expr(col("id"))
//!     .column_expr(json("contact_info", "phone").alias("phone"))
//!     .column_expr(coalesce([col("name"), text("Unknown")]).alias("name"))
//!     .filter_cond(cond(json("data", "status"), Operator::Eq, "active"))
//!     .order_desc("created_at")
//!     .limit(10);
//! ```

pub mod columns;
pub mod aggregates;
pub mod json;
pub mod functions;
pub mod literals;
pub mod conditions;
pub mod time;
pub mod case_when;
pub mod cast;
pub mod binary;
pub mod ext;

// Re-export everything for convenient `use qail_core::ast::builders::*;`

// Columns
pub use columns::{col, star, param};

// Aggregates
pub use aggregates::{count, count_distinct, count_filter, sum, avg, min, max, AggregateBuilder};

// JSON
pub use json::{json, json_path, json_obj, JsonBuilder};

// Functions
pub use functions::{func, coalesce, nullif, replace, substring, substring_for, concat, FunctionBuilder, ConcatBuilder};

// Literals
pub use literals::{int, float, text, boolean, null, bind};

// Conditions
pub use conditions::{eq, ne, gt, gte, lt, lte, is_in, not_in, is_null, is_not_null, like, ilike, cond};

// Time
pub use time::{now, interval, now_minus, now_plus};

// CASE WHEN
pub use case_when::{case_when, CaseBuilder};

// Cast
pub use cast::{cast, CastBuilder};

// Binary
pub use binary::{binary, BinaryBuilder};

// Extension traits
pub use ext::ExprExt;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::BinaryOp;

    #[test]
    fn test_count_filter() {
        let expr = count_filter(vec![
            eq("direction", "outbound"),
        ]).alias("sent_count");
        
        assert!(matches!(expr, crate::ast::Expr::Aggregate { alias: Some(a), .. } if a == "sent_count"));
    }

    #[test]
    fn test_now_minus() {
        let expr = now_minus("24 hours");
        assert!(matches!(expr, crate::ast::Expr::Binary { op: BinaryOp::Sub, .. }));
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
        assert!(matches!(expr, crate::ast::Expr::Cast { target_type, .. } if target_type == "float8"));
    }
    
    #[test]
    fn test_json_access() {
        let expr = json("contact_info", "phone").alias("phone");
        assert!(matches!(expr, crate::ast::Expr::JsonAccess { alias: Some(a), .. } if a == "phone"));
    }
    
    #[test]
    fn test_concat() {
        let expr: crate::ast::Expr = concat([col("a"), text(" "), col("b")]).into();
        assert!(matches!(expr, crate::ast::Expr::Binary { op: BinaryOp::Concat, .. }));
    }
}
