//! Extension traits for Expr.

use crate::ast::Expr;

/// Extension trait to add fluent methods to Expr
pub trait ExprExt {
    /// Add an alias to this expression
    fn as_alias(self, alias: &str) -> Expr;
}

impl ExprExt for Expr {
    fn as_alias(self, alias: &str) -> Expr {
        match self {
            Expr::Named(name) => Expr::Aliased { name, alias: alias.to_string() },
            Expr::Aggregate { col, func, distinct, filter, .. } => {
                Expr::Aggregate { col, func, distinct, filter, alias: Some(alias.to_string()) }
            }
            Expr::Cast { expr, target_type, .. } => {
                Expr::Cast { expr, target_type, alias: Some(alias.to_string()) }
            }
            Expr::Case { when_clauses, else_value, .. } => {
                Expr::Case { when_clauses, else_value, alias: Some(alias.to_string()) }
            }
            Expr::FunctionCall { name, args, .. } => {
                Expr::FunctionCall { name, args, alias: Some(alias.to_string()) }
            }
            Expr::Binary { left, op, right, .. } => {
                Expr::Binary { left, op, right, alias: Some(alias.to_string()) }
            }
            Expr::JsonAccess { column, path_segments, .. } => {
                Expr::JsonAccess { column, path_segments, alias: Some(alias.to_string()) }
            }
            Expr::SpecialFunction { name, args, .. } => {
                Expr::SpecialFunction { name, args, alias: Some(alias.to_string()) }
            }
            other => other,  // Star, Aliased, Literal, etc. - return as-is
        }
    }
}
