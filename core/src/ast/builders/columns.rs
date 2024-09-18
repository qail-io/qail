//! Column and basic expression builders.

use crate::ast::Expr;

/// Create a column reference expression
pub fn col(name: &str) -> Expr {
    Expr::Named(name.to_string())
}

/// Create a star (*) expression for SELECT *
pub fn star() -> Expr {
    Expr::Star
}

/// Create a parameter placeholder ($n)
/// # Example
/// ```ignore
/// param(1)  // $1
/// ```
pub fn param(n: u32) -> Expr {
    Expr::Named(format!("${}", n))
}
