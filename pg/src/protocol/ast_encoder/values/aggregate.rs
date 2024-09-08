//! Aggregate expression encoding.
//!
//! Handles COUNT, SUM, AVG, MIN, MAX, etc.

#![allow(dead_code)]

use bytes::BytesMut;
use qail_core::ast::Expr;

/// Encode an aggregate expression (COUNT, SUM, etc.).
pub fn encode_aggregate(
    col: &str,
    func: &qail_core::ast::AggregateFunc,
    distinct: bool,
    alias: &Option<String>,
    buf: &mut BytesMut,
) {
    buf.extend_from_slice(func.to_string().as_bytes());
    buf.extend_from_slice(b"(");
    if distinct {
        buf.extend_from_slice(b"DISTINCT ");
    }
    buf.extend_from_slice(col.as_bytes());
    buf.extend_from_slice(b")");
    if let Some(a) = alias {
        buf.extend_from_slice(b" AS ");
        buf.extend_from_slice(a.as_bytes());
    }
}

/// Check if expression is an aggregate.
pub fn is_aggregate(expr: &Expr) -> bool {
    matches!(expr, Expr::Aggregate { .. })
}
