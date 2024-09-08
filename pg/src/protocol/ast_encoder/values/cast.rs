//! Cast and type conversion encoding.
//!
//! Handles PostgreSQL type casting: ::type syntax

#![allow(dead_code)]

use bytes::BytesMut;

use super::expressions::encode_column_expr;
use qail_core::ast::Expr;

/// Encode a CAST expression (expr::type).
pub fn encode_cast(
    expr: &Expr,
    target_type: &str,
    alias: &Option<String>,
    buf: &mut BytesMut,
) {
    encode_column_expr(expr, buf);
    buf.extend_from_slice(b"::");
    buf.extend_from_slice(target_type.as_bytes());
    if let Some(a) = alias {
        buf.extend_from_slice(b" AS ");
        buf.extend_from_slice(a.as_bytes());
    }
}
