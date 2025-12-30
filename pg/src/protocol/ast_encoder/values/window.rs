//! Window function encoding.
//!
//! Handles PostgreSQL window functions: ROW_NUMBER(), RANK(), etc.

#![allow(dead_code)]

use bytes::BytesMut;
use qail_core::ast::{CageKind, SortOrder, Cage};

/// Encode a window function expression.
pub fn encode_window(
    name: &str,
    func: &str,
    params: &[qail_core::ast::Value],
    partition: &[String],
    order: &[Cage],
    buf: &mut BytesMut,
) {
    buf.extend_from_slice(func.to_uppercase().as_bytes());
    buf.extend_from_slice(b"(");
    for (i, p) in params.iter().enumerate() {
        if i > 0 {
            buf.extend_from_slice(b", ");
        }
        buf.extend_from_slice(p.to_string().as_bytes());
    }
    buf.extend_from_slice(b") OVER (");
    
    // PARTITION BY clause
    if !partition.is_empty() {
        buf.extend_from_slice(b"PARTITION BY ");
        for (i, col) in partition.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            buf.extend_from_slice(col.as_bytes());
        }
    }
    
    // ORDER BY clause
    if !order.is_empty() {
        if !partition.is_empty() {
            buf.extend_from_slice(b" ");
        }
        buf.extend_from_slice(b"ORDER BY ");
        for (i, cage) in order.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            if let Some(cond) = cage.conditions.first() {
                buf.extend_from_slice(cond.left.to_string().as_bytes());
            }
            if let CageKind::Sort(sort) = &cage.kind {
                encode_sort_order(sort, buf);
            }
        }
    }
    
    buf.extend_from_slice(b")");
    if !name.is_empty() {
        buf.extend_from_slice(b" AS ");
        buf.extend_from_slice(name.as_bytes());
    }
}

/// Encode sort order to buffer.
fn encode_sort_order(sort: &SortOrder, buf: &mut BytesMut) {
    match sort {
        SortOrder::Asc => buf.extend_from_slice(b" ASC"),
        SortOrder::Desc => buf.extend_from_slice(b" DESC"),
        SortOrder::AscNullsFirst => buf.extend_from_slice(b" ASC NULLS FIRST"),
        SortOrder::AscNullsLast => buf.extend_from_slice(b" ASC NULLS LAST"),
        SortOrder::DescNullsFirst => buf.extend_from_slice(b" DESC NULLS FIRST"),
        SortOrder::DescNullsLast => buf.extend_from_slice(b" DESC NULLS LAST"),
    }
}
