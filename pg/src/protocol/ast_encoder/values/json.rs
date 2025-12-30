//! JSON operator encoding.
//!
//! Handles PostgreSQL JSON/JSONB operators: ->, ->>, #>, #>>

#![allow(dead_code)]

use bytes::BytesMut;

/// Encode JSON access expression (column->'key' or column->>'key').
pub fn encode_json_access(
    column: &str,
    path_segments: &[(String, bool)],
    alias: &Option<String>,
    buf: &mut BytesMut,
) {
    buf.extend_from_slice(column.as_bytes());
    for (key, as_text) in path_segments {
        if *as_text {
            // ->> extracts as TEXT
            buf.extend_from_slice(b"->>'");
        } else {
            // -> extracts as JSON/JSONB
            buf.extend_from_slice(b"->'");
        }
        buf.extend_from_slice(key.as_bytes());
        buf.extend_from_slice(b"'");
    }
    if let Some(a) = alias {
        buf.extend_from_slice(b" AS ");
        buf.extend_from_slice(a.as_bytes());
    }
}

/// Encode JSON path operator (#> or #>>).
#[allow(dead_code)]
pub fn encode_json_path(
    column: &str,
    path: &[String],
    as_text: bool,
    alias: &Option<String>,
    buf: &mut BytesMut,
) {
    buf.extend_from_slice(column.as_bytes());
    if as_text {
        buf.extend_from_slice(b" #>> '{");
    } else {
        buf.extend_from_slice(b" #> '{");
    }
    for (i, key) in path.iter().enumerate() {
        if i > 0 {
            buf.extend_from_slice(b",");
        }
        buf.extend_from_slice(key.as_bytes());
    }
    buf.extend_from_slice(b"}'");
    if let Some(a) = alias {
        buf.extend_from_slice(b" AS ");
        buf.extend_from_slice(a.as_bytes());
    }
}
