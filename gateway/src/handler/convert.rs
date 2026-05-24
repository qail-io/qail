//! PgRow → JSON conversion utilities.
//!
//! Converts PostgreSQL wire-protocol rows to `serde_json::Value`
//! using OID-directed type mapping when column metadata is available.

/// Convert a PgRow to a JSON object with column names as keys.
///
/// Used by both the QAIL handler and the REST handler.
pub fn row_to_json(row: &qail_pg::PgRow) -> serde_json::Value {
    let column_names: Vec<(String, usize)> = if let Some(ref info) = row.column_info {
        let mut pairs: Vec<_> = info.name_to_index.iter().collect();
        pairs.sort_by_key(|(_, idx)| *idx);
        pairs
            .into_iter()
            .map(|(name, idx)| (name.clone(), *idx))
            .collect()
    } else {
        (0..row.columns.len())
            .map(|i| (format!("col_{}", i), i))
            .collect()
    };

    let mut obj = serde_json::Map::new();

    // Use OID + format conversion when ColumnInfo is available.
    let col_info = row.column_info.as_ref();

    for (col_name, i) in column_names {
        let value = if let Some(bytes) = row.columns.get(i).and_then(|v| v.as_deref()) {
            let oid = match col_info {
                Some(info) => match info.oids.get(i) {
                    Some(o) => *o,
                    None => 0,
                },
                None => 0,
            };
            let format = match col_info {
                Some(info) => match info.formats.get(i) {
                    Some(f) => *f,
                    None => 0,
                },
                None => 0,
            };
            bytes_to_json_typed(bytes, oid, format)
        } else {
            serde_json::Value::Null
        };

        obj.insert(col_name, value);
    }

    serde_json::Value::Object(obj)
}

/// Convert a PgRow to a JSON array (no column names — positional).
pub(crate) fn row_to_array(row: &qail_pg::PgRow) -> Vec<serde_json::Value> {
    row.columns
        .iter()
        .map(|col| match col {
            Some(bytes) => {
                let s = String::from_utf8_lossy(bytes);
                if (s.starts_with('{') && s.ends_with('}'))
                    || (s.starts_with('[') && s.ends_with(']'))
                {
                    match serde_json::from_str(&s) {
                        Ok(v) => v,
                        Err(_) => serde_json::Value::String(s.into_owned()),
                    }
                } else if let Ok(n) = s.parse::<i64>() {
                    serde_json::Value::Number(n.into())
                } else if let Ok(f) = s.parse::<f64>() {
                    match serde_json::Number::from_f64(f) {
                        Some(num) => serde_json::Value::Number(num),
                        None => serde_json::Value::String(s.into_owned()),
                    }
                } else if s == "t" || s == "true" {
                    serde_json::Value::Bool(true)
                } else if s == "f" || s == "false" {
                    serde_json::Value::Bool(false)
                } else {
                    serde_json::Value::String(s.into_owned())
                }
            }
            None => serde_json::Value::Null,
        })
        .collect()
}

/// Convert raw PostgreSQL bytes to JSON using OID + format (text/binary).
#[inline]
fn bytes_to_json_typed(bytes: &[u8], oid: u32, format: i16) -> serde_json::Value {
    use qail_pg::protocol::types::oid as pg_oid;
    use qail_pg::{Cidr, Date, FromPg, Inet, Json, MacAddr, Numeric, Time, Timestamp, Uuid};

    if format == 0 {
        // Text format.
        return match std::str::from_utf8(bytes) {
            Ok(s) => text_to_json_typed(s, oid),
            Err(_) => serde_json::Value::String(format!("\\x{}", hex_encode(bytes))),
        };
    }

    // Binary format.
    match oid {
        pg_oid::BOOL => match bool::from_pg(bytes, oid, format) {
            Ok(b) => serde_json::Value::Bool(b),
            Err(_) => serde_json::Value::Null,
        },
        pg_oid::INT2 | pg_oid::INT4 => match i32::from_pg(bytes, oid, format) {
            Ok(n) => serde_json::Value::Number(n.into()),
            Err(_) => serde_json::Value::Null,
        },
        pg_oid::INT8 | pg_oid::OID => match i64::from_pg(bytes, oid, format) {
            Ok(n) => serde_json::Value::Number(n.into()),
            Err(_) => serde_json::Value::Null,
        },
        pg_oid::FLOAT4 | pg_oid::FLOAT8 => match f64::from_pg(bytes, oid, format) {
            Ok(f) => match serde_json::Number::from_f64(f) {
                Some(num) => serde_json::Value::Number(num),
                None => serde_json::Value::Null,
            },
            Err(_) => serde_json::Value::Null,
        },
        pg_oid::NUMERIC => match Numeric::from_pg(bytes, oid, format) {
            Ok(n) => {
                if let Ok(i) = n.to_i64_exact() {
                    serde_json::Value::Number(i.into())
                } else if let Ok(f) = n.to_f64() {
                    match serde_json::Number::from_f64(f) {
                        Some(num) => serde_json::Value::Number(num),
                        None => serde_json::Value::String(n.0),
                    }
                } else {
                    serde_json::Value::String(n.0)
                }
            }
            Err(_) => serde_json::Value::String(format!("\\x{}", hex_encode(bytes))),
        },
        pg_oid::JSON | pg_oid::JSONB => match Json::from_pg(bytes, oid, format) {
            Ok(j) => match serde_json::from_str(&j.0) {
                Ok(v) => v,
                Err(_) => serde_json::Value::String(j.0),
            },
            Err(_) => serde_json::Value::String(format!("\\x{}", hex_encode(bytes))),
        },
        pg_oid::UUID => match Uuid::from_pg(bytes, oid, format) {
            Ok(u) => serde_json::Value::String(u.0),
            Err(_) => serde_json::Value::String(format!("\\x{}", hex_encode(bytes))),
        },
        pg_oid::INET => match Inet::from_pg(bytes, oid, format) {
            Ok(v) => serde_json::Value::String(v.0),
            Err(_) => serde_json::Value::String(format!("\\x{}", hex_encode(bytes))),
        },
        pg_oid::CIDR => match Cidr::from_pg(bytes, oid, format) {
            Ok(v) => serde_json::Value::String(v.0),
            Err(_) => serde_json::Value::String(format!("\\x{}", hex_encode(bytes))),
        },
        pg_oid::MACADDR => match MacAddr::from_pg(bytes, oid, format) {
            Ok(v) => serde_json::Value::String(v.0),
            Err(_) => serde_json::Value::String(format!("\\x{}", hex_encode(bytes))),
        },
        pg_oid::TIMESTAMP | pg_oid::TIMESTAMPTZ => match Timestamp::from_pg(bytes, oid, format) {
            Ok(ts) => serde_json::Value::Number(ts.to_unix_secs().into()),
            Err(_) => serde_json::Value::String(format!("\\x{}", hex_encode(bytes))),
        },
        pg_oid::DATE => match Date::from_pg(bytes, oid, format) {
            Ok(d) => serde_json::Value::Number(d.days.into()),
            Err(_) => serde_json::Value::String(format!("\\x{}", hex_encode(bytes))),
        },
        pg_oid::TIME => match Time::from_pg(bytes, oid, format) {
            Ok(t) => serde_json::Value::Number(t.usec.into()),
            Err(_) => serde_json::Value::String(format!("\\x{}", hex_encode(bytes))),
        },
        _ => {
            crate::metrics::record_rpc_binary_decode_fallback();
            serde_json::Value::String(format!("\\x{}", hex_encode(bytes)))
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// Convert a PG text value to JSON using the column's OID for type-directed conversion.
///
/// PG OID reference:
///   16 = bool, 20/21/23/26 = int8/int2/int4/oid, 700/701 = float4/float8
///   114/3802 = json/jsonb, 1700 = numeric
///   25/1042/1043 = text/bpchar/varchar, 2950 = uuid
///   1082/1114/1184 = date/timestamp/timestamptz
///
/// When OID is known, this skips the expensive try-parse chain entirely.
#[inline]
pub(crate) fn text_to_json_typed(s: &str, oid: u32) -> serde_json::Value {
    match oid {
        // ── Boolean (OID 16) ──────────────────────────────────────
        16 => match s.trim() {
            "t" | "T" | "true" | "TRUE" | "1" => serde_json::Value::Bool(true),
            "f" | "F" | "false" | "FALSE" | "0" => serde_json::Value::Bool(false),
            _ => serde_json::Value::String(s.to_string()),
        },

        // ── Integer types (OID 20=int8, 21=int2, 23=int4, 26=oid) ──
        20 | 21 | 23 | 26 => match s.parse::<i64>() {
            Ok(n) => serde_json::Value::Number(n.into()),
            Err(_) => serde_json::Value::String(s.to_string()),
        },

        // ── Float types (OID 700=float4, 701=float8) ──────────────
        700 | 701 => match s.parse::<f64>() {
            Ok(f) => match serde_json::Number::from_f64(f) {
                Some(num) => serde_json::Value::Number(num),
                None => serde_json::Value::String(s.to_string()),
            },
            Err(_) => serde_json::Value::String(s.to_string()),
        },

        // ── Numeric/Decimal (OID 1700) — preserve precision as string or number ──
        1700 => {
            if let Ok(n) = s.parse::<i64>() {
                serde_json::Value::Number(n.into())
            } else if let Ok(f) = s.parse::<f64>() {
                match serde_json::Number::from_f64(f) {
                    Some(num) => serde_json::Value::Number(num),
                    None => serde_json::Value::String(s.to_string()),
                }
            } else {
                serde_json::Value::String(s.to_string())
            }
        }

        // ── JSON/JSONB (OID 114, 3802) — parse directly ──────────
        114 | 3802 => match serde_json::from_str(s) {
            Ok(v) => v,
            Err(_) => serde_json::Value::String(s.to_string()),
        },

        // ── Array types (int[], text[], etc.) — parse as JSON array ──
        1005 | 1007 | 1009 | 1015 | 1016 | 1021 | 1022 | 1000 | 1231 => pg_array_to_json(s),

        // ── Text, varchar, uuid, date, timestamp, etc. — return as string ──
        25 | 1042 | 1043 | 2950 | 1082 | 1114 | 1184 | 17 | 142 | 1186 => {
            serde_json::Value::String(s.to_string())
        }

        // ── Unknown OID (0 = no metadata) — fall back to guessing ──
        _ => text_to_json_guess(s),
    }
}

/// Fallback: guess JSON type from text content (used when OID is unknown).
#[inline]
fn text_to_json_guess(s: &str) -> serde_json::Value {
    if (s.starts_with('{') && s.ends_with('}')) || (s.starts_with('[') && s.ends_with(']')) {
        match serde_json::from_str(s) {
            Ok(v) => v,
            Err(_) => serde_json::Value::String(s.to_string()),
        }
    } else if let Ok(n) = s.parse::<i64>() {
        serde_json::Value::Number(n.into())
    } else if let Ok(f) = s.parse::<f64>() {
        match serde_json::Number::from_f64(f) {
            Some(num) => serde_json::Value::Number(num),
            None => serde_json::Value::String(s.to_string()),
        }
    } else if s == "t" || s == "true" {
        serde_json::Value::Bool(true)
    } else if s == "f" || s == "false" {
        serde_json::Value::Bool(false)
    } else {
        serde_json::Value::String(s.to_string())
    }
}

/// Convert PostgreSQL text-format array (e.g., `{1,2,3}` or `{{1,2},{3,4}}`) to JSON array.
///
/// Handles:
/// - Quoted strings with commas: `{"New York, NY", "London"}`
/// - Escaped quotes: `{"He said \"Hello\""}`
/// - Nested arrays: `{{1,2},{3,4}}`
/// - NULL values: `{1,NULL,3}` vs `{"NULL"}`
fn pg_array_to_json(s: &str) -> serde_json::Value {
    let s = s.trim();
    if s.starts_with('{') && s.ends_with('}') {
        let inner = &s[1..s.len() - 1];
        if inner.is_empty() {
            return serde_json::Value::Array(vec![]);
        }

        let mut elements = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut escaped = false;
        let mut brace_depth = 0;
        let mut was_quoted = false;
        let mut element_started = false;
        let mut malformed = false;

        for c in inner.chars() {
            if escaped {
                current.push(c);
                escaped = false;
                element_started = true;
                continue;
            }

            match c {
                '\\' if brace_depth > 0 => {
                    current.push(c);
                    if in_quotes {
                        escaped = true;
                    }
                    element_started = true;
                }
                '\\' => {
                    escaped = true;
                    element_started = true;
                }
                '"' => {
                    if brace_depth > 0 {
                        in_quotes = !in_quotes;
                        current.push(c);
                        element_started = true;
                    } else if !in_quotes && !current.trim().is_empty() {
                        malformed = true;
                        break;
                    } else {
                        in_quotes = !in_quotes;
                        was_quoted = true;
                        element_started = true;
                    }
                }
                '{' if !in_quotes => {
                    brace_depth += 1;
                    current.push(c);
                    element_started = true;
                }
                '}' if !in_quotes => {
                    if brace_depth == 0 {
                        malformed = true;
                        break;
                    }
                    brace_depth -= 1;
                    current.push(c);
                    element_started = true;
                }
                ',' if !in_quotes && brace_depth == 0 => {
                    if !element_started {
                        malformed = true;
                        break;
                    }
                    elements.push(finish_array_element(current, was_quoted));
                    current = String::new();
                    was_quoted = false;
                    element_started = false;
                }
                _ => {
                    current.push(c);
                    element_started = true;
                }
            }
        }
        if malformed || escaped || in_quotes || brace_depth != 0 || !element_started {
            return serde_json::Value::String(s.to_string());
        }
        elements.push(finish_array_element(current, was_quoted));
        serde_json::Value::Array(elements)
    } else {
        serde_json::Value::String(s.to_string())
    }
}

fn finish_array_element(s: String, was_quoted: bool) -> serde_json::Value {
    let trimmed = s.trim();

    // Recursive case for nested arrays
    if !was_quoted && trimmed.starts_with('{') && trimmed.ends_with('}') {
        return pg_array_to_json(trimmed);
    }

    if !was_quoted {
        if trimmed.eq_ignore_ascii_case("null") {
            return serde_json::Value::Null;
        }
        if let Ok(n) = trimmed.parse::<i64>() {
            return serde_json::Value::Number(n.into());
        }
        if let Some(num) = trimmed
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
        {
            return serde_json::Value::Number(num);
        }
    }

    // For quoted strings or non-numeric types, return as string.
    // If it was quoted, the quotes were stripped by the state machine logic
    // (by toggling in_quotes but not pushing the '"' char).
    serde_json::Value::String(trimmed.to_string())
}

#[cfg(test)]
mod tests;
