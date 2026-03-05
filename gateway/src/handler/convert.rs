//! PgRow → JSON conversion utilities.
//!
//! Converts PostgreSQL wire-protocol rows to `serde_json::Value`
//! using OID-directed type mapping when column metadata is available.

/// Convert a PgRow to a JSON object with column names as keys.
///
/// Used by both the QAIL handler and the REST handler.
pub fn row_to_json(row: &qail_pg::PgRow) -> serde_json::Value {
    let column_names: Vec<String> = if let Some(ref info) = row.column_info {
        let mut pairs: Vec<_> = info.name_to_index.iter().collect();
        pairs.sort_by_key(|(_, idx)| *idx);
        pairs.into_iter().map(|(name, _)| name.clone()).collect()
    } else {
        (0..row.columns.len())
            .map(|i| format!("col_{}", i))
            .collect()
    };

    let mut obj = serde_json::Map::new();

    // Use OID + format conversion when ColumnInfo is available.
    let col_info = row.column_info.as_ref();

    for (i, col_name) in column_names.into_iter().enumerate() {
        let value = if let Some(bytes) = row.columns.get(i).and_then(|v| v.as_deref()) {
            let oid = col_info
                .and_then(|info| info.oids.get(i).copied())
                .unwrap_or(0);
            let format = col_info
                .and_then(|info| info.formats.get(i).copied())
                .unwrap_or(0);
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
                    serde_json::from_str(&s).unwrap_or(serde_json::Value::String(s.into_owned()))
                } else if let Ok(n) = s.parse::<i64>() {
                    serde_json::Value::Number(n.into())
                } else if let Ok(f) = s.parse::<f64>() {
                    serde_json::Number::from_f64(f)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::String(s.into_owned()))
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
        pg_oid::BOOL => bool::from_pg(bytes, oid, format)
            .map(serde_json::Value::Bool)
            .unwrap_or(serde_json::Value::Null),
        pg_oid::INT2 | pg_oid::INT4 => i32::from_pg(bytes, oid, format)
            .map(|n| serde_json::Value::Number(n.into()))
            .unwrap_or(serde_json::Value::Null),
        pg_oid::INT8 | pg_oid::OID => i64::from_pg(bytes, oid, format)
            .map(|n| serde_json::Value::Number(n.into()))
            .unwrap_or(serde_json::Value::Null),
        pg_oid::FLOAT4 | pg_oid::FLOAT8 => f64::from_pg(bytes, oid, format)
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        pg_oid::NUMERIC => Numeric::from_pg(bytes, oid, format)
            .map(|n| {
                if let Ok(i) = n.to_i64() {
                    serde_json::Value::Number(i.into())
                } else if let Ok(f) = n.to_f64() {
                    serde_json::Number::from_f64(f)
                        .map(serde_json::Value::Number)
                        .unwrap_or_else(|| serde_json::Value::String(n.0))
                } else {
                    serde_json::Value::String(n.0)
                }
            })
            .unwrap_or_else(|_| serde_json::Value::String(format!("\\x{}", hex_encode(bytes)))),
        pg_oid::JSON | pg_oid::JSONB => Json::from_pg(bytes, oid, format)
            .map(|j| serde_json::from_str(&j.0).unwrap_or(serde_json::Value::String(j.0)))
            .unwrap_or_else(|_| serde_json::Value::String(format!("\\x{}", hex_encode(bytes)))),
        pg_oid::UUID => Uuid::from_pg(bytes, oid, format)
            .map(|u| serde_json::Value::String(u.0))
            .unwrap_or_else(|_| serde_json::Value::String(format!("\\x{}", hex_encode(bytes)))),
        pg_oid::INET => Inet::from_pg(bytes, oid, format)
            .map(|v| serde_json::Value::String(v.0))
            .unwrap_or_else(|_| serde_json::Value::String(format!("\\x{}", hex_encode(bytes)))),
        pg_oid::CIDR => Cidr::from_pg(bytes, oid, format)
            .map(|v| serde_json::Value::String(v.0))
            .unwrap_or_else(|_| serde_json::Value::String(format!("\\x{}", hex_encode(bytes)))),
        pg_oid::MACADDR => MacAddr::from_pg(bytes, oid, format)
            .map(|v| serde_json::Value::String(v.0))
            .unwrap_or_else(|_| serde_json::Value::String(format!("\\x{}", hex_encode(bytes)))),
        pg_oid::TIMESTAMP | pg_oid::TIMESTAMPTZ => Timestamp::from_pg(bytes, oid, format)
            .map(|ts| serde_json::Value::Number(ts.to_unix_secs().into()))
            .unwrap_or_else(|_| serde_json::Value::String(format!("\\x{}", hex_encode(bytes)))),
        pg_oid::DATE => Date::from_pg(bytes, oid, format)
            .map(|d| serde_json::Value::Number(d.days.into()))
            .unwrap_or_else(|_| serde_json::Value::String(format!("\\x{}", hex_encode(bytes)))),
        pg_oid::TIME => Time::from_pg(bytes, oid, format)
            .map(|t| serde_json::Value::Number(t.usec.into()))
            .unwrap_or_else(|_| serde_json::Value::String(format!("\\x{}", hex_encode(bytes)))),
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
        16 => serde_json::Value::Bool(s == "t" || s == "true"),

        // ── Integer types (OID 20=int8, 21=int2, 23=int4, 26=oid) ──
        20 | 21 | 23 | 26 => s
            .parse::<i64>()
            .map(|n| serde_json::Value::Number(n.into()))
            .unwrap_or(serde_json::Value::String(s.to_string())),

        // ── Float types (OID 700=float4, 701=float8) ──────────────
        700 | 701 => s
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::String(s.to_string())),

        // ── Numeric/Decimal (OID 1700) — preserve precision as string or number ──
        1700 => {
            if let Ok(n) = s.parse::<i64>() {
                serde_json::Value::Number(n.into())
            } else if let Ok(f) = s.parse::<f64>() {
                serde_json::Number::from_f64(f)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::String(s.to_string()))
            } else {
                serde_json::Value::String(s.to_string())
            }
        }

        // ── JSON/JSONB (OID 114, 3802) — parse directly ──────────
        114 | 3802 => serde_json::from_str(s).unwrap_or(serde_json::Value::String(s.to_string())),

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
        serde_json::from_str(s).unwrap_or(serde_json::Value::String(s.to_string()))
    } else if let Ok(n) = s.parse::<i64>() {
        serde_json::Value::Number(n.into())
    } else if let Ok(f) = s.parse::<f64>() {
        serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::String(s.to_string()))
    } else if s == "t" || s == "true" {
        serde_json::Value::Bool(true)
    } else if s == "f" || s == "false" {
        serde_json::Value::Bool(false)
    } else {
        serde_json::Value::String(s.to_string())
    }
}

/// Convert PostgreSQL text-format array (e.g., `{1,2,3}`) to JSON array.
fn pg_array_to_json(s: &str) -> serde_json::Value {
    if s.starts_with('{') && s.ends_with('}') {
        let inner = &s[1..s.len() - 1];
        if inner.is_empty() {
            return serde_json::Value::Array(vec![]);
        }
        let elements: Vec<serde_json::Value> = inner
            .split(',')
            .map(|elem| {
                let elem = elem.trim();
                if elem.eq_ignore_ascii_case("null") {
                    serde_json::Value::Null
                } else if let Ok(n) = elem.parse::<i64>() {
                    serde_json::Value::Number(n.into())
                } else if let Ok(f) = elem.parse::<f64>() {
                    serde_json::Number::from_f64(f)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::String(elem.to_string()))
                } else {
                    // Strip surrounding quotes if present
                    let unquoted = elem.trim_matches('"');
                    serde_json::Value::String(unquoted.to_string())
                }
            })
            .collect();
        serde_json::Value::Array(elements)
    } else {
        serde_json::Value::String(s.to_string())
    }
}

#[cfg(test)]
mod tests;
