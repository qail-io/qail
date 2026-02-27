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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ── text_to_json_typed ──────────────────────────────────────────

    #[test]
    fn typed_bool_oid_16() {
        assert_eq!(text_to_json_typed("t", 16), serde_json::Value::Bool(true));
        assert_eq!(
            text_to_json_typed("true", 16),
            serde_json::Value::Bool(true)
        );
        assert_eq!(text_to_json_typed("f", 16), serde_json::Value::Bool(false));
        assert_eq!(
            text_to_json_typed("false", 16),
            serde_json::Value::Bool(false)
        );
    }

    #[test]
    fn typed_int_oid_23() {
        assert_eq!(text_to_json_typed("42", 23), serde_json::json!(42));
        assert_eq!(text_to_json_typed("-1", 23), serde_json::json!(-1));
        assert_eq!(text_to_json_typed("0", 23), serde_json::json!(0));
        // Non-numeric → string fallback
        assert_eq!(
            text_to_json_typed("not_a_number", 23),
            serde_json::json!("not_a_number")
        );
    }

    #[test]
    fn typed_bigint_oid_20() {
        assert_eq!(
            text_to_json_typed("9223372036854775807", 20),
            serde_json::json!(9223372036854775807_i64)
        );
    }

    #[test]
    fn typed_float_oid_701() {
        assert_eq!(text_to_json_typed("2.72", 701), serde_json::json!(2.72));
        assert_eq!(text_to_json_typed("0.0", 701), serde_json::json!(0.0));
        // NaN → string fallback (from_f64 returns None for NaN)
        assert_eq!(text_to_json_typed("NaN", 701), serde_json::json!("NaN"));
    }

    #[test]
    fn typed_numeric_oid_1700() {
        assert_eq!(text_to_json_typed("100", 1700), serde_json::json!(100));
        assert_eq!(text_to_json_typed("99.95", 1700), serde_json::json!(99.95));
        assert_eq!(
            text_to_json_typed("1e999", 1700),
            serde_json::json!("1e999")
        );
    }

    #[test]
    fn typed_json_oid_114() {
        assert_eq!(
            text_to_json_typed(r#"{"key":"val"}"#, 114),
            serde_json::json!({"key": "val"})
        );
        // Invalid JSON → string fallback
        assert_eq!(
            text_to_json_typed("not json", 114),
            serde_json::json!("not json")
        );
    }

    #[test]
    fn typed_jsonb_oid_3802() {
        assert_eq!(
            text_to_json_typed("[1,2,3]", 3802),
            serde_json::json!([1, 2, 3])
        );
    }

    #[test]
    fn typed_text_oids_return_string() {
        for oid in [25_u32, 1042, 1043, 2950, 1082, 1114, 1184] {
            assert_eq!(
                text_to_json_typed("hello", oid),
                serde_json::json!("hello"),
                "OID {} should return string",
                oid
            );
        }
    }

    #[test]
    fn typed_array_oid_1007() {
        assert_eq!(
            text_to_json_typed("{1,2,3}", 1007),
            serde_json::json!([1, 2, 3])
        );
    }

    #[test]
    fn typed_unknown_oid_falls_back_to_guess() {
        // OID 0 = no metadata → uses guessing
        assert_eq!(text_to_json_typed("42", 0), serde_json::json!(42));
        assert_eq!(text_to_json_typed("hello", 0), serde_json::json!("hello"));
    }

    // ── text_to_json_guess ──────────────────────────────────────────

    #[test]
    fn guess_integer() {
        assert_eq!(text_to_json_guess("42"), serde_json::json!(42));
    }

    #[test]
    fn guess_float() {
        assert_eq!(text_to_json_guess("2.72"), serde_json::json!(2.72));
    }

    #[test]
    fn guess_bool() {
        assert_eq!(text_to_json_guess("true"), serde_json::json!(true));
        assert_eq!(text_to_json_guess("t"), serde_json::json!(true));
        assert_eq!(text_to_json_guess("false"), serde_json::json!(false));
        assert_eq!(text_to_json_guess("f"), serde_json::json!(false));
    }

    #[test]
    fn guess_json_object() {
        assert_eq!(
            text_to_json_guess(r#"{"a":1}"#),
            serde_json::json!({"a": 1})
        );
    }

    #[test]
    fn guess_json_array() {
        assert_eq!(text_to_json_guess("[1,2]"), serde_json::json!([1, 2]));
    }

    #[test]
    fn guess_string_fallback() {
        assert_eq!(
            text_to_json_guess("hello world"),
            serde_json::json!("hello world")
        );
    }

    // ── pg_array_to_json ────────────────────────────────────────────

    #[test]
    fn pg_array_empty() {
        assert_eq!(pg_array_to_json("{}"), serde_json::json!([]));
    }

    #[test]
    fn pg_array_ints() {
        assert_eq!(pg_array_to_json("{1,2,3}"), serde_json::json!([1, 2, 3]));
    }

    #[test]
    fn pg_array_with_null() {
        assert_eq!(
            pg_array_to_json("{1,NULL,3}"),
            serde_json::json!([1, serde_json::Value::Null, 3])
        );
    }

    #[test]
    fn pg_array_quoted_strings() {
        assert_eq!(
            pg_array_to_json(r#"{"hello","world"}"#),
            serde_json::json!(["hello", "world"])
        );
    }

    #[test]
    fn pg_array_non_array_passthrough() {
        assert_eq!(
            pg_array_to_json("not an array"),
            serde_json::json!("not an array")
        );
    }

    #[test]
    fn pg_array_floats() {
        assert_eq!(pg_array_to_json("{1.5,2.7}"), serde_json::json!([1.5, 2.7]));
    }
}
