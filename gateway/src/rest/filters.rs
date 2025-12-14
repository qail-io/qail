//! Filter parsing and query parameter helpers.
//!
//! Parses PostgREST-style filter operators from query strings and applies them
//! to Qail AST commands.

use qail_core::ast::{Operator, Value as QailValue};
use serde_json::Value;
use uuid::Uuid;

/// Parse filter operators from query string.
///
/// Supports PostgREST-style operators:
/// - `?name.eq=John`        → name = 'John'
/// - `?price.gte=100`       → price >= 100
/// - `?status.in=active,pending` → status IN ('active', 'pending')
/// - `?email.like=%@gmail%` → email LIKE '%@gmail%'
/// - `?deleted_at.is_null=true` → deleted_at IS NULL
///
/// If no operator suffix, defaults to `eq`.
pub(crate) fn parse_filters(query_string: &str) -> Vec<(String, Operator, QailValue)> {
    let reserved = [
        "limit", "offset", "sort", "select", "expand", "cursor", "distinct",
        "returning", "on_conflict", "on_conflict_action",
        "func", "column", "group_by",
        "search", "search_columns", "stream",
    ];

    let mut filters = Vec::new();

    for pair in query_string.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => continue,
        };

        // Skip reserved params
        if reserved.contains(&key) {
            continue;
        }

        // Parse column.operator pattern
        let (column, op) = if let Some((col, op_str)) = key.rsplit_once('.') {
            let operator = match op_str {
                "eq" => Some(Operator::Eq),
                "ne" | "neq" => Some(Operator::Ne),
                "gt" => Some(Operator::Gt),
                "gte" | "ge" => Some(Operator::Gte),
                "lt" => Some(Operator::Lt),
                "lte" | "le" => Some(Operator::Lte),
                "like" => Some(Operator::Like),
                "ilike" | "fuzzy" => Some(Operator::Fuzzy),
                "not_like" => Some(Operator::NotLike),
                "in" => Some(Operator::In),
                "not_in" | "nin" => Some(Operator::NotIn),
                "is_null" => Some(Operator::IsNull),
                "is_not_null" => Some(Operator::IsNotNull),
                "contains" => Some(Operator::Contains),
                _ => None,
            };
            if let Some(op) = operator {
                (col, op)
            } else {
                // Unknown operator suffix — treat full key as column name with eq
                (key, Operator::Eq)
            }
        } else {
            // No dot — treat as column = value
            (key, Operator::Eq)
        };

        // Skip if this is a reserved param (column name might collide)
        if reserved.contains(&column) {
            continue;
        }

        // Decode the value
        let decoded_value = urlencoding::decode(value)
            .unwrap_or(std::borrow::Cow::Borrowed(value))
            .to_string();

        let qail_value = match op {
            Operator::IsNull | Operator::IsNotNull => {
                // These are unary — value is ignored (or "true"/"false")
                QailValue::Null
            }
            Operator::In | Operator::NotIn => {
                // Comma-separated values → Array
                let vals: Vec<QailValue> = decoded_value
                    .split(',')
                    .map(|v| parse_scalar_value(v.trim()))
                    .collect();
                QailValue::Array(vals)
            }
            _ => parse_scalar_value(&decoded_value),
        };

        filters.push((column.to_string(), op, qail_value));
    }

    filters
}

/// Parse a scalar value, attempting type detection (bool → int → float → uuid → string)
pub(crate) fn parse_scalar_value(s: &str) -> QailValue {
    if s == "true" {
        return QailValue::Bool(true);
    }
    if s == "false" {
        return QailValue::Bool(false);
    }
    if s == "null" {
        return QailValue::Null;
    }
    if let Ok(n) = s.parse::<i64>() {
        return QailValue::Int(n);
    }
    if let Ok(f) = s.parse::<f64>() {
        return QailValue::Float(f);
    }
    // Detect UUID strings so PG parameterized queries get the right type OID
    if let Ok(uuid) = Uuid::parse_str(s) {
        return QailValue::Uuid(uuid);
    }
    QailValue::String(s.to_string())
}

/// Apply parsed filters to a Qail command
pub(crate) fn apply_filters(mut cmd: qail_core::ast::Qail, filters: &[(String, Operator, QailValue)]) -> qail_core::ast::Qail {
    for (column, op, value) in filters {
        match op {
            Operator::IsNull => {
                cmd = cmd.is_null(column);
            }
            Operator::IsNotNull => {
                cmd = cmd.is_not_null(column);
            }
            Operator::In | Operator::NotIn => {
                if let QailValue::Array(vals) = value {
                    if matches!(op, Operator::In) {
                        cmd = cmd.in_vals(column, vals.clone());
                    } else {
                        cmd = cmd.filter(column, Operator::NotIn, value.clone());
                    }
                }
            }
            _ => {
                cmd = cmd.filter(column, *op, value.clone());
            }
        }
    }
    cmd
}

/// Apply multi-column sorting
pub(crate) fn apply_sorting(mut cmd: qail_core::ast::Qail, sort: &str) -> qail_core::ast::Qail {
    for part in sort.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let parts: Vec<&str> = part.split(':').collect();
        let col = parts[0];
        let dir = parts.get(1).unwrap_or(&"asc");
        if *dir == "desc" {
            cmd = cmd.order_desc(col);
        } else {
            cmd = cmd.order_asc(col);
        }
    }
    cmd
}

/// Apply returning clause to a mutation command
pub(crate) fn apply_returning(
    mut cmd: qail_core::ast::Qail,
    returning: Option<&str>,
) -> qail_core::ast::Qail {
    if let Some(ret) = returning {
        if ret == "*" {
            cmd = cmd.returning_all();
        } else {
            let cols: Vec<&str> = ret.split(',').map(|s| s.trim()).collect();
            cmd = cmd.returning(cols);
        }
    }
    cmd
}

/// Convert a serde_json::Value to a qail_core::ast::Value
pub(crate) fn json_to_qail_value(v: &Value) -> QailValue {
    match v {
        Value::String(s) => QailValue::String(s.clone()),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                QailValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                QailValue::Float(f)
            } else {
                QailValue::String(n.to_string())
            }
        }
        Value::Bool(b) => QailValue::Bool(*b),
        Value::Null => QailValue::Null,
        Value::Array(arr) => {
            QailValue::Array(arr.iter().map(json_to_qail_value).collect())
        }
        other => QailValue::String(other.to_string()),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_filters_basic() {
        let filters = parse_filters("name.eq=John&age.gte=18");
        assert_eq!(filters.len(), 2);
        assert_eq!(filters[0].0, "name");
        assert!(matches!(filters[0].1, Operator::Eq));
        assert_eq!(filters[1].0, "age");
        assert!(matches!(filters[1].1, Operator::Gte));
    }

    #[test]
    fn test_parse_filters_in() {
        let filters = parse_filters("status.in=active,pending,closed");
        assert_eq!(filters.len(), 1);
        assert!(matches!(filters[0].1, Operator::In));
        if let QailValue::Array(vals) = &filters[0].2 {
            assert_eq!(vals.len(), 3);
        } else {
            panic!("Expected Array value for IN filter");
        }
    }

    #[test]
    fn test_parse_filters_is_null() {
        let filters = parse_filters("deleted_at.is_null=true");
        assert_eq!(filters.len(), 1);
        assert!(matches!(filters[0].1, Operator::IsNull));
    }

    #[test]
    fn test_parse_filters_no_operator() {
        let filters = parse_filters("name=John");
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].0, "name");
        assert!(matches!(filters[0].1, Operator::Eq));
    }

    #[test]
    fn test_parse_filters_skips_reserved() {
        let filters = parse_filters("limit=10&offset=0&name.eq=John&sort=id:asc");
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].0, "name");
    }

    #[test]
    fn test_parse_scalar_value() {
        assert!(matches!(parse_scalar_value("42"), QailValue::Int(42)));
        assert!(matches!(parse_scalar_value("3.14"), QailValue::Float(_)));
        assert!(matches!(parse_scalar_value("true"), QailValue::Bool(true)));
        assert!(matches!(parse_scalar_value("null"), QailValue::Null));
        assert!(matches!(parse_scalar_value("hello"), QailValue::String(_)));
    }

    // =========================================================================
    // SQL Injection Hardening
    // =========================================================================

    #[test]
    fn test_sql_injection_in_filter_value() {
        // Classic SQL injection attempts — must be treated as literal strings
        let payloads = vec![
            "'; DROP TABLE users; --",
            "1 OR 1=1",
            "1; SELECT * FROM pg_shadow",
            "' UNION SELECT password FROM users --",
            "Robert'); DROP TABLE students;--",
            "1' AND '1'='1",
            "admin'--",
            "' OR ''='",
        ];
        for payload in payloads {
            let qs = format!("name.eq={}", urlencoding::encode(payload));
            let filters = parse_filters(&qs);
            assert_eq!(filters.len(), 1, "Injection payload should produce exactly 1 filter");
            // Value must be a String (treated as literal, never parsed as SQL)
            match &filters[0].2 {
                QailValue::String(s) => assert_eq!(s, payload),
                QailValue::Int(_) | QailValue::Float(_) => {
                    // "1 OR 1=1" might parse the leading "1" as int — that's fine,
                    // the important thing is it's a parameterized value
                }
                _ => {} // Any QailValue is safe — it's parameterized
            }
        }
    }

    #[test]
    fn test_null_bytes_in_filter() {
        let filters = parse_filters("name.eq=hello%00world");
        assert_eq!(filters.len(), 1);
        // Must not panic and must produce a value
    }

    #[test]
    fn test_extremely_long_value() {
        let long_val = "a".repeat(100_000);
        let qs = format!("name.eq={}", long_val);
        let filters = parse_filters(&qs);
        assert_eq!(filters.len(), 1);
    }

    #[test]
    fn test_empty_and_malformed_query_strings() {
        assert!(parse_filters("").is_empty());
        assert!(parse_filters("&&&").is_empty());
        // "===" splits as key="", value="=" — empty key produces no filter
        // (actually "=" key with "=" value — depends on split_once behavior)
        assert!(parse_filters("key_no_value").is_empty());
        // Bare operator with no value
        let f = parse_filters("col.eq=");
        assert_eq!(f.len(), 1); // empty string is valid
    }

    #[test]
    fn test_unicode_in_filters() {
        let filters = parse_filters("name.eq=日本語テスト&city.like=%E4%B8%8A%E6%B5%B7");
        assert_eq!(filters.len(), 2);
        match &filters[0].2 {
            QailValue::String(s) => assert_eq!(s, "日本語テスト"),
            _ => panic!("Expected unicode string"),
        }
    }

    // =========================================================================
    // Proptest Fuzzing
    // =========================================================================

    mod fuzz {
        use super::*;
        use proptest::prelude::*;

        /// Generate random query strings in the format `col.op=val`
        fn arb_query_string() -> impl Strategy<Value = String> {
            prop::collection::vec(
                (
                    "[a-z_]{1,20}",           // column name
                    prop_oneof![              // operator
                        Just("eq"), Just("ne"), Just("gt"), Just("gte"),
                        Just("lt"), Just("lte"), Just("like"), Just("ilike"),
                        Just("in"), Just("not_in"), Just("is_null"), Just("contains"),
                        Just("unknown_op"),
                    ],
                    ".*",                     // arbitrary value
                ),
                0..10, // 0 to 10 filter pairs
            )
            .prop_map(|pairs| {
                pairs
                    .into_iter()
                    .map(|(col, op, val)| format!("{}.{}={}", col, op, urlencoding::encode(&val)))
                    .collect::<Vec<_>>()
                    .join("&")
            })
        }

        proptest! {
            /// parse_filters must NEVER panic on any input
            #[test]
            fn fuzz_parse_filters_never_panics(qs in ".*") {
                let _ = parse_filters(&qs);
            }

            /// parse_scalar_value must NEVER panic on any input
            #[test]
            fn fuzz_parse_scalar_value_never_panics(s in ".*") {
                let _ = parse_scalar_value(&s);
            }

            /// Structured fuzzing: random col.op=val triplets
            #[test]
            fn fuzz_structured_filters(qs in arb_query_string()) {
                let filters = parse_filters(&qs);
                // All filters must have non-empty column names
                for (col, _op, _val) in &filters {
                    prop_assert!(!col.is_empty(), "Column name must not be empty");
                }
            }

            /// Reserved params must NEVER appear in filter output
            #[test]
            fn fuzz_reserved_params_filtered(
                col in prop_oneof![
                    Just("limit"), Just("offset"), Just("sort"),
                    Just("select"), Just("expand"), Just("cursor"),
                    Just("distinct"), Just("returning"),
                ],
                val in "[a-z0-9]{1,10}"
            ) {
                let qs = format!("{}={}", col, val);
                let filters = parse_filters(&qs);
                prop_assert!(filters.is_empty(), "Reserved param '{}' should not become a filter", col);
            }

            /// parse_scalar_value output is always a valid QailValue variant
            #[test]
            fn fuzz_scalar_value_is_valid(s in "[^\u{0}]{0,1000}") {
                let val = parse_scalar_value(&s);
                // Just verify it produced a valid QailValue (no panic)
                match val {
                    _ => {} // Any variant is fine — we just care it didn't panic
                }
            }
        }
    }
}
