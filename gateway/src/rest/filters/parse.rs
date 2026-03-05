use qail_core::ast::{Operator, Value as QailValue};
use uuid::Uuid;

/// SECURITY: Validate that a user-provided identifier (column name, sort key, select field)
/// contains only safe characters. Identifiers are written into SQL in non-parameterized
/// positions (SELECT list, WHERE left-side, ORDER BY), so they MUST NOT contain SQL syntax.
///
/// Allowed: dotted identifiers where each segment matches
/// `[A-Za-z_][A-Za-z0-9_]*` (e.g. `users`, `public.users`).
/// Rejected: quotes, semicolons, comments, parens, spaces, operators, etc.
pub(crate) fn is_safe_identifier(s: &str) -> bool {
    if s.is_empty() || s.len() > 128 || s.contains("--") {
        return false;
    }
    s.split('.').all(|segment| {
        if segment.is_empty() || segment.len() > 63 {
            return false;
        }
        let mut bytes = segment.bytes();
        match bytes.next() {
            Some(b) if b.is_ascii_alphabetic() || b == b'_' => {}
            _ => return false,
        }
        bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
    })
}

/// Parse filter operators from query string.
///
/// Supports both forms:
/// - Key-style: `?name.eq=John`, `?price.gte=100`, `?status.in=active,pending`
/// - Value-style: `?price=gte.100`, `?status=in.(active,pending)`, `?notes=is_null`
///
/// If no operator is provided, defaults to `eq`.
pub(crate) fn parse_filters(query_string: &str) -> Vec<(String, Operator, QailValue)> {
    let reserved = [
        "limit",
        "offset",
        "sort",
        "select",
        "expand",
        "cursor",
        "distinct",
        "returning",
        "on_conflict",
        "on_conflict_action",
        "func",
        "column",
        "group_by",
        "search",
        "search_columns",
        "stream",
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

        if reserved.contains(&key) {
            continue;
        }

        let (column, mut op, key_has_operator) = if let Some((col, op_str)) = key.rsplit_once('.') {
            if let Some(operator) = parse_operator_token(op_str) {
                (col, operator, true)
            } else {
                (key, Operator::Eq, false)
            }
        } else {
            (key, Operator::Eq, false)
        };

        if reserved.contains(&column) {
            continue;
        }

        if !is_safe_identifier(column) {
            tracing::warn!("Filter column rejected by identifier guard: {:?}", column);
            continue;
        }

        let decoded_value = urlencoding::decode(value)
            .unwrap_or(std::borrow::Cow::Borrowed(value))
            .to_string();

        let qail_value = if !key_has_operator {
            if let Some((value_op, value_val)) = parse_value_style_operator(&decoded_value) {
                op = value_op;
                value_val
            } else {
                parse_filter_value_for_op(op, &decoded_value)
            }
        } else {
            parse_filter_value_for_op(op, &decoded_value)
        };

        filters.push((column.to_string(), op, qail_value));
    }

    filters
}

/// Parse a scalar value, attempting type detection (bool → int → float → uuid → string).
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
    if let Ok(uuid) = Uuid::parse_str(s) {
        return QailValue::Uuid(uuid);
    }
    QailValue::String(s.to_string())
}

/// Parse an operator token used by key/value style filters.
fn parse_operator_token(op_str: &str) -> Option<Operator> {
    match op_str {
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
    }
}

/// Parse a filter value according to the resolved operator.
fn parse_filter_value_for_op(op: Operator, decoded_value: &str) -> QailValue {
    match op {
        Operator::IsNull | Operator::IsNotNull => QailValue::Null,
        Operator::In | Operator::NotIn => QailValue::Array(parse_csv_values(decoded_value)),
        Operator::Like | Operator::Fuzzy | Operator::NotLike => {
            QailValue::String(normalize_like_pattern(decoded_value))
        }
        _ => parse_scalar_value(decoded_value),
    }
}

/// Parse value-style operator syntax (`op.value`) such as `gt.100`, `in.(a,b)`.
fn parse_value_style_operator(decoded_value: &str) -> Option<(Operator, QailValue)> {
    let value = decoded_value.trim();

    if value == "is_null" {
        return Some((Operator::IsNull, QailValue::Null));
    }
    if value == "is_not_null" {
        return Some((Operator::IsNotNull, QailValue::Null));
    }

    let (op_token, raw_val) = value.split_once('.')?;
    let op = parse_operator_token(op_token)?;
    Some((op, parse_filter_value_for_op(op, raw_val)))
}

/// Parse comma-separated list values, accepting optional parenthesized form.
fn parse_csv_values(raw: &str) -> Vec<QailValue> {
    let trimmed = raw.trim();
    let inner = trimmed
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .unwrap_or(trimmed);

    if inner.is_empty() {
        return Vec::new();
    }

    inner
        .split(',')
        .map(|v| parse_scalar_value(v.trim()))
        .collect()
}

/// Normalize wildcard syntax for LIKE/ILIKE.
/// Accepts `*` from URL-style patterns and maps to SQL `%`.
fn normalize_like_pattern(s: &str) -> String {
    s.replace('*', "%")
}
