use qail_core::ast::{Operator, Value as QailValue};
use uuid::Uuid;

const MAX_FILTER_LIST_VALUES: usize = 256;
const MAX_FILTER_CLAUSES: usize = 128;
const MAX_IDENTIFIER_CSV_ITEMS: usize = 128;

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

/// Parse a comma-separated identifier list.
///
/// Returns an error when any entry is empty or unsafe.
pub(crate) fn parse_identifier_csv(input: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for raw in input.split(',') {
        let ident = raw.trim();
        if ident.is_empty() {
            return Err("Identifier list contains an empty entry".to_string());
        }
        if !is_safe_identifier(ident) {
            return Err(format!("Invalid identifier '{}'", ident));
        }
        if seen.insert(ident.to_string()) {
            if out.len() >= MAX_IDENTIFIER_CSV_ITEMS {
                return Err(format!(
                    "Identifier list contains more than {} entries",
                    MAX_IDENTIFIER_CSV_ITEMS
                ));
            }
            out.push(ident.to_string());
        }
    }

    if out.is_empty() {
        return Err("Identifier list cannot be empty".to_string());
    }

    Ok(out)
}

/// Parse the REST `select` parameter.
///
/// `*` is allowed only as the whole projection. Mixed projections such as
/// `*,id` are rejected because they make projection and tenant-guard behavior
/// ambiguous.
pub(crate) fn parse_select_columns(input: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for raw in input.split(',') {
        let ident = raw.trim();
        if ident.is_empty() {
            return Err("Select list contains an empty entry".to_string());
        }
        if ident == "*" {
            if input.split(',').count() != 1 {
                return Err("Wildcard select cannot be mixed with named columns".to_string());
            }
            return Ok(vec!["*".to_string()]);
        }
        if !is_safe_identifier(ident) {
            return Err(format!("Invalid select column '{}'", ident));
        }
        if seen.insert(ident.to_string()) {
            if out.len() >= MAX_IDENTIFIER_CSV_ITEMS {
                return Err(format!(
                    "Select list contains more than {} columns",
                    MAX_IDENTIFIER_CSV_ITEMS
                ));
            }
            out.push(ident.to_string());
        }
    }

    if out.is_empty() {
        return Err("Select list cannot be empty".to_string());
    }

    Ok(out)
}

/// Parse the REST `expand` parameter into flat and nested relation lists.
///
/// `nested:relation` is returned separately so callers that cannot represent
/// nested expansion can reject it explicitly instead of silently dropping it.
pub(crate) fn parse_expand_relations(
    expand: &str,
    max_expand_depth: usize,
) -> Result<(Vec<&str>, Vec<&str>), String> {
    let mut seen_flat = std::collections::HashSet::new();
    let mut seen_nested = std::collections::HashSet::new();
    let mut flat = Vec::new();
    let mut nested = Vec::new();

    for raw_relation in expand.split(',') {
        let relation = raw_relation.trim();
        if relation.is_empty() {
            return Err("Expand contains an empty relation".to_string());
        }

        if let Some(nested_relation) = relation.strip_prefix("nested:") {
            let nested_relation = nested_relation.trim();
            if nested_relation.is_empty() {
                return Err("Nested expand relation cannot be empty".to_string());
            }
            if !is_safe_identifier(nested_relation) {
                return Err(format!(
                    "Invalid nested expand relation '{}'",
                    nested_relation
                ));
            }
            if seen_nested.insert(nested_relation) {
                nested.push(nested_relation);
            }
        } else {
            if !is_safe_identifier(relation) {
                return Err(format!("Invalid expand relation '{}'", relation));
            }
            if seen_flat.insert(relation) {
                flat.push(relation);
            }
        }
    }

    let total = flat.len() + nested.len();
    if total > max_expand_depth {
        return Err(format!(
            "Too many expand relations ({}). Maximum is {}",
            total, max_expand_depth
        ));
    }

    Ok((flat, nested))
}

/// Parse filter operators from query string.
///
/// Supports both forms:
/// - Key-style: `?name.eq=John`, `?price.gte=100`, `?status.in=active,pending`
/// - Value-style: `?price=gte.100`, `?status=in.(active,pending)`, `?notes=is_null`
///
/// If no operator is provided, defaults to `eq`.
#[cfg(test)]
pub(crate) fn parse_filters(query_string: &str) -> Vec<(String, Operator, QailValue)> {
    parse_filters_impl(query_string, false).unwrap_or_default()
}

/// Parse filter operators and reject unsafe user-provided filter columns.
pub(crate) fn parse_filters_checked(
    query_string: &str,
) -> Result<Vec<(String, Operator, QailValue)>, String> {
    parse_filters_impl(query_string, true)
}

fn parse_filters_impl(
    query_string: &str,
    fail_on_invalid_identifier: bool,
) -> Result<Vec<(String, Operator, QailValue)>, String> {
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
            None if fail_on_invalid_identifier => {
                return Err(format!("Malformed filter parameter '{}'", pair));
            }
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
            if fail_on_invalid_identifier {
                return Err(format!("Invalid filter column '{}'", column));
            }
            continue;
        }

        let decoded_value = match urlencoding::decode(value) {
            Ok(decoded) => decoded.to_string(),
            Err(err) if fail_on_invalid_identifier => {
                return Err(format!("Invalid percent-encoded filter value: {}", err));
            }
            Err(_) => value.to_string(),
        };

        let qail_value = if !key_has_operator {
            if let Some((value_op, value_val)) = parse_value_style_operator(&decoded_value)? {
                op = value_op;
                value_val
            } else {
                parse_filter_value_for_op(op, &decoded_value)?
            }
        } else {
            parse_filter_value_for_op(op, &decoded_value)?
        };

        if matches!(op, Operator::In | Operator::NotIn)
            && matches!(&qail_value, QailValue::Array(vals) if vals.is_empty())
        {
            return Err(format!(
                "Filter '{}.{}' requires at least one value",
                column,
                if matches!(op, Operator::In) {
                    "in"
                } else {
                    "not_in"
                }
            ));
        }
        if qail_value_contains_non_finite_number(&qail_value) {
            if fail_on_invalid_identifier {
                return Err(format!(
                    "Filter '{}' contains a non-finite numeric value",
                    column
                ));
            }
            continue;
        }

        if filters.len() >= MAX_FILTER_CLAUSES {
            return Err(format!(
                "Filter query contains more than {} clauses",
                MAX_FILTER_CLAUSES
            ));
        }
        filters.push((column.to_string(), op, qail_value));
    }

    Ok(filters)
}

fn qail_value_contains_non_finite_number(value: &QailValue) -> bool {
    match value {
        QailValue::Float(value) => !value.is_finite(),
        QailValue::Array(items) => items.iter().any(qail_value_contains_non_finite_number),
        _ => false,
    }
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
    if is_integer_literal(s) {
        return QailValue::String(s.to_string());
    }
    if let Ok(f) = s.parse::<f64>() {
        return QailValue::Float(f);
    }
    if let Ok(uuid) = Uuid::parse_str(s) {
        return QailValue::Uuid(uuid);
    }
    QailValue::String(s.to_string())
}

fn is_integer_literal(s: &str) -> bool {
    let digits = s
        .strip_prefix('+')
        .or_else(|| s.strip_prefix('-'))
        .unwrap_or(s);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}

/// Parse a cursor value and reject non-finite numeric sentinels such as NaN/inf.
pub(crate) fn parse_cursor_value(cursor: &str) -> Result<QailValue, String> {
    let value = parse_scalar_value(cursor);
    if qail_value_contains_non_finite_number(&value) {
        return Err("cursor contains a non-finite numeric value".to_string());
    }
    Ok(value)
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
        "ilike" => Some(Operator::ILike),
        "not_ilike" | "nilike" => Some(Operator::NotILike),
        "fuzzy" => Some(Operator::Fuzzy),
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
fn parse_filter_value_for_op(op: Operator, decoded_value: &str) -> Result<QailValue, String> {
    let value = match op {
        Operator::IsNull | Operator::IsNotNull => QailValue::Null,
        Operator::In | Operator::NotIn => QailValue::Array(parse_csv_values(decoded_value)?),
        Operator::Like
        | Operator::ILike
        | Operator::NotLike
        | Operator::NotILike
        | Operator::Fuzzy => QailValue::String(normalize_like_pattern(decoded_value)),
        _ => parse_scalar_value(decoded_value),
    };
    Ok(value)
}

/// Parse value-style operator syntax (`op.value`) such as `gt.100`, `in.(a,b)`.
fn parse_value_style_operator(
    decoded_value: &str,
) -> Result<Option<(Operator, QailValue)>, String> {
    let value = decoded_value.trim();

    if value == "is_null" {
        return Ok(Some((Operator::IsNull, QailValue::Null)));
    }
    if value == "is_not_null" {
        return Ok(Some((Operator::IsNotNull, QailValue::Null)));
    }

    let Some((op_token, raw_val)) = value.split_once('.') else {
        return Ok(None);
    };
    let Some(op) = parse_operator_token(op_token) else {
        return Ok(None);
    };
    Ok(Some((op, parse_filter_value_for_op(op, raw_val)?)))
}

/// Parse comma-separated list values, accepting optional parenthesized form.
fn parse_csv_values(raw: &str) -> Result<Vec<QailValue>, String> {
    let trimmed = raw.trim();
    let inner = trimmed
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .unwrap_or(trimmed);

    if inner.is_empty() {
        return Ok(Vec::new());
    }

    let mut values = Vec::new();
    for raw_value in inner.split(',') {
        if values.len() >= MAX_FILTER_LIST_VALUES {
            return Err(format!(
                "Filter list contains more than {} values",
                MAX_FILTER_LIST_VALUES
            ));
        }
        values.push(parse_scalar_value(raw_value.trim()));
    }
    Ok(values)
}

/// Normalize wildcard syntax for LIKE/ILIKE.
/// Accepts `*` from URL-style patterns and maps to SQL `%`.
fn normalize_like_pattern(s: &str) -> String {
    s.replace('*', "%")
}
