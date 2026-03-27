use serde_json::Value;

pub(crate) fn normalize_pg_type_name(input: &str) -> String {
    input.trim().replace('"', "").to_ascii_lowercase()
}

pub(crate) fn minimum_required_rpc_args(
    total_args: usize,
    default_args: usize,
    variadic: bool,
) -> usize {
    let required = total_args.saturating_sub(default_args);
    if variadic && total_args > 0 {
        required.min(total_args.saturating_sub(1))
    } else {
        required
    }
}

fn is_pg_any_type(type_name: &str) -> bool {
    matches!(
        type_name,
        "any"
            | "anyelement"
            | "anyarray"
            | "anynonarray"
            | "anyenum"
            | "anyrange"
            | "anycompatible"
            | "anycompatiblearray"
            | "anycompatiblenonarray"
            | "anycompatiblerange"
            | "record"
            | "unknown"
    )
}

fn is_pg_bool_type(type_name: &str) -> bool {
    matches!(type_name, "bool" | "boolean")
}

fn is_pg_integer_type(type_name: &str) -> bool {
    matches!(
        type_name,
        "int2"
            | "smallint"
            | "int4"
            | "integer"
            | "int8"
            | "bigint"
            | "serial"
            | "bigserial"
            | "oid"
    )
}

fn is_pg_numeric_type(type_name: &str) -> bool {
    is_pg_integer_type(type_name)
        || matches!(
            type_name,
            "numeric" | "decimal" | "real" | "float4" | "double precision" | "float8" | "money"
        )
}

fn is_pg_json_type(type_name: &str) -> bool {
    matches!(type_name, "json" | "jsonb")
}

fn is_pg_string_like_type(type_name: &str) -> bool {
    matches!(
        type_name,
        "text"
            | "varchar"
            | "character varying"
            | "char"
            | "character"
            | "bpchar"
            | "uuid"
            | "name"
            | "citext"
            | "date"
            | "time"
            | "timetz"
            | "time with time zone"
            | "time without time zone"
            | "timestamp"
            | "timestamptz"
            | "timestamp with time zone"
            | "timestamp without time zone"
            | "interval"
            | "inet"
            | "cidr"
            | "macaddr"
            | "bytea"
    )
}

pub(super) fn is_json_value_compatible_with_pg_type(value: &Value, type_name: &str) -> bool {
    let normalized = normalize_pg_type_name(type_name);

    if value.is_null() || is_pg_any_type(&normalized) {
        return true;
    }

    if let Some(element_type) = normalized.strip_suffix("[]") {
        return match value {
            Value::Array(items) => items
                .iter()
                .all(|item| is_json_value_compatible_with_pg_type(item, element_type)),
            _ => false,
        };
    }

    match value {
        Value::Bool(_) => is_pg_bool_type(&normalized) || is_pg_json_type(&normalized),
        Value::Number(num) => {
            if num.is_i64() || num.is_u64() {
                is_pg_numeric_type(&normalized) || is_pg_json_type(&normalized)
            } else {
                matches!(
                    normalized.as_str(),
                    "numeric" | "decimal" | "real" | "float4" | "double precision" | "float8"
                ) || is_pg_json_type(&normalized)
            }
        }
        Value::String(_) => is_pg_string_like_type(&normalized) || is_pg_json_type(&normalized),
        Value::Array(_) | Value::Object(_) => is_pg_json_type(&normalized),
        Value::Null => true,
    }
}

pub(super) fn variadic_element_type(type_name: &str) -> &str {
    type_name.strip_suffix("[]").unwrap_or(type_name)
}
