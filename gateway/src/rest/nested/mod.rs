//! Nested route handler and FK expansion.
//!
//! - `nested_list_handler` — GET /api/{parent}/:id/{child}
//! - `expand_nested` — Resolve `?expand=nested:rel` into nested JSON

use qail_core::ast::Value as QailValue;
use serde_json::Value;

mod expand;
mod list;

pub use expand::expand_nested;
pub(crate) use list::nested_list_handler;

fn json_to_qail_value(v: Value) -> QailValue {
    match v {
        Value::String(s) => QailValue::String(s),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                QailValue::Int(i)
            } else {
                QailValue::String(n.to_string())
            }
        }
        other => QailValue::String(other.to_string()),
    }
}

fn json_value_key(v: &Value) -> String {
    v.as_str().unwrap_or(&v.to_string()).to_string()
}
