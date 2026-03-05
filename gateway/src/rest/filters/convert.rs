use qail_core::ast::Value as QailValue;
use serde_json::Value;

/// Convert a serde_json::Value to a qail_core::ast::Value.
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
        Value::Array(arr) => QailValue::Array(arr.iter().map(json_to_qail_value).collect()),
        other => QailValue::String(other.to_string()),
    }
}
