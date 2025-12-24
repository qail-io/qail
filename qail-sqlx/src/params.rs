//! Named parameters support for QAIL-SQLx.
//!
//! Provides `QailParams` and `qail_params!` macro for ergonomic parameter binding.

use qail_core::ast::Value;
use std::collections::HashMap;

/// Named parameters for QAIL queries.
#[derive(Debug, Clone, Default)]
pub struct QailParams {
    params: HashMap<String, Value>,
}

impl QailParams {
    /// Create empty params.
    pub fn new() -> Self {
        Self { params: HashMap::new() }
    }

    /// Add a parameter.
    pub fn insert(&mut self, name: impl Into<String>, value: impl Into<Value>) {
        self.params.insert(name.into(), value.into());
    }

    /// Get a parameter by name.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.params.get(name)
    }

    /// Get ordered values for binding based on param order.
    /// Returns values in the order they appear in `param_order`.
    pub fn bind_values(&self, param_order: &[String]) -> Vec<Value> {
        param_order
            .iter()
            .map(|name| self.params.get(name).cloned().unwrap_or(Value::Null))
            .collect()
    }
}

/// Convenience macro for creating QailParams.
///
/// # Example
/// ```ignore
/// let params = qail_params! {
///     id: user_id,
///     status: "active",
///     count: 10
/// };
/// ```
#[macro_export]
macro_rules! qail_params {
    ($($name:ident : $value:expr),* $(,)?) => {{
        let mut params = $crate::params::QailParams::new();
        $(
            params.insert(stringify!($name), $value);
        )*
        params
    }};
}

// Re-export chrono types for convenience
pub use chrono::{DateTime, Utc};

impl QailParams {
    /// Insert a DateTime<Utc> value, converting to Timestamp.
    pub fn insert_datetime(&mut self, name: impl Into<String>, dt: DateTime<Utc>) {
        self.params.insert(name.into(), Value::Timestamp(dt.to_rfc3339()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_params_creation() {
        let mut params = QailParams::new();
        params.insert("id", 42i64);
        params.insert("name", "test");
        
        assert_eq!(params.get("id"), Some(&Value::Int(42)));
        assert_eq!(params.get("name"), Some(&Value::String("test".to_string())));
    }

    #[test]
    fn test_bind_values_ordering() {
        let mut params = QailParams::new();
        params.insert("b", 2i64);
        params.insert("a", 1i64);
        params.insert("c", 3i64);
        
        let order = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let values = params.bind_values(&order);
        
        assert_eq!(values, vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
    }
}
