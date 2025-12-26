//! Type conversions between Python and QAIL AST.
//!
//! This module provides the `PyOperator` enum and `py_to_value` conversion.

use pyo3::prelude::*;
use pyo3::exceptions::PyTypeError;
use qail_core::ast::{Operator, Value};

/// Python-exposed Operator enum.
/// 
/// Uses static methods instead of enum variants for Pythonic API.
#[pyclass(name = "Operator")]
#[derive(Clone)]
pub struct PyOperator {
    pub inner: Operator,
}

#[pymethods]
impl PyOperator {
    // Comparison operators
    #[staticmethod]
    fn eq() -> Self { Self { inner: Operator::Eq } }
    
    #[staticmethod]
    fn ne() -> Self { Self { inner: Operator::Ne } }
    
    #[staticmethod]
    fn gt() -> Self { Self { inner: Operator::Gt } }
    
    #[staticmethod]
    fn gte() -> Self { Self { inner: Operator::Gte } }
    
    #[staticmethod]
    fn lt() -> Self { Self { inner: Operator::Lt } }
    
    #[staticmethod]
    fn lte() -> Self { Self { inner: Operator::Lte } }
    
    // Pattern matching
    #[staticmethod]
    fn like() -> Self { Self { inner: Operator::Like } }
    
    #[staticmethod]
    fn ilike() -> Self { Self { inner: Operator::ILike } }
    
    #[staticmethod]
    fn fuzzy() -> Self { Self { inner: Operator::Fuzzy } }
    
    // Null checks
    #[staticmethod]
    fn is_null() -> Self { Self { inner: Operator::IsNull } }
    
    #[staticmethod]
    fn is_not_null() -> Self { Self { inner: Operator::IsNotNull } }
    
    // Array/Set operators
    #[staticmethod]
    fn r#in() -> Self { Self { inner: Operator::In } }
    
    #[staticmethod]
    fn not_in() -> Self { Self { inner: Operator::NotIn } }
    
    // Range operators
    #[staticmethod]
    fn between() -> Self { Self { inner: Operator::Between } }
    
    #[staticmethod]
    fn not_between() -> Self { Self { inner: Operator::NotBetween } }
    
    // JSON operators
    #[staticmethod]
    fn contains() -> Self { Self { inner: Operator::Contains } }
    
    #[staticmethod]
    fn key_exists() -> Self { Self { inner: Operator::KeyExists } }
    
    fn __repr__(&self) -> String {
        format!("Operator.{}", self.inner.sql_symbol())
    }
}

/// Convert a Python object to a QAIL Value.
/// 
/// Supports: None, bool, int, float, str, list
pub fn py_to_value(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
    // None -> Null
    if ob.is_none() {
        return Ok(Value::Null);
    }
    
    // Bool (must check before int, since bool is subclass of int in Python)
    if let Ok(b) = ob.extract::<bool>() {
        return Ok(Value::Bool(b));
    }
    
    // Integer
    if let Ok(i) = ob.extract::<i64>() {
        return Ok(Value::Int(i));
    }
    
    // Float
    if let Ok(f) = ob.extract::<f64>() {
        return Ok(Value::Float(f));
    }
    
    // String
    if let Ok(s) = ob.extract::<String>() {
        return Ok(Value::String(s));
    }
    
    // List -> Array (extract instead of deprecated downcast)
    if let Ok(list) = ob.extract::<Vec<Bound<'_, PyAny>>>() {
        let mut arr = Vec::with_capacity(list.len());
        for item in list.iter() {
            arr.push(py_to_value(item)?);
        }
        return Ok(Value::Array(arr));
    }
    
    Err(PyTypeError::new_err(format!(
        "Cannot convert {} to QAIL Value",
        ob.get_type().name()?
    )))
}

