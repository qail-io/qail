//! Row wrapper for PostgreSQL query results.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyString, PyBool};
use qail_pg::PgRow;

/// Python-exposed Row from query results.
#[pyclass(name = "Row")]
pub struct PyRow {
    pub inner: PgRow,
}

impl PyRow {
    pub fn new(row: PgRow) -> Self {
        Self { inner: row }
    }
}

#[pymethods]
impl PyRow {
    /// Get column value by index.
    fn get(&self, py: Python<'_>, index: usize) -> PyResult<Py<PyAny>> {
        if let Some(value) = self.inner.columns.get(index) {
            Ok(pg_value_to_py(py, value))
        } else {
            Ok(py.None())
        }
    }
    
    /// Get column value by name.
    fn get_by_name(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        if let Some(col_info) = &self.inner.column_info {
            if let Some(&idx) = col_info.name_to_index.get(name) {
                return self.get(py, idx);
            }
        }
        Ok(py.None())
    }
    
    /// Convert row to Python dict.
    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        
        if let Some(col_info) = &self.inner.column_info {
            for (name, &idx) in col_info.name_to_index.iter() {
                if let Some(value) = self.inner.columns.get(idx) {
                    dict.set_item(name, pg_value_to_py(py, value))?;
                }
            }
        }
        
        Ok(dict.unbind().into_any())
    }
    
    /// Support row[index] and row["column_name"] syntax.
    fn __getitem__(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        if let Ok(idx) = key.extract::<usize>() {
            self.get(py, idx)
        } else if let Ok(name) = key.extract::<String>() {
            self.get_by_name(py, &name)
        } else {
            Ok(py.None())
        }
    }
    
    /// Number of columns.
    fn __len__(&self) -> usize {
        self.inner.columns.len()
    }
    
    fn __repr__(&self) -> String {
        format!("Row({} columns)", self.inner.columns.len())
    }
}

/// Convert PostgreSQL wire protocol value to Python.
fn pg_value_to_py(py: Python<'_>, value: &Option<Vec<u8>>) -> Py<PyAny> {
    match value {
        None => py.None(),
        Some(bytes) => {
            // PostgreSQL sends values as UTF-8 text in simple query protocol
            if let Ok(s) = std::str::from_utf8(bytes) {
                // Try boolean
                if s == "t" {
                    let obj = PyBool::new(py, true);
                    return obj.to_owned().unbind().into_any();
                } else if s == "f" {
                    let obj = PyBool::new(py, false);
                    return obj.to_owned().unbind().into_any();
                }
                
                // Try integer - use into_pyobject with owned binding
                if let Ok(i) = s.parse::<i64>() {
                    let bound = i.into_pyobject(py).unwrap();
                    return bound.unbind().into_any();
                }
                
                // Try float
                if let Ok(f) = s.parse::<f64>() {
                    let bound = f.into_pyobject(py).unwrap();
                    return bound.unbind().into_any();
                }
                
                // Return as string
                PyString::new(py, s).unbind().into_any()
            } else {
                // Return raw bytes
                PyBytes::new(py, bytes).unbind().into_any()
            }
        }
    }
}
