//! QailCmd wrapper - AST builder for Python.

use pyo3::prelude::*;
use qail_core::ast::{QailCmd, Operator, SortOrder, JoinKind};
use crate::types::{py_to_value, PyOperator};

/// Python-exposed QailCmd AST builder.
/// 
/// All methods return `Self` for fluent chaining.
#[pyclass(name = "QailCmd")]
#[derive(Clone)]
pub struct PyQailCmd {
    pub inner: QailCmd,
}

#[pymethods]
impl PyQailCmd {
    // =========================================================================
    // Static Constructors
    // =========================================================================
    
    /// Create a GET (SELECT) command.
    #[staticmethod]
    fn get(table: &str) -> Self {
        Self { inner: QailCmd::get(table) }
    }
    
    /// Create a SET (UPDATE) command.
    #[staticmethod]
    fn set(table: &str) -> Self {
        Self { inner: QailCmd::set(table) }
    }
    
    /// Create a DEL (DELETE) command.
    #[staticmethod]
    fn del(table: &str) -> Self {
        Self { inner: QailCmd::del(table) }
    }
    
    /// Create an ADD (INSERT) command.
    #[staticmethod]
    fn add(table: &str) -> Self {
        Self { inner: QailCmd::add(table) }
    }
    
    /// Create a PUT (UPSERT) command.
    #[staticmethod]
    fn put(table: &str) -> Self {
        Self { inner: QailCmd::put(table) }
    }
    
    // =========================================================================
    // Column Selection
    // =========================================================================
    
    /// Select all columns (*).
    fn select_all(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.inner = std::mem::take(&mut slf.inner).select_all();
        slf
    }
    
    /// Select specific columns.
    fn columns(mut slf: PyRefMut<'_, Self>, cols: Vec<String>) -> PyRefMut<'_, Self> {
        let col_refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        slf.inner = std::mem::take(&mut slf.inner).columns(col_refs);
        slf
    }
    
    /// Add a single column.
    fn column(mut slf: PyRefMut<'_, Self>, col: String) -> PyRefMut<'_, Self> {
        slf.inner = std::mem::take(&mut slf.inner).column(&col);
        slf
    }
    
    // =========================================================================
    // Filtering
    // =========================================================================
    
    /// Add a filter condition.
    fn filter<'py>(
        mut slf: PyRefMut<'py, Self>,
        column: &str,
        op: &PyOperator,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let val = py_to_value(value)?;
        slf.inner = std::mem::take(&mut slf.inner).filter(column, op.inner, val);
        Ok(slf)
    }
    
    /// Add an equality filter (shorthand for filter with Operator.eq()).
    fn eq<'py>(
        mut slf: PyRefMut<'py, Self>,
        column: &str,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let val = py_to_value(value)?;
        slf.inner = std::mem::take(&mut slf.inner).filter(column, Operator::Eq, val);
        Ok(slf)
    }
    
    /// Add an OR condition group.
    fn or_filter<'py>(
        mut slf: PyRefMut<'py, Self>,
        column: &str,
        op: &PyOperator,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let val = py_to_value(value)?;
        slf.inner = std::mem::take(&mut slf.inner).or_filter(column, op.inner, val);
        Ok(slf)
    }
    
    // =========================================================================
    // Pagination & Ordering
    // =========================================================================
    
    /// Set result limit.
    fn limit(mut slf: PyRefMut<'_, Self>, n: i64) -> PyRefMut<'_, Self> {
        slf.inner = std::mem::take(&mut slf.inner).limit(n);
        slf
    }
    
    /// Set result offset.
    fn offset(mut slf: PyRefMut<'_, Self>, n: i64) -> PyRefMut<'_, Self> {
        slf.inner = std::mem::take(&mut slf.inner).offset(n);
        slf
    }
    
    /// Order by column ascending.
    fn order_by(mut slf: PyRefMut<'_, Self>, column: String) -> PyRefMut<'_, Self> {
        slf.inner = std::mem::take(&mut slf.inner).order_by(&column, SortOrder::Asc);
        slf
    }
    
    /// Order by column descending.
    fn order_by_desc(mut slf: PyRefMut<'_, Self>, column: String) -> PyRefMut<'_, Self> {
        slf.inner = std::mem::take(&mut slf.inner).order_by(&column, SortOrder::Desc);
        slf
    }
    
    // =========================================================================
    // Joins
    // =========================================================================
    
    /// Add an INNER JOIN.
    fn join(
        mut slf: PyRefMut<'_, Self>,
        table: String,
        left_col: String,
        right_col: String,
    ) -> PyRefMut<'_, Self> {
        slf.inner = std::mem::take(&mut slf.inner).join(JoinKind::Inner, &table, &left_col, &right_col);
        slf
    }
    
    /// Add a LEFT JOIN.
    fn left_join(
        mut slf: PyRefMut<'_, Self>,
        table: String,
        left_col: String,
        right_col: String,
    ) -> PyRefMut<'_, Self> {
        slf.inner = std::mem::take(&mut slf.inner).join(JoinKind::Left, &table, &left_col, &right_col);
        slf
    }
    
    // =========================================================================
    // Grouping & Aggregation
    // =========================================================================
    
    /// Group by columns.
    fn group_by(mut slf: PyRefMut<'_, Self>, cols: Vec<String>) -> PyRefMut<'_, Self> {
        let col_refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        slf.inner = std::mem::take(&mut slf.inner).group_by(col_refs);
        slf
    }
    
    // =========================================================================
    // Mutations (INSERT/UPDATE)
    // =========================================================================
    
    /// Set values for INSERT.
    fn values<'py>(
        mut slf: PyRefMut<'py, Self>,
        vals: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut values: Vec<qail_core::ast::Value> = Vec::with_capacity(vals.len());
        for item in vals.iter() {
            values.push(py_to_value(item)?);
        }
        slf.inner = std::mem::take(&mut slf.inner).values(values);
        Ok(slf)
    }
    
    /// Set assignment for UPDATE (column = value).
    fn set_value<'py>(
        mut slf: PyRefMut<'py, Self>,
        column: &str,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let val = py_to_value(value)?;
        slf.inner = std::mem::take(&mut slf.inner).set_value(column, val);
        Ok(slf)
    }
    
    // =========================================================================
    // Returning
    // =========================================================================
    
    /// Add RETURNING clause.
    fn returning(mut slf: PyRefMut<'_, Self>, cols: Vec<String>) -> PyRefMut<'_, Self> {
        let col_refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        slf.inner = std::mem::take(&mut slf.inner).returning(col_refs);
        slf
    }
    
    /// Return all columns.
    fn returning_all(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.inner = std::mem::take(&mut slf.inner).returning_all();
        slf
    }
    
    // =========================================================================
    // Debug
    // =========================================================================
    
    fn __repr__(&self) -> String {
        format!("QailCmd({:?})", self.inner.action)
    }
}
