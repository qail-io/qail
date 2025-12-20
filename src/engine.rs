//! Database execution engine for QAIL.
//!
//! This module provides the runtime for executing parsed QAIL queries
//! against PostgreSQL, MySQL, or SQLite databases using sqlx.

use crate::error::QailError;
use crate::parser;
use crate::transpiler::ToSql;

use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::{Column, Row, TypeInfo};
use std::collections::HashMap;

/// A database connection for executing QAIL queries.
#[derive(Clone)]
pub struct QailDB {
    pool: PgPool,
}

impl QailDB {
    /// Connect to a database using a connection URL.
    ///
    /// Supported URL formats:
    /// - `postgres://user:pass@host/db`
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let db = QailDB::connect("postgres://localhost/mydb").await?;
    /// ```
    pub async fn connect(url: &str) -> Result<Self, QailError> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await
            .map_err(|e| QailError::Connection(e.to_string()))?;

        Ok(Self { pool })
    }

    /// Create a new query from a QAIL string.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let users = db
    ///     .query("get::users•@*[active=$1]")
    ///     .bind(true)
    ///     .fetch_all()
    ///     .await?;
    /// ```
    pub fn query(&self, qail: &str) -> QailQuery {
        QailQuery::new(self.pool.clone(), qail.to_string())
    }

    /// Execute a raw SQL query (escape hatch).
    pub fn raw(&self, sql: &str) -> QailQuery {
        QailQuery::raw(self.pool.clone(), sql.to_string())
    }

    /// Get a reference to the underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// A QAIL query builder with parameter bindings.
pub struct QailQuery {
    pool: PgPool,
    qail: String,
    sql: Option<String>,
    bindings: Vec<QailValue>,
    is_raw: bool,
}

/// Dynamic value type for query bindings.
#[derive(Debug, Clone)]
pub enum QailValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

impl QailQuery {
    fn new(pool: PgPool, qail: String) -> Self {
        Self {
            pool,
            qail,
            sql: None,
            bindings: Vec::new(),
            is_raw: false,
        }
    }

    fn raw(pool: PgPool, sql: String) -> Self {
        Self {
            pool,
            qail: String::new(),
            sql: Some(sql),
            bindings: Vec::new(),
            is_raw: true,
        }
    }

    /// Bind a boolean value.
    pub fn bind_bool(mut self, value: bool) -> Self {
        self.bindings.push(QailValue::Bool(value));
        self
    }

    /// Bind an integer value.
    pub fn bind_int(mut self, value: i64) -> Self {
        self.bindings.push(QailValue::Int(value));
        self
    }

    /// Bind a float value.
    pub fn bind_float(mut self, value: f64) -> Self {
        self.bindings.push(QailValue::Float(value));
        self
    }

    /// Bind a string value.
    pub fn bind_str(mut self, value: &str) -> Self {
        self.bindings.push(QailValue::String(value.to_string()));
        self
    }

    /// Bind a value (auto-detect type from common types).
    pub fn bind<T: Into<QailValue>>(mut self, value: T) -> Self {
        self.bindings.push(value.into());
        self
    }

    /// Get the generated SQL without executing.
    pub fn sql(&self) -> Result<String, QailError> {
        if self.is_raw {
            return Ok(self.sql.clone().unwrap_or_default());
        }
        let cmd = parser::parse(&self.qail)?;
        Ok(cmd.to_sql())
    }

    /// Fetch all rows as JSON-like maps.
    pub async fn fetch_all(&self) -> Result<Vec<HashMap<String, serde_json::Value>>, QailError> {
        let sql = self.sql()?;
        let mut query = sqlx::query(&sql);

        // Bind parameters
        for binding in &self.bindings {
            query = match binding {
                QailValue::Null => query,
                QailValue::Bool(v) => query.bind(*v),
                QailValue::Int(v) => query.bind(*v),
                QailValue::Float(v) => query.bind(*v),
                QailValue::String(v) => query.bind(v.as_str()),
            };
        }

        let rows: Vec<PgRow> = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| QailError::Execution(e.to_string()))?;

        // Convert rows to HashMaps
        let results: Vec<HashMap<String, serde_json::Value>> = rows
            .iter()
            .map(|row| row_to_map(row))
            .collect();

        Ok(results)
    }

    /// Fetch a single row as a JSON-like map.
    pub async fn fetch_one(&self) -> Result<HashMap<String, serde_json::Value>, QailError> {
        let sql = self.sql()?;
        let mut query = sqlx::query(&sql);

        for binding in &self.bindings {
            query = match binding {
                QailValue::Null => query,
                QailValue::Bool(v) => query.bind(*v),
                QailValue::Int(v) => query.bind(*v),
                QailValue::Float(v) => query.bind(*v),
                QailValue::String(v) => query.bind(v.as_str()),
            };
        }

        let row: PgRow = query
            .fetch_one(&self.pool)
            .await
            .map_err(|e| QailError::Execution(e.to_string()))?;

        Ok(row_to_map(&row))
    }

    /// Execute a mutation query (INSERT, UPDATE, DELETE).
    /// Returns the number of affected rows.
    pub async fn execute(&self) -> Result<u64, QailError> {
        let sql = self.sql()?;
        let mut query = sqlx::query(&sql);

        for binding in &self.bindings {
            query = match binding {
                QailValue::Null => query,
                QailValue::Bool(v) => query.bind(*v),
                QailValue::Int(v) => query.bind(*v),
                QailValue::Float(v) => query.bind(*v),
                QailValue::String(v) => query.bind(v.as_str()),
            };
        }

        let result = query
            .execute(&self.pool)
            .await
            .map_err(|e| QailError::Execution(e.to_string()))?;

        Ok(result.rows_affected())
    }
}

/// Convert a PgRow to a HashMap, handling Postgres-specific types.
fn row_to_map(row: &PgRow) -> HashMap<String, serde_json::Value> {
    use sqlx::ValueRef;
    
    let mut map = HashMap::new();

    for (i, column) in row.columns().iter().enumerate() {
        let name = column.name().to_string();
        let type_name = column.type_info().name();

        // Try to get the raw value first to check for NULL
        let value_ref = row.try_get_raw(i);
        if value_ref.is_err() || value_ref.as_ref().map(|v| v.is_null()).unwrap_or(true) {
            map.insert(name, serde_json::Value::Null);
            continue;
        }

        let value: serde_json::Value = match type_name {
            "BOOL" => row
                .try_get::<bool, _>(i)
                .map(serde_json::Value::Bool)
                .unwrap_or(serde_json::Value::Null),
            "INT2" | "INT4" => row
                .try_get::<i32, _>(i)
                .map(|v| serde_json::Value::Number(v.into()))
                .unwrap_or(serde_json::Value::Null),
            "INT8" => row
                .try_get::<i64, _>(i)
                .map(|v| serde_json::Value::Number(v.into()))
                .unwrap_or(serde_json::Value::Null),
            "FLOAT4" => row
                .try_get::<f32, _>(i)
                .ok()
                .and_then(|v| serde_json::Number::from_f64(v as f64))
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            "FLOAT8" => row
                .try_get::<f64, _>(i)
                .ok()
                .and_then(|v| serde_json::Number::from_f64(v))
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            "UUID" => row
                .try_get::<sqlx::types::Uuid, _>(i)
                .map(|v| serde_json::Value::String(v.to_string()))
                .unwrap_or(serde_json::Value::Null),
            "TIMESTAMPTZ" | "TIMESTAMP" => row
                .try_get::<chrono::DateTime<chrono::Utc>, _>(i)
                .map(|v| serde_json::Value::String(v.to_rfc3339()))
                .or_else(|_| {
                    row.try_get::<chrono::NaiveDateTime, _>(i)
                        .map(|v| serde_json::Value::String(v.to_string()))
                })
                .unwrap_or(serde_json::Value::Null),
            "DATE" => row
                .try_get::<chrono::NaiveDate, _>(i)
                .map(|v| serde_json::Value::String(v.to_string()))
                .unwrap_or(serde_json::Value::Null),
            "TEXT" | "VARCHAR" | "CHAR" | "NAME" => row
                .try_get::<String, _>(i)
                .map(serde_json::Value::String)
                .unwrap_or(serde_json::Value::Null),
            "JSONB" | "JSON" => row
                .try_get::<serde_json::Value, _>(i)
                .unwrap_or(serde_json::Value::Null),
            _ => {
                // Fallback: try to get as string
                row.try_get::<String, _>(i)
                    .map(serde_json::Value::String)
                    .unwrap_or_else(|_| serde_json::Value::String(format!("<{}>", type_name)))
            }
        };

        map.insert(name, value);
    }

    map
}

// Implement From traits for QailValue
impl From<bool> for QailValue {
    fn from(v: bool) -> Self {
        QailValue::Bool(v)
    }
}

impl From<i32> for QailValue {
    fn from(v: i32) -> Self {
        QailValue::Int(v as i64)
    }
}

impl From<i64> for QailValue {
    fn from(v: i64) -> Self {
        QailValue::Int(v)
    }
}

impl From<f64> for QailValue {
    fn from(v: f64) -> Self {
        QailValue::Float(v)
    }
}

impl From<&str> for QailValue {
    fn from(v: &str) -> Self {
        QailValue::String(v.to_string())
    }
}

impl From<String> for QailValue {
    fn from(v: String) -> Self {
        QailValue::String(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_qail_value_from() {
        let _b: QailValue = true.into();
        let _i: QailValue = 42i32.into();
        let _f: QailValue = 3.14f64.into();
        let _s: QailValue = "hello".into();
    }
}
