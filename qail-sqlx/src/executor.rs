//! QAIL Executor trait for SQLx pools.
//!
//! Provides direct QAIL query execution with automatic parameter binding.

use qail_core::ast::{QailCmd, Value};
use qail_core::transpiler::{ToSqlParameterized, TranspileResult};
use sqlx::{postgres::PgRow, FromRow, PgPool};
use std::future::Future;
use std::pin::Pin;

/// Error type for QAIL-SQLx operations.
#[derive(Debug)]
pub enum QailSqlxError {
    /// QAIL parsing error
    Parse(qail_core::error::QailError),
    /// SQLx execution error
    Sqlx(sqlx::Error),
}

impl std::fmt::Display for QailSqlxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QailSqlxError::Parse(e) => write!(f, "QAIL parse error: {:?}", e),
            QailSqlxError::Sqlx(e) => write!(f, "SQLx error: {}", e),
        }
    }
}

impl std::error::Error for QailSqlxError {}

impl From<qail_core::error::QailError> for QailSqlxError {
    fn from(e: qail_core::error::QailError) -> Self {
        QailSqlxError::Parse(e)
    }
}

impl From<sqlx::Error> for QailSqlxError {
    fn from(e: sqlx::Error) -> Self {
        QailSqlxError::Sqlx(e)
    }
}

/// Result type for QAIL-SQLx operations.
pub type QailResult<T> = Result<T, QailSqlxError>;

/// Extension trait for executing QAIL queries directly on SQLx pools.
pub trait QailExecutor {
    /// Fetch all rows using a QAIL query string.
    fn qail_fetch_all<'a, T>(&'a self, qail: &'a str) -> Pin<Box<dyn Future<Output = QailResult<Vec<T>>> + Send + 'a>>
    where
        T: for<'r> FromRow<'r, PgRow> + Send + Unpin;

    /// Fetch all rows with named parameters.
    fn qail_fetch_all_with<'a, T>(&'a self, qail: &'a str, params: &'a crate::params::QailParams) -> Pin<Box<dyn Future<Output = QailResult<Vec<T>>> + Send + 'a>>
    where
        T: for<'r> FromRow<'r, PgRow> + Send + Unpin;

    /// Fetch one row using a QAIL query string.
    fn qail_fetch_one<'a, T>(&'a self, qail: &'a str) -> Pin<Box<dyn Future<Output = QailResult<T>> + Send + 'a>>
    where
        T: for<'r> FromRow<'r, PgRow> + Send + Unpin;

    /// Fetch one row with named parameters.
    fn qail_fetch_one_with<'a, T>(&'a self, qail: &'a str, params: &'a crate::params::QailParams) -> Pin<Box<dyn Future<Output = QailResult<T>> + Send + 'a>>
    where
        T: for<'r> FromRow<'r, PgRow> + Send + Unpin;

    /// Fetch optional row using a QAIL query string.
    fn qail_fetch_optional<'a, T>(&'a self, qail: &'a str) -> Pin<Box<dyn Future<Output = QailResult<Option<T>>> + Send + 'a>>
    where
        T: for<'r> FromRow<'r, PgRow> + Send + Unpin;

    /// Fetch optional row with named parameters.
    fn qail_fetch_optional_with<'a, T>(&'a self, qail: &'a str, params: &'a crate::params::QailParams) -> Pin<Box<dyn Future<Output = QailResult<Option<T>>> + Send + 'a>>
    where
        T: for<'r> FromRow<'r, PgRow> + Send + Unpin;

    /// Execute a QAIL query (for INSERT/UPDATE/DELETE).
    fn qail_execute<'a>(&'a self, qail: &'a str) -> Pin<Box<dyn Future<Output = QailResult<u64>> + Send + 'a>>;

    /// Execute a QAIL query with named parameters.
    fn qail_execute_with<'a>(&'a self, qail: &'a str, params: &'a crate::params::QailParams) -> Pin<Box<dyn Future<Output = QailResult<u64>> + Send + 'a>>;

    /// Execute a QailCmd directly (for dynamic query building).
    fn qail_execute_cmd<'a, T>(&'a self, cmd: &'a QailCmd) -> Pin<Box<dyn Future<Output = QailResult<Vec<T>>> + Send + 'a>>
    where
        T: for<'r> FromRow<'r, PgRow> + Send + Unpin;
}

impl QailExecutor for PgPool {
    fn qail_fetch_all<'a, T>(&'a self, qail: &'a str) -> Pin<Box<dyn Future<Output = QailResult<Vec<T>>> + Send + 'a>>
    where
        T: for<'r> FromRow<'r, PgRow> + Send + Unpin,
    {
        Box::pin(async move {
            let result = parse_and_parameterize(qail)?;
            let rows = execute_query_as::<T>(self, &result).await?;
            Ok(rows)
        })
    }

    fn qail_fetch_one<'a, T>(&'a self, qail: &'a str) -> Pin<Box<dyn Future<Output = QailResult<T>> + Send + 'a>>
    where
        T: for<'r> FromRow<'r, PgRow> + Send + Unpin,
    {
        Box::pin(async move {
            let result = parse_and_parameterize(qail)?;
            let rows = execute_query_as::<T>(self, &result).await?;
            rows.into_iter().next().ok_or_else(|| {
                QailSqlxError::Sqlx(sqlx::Error::RowNotFound)
            })
        })
    }

    fn qail_fetch_optional<'a, T>(&'a self, qail: &'a str) -> Pin<Box<dyn Future<Output = QailResult<Option<T>>> + Send + 'a>>
    where
        T: for<'r> FromRow<'r, PgRow> + Send + Unpin,
    {
        Box::pin(async move {
            let result = parse_and_parameterize(qail)?;
            let rows = execute_query_as::<T>(self, &result).await?;
            Ok(rows.into_iter().next())
        })
    }

    fn qail_execute<'a>(&'a self, qail: &'a str) -> Pin<Box<dyn Future<Output = QailResult<u64>> + Send + 'a>> {
        Box::pin(async move {
            let result = parse_and_parameterize(qail)?;
            let affected = execute_query(self, &result).await?;
            Ok(affected)
        })
    }

    fn qail_execute_cmd<'a, T>(&'a self, cmd: &'a QailCmd) -> Pin<Box<dyn Future<Output = QailResult<Vec<T>>> + Send + 'a>>
    where
        T: for<'r> FromRow<'r, PgRow> + Send + Unpin,
    {
        Box::pin(async move {
            let result = cmd.to_sql_parameterized();
            let rows = execute_query_as::<T>(self, &result).await?;
            Ok(rows)
        })
    }

    fn qail_fetch_all_with<'a, T>(&'a self, qail: &'a str, params: &'a crate::params::QailParams) -> Pin<Box<dyn Future<Output = QailResult<Vec<T>>> + Send + 'a>>
    where
        T: for<'r> FromRow<'r, PgRow> + Send + Unpin,
    {
        Box::pin(async move {
            let result = parse_and_parameterize(qail)?;
            let rows = execute_query_as_with::<T>(self, &result, params).await?;
            Ok(rows)
        })
    }

    fn qail_fetch_one_with<'a, T>(&'a self, qail: &'a str, params: &'a crate::params::QailParams) -> Pin<Box<dyn Future<Output = QailResult<T>> + Send + 'a>>
    where
        T: for<'r> FromRow<'r, PgRow> + Send + Unpin,
    {
        Box::pin(async move {
            let result = parse_and_parameterize(qail)?;
            let rows = execute_query_as_with::<T>(self, &result, params).await?;
            rows.into_iter().next().ok_or_else(|| {
                QailSqlxError::Sqlx(sqlx::Error::RowNotFound)
            })
        })
    }

    fn qail_fetch_optional_with<'a, T>(&'a self, qail: &'a str, params: &'a crate::params::QailParams) -> Pin<Box<dyn Future<Output = QailResult<Option<T>>> + Send + 'a>>
    where
        T: for<'r> FromRow<'r, PgRow> + Send + Unpin,
    {
        Box::pin(async move {
            let result = parse_and_parameterize(qail)?;
            let rows = execute_query_as_with::<T>(self, &result, params).await?;
            Ok(rows.into_iter().next())
        })
    }

    fn qail_execute_with<'a>(&'a self, qail: &'a str, params: &'a crate::params::QailParams) -> Pin<Box<dyn Future<Output = QailResult<u64>> + Send + 'a>> {
        Box::pin(async move {
            let result = parse_and_parameterize(qail)?;
            let affected = execute_query_with(self, &result, params).await?;
            Ok(affected)
        })
    }
}

/// Parse QAIL and generate parameterized SQL.
fn parse_and_parameterize(qail: &str) -> QailResult<TranspileResult> {
    let cmd = qail_core::parse(qail)?;
    Ok(cmd.to_sql_parameterized())
}

/// Execute a parameterized query and return typed results.
async fn execute_query_as<T>(pool: &PgPool, result: &TranspileResult) -> QailResult<Vec<T>>
where
    T: for<'r> FromRow<'r, PgRow> + Send + Unpin,
{
    let mut query = sqlx::query_as::<_, T>(&result.sql);
    
    // Bind each parameter
    for param in &result.params {
        query = bind_value(query, param);
    }
    
    let rows = query.fetch_all(pool).await?;
    Ok(rows)
}

/// Execute a parameterized query and return affected rows.
async fn execute_query(pool: &PgPool, result: &TranspileResult) -> QailResult<u64> {
    let mut query = sqlx::query(&result.sql);
    
    // Bind each parameter
    for param in &result.params {
        query = bind_value_raw(query, param);
    }
    
    let result = query.execute(pool).await?;
    Ok(result.rows_affected())
}

/// Execute a parameterized query with named params and return typed results.
async fn execute_query_as_with<T>(pool: &PgPool, result: &TranspileResult, params: &crate::params::QailParams) -> QailResult<Vec<T>>
where
    T: for<'r> FromRow<'r, PgRow> + Send + Unpin,
{
    let mut query = sqlx::query_as::<_, T>(&result.sql);
    
    // Bind values from QailParams in the order they appear in the query
    let values = params.bind_values(&result.named_params);
    for value in &values {
        query = bind_value(query, value);
    }
    
    let rows = query.fetch_all(pool).await?;
    Ok(rows)
}

/// Execute a parameterized query with named params and return affected rows.
async fn execute_query_with(pool: &PgPool, result: &TranspileResult, params: &crate::params::QailParams) -> QailResult<u64> {
    let mut query = sqlx::query(&result.sql);
    
    // Bind values from QailParams in the order they appear in the query
    let values = params.bind_values(&result.named_params);
    for value in &values {
        query = bind_value_raw(query, value);
    }
    
    let result = query.execute(pool).await?;
    Ok(result.rows_affected())
}

/// Bind a QAIL Value to a typed SQLx query.
fn bind_value<'q, T>(
    query: sqlx::query::QueryAs<'q, sqlx::Postgres, T, sqlx::postgres::PgArguments>,
    value: &Value,
) -> sqlx::query::QueryAs<'q, sqlx::Postgres, T, sqlx::postgres::PgArguments>
where
    T: for<'r> FromRow<'r, PgRow>,
{
    match value {
        Value::String(s) => query.bind(s.clone()),
        Value::Int(i) => query.bind(*i),
        Value::Float(f) => query.bind(*f),
        Value::Bool(b) => query.bind(*b),
        Value::Null => query.bind(None::<String>),
        Value::Array(arr) => {
            // Convert array to strings for now
            let strings: Vec<String> = arr.iter().map(|v| v.to_string()).collect();
            query.bind(strings)
        }
        // These shouldn't appear in parameterized output, but handle gracefully
        Value::Param(_) | Value::NamedParam(_) | Value::Function(_) | Value::Subquery(_) | Value::Column(_) => {
            // Bind as string representation
            query.bind(value.to_string())
        }
    }
}

/// Bind a QAIL Value to a raw SQLx query.
fn bind_value_raw<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    value: &Value,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    match value {
        Value::String(s) => query.bind(s.clone()),
        Value::Int(i) => query.bind(*i),
        Value::Float(f) => query.bind(*f),
        Value::Bool(b) => query.bind(*b),
        Value::Null => query.bind(None::<String>),
        Value::Array(arr) => {
            let strings: Vec<String> = arr.iter().map(|v| v.to_string()).collect();
            query.bind(strings)
        }
        // These shouldn't appear in parameterized output, but handle gracefully
        Value::Param(_) | Value::NamedParam(_) | Value::Function(_) | Value::Subquery(_) | Value::Column(_) => {
            query.bind(value.to_string())
        }
    }
}
