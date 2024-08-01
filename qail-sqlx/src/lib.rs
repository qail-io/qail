//! SQLx Integration for QAIL.
//!
//! Execute QAIL queries with typed results using SQLx.
//!
//! # Example
//! ```no_run
//! use qail_sqlx::prelude::*;
//! use sqlx::PgPool;
//!
//! #[derive(sqlx::FromRow)]
//! struct User {
//!     id: i32,
//!     name: String,
//! }
//!
//! async fn example(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
//!     // Direct QAIL execution (native integration)
//!     let users: Vec<User> = pool.qail_fetch_all("get users fields * where active = true").await?;
//!     
//!     // Or using the SQL string approach
//!     let sql = qail_to_sql("get users fields id, name where active = true")?;
//!     let users: Vec<User> = sqlx::query_as(&sql).fetch_all(pool).await?;
//!     Ok(())
//! }
//! ```

use qail_core::transpiler::{ToSql, Dialect, TranspileResult, ToSqlParameterized};

pub mod executor;
pub mod params;

pub use executor::{QailExecutor, QailSqlxError, QailResult};
pub use params::QailParams;

/// Parse QAIL and return PostgreSQL.
pub fn qail_to_sql(qail: &str) -> Result<String, qail_core::error::QailError> {
    let cmd = qail_core::parse(qail)?;
    Ok(cmd.to_sql())
}

/// Parse QAIL and return SQL with specific dialect.
pub fn qail_to_sql_with_dialect(qail: &str, dialect: Dialect) -> Result<String, qail_core::error::QailError> {
    let cmd = qail_core::parse(qail)?;
    Ok(cmd.to_sql_with_dialect(dialect))
}

/// Parse QAIL and return SQL with extracted parameters.
pub fn qail_to_sql_parameterized(qail: &str) -> Result<TranspileResult, qail_core::error::QailError> {
    let cmd = qail_core::parse(qail)?;
    Ok(cmd.to_sql_parameterized())
}

/// Parse QAIL and return SQL with extracted parameters for specific dialect.
pub fn qail_to_sql_parameterized_with_dialect(
    qail: &str, 
    dialect: Dialect
) -> Result<TranspileResult, qail_core::error::QailError> {
    let cmd = qail_core::parse(qail)?;
    Ok(cmd.to_sql_parameterized_with_dialect(dialect))
}

/// Prelude for convenient imports.
pub mod prelude {
    pub use super::{
        qail_to_sql, 
        qail_to_sql_with_dialect, 
        qail_to_sql_parameterized,
        qail_to_sql_parameterized_with_dialect
    };
    pub use super::executor::{QailExecutor, QailSqlxError, QailResult};
    pub use super::params::QailParams;
    pub use qail_core::transpiler::{ToSql, Dialect, TranspileResult};
    pub use qail_core::ast::{QailCmd, Value};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_qail_to_sql() {
        let sql = qail_to_sql("get users fields *").unwrap();
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("users"));
    }

    #[test]
    fn test_qail_with_dialect() {
        let sql = qail_to_sql_with_dialect("get users fields *", Dialect::MySQL).unwrap();
        assert!(sql.contains("`users`")); // MySQL backticks
    }

    #[test]
    fn test_parameterized() {
        let result = qail_to_sql_parameterized("get users fields * where active = true").unwrap();
        assert!(result.sql.contains("users"));
        // Params should contain the 'true' value
        assert!(!result.params.is_empty());
    }
}
