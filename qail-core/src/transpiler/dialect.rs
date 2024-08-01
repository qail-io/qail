use crate::transpiler::traits::SqlGenerator;
use crate::transpiler::sql::postgres::PostgresGenerator;
use crate::transpiler::sql::mysql::MysqlGenerator;
use crate::transpiler::sql::sqlite::SqliteGenerator;
use crate::transpiler::sql::sqlserver::SqlServerGenerator;
use crate::transpiler::sql::oracle::OracleGenerator;
use crate::transpiler::sql::duckdb::DuckDbGenerator;
use crate::transpiler::sql::mariadb::MariaDbGenerator;
use crate::transpiler::sql::snowflake::SnowflakeGenerator;
use crate::transpiler::sql::bigquery::BigQueryGenerator;
use crate::transpiler::sql::redshift::RedshiftGenerator;

/// Supported SQL Dialects.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Dialect {
    Postgres,
    MySQL,
    SQLite,
    SqlServer,
    Oracle,
    DuckDB,
    MariaDB,
    Snowflake,
    BigQuery,
    Redshift,
    InfluxDB,
}

impl Default for Dialect {
    fn default() -> Self {
        Self::Postgres
    }
}

impl Dialect {
    pub fn generator(&self) -> Box<dyn SqlGenerator> {
        match self {
            Dialect::Postgres => Box::new(PostgresGenerator),
            Dialect::MySQL => Box::new(MysqlGenerator),
            Dialect::SQLite => Box::new(SqliteGenerator),
            Dialect::SqlServer => Box::new(SqlServerGenerator),
            Dialect::Oracle => Box::new(OracleGenerator),
            Dialect::DuckDB => Box::new(DuckDbGenerator),
            Dialect::MariaDB => Box::new(MariaDbGenerator),
            Dialect::Snowflake => Box::new(SnowflakeGenerator),
            Dialect::BigQuery => Box::new(BigQueryGenerator),
            Dialect::Redshift => Box::new(RedshiftGenerator),
            Dialect::InfluxDB => Box::new(crate::transpiler::sql::influx::InfluxGenerator),
        }
    }
}
