use crate::transpiler::sql::postgres::PostgresGenerator;
use crate::transpiler::sql::sqlite::SqliteGenerator;
use crate::transpiler::traits::SqlGenerator;

/// Supported SQL Dialects.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Dialect {
    Postgres,
    SQLite,
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
            Dialect::SQLite => Box::new(SqliteGenerator),
        }
    }
}
