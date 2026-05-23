use crate::transpiler::sql::postgres::PostgresGenerator;
use crate::transpiler::sql::sqlite::SqliteGenerator;
use crate::transpiler::traits::SqlGenerator;

/// SQL dialect selection for transpilation.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum Dialect {
    /// PostgreSQL dialect (default).
    #[default]
    Postgres,
    /// SQLite dialect compatibility surface.
    ///
    /// PostgreSQL is the supported SQL runtime; this variant remains so 1.x
    /// consumers that selected SQLite still compile.
    SQLite,
}

impl Dialect {
    /// Create the dialect-specific SQL generator.
    pub fn generator(&self) -> Box<dyn SqlGenerator> {
        match self {
            Dialect::Postgres => Box::new(PostgresGenerator),
            Dialect::SQLite => Box::new(SqliteGenerator),
        }
    }
}
