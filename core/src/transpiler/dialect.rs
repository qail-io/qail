use crate::transpiler::sql::postgres::PostgresGenerator;
use crate::transpiler::traits::SqlGenerator;

/// SQL dialect selection for transpilation.
///
/// PostgreSQL is the only SQL target. The enum is kept so
/// `to_sql_with_dialect` remains a stable call shape; the 1.x `SQLite`
/// variant was removed in 2.0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Dialect {
    /// PostgreSQL dialect.
    #[default]
    Postgres,
}

impl Dialect {
    /// Create the dialect-specific SQL generator.
    pub fn generator(&self) -> Box<dyn SqlGenerator> {
        match self {
            Dialect::Postgres => Box::new(PostgresGenerator),
        }
    }
}
