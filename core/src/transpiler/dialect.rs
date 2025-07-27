use crate::transpiler::sql::postgres::PostgresGenerator;
use crate::transpiler::sql::sqlite::SqliteGenerator;
use crate::transpiler::traits::SqlGenerator;

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum Dialect {
    #[default]
    Postgres,
    SQLite,
}

impl Dialect {
    pub fn generator(&self) -> Box<dyn SqlGenerator> {
        match self {
            Dialect::Postgres => Box::new(PostgresGenerator),
            Dialect::SQLite => Box::new(SqliteGenerator),
        }
    }
}
