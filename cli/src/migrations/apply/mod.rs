//! Apply migrations from migrations/ folder
//!
//! Reads `.qail` migration files in order and executes them against the database.
//! Tracks applied migrations in `_qail_migrations` table.
//!
//! Sub-modules:
//! - `types` — enums, structs, constants
//! - `discovery` — migration file discovery and helpers
//! - `execute` — main `migrate_apply` entry point
//! - `backfill` — chunked backfill system
//! - `codegen` — .qail → SQL generation

mod backfill;
mod codegen;
mod discovery;
mod execute;
#[cfg(test)]
mod tests;
pub(crate) mod types;

pub use execute::migrate_apply;
pub use types::{ApplyPhase, MigrateDirection};
pub(crate) use codegen::{commands_to_sql, parse_qail_to_commands_strict};
pub(crate) use discovery::discover_migrations;
pub(crate) use types::MigrationFile;
