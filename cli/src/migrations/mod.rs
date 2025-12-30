//! Migration operations for QAIL CLI
//!
//! Modular migration system with classification support.
//!
//! Submodules:
//! - `types`: MigrationClass enum and helpers
//! - `status`: Migration status and history
//! - `up`: Apply migrations forward
//! - `down`: Rollback migrations
//! - `plan`: Preview SQL without executing
//! - `analyze`: Impact analysis on codebase
//! - `watch`: Live schema monitoring
//! - `create`: Create new migration files

mod analyze;
mod create;
mod down;
mod plan;
mod status;
pub mod types;
mod up;
mod watch;

pub use analyze::migrate_analyze;
pub use create::migrate_create;
pub use down::migrate_down;
pub use plan::migrate_plan;
pub use status::migrate_status;
pub use up::migrate_up;
pub use watch::watch_schema;

use qail_core::parser::schema::Schema;

/// Migration table schema in QAIL format (AST-native).
pub const MIGRATION_TABLE_SCHEMA: &str = r#"
table _qail_migrations (
    id serial primary_key,
    version varchar(255) not null unique,
    name varchar(255),
    applied_at timestamptz default NOW(),
    checksum varchar(64) not null,
    sql_up text not null,
    sql_down text
)
"#;

/// Generate migration table DDL from AST (AST-native bootstrap).
pub fn migration_table_ddl() -> String {
    Schema::parse(MIGRATION_TABLE_SCHEMA)
        .expect("Invalid migration table schema")
        .tables
        .first()
        .expect("No table in migration schema")
        .to_ddl()
}
