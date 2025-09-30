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
mod apply;
mod create;
mod down;
mod plan;
mod reset;
mod status;
pub mod types;
mod up;
mod watch;

pub use analyze::migrate_analyze;
pub use apply::{migrate_apply, MigrateDirection};
pub use create::migrate_create;
pub use down::migrate_down;
pub use plan::migrate_plan;
pub use reset::migrate_reset;
pub use status::migrate_status;
pub use up::migrate_up;
pub use watch::watch_schema;

use qail_core::parser::schema::Schema;
use std::path::{Path, PathBuf};

/// Resolve the deltas directory for migration files.
///
/// Resolution order:
/// 1. `migrations_dir` from `qail.toml` `[project]` section (if set)
/// 2. `deltas/` (Qail default)
/// 3. `migrations/` (SQLx compatibility fallback)
///
/// Returns the resolved path, or an error if none exist and `create` is false.
pub fn resolve_deltas_dir(create_if_missing: bool) -> anyhow::Result<PathBuf> {
    // 1. Check qail.toml for explicit override
    if let Ok(content) = std::fs::read_to_string("qail.toml")
        && let Ok(config) = toml::from_str::<toml::Value>(&content)
        && let Some(dir) = config
            .get("project")
            .and_then(|p| p.get("migrations_dir"))
            .and_then(|v| v.as_str())
    {
        let path = PathBuf::from(dir);
        if path.exists() || create_if_missing {
            if create_if_missing && !path.exists() {
                std::fs::create_dir_all(&path)?;
            }
            return Ok(path);
        }
    }

    // 2. Qail default: deltas/
    let deltas = Path::new("deltas");
    if deltas.exists() {
        return Ok(deltas.to_path_buf());
    }

    // 3. SQLx compatibility fallback: migrations/
    let migrations = Path::new("migrations");
    if migrations.exists() {
        return Ok(migrations.to_path_buf());
    }

    // None exist — create the default if requested
    if create_if_missing {
        std::fs::create_dir_all(deltas)?;
        return Ok(deltas.to_path_buf());
    }

    anyhow::bail!(
        "No deltas/ or migrations/ directory found. Run 'qail init' first.\n\
         Tip: Set a custom path in qail.toml:\n\
         [project]\n\
         migrations_dir = \"my_deltas\""
    )
}

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
