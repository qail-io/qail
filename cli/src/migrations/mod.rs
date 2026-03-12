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
mod receipt;
mod reset;
mod risk;
mod status;
pub mod types;
mod up;
mod verify;
#[cfg(feature = "watch")]
mod watch;

pub use analyze::migrate_analyze;
pub use apply::migrate_apply;
pub use apply::{ApplyPhase, MigrateDirection};
pub use create::migrate_create;
pub use down::migrate_down;
pub use plan::migrate_plan;
pub use receipt::{
    MigrationReceipt, ensure_migration_receipt_columns, now_epoch_ms, runtime_actor,
    runtime_git_sha, write_migration_receipt,
};
pub use reset::migrate_reset;
pub use status::migrate_status;
pub use up::migrate_up;
#[cfg(feature = "watch")]
pub use watch::watch_schema;

use qail_core::ast::{Action, Constraint, Expr, Qail};
use qail_core::parser::schema::Schema;
use qail_pg::PgDriver;
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
    sql_down text,
    git_sha varchar(64),
    qail_version varchar(32),
    actor varchar(255),
    started_at_ms bigint,
    finished_at_ms bigint,
    duration_ms bigint,
    affected_rows_est bigint,
    risk_summary text,
    shadow_checksum varchar(64)
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

/// Ensure migration table exists and has the latest receipt columns.
pub async fn ensure_migration_table(driver: &mut PgDriver) -> anyhow::Result<()> {
    let exists_cmd = Qail::get("information_schema.tables")
        .column("1")
        .where_eq("table_schema", "public")
        .where_eq("table_name", "_qail_migrations")
        .limit(1);
    let exists = driver.fetch_all(&exists_cmd).await?;

    if exists.is_empty() {
        let cmd = Qail {
            action: Action::Make,
            table: "_qail_migrations".to_string(),
            columns: vec![
                Expr::Def {
                    name: "id".to_string(),
                    data_type: "serial".to_string(),
                    constraints: vec![Constraint::PrimaryKey],
                },
                Expr::Def {
                    name: "version".to_string(),
                    data_type: "varchar".to_string(),
                    constraints: vec![Constraint::Unique],
                },
                Expr::Def {
                    name: "name".to_string(),
                    data_type: "varchar".to_string(),
                    constraints: vec![Constraint::Nullable],
                },
                Expr::Def {
                    name: "applied_at".to_string(),
                    data_type: "timestamptz".to_string(),
                    constraints: vec![
                        Constraint::Nullable,
                        Constraint::Default("now()".to_string()),
                    ],
                },
                Expr::Def {
                    name: "checksum".to_string(),
                    data_type: "varchar".to_string(),
                    constraints: vec![],
                },
                Expr::Def {
                    name: "sql_up".to_string(),
                    data_type: "text".to_string(),
                    constraints: vec![],
                },
                Expr::Def {
                    name: "sql_down".to_string(),
                    data_type: "text".to_string(),
                    constraints: vec![Constraint::Nullable],
                },
                Expr::Def {
                    name: "git_sha".to_string(),
                    data_type: "varchar".to_string(),
                    constraints: vec![Constraint::Nullable],
                },
                Expr::Def {
                    name: "qail_version".to_string(),
                    data_type: "varchar".to_string(),
                    constraints: vec![Constraint::Nullable],
                },
                Expr::Def {
                    name: "actor".to_string(),
                    data_type: "varchar".to_string(),
                    constraints: vec![Constraint::Nullable],
                },
                Expr::Def {
                    name: "started_at_ms".to_string(),
                    data_type: "bigint".to_string(),
                    constraints: vec![Constraint::Nullable],
                },
                Expr::Def {
                    name: "finished_at_ms".to_string(),
                    data_type: "bigint".to_string(),
                    constraints: vec![Constraint::Nullable],
                },
                Expr::Def {
                    name: "duration_ms".to_string(),
                    data_type: "bigint".to_string(),
                    constraints: vec![Constraint::Nullable],
                },
                Expr::Def {
                    name: "affected_rows_est".to_string(),
                    data_type: "bigint".to_string(),
                    constraints: vec![Constraint::Nullable],
                },
                Expr::Def {
                    name: "risk_summary".to_string(),
                    data_type: "text".to_string(),
                    constraints: vec![Constraint::Nullable],
                },
                Expr::Def {
                    name: "shadow_checksum".to_string(),
                    data_type: "varchar".to_string(),
                    constraints: vec![Constraint::Nullable],
                },
            ],
            ..Default::default()
        };
        driver.execute(&cmd).await?;
    }

    ensure_migration_receipt_columns(driver).await?;
    Ok(())
}
