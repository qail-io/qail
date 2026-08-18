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
mod failpoint;
mod lock;
mod plan;
mod policy;
mod receipt;
mod reset;
mod risk;
mod rollback;
mod status;
pub mod types;
mod up;
mod verify;
#[cfg(feature = "watch")]
mod watch;

pub use analyze::migrate_analyze;
pub use apply::{ApplyPhase, MigrateApplyOptions, MigrateDirection, migrate_apply};
pub use create::migrate_create;
pub use down::migrate_down;
pub use failpoint::maybe_failpoint;
pub use lock::acquire_migration_lock;
pub use plan::migrate_plan;
pub use policy::{
    EnforcementMode, MigrationPolicy, PolicyResolution, ReceiptValidationMode,
    ensure_destructive_policy_declared, load_migration_policy, resolve_migration_policy,
};
pub use receipt::{
    MigrationReceipt, ReceiptSignatureStatus, StoredMigrationReceipt,
    ensure_migration_receipt_columns, now_epoch_ms, runtime_actor, runtime_git_sha,
    verify_stored_receipt_signature, write_migration_receipt,
};
pub use reset::migrate_reset;
pub use rollback::migrate_rollback;
pub use status::migrate_status;
pub use up::{MigrateUpOptions, migrate_up};
#[cfg(feature = "watch")]
pub use watch::watch_schema;

use anyhow::Context;
use qail_core::ast::{Action, Constraint, Expr, Qail};
use qail_core::parser::schema::Schema;
use qail_core::transpiler::ToSql;
use qail_pg::PgDriver;
use std::path::{Path, PathBuf};

/// Resolve the deltas directory for migration files.
///
/// Resolution order, walking from the current directory to the filesystem root:
/// 1. `migrations_dir` from the nearest `qail.toml` `[project]` that declares it
/// 2. `deltas/` beside the nearest `qail.toml`, or in the current directory when
///    there is no config at all
///
/// A declared `migrations_dir` is resolved against the directory of the file
/// that declared it, so `migrations_dir = "../db/deltas"` names the same
/// directory no matter where `qail` was invoked from.
pub fn resolve_deltas_dir(create_if_missing: bool) -> anyhow::Result<PathBuf> {
    let cwd = crate::project::current_dir()?;
    resolve_deltas_dir_from(&cwd, create_if_missing)
}

/// [`resolve_deltas_dir`], starting the walk at `start` instead of the current
/// directory.
pub fn resolve_deltas_dir_from(start: &Path, create_if_missing: bool) -> anyhow::Result<PathBuf> {
    let configs = crate::project::ancestor_configs(start);

    // 1. The nearest qail.toml that declares [project].migrations_dir wins. A
    //    nearer file that declares none does not mask an ancestor that does.
    for config_path in &configs {
        let Some(declared) = declared_migrations_dir(config_path)? else {
            continue;
        };
        let path = crate::project::config_root(config_path).join(&declared);

        if path.is_dir() {
            return Ok(path);
        }
        if create_if_missing {
            std::fs::create_dir_all(&path).with_context(|| {
                format!("Failed to create migrations_dir '{}'", path.display())
            })?;
            return Ok(path);
        }
        // Declared but absent. Falling back to `deltas/` here would run a
        // different set of migrations than the config asked for.
        anyhow::bail!(
            "migrations_dir '{}' declared in {} resolves to '{}', which does not exist.\n\
             Create that directory, correct the path, or run 'qail init'.",
            declared,
            config_path.display(),
            path.display()
        );
    }

    // 2. Default: `deltas/` beside the project config, else the starting dir.
    let base = configs
        .first()
        .map(|config| crate::project::config_root(config).to_path_buf())
        .unwrap_or_else(|| start.to_path_buf());
    let deltas = base.join("deltas");

    if deltas.is_dir() {
        return Ok(deltas);
    }
    if create_if_missing {
        std::fs::create_dir_all(&deltas)
            .with_context(|| format!("Failed to create '{}'", deltas.display()))?;
        return Ok(deltas);
    }

    anyhow::bail!(
        "No deltas/ directory found (looked for '{}'). Run 'qail init' first.\n\
         Tip: Set a custom path in qail.toml:\n\
         [project]\n\
         migrations_dir = \"my_deltas\"",
        deltas.display()
    )
}

/// Read `[project].migrations_dir` out of one `qail.toml`.
///
/// Returns `None` when the file declares no `migrations_dir`. A file that
/// cannot be read or parsed is an error rather than a silent fallback — a
/// malformed config that quietly resolves to the default `deltas/` would apply
/// a different set of migrations than the one it names.
fn declared_migrations_dir(config_path: &Path) -> anyhow::Result<Option<String>> {
    let content = std::fs::read_to_string(config_path)
        .with_context(|| format!("Failed to read {}", config_path.display()))?;

    let config: toml::Value = toml::from_str(&content)
        .map_err(|e| anyhow::anyhow!("Failed to parse {}: {}", config_path.display(), e))?;

    Ok(config
        .get("project")
        .and_then(|project| project.get("migrations_dir"))
        .and_then(|value| value.as_str())
        .map(str::to_string))
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
    shadow_checksum varchar(64),
    receipt_sig text
)
"#;

/// Generate migration table DDL from AST (AST-native bootstrap).
pub fn migration_table_ddl() -> String {
    let Ok(schema) = Schema::parse(MIGRATION_TABLE_SCHEMA) else {
        return String::new();
    };

    schema
        .tables
        .first()
        .map(|table| table.to_ddl())
        .unwrap_or_default()
}

/// Stable checksum for a sequence of migration commands.
///
/// Uses both transpiled SQL and serialized AST so checksums remain distinct even
/// when preview SQL is lossy for a specific action shape.
pub fn stable_cmds_checksum(cmds: &[Qail]) -> String {
    let mut material = String::new();
    for cmd in cmds {
        let sql = cmd.to_sql();
        let ast = qail_core::wire::encode_cmd_text(cmd);
        material.push_str("SQL:");
        material.push_str(sql.trim());
        material.push('\n');
        material.push_str("AST:");
        material.push_str(&ast);
        material.push('\n');
    }
    crate::time::md5_hex(&material)
}

/// Ensure migration table exists and has the latest receipt columns.
pub async fn ensure_migration_table(driver: &mut PgDriver) -> anyhow::Result<()> {
    let exists_cmd = Qail::get("information_schema.tables")
        .column_expr(crate::util::qail_exists_projection())
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
                Expr::Def {
                    name: "receipt_sig".to_string(),
                    data_type: "text".to_string(),
                    constraints: vec![Constraint::Nullable],
                },
            ],
            ..Default::default()
        };
        if let Err(create_err) = driver.execute(&cmd).await {
            // A concurrent bootstrap can race this CREATE TABLE. Re-check table
            // existence and only fail if it is still absent.
            let exists_after = driver.fetch_all(&exists_cmd).await?;
            if exists_after.is_empty() {
                return Err(create_err.into());
            }
        }
    }

    ensure_migration_receipt_columns(driver).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{resolve_deltas_dir_from, stable_cmds_checksum};
    use crate::project::TempTree;
    use qail_core::ast::{Action, Expr, IndexDef, Qail};

    /// The shape in engine.qail.io: the deltas live at the repo root, and the
    /// gateway config reaches them with a `..`. That `..` is written relative to
    /// the gateway directory, so resolving it against the working directory
    /// gives a different answer depending on where `qail` was run.
    #[test]
    fn migrations_dir_resolves_against_the_declaring_file() {
        let tree = TempTree::new();
        tree.dir("db/deltas");
        tree.write(
            "gateway/qail.toml",
            "[project]\nmigrations_dir = \"../db/deltas\"\n",
        );
        let deep = tree.dir("gateway/sub/deeper");

        for start in [tree.path("gateway"), deep] {
            let resolved = resolve_deltas_dir_from(&start, false).expect("resolve");
            assert_eq!(
                resolved.canonicalize().expect("canonicalize"),
                tree.path("db/deltas").canonicalize().expect("canonicalize"),
                "'..' must be relative to the config, not the cwd (from {})",
                start.display()
            );
        }
    }

    #[test]
    fn deltas_dir_is_the_same_from_any_subdirectory() {
        let tree = TempTree::new();
        tree.dir("db/deltas");
        tree.write("qail.toml", "[project]\nmigrations_dir = \"db/deltas\"\n");
        let nested = tree.dir("workers/src/domains");

        let from_root = resolve_deltas_dir_from(&tree.path(""), false).expect("resolve");
        let from_nested = resolve_deltas_dir_from(&nested, false).expect("resolve");

        assert_eq!(from_root, from_nested);
    }

    #[test]
    fn nearer_config_without_migrations_dir_does_not_mask_an_ancestor() {
        let tree = TempTree::new();
        tree.dir("db/deltas");
        tree.write("qail.toml", "[project]\nmigrations_dir = \"db/deltas\"\n");
        tree.write("gateway/qail.toml", "[project]\nname = \"gateway\"\n");

        let resolved = resolve_deltas_dir_from(&tree.path("gateway"), false).expect("resolve");

        assert_eq!(
            resolved.canonicalize().expect("canonicalize"),
            tree.path("db/deltas").canonicalize().expect("canonicalize")
        );
    }

    #[test]
    fn declared_but_missing_dir_is_an_error_not_a_silent_default() {
        let tree = TempTree::new();
        // A `deltas/` that would be picked up by the default branch.
        tree.dir("deltas");
        tree.write("qail.toml", "[project]\nmigrations_dir = \"db/deltas\"\n");

        let err = resolve_deltas_dir_from(&tree.path(""), false)
            .expect_err("a declared path that does not exist must not fall back");

        let msg = err.to_string();
        assert!(msg.contains("db/deltas"), "{msg}");
        assert!(msg.contains("does not exist"), "{msg}");
    }

    #[test]
    fn declared_dir_is_created_when_requested() {
        let tree = TempTree::new();
        tree.write("qail.toml", "[project]\nmigrations_dir = \"db/deltas\"\n");

        let resolved = resolve_deltas_dir_from(&tree.path(""), true).expect("resolve");

        assert!(resolved.is_dir(), "create_if_missing must create the path");
        assert_eq!(resolved, tree.path("db/deltas"));
    }

    #[test]
    fn default_deltas_sits_beside_the_config() {
        let tree = TempTree::new();
        tree.dir("deltas");
        tree.write("qail.toml", "[project]\nname = \"t\"\n");
        let nested = tree.dir("gateway/sub");

        let resolved = resolve_deltas_dir_from(&nested, false).expect("resolve");

        assert_eq!(
            resolved.canonicalize().expect("canonicalize"),
            tree.path("deltas").canonicalize().expect("canonicalize"),
            "the default must be the project's deltas/, not one relative to cwd"
        );
    }

    #[test]
    fn a_malformed_config_is_an_error_not_a_silent_default() {
        let tree = TempTree::new();
        tree.dir("deltas");
        tree.write("qail.toml", "[project\nmigrations_dir = broken");

        let err = resolve_deltas_dir_from(&tree.path(""), false)
            .expect_err("a malformed config must not silently resolve to deltas/");

        assert!(err.to_string().contains("qail.toml"), "{err}");
    }

    #[test]
    fn missing_deltas_reports_where_it_looked() {
        let tree = TempTree::new();
        tree.write("qail.toml", "[project]\nname = \"t\"\n");

        let err = resolve_deltas_dir_from(&tree.path(""), false).expect_err("nothing to resolve");

        assert!(
            err.to_string().contains("deltas"),
            "the error must name the path it searched: {err}"
        );
    }

    #[test]
    fn stable_checksum_distinguishes_column_renames() {
        let rename_a = Qail {
            action: Action::Mod,
            table: "users".to_string(),
            columns: vec![Expr::Named("email -> email_address".to_string())],
            ..Default::default()
        };
        let rename_b = Qail {
            action: Action::Mod,
            table: "users".to_string(),
            columns: vec![Expr::Named("email -> primary_email".to_string())],
            ..Default::default()
        };

        let a = stable_cmds_checksum(&[rename_a]);
        let b = stable_cmds_checksum(&[rename_b]);
        assert_ne!(a, b, "different renames must produce different checksums");
    }

    #[test]
    fn stable_checksum_uses_index_def_table() {
        let idx_users = Qail {
            action: Action::Index,
            table: String::new(),
            index_def: Some(IndexDef {
                name: "idx_lookup".to_string(),
                table: "users".to_string(),
                columns: vec!["email".to_string()],
                unique: false,
                index_type: None,
                include: vec![],
                concurrently: false,
                where_clause: None,
            }),
            ..Default::default()
        };
        let idx_orgs = Qail {
            action: Action::Index,
            table: String::new(),
            index_def: Some(IndexDef {
                name: "idx_lookup".to_string(),
                table: "organizations".to_string(),
                columns: vec!["email".to_string()],
                unique: false,
                index_type: None,
                include: vec![],
                concurrently: false,
                where_clause: None,
            }),
            ..Default::default()
        };

        let users = stable_cmds_checksum(&[idx_users]);
        let orgs = stable_cmds_checksum(&[idx_orgs]);
        assert_ne!(
            users, orgs,
            "index checksums must differ when target tables differ"
        );
    }
}
