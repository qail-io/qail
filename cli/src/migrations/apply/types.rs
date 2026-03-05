//! Types, enums, structs, and constants for the migration apply system.

use std::path::PathBuf;

/// Apply filter for phased migration execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyPhase {
    All,
    Expand,
    Backfill,
    Contract,
}

impl ApplyPhase {
    pub(super) fn allows(self, phase: MigrationPhase) -> bool {
        match self {
            Self::All => true,
            Self::Expand => phase == MigrationPhase::Expand,
            Self::Backfill => phase == MigrationPhase::Backfill,
            Self::Contract => phase == MigrationPhase::Contract,
        }
    }
}

impl std::fmt::Display for ApplyPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::All => write!(f, "all"),
            Self::Expand => write!(f, "expand"),
            Self::Backfill => write!(f, "backfill"),
            Self::Contract => write!(f, "contract"),
        }
    }
}

/// Expand/Backfill/Contract phase for a discovered migration file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum MigrationPhase {
    Expand = 0,
    Backfill = 1,
    Contract = 2,
}

impl std::fmt::Display for MigrationPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Expand => write!(f, "expand"),
            Self::Backfill => write!(f, "backfill"),
            Self::Contract => write!(f, "contract"),
        }
    }
}

/// A discovered migration, from either flat or subdirectory layout.
pub(super) struct MigrationFile {
    /// Group key (timestamp/name without phase suffix)
    pub(super) group_key: String,
    /// Sort key (directory/file name prefix)
    pub(super) sort_key: String,
    /// Display name
    pub(super) display_name: String,
    /// Full path to the .qail file
    pub(super) path: PathBuf,
    /// Workflow phase this file belongs to
    pub(super) phase: MigrationPhase,
}

#[derive(Debug, Clone)]
pub(super) struct BackfillSpec {
    pub(super) table: String,
    pub(super) pk_column: String,
    pub(super) set_clause: String,
    pub(super) where_clause: Option<String>,
    pub(super) chunk_size: usize,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct BackfillRun {
    pub(super) resumed: bool,
    pub(super) rows_updated: i64,
    pub(super) chunks: i64,
}

pub(super) const BACKFILL_CHECKPOINT_TABLE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS _qail_backfill_checkpoints (
    migration_version varchar(255) primary key,
    table_name varchar(255) not null,
    pk_column varchar(255) not null,
    last_pk bigint not null default 0,
    chunk_size integer not null,
    rows_processed bigint not null default 0,
    started_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    finished_at timestamptz
)
"#;

/// Direction for migration
#[derive(Clone, Copy)]
pub enum MigrateDirection {
    Up,
    Down,
}
