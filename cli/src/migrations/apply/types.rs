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
pub(crate) enum MigrationPhase {
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
#[derive(Clone)]
pub(crate) struct MigrationFile {
    /// Group key (timestamp/name without phase suffix)
    pub(crate) group_key: String,
    /// Sort key (directory/file name prefix)
    pub(crate) sort_key: String,
    /// Display name
    pub(crate) display_name: String,
    /// Full path to the .qail file
    pub(crate) path: PathBuf,
    /// Workflow phase this file belongs to
    pub(crate) phase: MigrationPhase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BackfillTransformOp {
    Lower,
    Upper,
    Trim,
    Initcap,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum BackfillTransform {
    Identity,
    Lower,
    Upper,
    Trim,
    Initcap,
    Pipeline(Vec<BackfillTransformOp>),
}

#[derive(Debug, Clone)]
pub(super) struct BackfillSpec {
    pub(super) table: String,
    pub(super) pk_column: String,
    pub(super) set_column: String,
    pub(super) source_column: String,
    pub(super) transform: BackfillTransform,
    pub(super) where_null_column: Option<String>,
    pub(super) chunk_size: usize,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct BackfillRun {
    pub(super) resumed: bool,
    pub(super) rows_updated: i64,
    pub(super) chunks: i64,
}

/// Direction for migration
#[derive(Clone, Copy)]
pub enum MigrateDirection {
    Up,
    Down,
}
