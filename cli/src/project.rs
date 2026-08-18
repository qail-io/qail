//! Locating the project's `qail.toml`.
//!
//! Every lookup walks from a starting directory to the filesystem root, so the
//! answer does not depend on which directory `qail` was invoked from. This is
//! the same walk the LSP (`nearest_qail_toml`) and `schema_tools` already use.
//!
//! Paths declared inside a `qail.toml` are resolved against **that file's**
//! directory, never the process working directory — otherwise a relative value
//! like `migrations_dir = "../db/deltas"` would mean a different directory
//! depending on where the command was run from.

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

/// Every `qail.toml` at or above `start`, nearest first.
pub fn ancestor_configs(start: &Path) -> Vec<PathBuf> {
    start
        .ancestors()
        .map(|dir| dir.join("qail.toml"))
        .filter(|candidate| candidate.is_file())
        .collect()
}

/// The directory a `qail.toml` lives in — the root that its relative paths are
/// resolved against.
pub fn config_root(config_path: &Path) -> &Path {
    config_path.parent().unwrap_or(Path::new("."))
}

/// The current directory, with a message that says what it was needed for.
pub fn current_dir() -> Result<PathBuf> {
    std::env::current_dir().context("Failed to determine the current directory for qail.toml lookup")
}

/// A temp directory tree, removed on drop.
///
/// Config resolution is a filesystem behaviour, so its tests need real
/// directories. This avoids a dev-dependency on `tempfile` for that alone.
#[cfg(test)]
pub(crate) struct TempTree(PathBuf);

#[cfg(test)]
impl TempTree {
    pub(crate) fn new() -> Self {
        use std::sync::atomic::{AtomicU32, Ordering};
        static COUNTER: AtomicU32 = AtomicU32::new(0);

        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let root =
            std::env::temp_dir().join(format!("qail-cfg-test-{}-{}", std::process::id(), n));
        std::fs::create_dir_all(&root).expect("create temp root");
        Self(root)
    }

    /// Create a directory under the tree and return its path.
    pub(crate) fn dir(&self, rel: &str) -> PathBuf {
        let path = self.0.join(rel);
        std::fs::create_dir_all(&path).expect("create dir");
        path
    }

    /// Write a file under the tree, creating parents as needed.
    pub(crate) fn write(&self, rel: &str, contents: &str) {
        let path = self.0.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        std::fs::write(path, contents).expect("write file");
    }

    pub(crate) fn path(&self, rel: &str) -> PathBuf {
        self.0.join(rel)
    }

    /// True when nothing above the tree holds a `qail.toml` that would join a
    /// walk. Guards the assertions that depend on the tree being closed.
    pub(crate) fn ancestors_are_clean(&self) -> bool {
        self.0
            .ancestors()
            .skip(1)
            .all(|dir| !dir.join("qail.toml").is_file())
    }
}

#[cfg(test)]
impl Drop for TempTree {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::{ancestor_configs, config_root};
    use std::path::Path;

    #[test]
    fn config_root_is_the_declaring_files_directory() {
        assert_eq!(
            config_root(Path::new("/repo/gateway/qail.toml")),
            Path::new("/repo/gateway")
        );
    }

    #[test]
    fn ancestor_configs_is_empty_for_a_path_with_none() {
        // A path that cannot exist, so no ancestor can hold a qail.toml.
        let found = ancestor_configs(Path::new("/nonexistent-qail-probe-6f2a/deep"));
        assert!(found.is_empty());
    }
}
