//! Filesystem loader for QAIL schema sources.
//!
//! Supports:
//! - single file (`schema.qail`)
//! - modular directory (`schema/*.qail`, recursive)
//! - optional module-order manifest (`schema/_order.qail`)
//!
//! Directory modules are merged in deterministic lexical path order.
//! If `_order.qail` exists, listed modules are loaded first in listed
//! order; unlisted modules are appended in lexical order.
//!
//! Strict manifest mode (optional):
//! - add `-- qail: strict-manifest` or `!strict` in `_order.qail`
//! - then every discovered module must be listed (directly or via listed directories)
//! - unlisted modules cause an error

use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

const MODULE_ORDER_FILE: &str = "_order.qail";
const ORDER_STRICT_DIRECTIVE: &str = "qail: strict-manifest";
const ORDER_STRICT_SHORTHAND: &str = "!strict";
const STRICT_ENV_VAR: &str = "QAIL_SCHEMA_STRICT_MANIFEST";

/// Resolved schema source (single file or directory of modules).
#[derive(Debug, Clone)]
pub struct ResolvedSchemaSource {
    /// Original path requested by caller.
    pub requested: PathBuf,
    /// Effective path used after fallback resolution.
    pub root: PathBuf,
    /// Ordered list of `.qail` files to merge.
    pub files: Vec<PathBuf>,
}

impl ResolvedSchemaSource {
    /// Returns `true` when source is a modular directory.
    pub fn is_directory(&self) -> bool {
        self.root.is_dir()
    }

    /// Paths useful for change-watching.
    ///
    /// Includes:
    /// - root path
    /// - all resolved module files
    pub fn watch_paths(&self) -> Vec<PathBuf> {
        let mut out = Vec::with_capacity(1 + self.files.len());
        out.push(self.root.clone());
        if self.root.is_dir() {
            let order_file = self.root.join(MODULE_ORDER_FILE);
            if order_file.exists() {
                out.push(order_file);
            }
        }
        for p in &self.files {
            if !out.contains(p) {
                out.push(p.clone());
            }
        }
        out
    }

    /// Read and merge source content into a single QAIL string.
    pub fn read_merged(&self) -> Result<String, String> {
        if self.files.len() == 1 && self.root.is_file() {
            return fs::read_to_string(&self.files[0]).map_err(|e| {
                format!(
                    "Failed to read schema file '{}': {}",
                    self.files[0].display(),
                    e
                )
            });
        }

        let mut merged = String::new();
        for file in &self.files {
            let content = fs::read_to_string(file)
                .map_err(|e| format!("Failed to read schema module '{}': {}", file.display(), e))?;

            let rel = file.strip_prefix(&self.root).ok().unwrap_or(file);
            merged.push_str(&format!("-- qail: module={}\n", rel.display()));
            merged.push_str(&content);
            if !content.ends_with('\n') {
                merged.push('\n');
            }
            merged.push('\n');
        }

        Ok(merged)
    }
}

/// Resolve a schema source path into concrete module files.
///
/// Fallback behavior:
/// - if requested path is missing and equals `schema.qail`,
///   automatically use sibling `schema/` directory when present.
pub fn resolve_schema_source(path: impl AsRef<Path>) -> Result<ResolvedSchemaSource, String> {
    let requested = path.as_ref();
    let root = resolve_root_path(requested)?;

    if root.is_file() {
        return Ok(ResolvedSchemaSource {
            requested: requested.to_path_buf(),
            root: root.clone(),
            files: vec![root],
        });
    }

    if root.is_dir() {
        let mut discovered_files = Vec::new();
        let root_canonical = root.canonicalize().map_err(|e| {
            format!(
                "Failed to canonicalize schema root '{}': {}",
                root.display(),
                e
            )
        })?;
        let mut visited_dirs = HashSet::new();
        visited_dirs.insert(root_canonical.clone());
        collect_qail_files(
            &root,
            &root_canonical,
            &mut visited_dirs,
            &mut discovered_files,
        )?;
        sort_paths_by_relative_path(&root, &mut discovered_files);

        if discovered_files.is_empty() {
            return Err(format!(
                "Schema directory '{}' contains no .qail files",
                root.display()
            ));
        }

        let files = apply_module_order(&root, discovered_files)?;

        return Ok(ResolvedSchemaSource {
            requested: requested.to_path_buf(),
            root,
            files,
        });
    }

    Err(format!(
        "Schema path '{}' is neither a file nor a directory",
        root.display()
    ))
}

/// Read schema source (file or directory modules) as merged QAIL text.
pub fn read_qail_schema_source(path: impl AsRef<Path>) -> Result<String, String> {
    resolve_schema_source(path)?.read_merged()
}

fn resolve_root_path(requested: &Path) -> Result<PathBuf, String> {
    if requested.exists() {
        return Ok(requested.to_path_buf());
    }

    // Backward-compatible default:
    // if "schema.qail" is missing, try sibling "schema/" directory.
    if requested.file_name() == Some(OsStr::new("schema.qail")) {
        let parent = requested.parent().unwrap_or_else(|| Path::new("."));
        let modular_dir = parent.join("schema");
        if modular_dir.is_dir() {
            return Ok(modular_dir);
        }
    }

    Err(format!(
        "Schema source '{}' not found (expected file or directory)",
        requested.display()
    ))
}

fn collect_qail_files(
    dir: &Path,
    root_canonical: &Path,
    visited_dirs: &mut HashSet<PathBuf>,
    out: &mut Vec<PathBuf>,
) -> Result<(), String> {
    let entries = fs::read_dir(dir)
        .map_err(|e| format!("Failed to read schema directory '{}': {}", dir.display(), e))?;

    for entry in entries {
        let entry = entry.map_err(|e| {
            format!(
                "Failed to read entry in schema directory '{}': {}",
                dir.display(),
                e
            )
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|e| {
            format!(
                "Failed to read file type in schema directory '{}': {}",
                dir.display(),
                e
            )
        })?;

        let hidden = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with('.'));
        if hidden {
            continue;
        }

        if file_type.is_dir() {
            let canonical = path.canonicalize().map_err(|e| {
                format!(
                    "Failed to canonicalize schema directory '{}': {}",
                    path.display(),
                    e
                )
            })?;
            if !canonical.starts_with(root_canonical) {
                continue;
            }
            if !visited_dirs.insert(canonical) {
                continue;
            }
            collect_qail_files(&path, root_canonical, visited_dirs, out)?;
        } else if path
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| e.eq_ignore_ascii_case("qail"))
            && path.file_name() != Some(OsStr::new(MODULE_ORDER_FILE))
        {
            let canonical = path.canonicalize().map_err(|e| {
                format!(
                    "Failed to canonicalize schema module '{}': {}",
                    path.display(),
                    e
                )
            })?;
            if !canonical.starts_with(root_canonical) {
                continue;
            }
            out.push(path);
        }
    }

    Ok(())
}

fn sort_paths_by_relative_path(root: &Path, files: &mut [PathBuf]) {
    files.sort_by(|a, b| {
        let ar = a.strip_prefix(root).ok().unwrap_or(a);
        let br = b.strip_prefix(root).ok().unwrap_or(b);
        ar.to_string_lossy().cmp(&br.to_string_lossy())
    });
}

fn apply_module_order(root: &Path, all_files: Vec<PathBuf>) -> Result<Vec<PathBuf>, String> {
    let order_path = root.join(MODULE_ORDER_FILE);
    if !order_path.exists() {
        return Ok(all_files);
    }

    let order_text = fs::read_to_string(&order_path).map_err(|e| {
        format!(
            "Failed to read schema module order file '{}': {}",
            order_path.display(),
            e
        )
    })?;

    let root_canonical = root.canonicalize().map_err(|e| {
        format!(
            "Failed to canonicalize schema root '{}': {}",
            root.display(),
            e
        )
    })?;

    let mut known_modules: HashMap<PathBuf, PathBuf> = HashMap::new();
    for module in &all_files {
        let canonical = module.canonicalize().map_err(|e| {
            format!(
                "Failed to canonicalize schema module '{}': {}",
                module.display(),
                e
            )
        })?;
        known_modules.insert(canonical, module.clone());
    }

    let mut ordered = Vec::new();
    let mut seen = HashSet::new();
    let mut strict_manifest = strict_manifest_default_enabled(root)?;

    let mut push_module = |canonical: PathBuf, source_entry: &str| -> Result<(), String> {
        if let Some(original) = known_modules.get(&canonical) {
            if seen.insert(canonical) {
                ordered.push(original.clone());
            }
            Ok(())
        } else {
            Err(format!(
                "Order file '{}' references '{}' but it is not a loadable .qail module",
                order_path.display(),
                source_entry
            ))
        }
    };

    for (line_no, raw) in order_text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some(comment) = line.strip_prefix("--") {
            let comment = comment.trim();
            if comment.eq_ignore_ascii_case(ORDER_STRICT_DIRECTIVE) {
                strict_manifest = true;
            }
            continue;
        }

        if line.eq_ignore_ascii_case(ORDER_STRICT_SHORTHAND) {
            strict_manifest = true;
            continue;
        }

        let requested = root.join(line);
        let canonical = requested.canonicalize().map_err(|e| {
            format!(
                "Order file '{}': line {} references '{}' which cannot be resolved: {}",
                order_path.display(),
                line_no + 1,
                line,
                e
            )
        })?;

        if !canonical.starts_with(&root_canonical) {
            return Err(format!(
                "Order file '{}': line {} escapes schema root with '{}'",
                order_path.display(),
                line_no + 1,
                line
            ));
        }

        if canonical.is_dir() {
            let mut nested = Vec::new();
            let mut nested_visited = HashSet::new();
            nested_visited.insert(canonical.clone());
            collect_qail_files(
                &requested,
                &root_canonical,
                &mut nested_visited,
                &mut nested,
            )?;
            sort_paths_by_relative_path(root, &mut nested);

            if nested.is_empty() {
                return Err(format!(
                    "Order file '{}': line {} directory '{}' has no .qail modules",
                    order_path.display(),
                    line_no + 1,
                    line
                ));
            }

            for module in nested {
                let module_canonical = module.canonicalize().map_err(|e| {
                    format!(
                        "Order file '{}': failed to canonicalize module '{}': {}",
                        order_path.display(),
                        module.display(),
                        e
                    )
                })?;
                push_module(module_canonical, line)?;
            }
            continue;
        }

        if canonical.file_name() == Some(OsStr::new(MODULE_ORDER_FILE)) {
            return Err(format!(
                "Order file '{}': line {} cannot include '{}' recursively",
                order_path.display(),
                line_no + 1,
                MODULE_ORDER_FILE
            ));
        }

        if canonical
            .extension()
            .and_then(|e| e.to_str())
            .is_none_or(|e| !e.eq_ignore_ascii_case("qail"))
        {
            return Err(format!(
                "Order file '{}': line {} must reference .qail files or directories (got '{}')",
                order_path.display(),
                line_no + 1,
                line
            ));
        }

        push_module(canonical, line)?;
    }

    let mut unlisted = Vec::new();
    for module in all_files {
        let canonical = module.canonicalize().map_err(|e| {
            format!(
                "Failed to canonicalize schema module '{}': {}",
                module.display(),
                e
            )
        })?;
        if seen.insert(canonical) {
            if strict_manifest {
                unlisted.push(module);
            } else {
                ordered.push(module);
            }
        }
    }

    if strict_manifest && !unlisted.is_empty() {
        let preview: Vec<String> = unlisted
            .iter()
            .take(10)
            .map(|p| {
                p.strip_prefix(root)
                    .ok()
                    .unwrap_or(p)
                    .to_string_lossy()
                    .to_string()
            })
            .collect();
        let suffix = if unlisted.len() > preview.len() {
            format!(" (+{} more)", unlisted.len() - preview.len())
        } else {
            String::new()
        };
        return Err(format!(
            "Order file '{}' has strict manifest enabled, but {} module(s) are unlisted: {}{}",
            order_path.display(),
            unlisted.len(),
            preview.join(", "),
            suffix
        ));
    }

    Ok(ordered)
}

fn strict_manifest_default_enabled(schema_root: &Path) -> Result<bool, String> {
    if let Ok(raw) = std::env::var(STRICT_ENV_VAR) {
        let normalized = raw.trim().to_ascii_lowercase();
        return Ok(matches!(normalized.as_str(), "1" | "true" | "yes" | "on"));
    }

    for dir in schema_root.ancestors() {
        let candidate = dir.join("qail.toml");
        if !candidate.is_file() {
            continue;
        }
        let cfg = crate::config::QailConfig::load_from(&candidate).map_err(|err| {
            format!(
                "Failed to load strict-manifest default from '{}': {}",
                candidate.display(),
                err
            )
        })?;
        return Ok(cfg.project.schema_strict_manifest.unwrap_or(false));
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(name: &str) -> PathBuf {
        let base = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock ok")
            .as_nanos();
        base.join(format!("qail_schema_source_{name}_{nanos}"))
    }

    #[test]
    fn resolve_schema_qail_falls_back_to_schema_dir() {
        let root = tmp_dir("fallback");
        fs::create_dir_all(root.join("schema")).expect("mkdir schema");
        fs::write(
            root.join("schema").join("auth.qail"),
            "table auth_users {\n  id uuid primary_key\n}\n",
        )
        .expect("write auth");
        fs::write(
            root.join("schema").join("user.qail"),
            "table users {\n  id uuid primary_key\n}\n",
        )
        .expect("write user");

        let requested = root.join("schema.qail");
        let resolved = resolve_schema_source(&requested).expect("resolved");
        assert!(resolved.is_directory());
        assert_eq!(resolved.files.len(), 2);

        let merged = resolved.read_merged().expect("merged");
        assert!(merged.contains("table auth_users"));
        assert!(merged.contains("table users"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn resolve_single_file() {
        let root = tmp_dir("single");
        fs::create_dir_all(&root).expect("mkdir");
        let schema_file = root.join("schema.qail");
        fs::write(&schema_file, "table users {\n  id uuid primary_key\n}\n").expect("write file");

        let resolved = resolve_schema_source(&schema_file).expect("resolved");
        assert!(!resolved.is_directory());
        assert_eq!(resolved.files, vec![schema_file.clone()]);
        assert!(
            resolved
                .read_merged()
                .expect("read")
                .contains("table users")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn order_file_reorders_modules_and_appends_unlisted() {
        let root = tmp_dir("order");
        let schema_dir = root.join("schema");
        fs::create_dir_all(&schema_dir).expect("mkdir schema");
        fs::write(
            schema_dir.join("auth.qail"),
            "table auth_users {\n  id uuid primary_key\n}\n",
        )
        .expect("write auth");
        fs::write(
            schema_dir.join("user.qail"),
            "table users {\n  id uuid primary_key\n}\n",
        )
        .expect("write user");
        fs::write(
            schema_dir.join("billing.qail"),
            "table invoices {\n  id uuid primary_key\n}\n",
        )
        .expect("write billing");
        fs::write(schema_dir.join(MODULE_ORDER_FILE), "user.qail\nauth.qail\n")
            .expect("write order");

        let resolved = resolve_schema_source(root.join("schema.qail")).expect("resolved");
        assert_eq!(resolved.files.len(), 3);
        assert_eq!(
            resolved.files[0].file_name().and_then(|n| n.to_str()),
            Some("user.qail")
        );
        assert_eq!(
            resolved.files[1].file_name().and_then(|n| n.to_str()),
            Some("auth.qail")
        );
        assert_eq!(
            resolved.files[2].file_name().and_then(|n| n.to_str()),
            Some("billing.qail")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn order_file_strict_manifest_requires_full_listing() {
        let root = tmp_dir("order_strict_missing");
        let schema_dir = root.join("schema");
        fs::create_dir_all(&schema_dir).expect("mkdir schema");
        fs::write(
            schema_dir.join("auth.qail"),
            "table auth_users {\n  id uuid primary_key\n}\n",
        )
        .expect("write auth");
        fs::write(
            schema_dir.join("user.qail"),
            "table users {\n  id uuid primary_key\n}\n",
        )
        .expect("write user");
        fs::write(
            schema_dir.join("billing.qail"),
            "table invoices {\n  id uuid primary_key\n}\n",
        )
        .expect("write billing");
        fs::write(
            schema_dir.join(MODULE_ORDER_FILE),
            "-- qail: strict-manifest\nuser.qail\nauth.qail\n",
        )
        .expect("write order");

        let err = resolve_schema_source(root.join("schema.qail")).expect_err("should error");
        assert!(err.contains("strict manifest enabled"));
        assert!(err.contains("billing.qail"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn order_file_strict_manifest_allows_complete_listing() {
        let root = tmp_dir("order_strict_ok");
        let schema_dir = root.join("schema");
        fs::create_dir_all(&schema_dir).expect("mkdir schema");
        fs::write(
            schema_dir.join("auth.qail"),
            "table auth_users {\n  id uuid primary_key\n}\n",
        )
        .expect("write auth");
        fs::write(
            schema_dir.join("user.qail"),
            "table users {\n  id uuid primary_key\n}\n",
        )
        .expect("write user");
        fs::write(
            schema_dir.join("billing.qail"),
            "table invoices {\n  id uuid primary_key\n}\n",
        )
        .expect("write billing");
        fs::write(
            schema_dir.join(MODULE_ORDER_FILE),
            "-- qail: strict-manifest\nuser.qail\nauth.qail\nbilling.qail\n",
        )
        .expect("write order");

        let resolved = resolve_schema_source(root.join("schema.qail")).expect("resolved");
        assert_eq!(resolved.files.len(), 3);
        assert_eq!(
            resolved.files[0].file_name().and_then(|n| n.to_str()),
            Some("user.qail")
        );
        assert_eq!(
            resolved.files[1].file_name().and_then(|n| n.to_str()),
            Some("auth.qail")
        );
        assert_eq!(
            resolved.files[2].file_name().and_then(|n| n.to_str()),
            Some("billing.qail")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn order_file_missing_module_errors() {
        let root = tmp_dir("order_missing");
        let schema_dir = root.join("schema");
        fs::create_dir_all(&schema_dir).expect("mkdir schema");
        fs::write(
            schema_dir.join("user.qail"),
            "table users {\n  id uuid primary_key\n}\n",
        )
        .expect("write user");
        fs::write(schema_dir.join(MODULE_ORDER_FILE), "missing.qail\n").expect("write order");

        let err = resolve_schema_source(root.join("schema.qail")).expect_err("should error");
        assert!(err.contains("cannot be resolved") || err.contains("not a loadable"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn order_file_rejects_path_escape() {
        let root = tmp_dir("order_escape");
        let schema_dir = root.join("schema");
        fs::create_dir_all(&schema_dir).expect("mkdir schema");
        fs::write(
            schema_dir.join("user.qail"),
            "table users {\n  id uuid primary_key\n}\n",
        )
        .expect("write user");

        let outside = root.join("outside.qail");
        fs::write(&outside, "table outside { id uuid primary_key }\n").expect("write outside");
        fs::write(schema_dir.join(MODULE_ORDER_FILE), "../outside.qail\n").expect("write order");

        let err = resolve_schema_source(root.join("schema.qail")).expect_err("should error");
        assert!(err.contains("escapes schema root"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn watch_paths_include_order_file() {
        let root = tmp_dir("order_watch");
        let schema_dir = root.join("schema");
        fs::create_dir_all(&schema_dir).expect("mkdir schema");
        fs::write(
            schema_dir.join("user.qail"),
            "table users {\n  id uuid primary_key\n}\n",
        )
        .expect("write user");
        fs::write(schema_dir.join(MODULE_ORDER_FILE), "user.qail\n").expect("write order");

        let resolved = resolve_schema_source(root.join("schema.qail")).expect("resolved");
        let watch_paths = resolved.watch_paths();
        assert!(watch_paths.iter().any(|p| p.ends_with(MODULE_ORDER_FILE)));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn strict_manifest_default_from_env() {
        let root = tmp_dir("strict_env");
        fs::create_dir_all(&root).expect("mkdir");
        // SAFETY: test mutates process env, keep scoped and restore after test.
        unsafe { std::env::set_var(STRICT_ENV_VAR, "true") };
        assert!(strict_manifest_default_enabled(&root).expect("strict manifest should parse"));
        // SAFETY: restore env for test isolation.
        unsafe { std::env::remove_var(STRICT_ENV_VAR) };
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn strict_manifest_default_from_ancestor_qail_toml() {
        let root = tmp_dir("strict_cfg");
        let schema_dir = root.join("schema");
        fs::create_dir_all(&schema_dir).expect("mkdir schema");
        fs::write(
            root.join("qail.toml"),
            "[project]\nname = \"strict-cfg\"\nschema_strict_manifest = true\n",
        )
        .expect("write config");
        fs::write(
            schema_dir.join("users.qail"),
            "table users {\n  id uuid primary_key\n}\n",
        )
        .expect("write users");
        fs::write(
            schema_dir.join("billing.qail"),
            "table invoices {\n  id uuid primary_key\n}\n",
        )
        .expect("write billing");
        fs::write(schema_dir.join(MODULE_ORDER_FILE), "users.qail\n").expect("write order");

        let err = resolve_schema_source(root.join("schema.qail")).expect_err("should error");
        assert!(err.contains("strict manifest enabled"));
        assert!(err.contains("billing.qail"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn strict_manifest_default_from_malformed_ancestor_qail_toml_errors() {
        let root = tmp_dir("strict_cfg_malformed");
        let schema_dir = root.join("schema");
        fs::create_dir_all(&schema_dir).expect("mkdir schema");
        fs::write(
            root.join("qail.toml"),
            "[project\nschema_strict_manifest = true\n",
        )
        .expect("write malformed config");
        fs::write(
            schema_dir.join("users.qail"),
            "table users {\n  id uuid primary_key\n}\n",
        )
        .expect("write users");
        fs::write(schema_dir.join(MODULE_ORDER_FILE), "users.qail\n").expect("write order");

        let err = resolve_schema_source(root.join("schema.qail")).expect_err("should error");
        assert!(err.contains("Failed to load strict-manifest default"));
        assert!(err.contains("qail.toml"));

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn resolve_ignores_symlinked_outside_modules() {
        use std::os::unix::fs::symlink;

        let root = tmp_dir("symlink_outside");
        let schema_dir = root.join("schema");
        let outside_dir = root.join("outside");
        fs::create_dir_all(&schema_dir).expect("mkdir schema");
        fs::create_dir_all(&outside_dir).expect("mkdir outside");
        fs::write(
            schema_dir.join("users.qail"),
            "table users {\n  id uuid primary_key\n}\n",
        )
        .expect("write users");
        fs::write(
            outside_dir.join("leak.qail"),
            "table leaked {\n  id uuid primary_key\n}\n",
        )
        .expect("write leak");
        symlink(&outside_dir, schema_dir.join("ext")).expect("symlink outside");

        let resolved = resolve_schema_source(root.join("schema.qail")).expect("resolved");
        assert_eq!(resolved.files.len(), 1);
        assert!(resolved.files[0].ends_with("users.qail"));

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn resolve_ignores_symlink_directory_loops() {
        use std::os::unix::fs::symlink;

        let root = tmp_dir("symlink_loop");
        let schema_dir = root.join("schema");
        fs::create_dir_all(&schema_dir).expect("mkdir schema");
        fs::write(
            schema_dir.join("users.qail"),
            "table users {\n  id uuid primary_key\n}\n",
        )
        .expect("write users");
        symlink(&schema_dir, schema_dir.join("loop")).expect("symlink loop");

        let resolved = resolve_schema_source(root.join("schema.qail")).expect("resolved");
        assert_eq!(resolved.files.len(), 1);
        assert!(resolved.files[0].ends_with("users.qail"));

        let _ = fs::remove_dir_all(root);
    }
}
