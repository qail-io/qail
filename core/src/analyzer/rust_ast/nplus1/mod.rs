//! Compile-time N+1 query detection via `syn::Visit` AST traversal.
//!
//! Detects query executions inside loops at compile time, before the code runs.
//!
//! # Rules
//!
//! | Code   | Severity | Description |
//! |--------|----------|-------------|
//! | N1-001 | Warning  | Query execution inside a loop |
//! | N1-002 | Warning  | Loop variable used in query-building chain (suggests IN/ANY) |
//! | N1-003 | Warning  | Query-executing function/method called inside a work loop |
//! | N1-004 | Error    | Query execution inside nested loops (loop_depth ≥ 2) |
//!
//! # Suppression
//!
//! ```ignore
//! // qail-lint:disable-next-line N1-001
//! for item in items {
//!     conn.fetch_all(&query).await?;
//! }
//! ```

mod types;
mod patterns;
mod suppressions;
mod collector;
mod detector;
#[cfg(test)]
mod tests;

// ── Public API ──────────────────────────────────────────────────────
pub use types::{NPlusOneCode, NPlusOneDiagnostic, NPlusOneSeverity};

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use syn::visit::Visit;

use collector::{FunctionQueryCollector, compute_query_index_from_infos, FunctionCallInfo};
use detector::NPlusOneDetector;
use suppressions::parse_suppressions;

/// Detect N+1 patterns in a single Rust source file.
pub fn detect_n_plus_one_in_file(file: &str, source: &str) -> Vec<NPlusOneDiagnostic> {
    let Ok(syntax) = syn::parse_file(source) else {
        return vec![];
    };

    let suppressions = parse_suppressions(source);

    // Pass 1: collect functions that contain actual query *execution* calls
    let mut fn_collector = FunctionQueryCollector::new(Vec::new());
    fn_collector.visit_file(&syntax);
    let query_index = fn_collector.compute_query_index();

    // Pass 2: detect N+1 patterns
    let mut detector = NPlusOneDetector::new(
        file.to_string(),
        Vec::new(),
        suppressions,
        query_index.paths,
    );
    detector.visit_file(&syntax);

    detector.diagnostics
}

/// Detect N+1 patterns in all Rust files under a directory.
pub fn detect_n_plus_one_in_dir(dir: &Path) -> Vec<NPlusOneDiagnostic> {
    let files = collect_rust_files(dir);
    if files.is_empty() {
        return Vec::new();
    }

    // Pass 1: build module-level symbol index across files.
    let mut global_infos: HashMap<String, FunctionCallInfo> = HashMap::new();
    let mut file_entries: Vec<(PathBuf, String, Vec<String>)> = Vec::new();
    for path in files {
        let Ok(source) = std::fs::read_to_string(&path) else {
            continue;
        };
        let module_prefix = module_prefix_for_file(dir, &path);
        file_entries.push((path.clone(), source.clone(), module_prefix.clone()));

        let Ok(syntax) = syn::parse_file(&source) else {
            continue;
        };
        let mut collector = FunctionQueryCollector::new(module_prefix);
        collector.visit_file(&syntax);
        collector.merge_into(&mut global_infos);
    }

    let query_index = compute_query_index_from_infos(&global_infos);

    // Pass 2: run detection using global propagated index.
    let mut diagnostics = Vec::new();
    for (path, source, module_prefix) in file_entries {
        let Ok(syntax) = syn::parse_file(&source) else {
            continue;
        };
        let suppressions = parse_suppressions(&source);
        let mut detector = NPlusOneDetector::new(
            path.display().to_string(),
            module_prefix,
            suppressions,
            query_index.paths.clone(),
        );
        detector.visit_file(&syntax);
        diagnostics.extend(detector.diagnostics);
    }
    diagnostics
}

fn collect_rust_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    collect_rust_files_recursive(dir, &mut out);
    out
}

fn collect_rust_files_recursive(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str())
                && (name.starts_with('.') || name == "target")
            {
                continue;
            }
            collect_rust_files_recursive(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

fn module_prefix_for_file(root: &Path, file: &Path) -> Vec<String> {
    let Ok(rel) = file.strip_prefix(root) else {
        return Vec::new();
    };

    let mut segs: Vec<String> = rel
        .parent()
        .into_iter()
        .flat_map(|p| p.components())
        .filter_map(|c| c.as_os_str().to_str())
        .map(ToOwned::to_owned)
        .collect();

    let file_stem = rel.file_stem().and_then(|s| s.to_str()).unwrap_or_default();
    let file_name = rel.file_name().and_then(|s| s.to_str()).unwrap_or_default();

    if file_name != "mod.rs" && file_stem != "lib" && file_stem != "main" && !file_stem.is_empty() {
        segs.push(file_stem.to_string());
    }
    segs
}
