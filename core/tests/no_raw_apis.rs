use std::fs;
use std::path::{Path, PathBuf};

fn collect_rs_files(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            let name = path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or_default();
            if name == "tests" || name == "examples" || name == "target" {
                continue;
            }
            collect_rs_files(&path, out);
            continue;
        }
        if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

fn should_skip(path: &Path) -> bool {
    let p = path.to_string_lossy();
    p.ends_with("/core/src/build/sql_guard.rs")
        || p.ends_with("/core/src/build/scanner.rs")
        || p.ends_with("/core/src/build/tests.rs")
}

#[test]
fn runtime_source_has_no_removed_raw_ast_apis() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("core crate must have repo root parent");

    let scan_roots = [
        repo_root.join("core/src"),
        repo_root.join("pg/src"),
        repo_root.join("gateway/src"),
    ];

    let banned = [
        "pub fn raw_sql(",
        "pub fn is_raw_sql(",
        "Expr::Raw",
        ".raw_where(",
        ".execute_raw(",
        ".fetch_raw(",
    ];

    let mut hits: Vec<String> = Vec::new();
    for root in scan_roots {
        let mut files = Vec::new();
        collect_rs_files(&root, &mut files);
        for file in files {
            if should_skip(&file) {
                continue;
            }
            let Ok(src) = fs::read_to_string(&file) else {
                continue;
            };
            for needle in &banned {
                if src.contains(needle) {
                    hits.push(format!("{} contains '{}'", file.display(), needle));
                }
            }
        }
    }

    assert!(
        hits.is_empty(),
        "Removed raw AST APIs were reintroduced:\n{}",
        hits.join("\n")
    );
}
