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
            if name == "target" {
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
    path.ends_with("raw_usage_guard.rs")
}

#[test]
fn raw_sql_callsites_are_forbidden_in_gateway_source_and_tests() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let scan_roots = [root.join("src"), root.join("tests")];
    let needles = ["execute_raw(", "fetch_raw("];

    let mut offenders = Vec::new();
    for scan_root in scan_roots {
        let mut files = Vec::new();
        collect_rs_files(&scan_root, &mut files);
        for file in files {
            if should_skip(&file) {
                continue;
            }
            let Ok(src) = fs::read_to_string(&file) else {
                continue;
            };
            if needles.iter().any(|needle| src.contains(needle)) {
                offenders.push(file.display().to_string());
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "Raw SQL API usage found in gateway source/tests:\n{}",
        offenders.join("\n")
    );
}
