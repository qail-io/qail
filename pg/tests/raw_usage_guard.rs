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

fn is_allowlisted(path: &Path) -> bool {
    let _ = path;
    false
}

#[test]
fn raw_sql_callsites_are_confined_to_allowlist() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let src_root = root.join("src");

    let needles = ["execute_raw(", "fetch_raw("];

    let mut files = Vec::new();
    collect_rs_files(&src_root, &mut files);

    let mut offenders = Vec::new();
    for file in files {
        let Ok(src) = fs::read_to_string(&file) else {
            continue;
        };
        if needles.iter().any(|needle| src.contains(needle)) && !is_allowlisted(&file) {
            offenders.push(file.display().to_string());
        }
    }

    assert!(
        offenders.is_empty(),
        "Raw SQL API usage escaped allowlist:\n{}",
        offenders.join("\n")
    );
}
