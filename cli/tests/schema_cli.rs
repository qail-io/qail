use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

struct TempProject {
    root: PathBuf,
}

impl TempProject {
    fn new(tag: &str) -> Self {
        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be monotonic")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "qail_cli_schema_{tag}_{}_{}",
            std::process::id(),
            now_nanos
        ));
        fs::create_dir_all(&root).expect("failed to create temp project root");
        Self { root }
    }

    fn write(&self, rel: &str, content: &str) {
        let path = self.path(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("failed to create parent dirs");
        }
        fs::write(path, content).expect("failed to write temp project file");
    }

    fn path(&self, rel: &str) -> PathBuf {
        self.root.join(rel)
    }
}

impl Drop for TempProject {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn qail_bin() -> PathBuf {
    if let Some(path) = std::env::var_os("CARGO_BIN_EXE_qail") {
        let path = PathBuf::from(path);
        return if path.is_absolute() {
            path
        } else {
            std::env::current_dir()
                .expect("current_dir should be available")
                .join(path)
        };
    }

    let exe = if cfg!(windows) { "qail.exe" } else { "qail" };
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .expect("cli crate should live in workspace root");
    let target_dir = match std::env::var_os("CARGO_TARGET_DIR") {
        Some(path) => {
            let path = PathBuf::from(path);
            if path.is_absolute() {
                path
            } else {
                workspace_root.join(path)
            }
        }
        None => workspace_root.join("target"),
    };
    let fallback = target_dir.join("debug").join(exe);
    assert!(
        fallback.is_file(),
        "unable to locate qail binary at '{}' and CARGO_BIN_EXE_qail is unset",
        fallback.display()
    );
    fallback
}

fn run_qail<I, S>(cwd: &Path, args: I) -> Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    Command::new(qail_bin())
        .current_dir(cwd)
        .args(args)
        .output()
        .expect("failed to execute qail binary")
}

fn output_text(output: &Output) -> String {
    format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    )
}

#[test]
fn schema_split_doctor_merge_cli_flow() {
    let project = TempProject::new("flow");
    project.write(
        "schema.qail",
        r#"
table users {
  id uuid primary_key
  email text unique
}

table posts {
  id uuid primary_key
  user_id uuid references users(id)
}
"#,
    );

    let out = run_qail(
        &project.root,
        ["schema", "split", "schema.qail", "-o", "schema"],
    );
    assert!(out.status.success(), "{}", output_text(&out));
    assert!(project.path("schema/_order.qail").is_file());
    assert!(project.path("schema/users.qail").is_file());
    assert!(project.path("schema/posts.qail").is_file());

    let out = run_qail(&project.root, ["schema", "doctor", "schema"]);
    assert!(out.status.success(), "{}", output_text(&out));

    let out = run_qail(
        &project.root,
        ["schema", "merge", "schema", "-o", "merged.qail"],
    );
    assert!(out.status.success(), "{}", output_text(&out));

    let merged = fs::read_to_string(project.path("merged.qail")).expect("read merged schema");
    assert!(merged.contains("table users"), "{merged}");
    assert!(merged.contains("table posts"), "{merged}");
    assert!(merged.contains("references users(id)"), "{merged}");
}

#[test]
fn schema_doctor_honors_qail_toml_strict_manifest_default() {
    let project = TempProject::new("strict_default");
    project.write(
        "qail.toml",
        r#"
[project]
name = "strict-default"
schema_strict_manifest = true
"#,
    );
    project.write(
        "schema/users.qail",
        r#"
table users {
  id uuid primary_key
}
"#,
    );
    project.write(
        "schema/posts.qail",
        r#"
table posts {
  id uuid primary_key
}
"#,
    );
    project.write("schema/_order.qail", "users.qail\n");

    let schema_abs = project.path("schema");
    let schema_arg = schema_abs.to_string_lossy().to_string();

    // Intentionally run from outside the project root to verify schema-path based
    // qail.toml discovery (not process CWD lookup).
    let out = run_qail(
        std::env::temp_dir().as_path(),
        ["schema", "doctor", &schema_arg],
    );
    assert!(!out.status.success(), "{}", output_text(&out));
    let text = output_text(&out);
    assert!(text.contains("Strict manifest enabled"), "{text}");
    assert!(text.contains("posts.qail"), "{text}");
}

#[test]
fn schema_doctor_strict_flag_fails_on_warnings() {
    let project = TempProject::new("doctor_strict_flag");
    project.write(
        "schema/users.qail",
        r#"
table users {
  id uuid primary_key
}
"#,
    );
    project.write(
        "schema/posts.qail",
        r#"
table posts {
  id uuid primary_key
}
"#,
    );
    project.write("schema/_order.qail", "users.qail\n");

    let out = run_qail(&project.root, ["schema", "doctor", "schema"]);
    assert!(out.status.success(), "{}", output_text(&out));
    assert!(
        output_text(&out).contains("warning"),
        "{}",
        output_text(&out)
    );

    let out = run_qail(&project.root, ["schema", "doctor", "schema", "--strict"]);
    assert!(!out.status.success(), "{}", output_text(&out));
    let text = output_text(&out);
    assert!(text.contains("warning"), "{text}");
}
