//! Validation pipeline: schema validation, RLS audit, N+1 detection, SQL policy.

use std::collections::HashSet;
use std::path::Path;

use super::scanner::{QailUsage, scan_source_files};
use super::schema::Schema;

fn has_explicit_tenant_scope(cmd: &crate::ast::Qail) -> bool {
    cmd.cages.iter().any(|cage| {
        matches!(
            cage.kind,
            crate::ast::CageKind::Filter | crate::ast::CageKind::Payload
        ) && cage
            .conditions
            .iter()
            .any(is_explicit_tenant_scope_condition)
    })
}

fn is_explicit_tenant_scope_condition(cond: &crate::ast::Condition) -> bool {
    let crate::ast::Expr::Named(raw_left) = &cond.left else {
        return false;
    };
    if !is_tenant_identifier(raw_left) {
        return false;
    }
    matches!(
        cond.op,
        crate::ast::Operator::Eq | crate::ast::Operator::IsNull
    )
}

fn is_tenant_identifier(raw_ident: &str) -> bool {
    let without_cast = raw_ident.split("::").next().unwrap_or(raw_ident).trim();
    let last_segment = without_cast.rsplit('.').next().unwrap_or(without_cast);
    let normalized = last_segment
        .trim_matches('"')
        .trim_matches('`')
        .to_ascii_lowercase();
    normalized == "tenant_id"
}

/// Validation diagnostic category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationDiagnosticKind {
    /// Hard schema validation failure (must fail build).
    SchemaError,
    /// Advisory RLS audit warning.
    RlsWarning,
}

/// Structured diagnostic emitted by build validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationDiagnostic {
    pub kind: ValidationDiagnosticKind,
    pub message: String,
}

impl ValidationDiagnostic {
    fn schema_error(message: String) -> Self {
        Self {
            kind: ValidationDiagnosticKind::SchemaError,
            message,
        }
    }

    fn rls_warning(message: String) -> Self {
        Self {
            kind: ValidationDiagnosticKind::RlsWarning,
            message,
        }
    }
}

#[cold]
#[inline(never)]
fn fail_build(message: impl AsRef<str>) -> ! {
    let msg = message.as_ref();
    println!("cargo:warning={}", msg);
    eprintln!("{}", msg);
    std::process::exit(1);
}

/// Provides "Did you mean?" suggestions for typos, type validation, and RLS audit
pub fn validate_against_schema_diagnostics(
    schema: &Schema,
    usages: &[QailUsage],
) -> Vec<ValidationDiagnostic> {
    use crate::validator::Validator;

    // Build Validator from Schema with column types
    let mut validator = Validator::new();
    for (table_name, table_schema) in &schema.tables {
        // Convert HashMap<String, ColumnType> to Vec<(&str, &str)> for validator
        let type_strings: Vec<(String, String)> = table_schema
            .columns
            .iter()
            .map(|(name, typ)| (name.clone(), typ.to_pg_type()))
            .collect();
        let cols_with_types: Vec<(&str, &str)> = type_strings
            .iter()
            .map(|(name, typ)| (name.as_str(), typ.as_str()))
            .collect();
        validator.add_table_with_types(table_name, &cols_with_types);
    }

    let mut diagnostics = Vec::new();
    let mut seen_diagnostics: HashSet<String> = HashSet::new();
    let mut push_unique = |diag: ValidationDiagnostic| {
        let key = format!("{:?}|{}", diag.kind, diag.message);
        if seen_diagnostics.insert(key) {
            diagnostics.push(diag);
        }
    };
    let query_ir = super::query_ir::build_query_ir(usages);

    for query in query_ir {
        // Skip CTE alias refs — but only if the name doesn't also exist as a
        // real schema table. If there's a collision (CTE alias == real table name),
        // always validate to avoid false negatives.
        if query.is_cte_ref && !schema.has_table(&query.table) {
            continue;
        }

        // Skip unresolvable dynamic table names only.
        // Static literals must be validated so typos are caught reliably.
        if query.is_dynamic_table && !schema.has_table(&query.table) {
            continue;
        }

        // ── Validate canonical query IR ───────────────────────────────
        match validator.validate_command(&query.cmd) {
            Ok(()) => {}
            Err(validation_errors) => {
                for e in validation_errors {
                    push_unique(ValidationDiagnostic::schema_error(format!(
                        "{}:{}: {}",
                        query.file, query.line, e
                    )));
                }
            }
        }

        // RLS Audit: warn if query targets RLS-enabled table without .with_rls()
        if schema.is_rls_table(&query.table) && !query.has_rls {
            push_unique(ValidationDiagnostic::rls_warning(format!(
                "{}:{}: ⚠️ RLS AUDIT: Qail::{}(\"{}\") has no .with_rls() — table has RLS enabled, query may leak tenant data",
                query.file,
                query.line,
                query.action.to_lowercase(),
                query.table
            )));
        }

        // SuperAdmin Audit: warn if file uses for_system_process() and queries
        // a table that has tenant_id (tenant-scoped). This catches cases where
        // acquire_with_rls(SuperAdmin) bypasses tenant isolation at the
        // connection level — invisible to the per-command .with_rls() check.
        if query.file_uses_super_admin {
            let table_has_tenant_id = schema
                .table(&query.table)
                .map(|t| t.has_column("tenant_id"))
                .unwrap_or(false);
            if table_has_tenant_id
                && !(query.has_explicit_tenant_scope || has_explicit_tenant_scope(&query.cmd))
            {
                push_unique(ValidationDiagnostic::rls_warning(format!(
                    "{}:{}: ⚠️ RLS AUDIT: Qail::{}(\"{}\") in file using SuperAdminToken::for_system_process() \
   — query has no explicit tenant scope (`tenant_id = ...` or `tenant_id IS NULL`) and may bypass tenant isolation. \
Use claims-based scoping, `RlsContext::global()` for shared data, or add explicit tenant scope. If intentional, add `// qail:allow(super_admin)`.",
                    query.file,
                    query.line,
                    query.action.to_lowercase(),
                    query.table
                )));
            }
        }
    }

    diagnostics
}

/// Backward-compatible string diagnostics output.
pub fn validate_against_schema(schema: &Schema, usages: &[QailUsage]) -> Vec<String> {
    validate_against_schema_diagnostics(schema, usages)
        .into_iter()
        .map(|d| d.message)
        .collect()
}

/// Run N+1 compile-time check.
///
/// Controlled by environment variables:
/// - `QAIL_NPLUS1`: `off` | `warn` (default) | `deny`
/// - `QAIL_NPLUS1_MAX_WARNINGS`: max warnings before truncation (default 50)
fn run_nplus1_check(src_dir: &str) {
    use super::nplus1_semantic::{NPlusOneSeverity, detect_n_plus_one_in_dir};

    println!("cargo:rerun-if-env-changed=QAIL_NPLUS1");
    println!("cargo:rerun-if-env-changed=QAIL_NPLUS1_MAX_WARNINGS");

    let mode = std::env::var("QAIL_NPLUS1").unwrap_or_else(|_| "warn".to_string());

    if mode == "off" || mode == "false" || mode == "0" {
        return;
    }

    let max_warnings: usize = std::env::var("QAIL_NPLUS1_MAX_WARNINGS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);

    let diagnostics = detect_n_plus_one_in_dir(Path::new(src_dir));

    if diagnostics.is_empty() {
        println!("cargo:warning=QAIL: N+1 scan clean ✓");
        return;
    }

    let total = diagnostics.len();
    let shown = total.min(max_warnings);

    for diag in diagnostics.iter().take(shown) {
        let prefix = match diag.severity {
            NPlusOneSeverity::Error => "QAIL N+1 ERROR",
            NPlusOneSeverity::Warning => "QAIL N+1",
        };
        println!("cargo:warning={}: {}", prefix, diag);
    }

    if total > shown {
        println!(
            "cargo:warning=QAIL N+1: ... and {} more (set QAIL_NPLUS1_MAX_WARNINGS to see all)",
            total - shown
        );
    }

    if mode == "deny" {
        fail_build(format!(
            "QAIL N+1: {} diagnostic(s) found. Fix N+1 patterns or set QAIL_NPLUS1=warn",
            total
        ));
    }
}

fn configured_scan_roots() -> Vec<String> {
    println!("cargo:rerun-if-env-changed=QAIL_SCAN_DIRS");

    let raw = std::env::var("QAIL_SCAN_DIRS").unwrap_or_else(|_| "src".to_string());
    let mut roots: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
        .collect();

    roots.sort();
    roots.dedup();

    if roots.is_empty() {
        vec!["src".to_string()]
    } else {
        roots
    }
}

fn scan_all_roots(scan_roots: &[String]) -> Vec<QailUsage> {
    let mut usages = Vec::new();
    for root in scan_roots {
        usages.extend(scan_source_files(root));
    }
    usages
}

fn emit_scan_watchers(scan_roots: &[String]) {
    for root in scan_roots {
        println!("cargo:rerun-if-changed={}", root);
    }
}

fn emit_validation_results(
    diagnostics: &[ValidationDiagnostic],
    usage_count: usize,
    mode_label: &str,
) {
    let schema_errors: Vec<_> = diagnostics
        .iter()
        .filter(|d| matches!(d.kind, ValidationDiagnosticKind::SchemaError))
        .collect();
    let rls_warnings: Vec<_> = diagnostics
        .iter()
        .filter(|d| matches!(d.kind, ValidationDiagnosticKind::RlsWarning))
        .collect();

    for warning in &rls_warnings {
        println!("cargo:warning=QAIL RLS: {}", warning.message);
    }

    if schema_errors.is_empty() {
        println!(
            "cargo:warning=QAIL: Validated {} queries against {} ✓",
            usage_count, mode_label
        );
        return;
    }

    for error in &schema_errors {
        println!("cargo:warning=QAIL ERROR: {}", error.message);
    }

    fail_build(format!(
        "QAIL validation failed with {} errors",
        schema_errors.len()
    ));
}

fn run_nplus1_checks(scan_roots: &[String]) {
    for root in scan_roots {
        run_nplus1_check(root);
    }
}

/// Run raw SQL policy check.
///
/// Controlled by environment variables:
/// - `QAIL_SQL`: `off` (default) | `warn` | `deny`
/// - `QAIL_SQL_MAX_WARNINGS`: max warnings before truncation (default 50)
fn run_sql_policy_checks(scan_roots: &[String]) {
    println!("cargo:rerun-if-env-changed=QAIL_SQL");
    println!("cargo:rerun-if-env-changed=QAIL_SQL_MAX_WARNINGS");

    let mode = std::env::var("QAIL_SQL").unwrap_or_else(|_| "off".to_string());
    if mode == "off" || mode == "false" || mode == "0" {
        return;
    }

    let max_warnings: usize = std::env::var("QAIL_SQL_MAX_WARNINGS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);

    let mut all = Vec::new();
    for root in scan_roots {
        all.extend(super::sql_guard::detect_sql_usage_in_dir(Path::new(root)));
    }

    if all.is_empty() {
        println!("cargo:warning=QAIL: SQL policy scan clean ✓");
        return;
    }

    let total = all.len();
    let shown = total.min(max_warnings);

    for diag in all.iter().take(shown) {
        println!(
            "cargo:warning=QAIL SQL {}: {}:{}:{}: {}",
            diag.code, diag.file, diag.line, diag.column, diag.message
        );
    }
    if total > shown {
        println!(
            "cargo:warning=QAIL SQL: ... and {} more (set QAIL_SQL_MAX_WARNINGS to see all)",
            total - shown
        );
    }

    if mode == "deny" {
        fail_build(format!(
            "QAIL SQL policy failed with {} diagnostic(s). Migrate raw SQL to QAIL DSL or set QAIL_SQL=warn",
            total
        ));
    }
}

/// Build validation entrypoint for build.rs.
/// Failures are reported via `cargo:warning` and process exit code 1.
pub fn validate() {
    let mode = std::env::var("QAIL").unwrap_or_else(|_| {
        if Path::new("schema.qail").exists() || Path::new("schema").is_dir() {
            "schema".to_string()
        } else {
            "false".to_string()
        }
    });

    match mode.as_str() {
        "schema" => {
            let scan_roots = configured_scan_roots();
            if let Ok(source) = crate::schema_source::resolve_schema_source("schema.qail") {
                for path in source.watch_paths() {
                    println!("cargo:rerun-if-changed={}", path.display());
                }
            } else {
                // Keep backward-compatible watcher even if resolution fails;
                // parse step below will emit the concrete error.
                println!("cargo:rerun-if-changed=schema.qail");
                println!("cargo:rerun-if-changed=schema");
            }
            println!("cargo:rerun-if-changed=migrations");
            println!("cargo:rerun-if-env-changed=QAIL");
            emit_scan_watchers(&scan_roots);

            match Schema::parse_file("schema.qail") {
                Ok(mut schema) => {
                    // Merge pending migrations with pulled schema
                    let merged = match schema.merge_migrations("migrations") {
                        Ok(n) => n,
                        Err(e) => {
                            println!("cargo:warning=QAIL: Migration merge failed: {}", e);
                            0
                        }
                    };
                    if merged > 0 {
                        println!(
                            "cargo:warning=QAIL: Merged {} schema changes from migrations",
                            merged
                        );
                    }

                    let usages = scan_all_roots(&scan_roots);
                    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);
                    emit_validation_results(&diagnostics, usages.len(), "schema source");

                    // ── N+1 detection ──────────────────────────────────────
                    run_nplus1_checks(&scan_roots);
                    run_sql_policy_checks(&scan_roots);
                }
                Err(e) => {
                    fail_build(format!("QAIL: Failed to parse schema source: {}", e));
                }
            }
        }
        "live" => {
            let scan_roots = configured_scan_roots();
            println!("cargo:rerun-if-env-changed=QAIL");
            println!("cargo:rerun-if-env-changed=DATABASE_URL");
            emit_scan_watchers(&scan_roots);

            // Get DATABASE_URL for qail pull
            let db_url = match std::env::var("DATABASE_URL") {
                Ok(url) => url,
                Err(_) => {
                    fail_build("QAIL=live requires DATABASE_URL environment variable");
                }
            };

            // Step 1: Run qail pull to update schema.qail
            println!("cargo:warning=QAIL: Pulling schema from live database...");

            let pull_result = std::process::Command::new("qail")
                .args(["pull", &db_url])
                .output();

            match pull_result {
                Ok(output) => {
                    if !output.status.success() {
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        fail_build(format!("QAIL: Failed to pull schema: {}", stderr));
                    }
                    println!("cargo:warning=QAIL: Schema pulled successfully ✓");
                }
                Err(e) => {
                    // qail CLI not found, try using cargo run
                    println!("cargo:warning=QAIL: qail CLI not in PATH, trying cargo...");

                    let cargo_result = std::process::Command::new("cargo")
                        .args(["run", "-p", "qail", "--", "pull", &db_url])
                        .current_dir(
                            std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string()),
                        )
                        .output();

                    match cargo_result {
                        Ok(output) if output.status.success() => {
                            println!("cargo:warning=QAIL: Schema pulled via cargo ✓");
                        }
                        _ => {
                            fail_build(format!(
                                "QAIL: Cannot run qail pull: {}. Install qail CLI or set QAIL=schema",
                                e
                            ));
                        }
                    }
                }
            }

            // Step 2: Parse the updated schema and validate
            match Schema::parse_file("schema.qail") {
                Ok(mut schema) => {
                    // Merge pending migrations (in case live DB doesn't have them yet)
                    let merged = match schema.merge_migrations("migrations") {
                        Ok(n) => n,
                        Err(e) => {
                            println!("cargo:warning=QAIL: Migration merge failed: {}", e);
                            0
                        }
                    };
                    if merged > 0 {
                        println!(
                            "cargo:warning=QAIL: Merged {} schema changes from pending migrations",
                            merged
                        );
                    }

                    let usages = scan_all_roots(&scan_roots);
                    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);
                    emit_validation_results(&diagnostics, usages.len(), "live database");

                    // ── N+1 detection ──────────────────────────────────────
                    run_nplus1_checks(&scan_roots);
                    run_sql_policy_checks(&scan_roots);
                }
                Err(e) => {
                    fail_build(format!("QAIL: Failed to parse schema after pull: {}", e));
                }
            }
        }
        "false" | "off" | "0" => {
            println!("cargo:rerun-if-env-changed=QAIL");
            // Silently skip validation
        }
        _ => {
            fail_build(format!(
                "QAIL: Unknown mode '{}'. Use: schema, live, or false",
                mode
            ));
        }
    }
}
