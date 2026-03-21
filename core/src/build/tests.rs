//! Tests for build-time validation.

use super::scanner::*;
use super::schema::*;
use super::validate::*;

#[test]
fn test_parse_schema() {
    // Format matches qail pull output (space-separated, not colon)
    let content = r#"
# Test schema

table users {
  id UUID primary_key
  name TEXT not_null
  email TEXT unique
}

table posts {
  id UUID
  user_id UUID
  title TEXT
}
"#;
    let schema = Schema::parse(content).unwrap();
    assert!(schema.has_table("users"));
    assert!(schema.has_table("posts"));
    assert!(schema.table("users").unwrap().has_column("id"));
    assert!(schema.table("users").unwrap().has_column("name"));
    assert!(!schema.table("users").unwrap().has_column("foo"));
}

#[test]
fn test_parse_schema_skips_double_dash_comments_in_table_block() {
    let content = r#"
table users {
  id UUID
  -- this is a comment and must not become a column
  email TEXT
}
"#;
    let schema = Schema::parse(content).unwrap();
    let users = schema.table("users").unwrap();
    assert!(users.has_column("id"));
    assert!(users.has_column("email"));
    assert!(!users.has_column("--"));
}

#[test]
fn test_parse_schema_unclosed_table_is_error() {
    let content = r#"
table users {
  id UUID
"#;
    let err = Schema::parse(content).expect_err("unclosed table must fail");
    assert!(err.contains("Unclosed table definition"));
    assert!(err.contains("users"));
}

#[test]
fn test_parse_schema_tracks_views() {
    let content = r#"
table users {
  id UUID
}

view v_users $$
SELECT id
FROM users
$$

materialized view mv_users $$
SELECT id
FROM users
$$
"#;

    let schema = Schema::parse(content).unwrap();
    assert!(schema.has_table("users"));
    assert!(schema.has_table("v_users"));
    assert!(schema.has_table("mv_users"));
}

#[test]
fn test_extract_string_arg() {
    assert_eq!(extract_string_arg(r#""users")"#), Some("users".to_string()));
    assert_eq!(
        extract_string_arg(r#""table_name")"#),
        Some("table_name".to_string())
    );
}

#[test]
fn test_scan_file() {
    // Test single-line pattern
    let content = r#"
let query = Qail::get("users").column("id").column("name").eq("active", true);
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 1);
    assert_eq!(usages[0].table, "users");
    assert_eq!(usages[0].action, "GET");
    assert!(usages[0].columns.contains(&"id".to_string()));
    assert!(usages[0].columns.contains(&"name".to_string()));
}

#[test]
fn test_scan_file_multiple_qail_chains_same_line() {
    let content = r#"
let a = Qail::get("users").column("id"); let b = Qail::get("orders").column("status");
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 2);
    assert_eq!(usages[0].table, "users");
    assert_eq!(usages[1].table, "orders");
}

#[test]
fn test_scan_file_multiline() {
    // Test multi-line chain pattern (common in real code)
    let content = r#"
let query = Qail::get("posts")
    .column("id")
    .column("title")
    .column("author")
    .eq("published", true)
    .order_by("created_at", Desc);
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 1);
    assert_eq!(usages[0].table, "posts");
    assert_eq!(usages[0].action, "GET");
    assert!(usages[0].columns.contains(&"id".to_string()));
    assert!(usages[0].columns.contains(&"title".to_string()));
    assert!(usages[0].columns.contains(&"author".to_string()));
}

#[test]
fn test_scan_file_multiline_array() {
    // Regression: multi-line array args like .columns(&["a", "b", ...])
    // must NOT truncate the chain — methods after the array must be scanned.
    let content = r#"
let cmd = Qail::get("orders")
    .columns(&[
        "id",
        "booking_number",
        "status",
    ])
    .eq("guest_phone", phone)
    .order_desc("created_at")
    .limit(20);
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 1);
    assert_eq!(usages[0].table, "orders");
    // Columns from .columns(&[...])
    assert!(usages[0].columns.contains(&"id".to_string()));
    assert!(usages[0].columns.contains(&"booking_number".to_string()));
    assert!(usages[0].columns.contains(&"status".to_string()));
    // Column from .eq() — MUST be captured (was the original bug)
    assert!(
        usages[0].columns.contains(&"guest_phone".to_string()),
        "expected 'guest_phone' from .eq() after multi-line array, got: {:?}",
        usages[0].columns
    );
    // Column from .order_desc()
    assert!(usages[0].columns.contains(&"created_at".to_string()));
}

#[test]
fn test_count_net_delimiters() {
    assert_eq!(count_net_delimiters("foo("), 1);
    assert_eq!(count_net_delimiters("foo()"), 0);
    assert_eq!(count_net_delimiters(".columns(&["), 2);
    assert_eq!(count_net_delimiters("])"), -2);
    assert_eq!(count_net_delimiters(r#""hello(""#), 0); // inside string
    assert_eq!(count_net_delimiters(""), 0);
}

#[test]
fn test_scan_typed_api() {
    let content = r#"
let q = Qail::typed(users::table).column("email");
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 1);
    assert_eq!(usages[0].table, "users");
    assert_eq!(usages[0].action, "TYPED");
    assert!(usages[0].columns.contains(&"email".to_string()));
}

#[test]
fn test_scan_raw_sql_not_validated() {
    let content = r#"
let q = Qail::raw_sql("SELECT * FROM users");
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);
    // raw_sql should NOT produce a QailUsage — it just emits a warning
    assert_eq!(usages.len(), 0);
}

#[test]
fn test_extract_columns_is_null() {
    let line = r#"Qail::get("t").is_null("deleted_at").is_not_null("name")"#;
    let cols = extract_columns(line);
    assert!(cols.contains(&"deleted_at".to_string()));
    assert!(cols.contains(&"name".to_string()));
}

#[test]
fn test_extract_columns_set_value() {
    let line =
        r#"Qail::set("orders").set_value("status", "Paid").set_coalesce("notes", "default")"#;
    let cols = extract_columns(line);
    assert!(cols.contains(&"status".to_string()));
    assert!(cols.contains(&"notes".to_string()));
}

#[test]
fn test_extract_columns_returning() {
    let line = r#"Qail::add("orders").returning(["id", "status"])"#;
    let cols = extract_columns(line);
    assert!(cols.contains(&"id".to_string()));
    assert!(cols.contains(&"status".to_string()));
}

#[test]
fn test_extract_columns_on_conflict() {
    let line = r#"Qail::put("t").on_conflict_update(&["id"], &[("name", Expr::Named("excluded.name".into()))])"#;
    let cols = extract_columns(line);
    assert!(cols.contains(&"id".to_string()));
}

#[test]
fn test_validate_against_schema_casted_column_no_false_positive() {
    let schema = Schema::parse(
        r#"
table users {
  id TEXT
}
"#,
    )
    .unwrap();

    let content = r#"
let q = Qail::get("users").eq("id::text", "abc");
"#;

    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);
    let errors = validate_against_schema(&schema, &usages);
    assert!(
        errors.is_empty(),
        "casted column should not produce schema error: {:?}",
        errors
    );
}

#[test]
fn test_validate_against_schema_view_table_name_is_allowed() {
    let schema = Schema::parse(
        r#"
table users {
  id UUID
}

view v_users $$
SELECT id
FROM users
$$
"#,
    )
    .unwrap();

    let content = r#"
let q = Qail::get("v_users").column("v_users.id").eq("v_users.some_projection", "x");
"#;

    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);
    let errors = validate_against_schema(&schema, &usages);

    assert!(
        errors.is_empty(),
        "view-backed query should not fail table validation: {:?}",
        errors
    );
}

#[test]
fn test_cte_cross_chain_detection() {
    // Chain 1 defines CTE "agg" via .to_cte(), chain 2 uses Qail::get("agg")
    // File-level CTE detection means chain 2 IS recognized as a CTE ref
    let content = r#"
let cte = Qail::get("orders").columns(["total"]).to_cte("agg");
let q = Qail::get("agg").columns(["total"]);
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 2);
    // Chain 1: GET on "orders", not a CTE ref
    assert_eq!(usages[0].table, "orders");
    assert!(!usages[0].is_cte_ref);
    // Chain 2: "agg" is recognized as CTE alias from chain 1
    assert_eq!(usages[1].table, "agg");
    assert!(usages[1].is_cte_ref);
}

#[test]
fn test_cte_with_inline_detection() {
    // .with("alias", query) should also be detected as CTE
    let content = r#"
let q = Qail::get("results").with("agg", Qail::get("orders"));
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    // "results" is the main table
    assert_eq!(usages.len(), 1);
    // It should NOT be a CTE ref since "results" != "agg"
    assert!(!usages[0].is_cte_ref);
}

#[test]
fn test_cte_with_non_qail_rhs_not_marked_as_cte_alias() {
    let content = r#"
let q = Qail::get("results").with("agg", some_non_qail_value);
let read = Qail::get("agg").column("id");
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 2);
    assert_eq!(usages[1].table, "agg");
    assert!(
        !usages[1].is_cte_ref,
        "non-Qail .with() rhs should not create a CTE alias"
    );
}

#[test]
fn test_diagnostics_kind_not_based_on_substring() {
    let schema = Schema::parse(
        r#"
table users {
  id UUID
}
"#,
    )
    .unwrap();

    let usages = vec![QailUsage {
        file: "test.rs".to_string(),
        line: 1,
        column: 1,
        table: "RLS AUDIT_users".to_string(),
        is_dynamic_table: false,
        columns: vec!["id".to_string()],
        action: "GET".to_string(),
        is_cte_ref: false,
        has_rls: false,
        has_explicit_tenant_scope: false,
        file_uses_super_admin: false,
    }];

    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);
    assert!(
        diagnostics
            .iter()
            .any(|d| matches!(d.kind, ValidationDiagnosticKind::SchemaError)),
        "table error must remain schema-fatal even when text contains 'RLS AUDIT'"
    );
}

#[test]
fn test_schema_validation_unknown_static_table_without_underscore_is_error() {
    let schema = Schema::parse(
        r#"
table users {
  id UUID
}
"#,
    )
    .unwrap();

    let usages = vec![QailUsage {
        file: "test.rs".to_string(),
        line: 1,
        column: 1,
        table: "usersx".to_string(),
        is_dynamic_table: false,
        columns: vec!["id".to_string()],
        action: "GET".to_string(),
        is_cte_ref: false,
        has_rls: false,
        has_explicit_tenant_scope: false,
        file_uses_super_admin: false,
    }];

    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);
    assert!(
        diagnostics
            .iter()
            .any(|d| matches!(d.kind, ValidationDiagnosticKind::SchemaError)),
        "static unknown table must fail validation even without underscore"
    );
}

#[test]
fn test_schema_validation_unknown_dynamic_table_is_skipped() {
    let schema = Schema::parse(
        r#"
table users {
  id UUID
}
"#,
    )
    .unwrap();

    let usages = vec![QailUsage {
        file: "test.rs".to_string(),
        line: 1,
        column: 1,
        table: "table_var".to_string(),
        is_dynamic_table: true,
        columns: vec!["id".to_string()],
        action: "GET".to_string(),
        is_cte_ref: false,
        has_rls: false,
        has_explicit_tenant_scope: false,
        file_uses_super_admin: false,
    }];

    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);
    assert!(
        !diagnostics
            .iter()
            .any(|d| matches!(d.kind, ValidationDiagnosticKind::SchemaError)),
        "dynamic unresolved table references should be skipped"
    );
}

#[test]
fn test_rls_detection_typed_api() {
    // .rls() from typed API should be detected
    let content = r#"
let q = Qail::get("orders")
    .columns(["id"])
    .rls(&ctx);
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 1);
    assert!(usages[0].has_rls);
}

#[test]
fn test_rls_detection_with_rls() {
    let content = r#"
let q = Qail::get("orders")
    .columns(["id"])
    .with_rls(&ctx);
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 1);
    assert!(usages[0].has_rls);
}

#[test]
fn test_extract_typed_table_arg() {
    assert_eq!(
        extract_typed_table_arg("users::table)"),
        Some("users".to_string())
    );
    assert_eq!(
        extract_typed_table_arg("users::Users)"),
        Some("users".to_string())
    );
    assert_eq!(
        extract_typed_table_arg("schema::users::table)"),
        Some("users".to_string())
    );
    assert_eq!(
        extract_typed_table_arg("Orders)"),
        Some("orders".to_string())
    );
    assert_eq!(extract_typed_table_arg(""), None);
}

#[test]
fn test_sql_migration_ignores_non_ddl_alter_table_mentions() {
    let mut schema = Schema::default();
    schema.tables.insert(
        "users".to_string(),
        TableSchema {
            name: "users".to_string(),
            columns: std::collections::HashMap::new(),
            policies: std::collections::HashMap::new(),
            foreign_keys: vec![],
            rls_enabled: false,
        },
    );

    let sql = r#"
SELECT 'ALTER TABLE users ADD COLUMN injected TEXT' AS note;
-- ALTER TABLE users ADD COLUMN skipped TEXT;
/* ALTER TABLE users ADD COLUMN skipped2 TEXT; */
"#;
    let changes = schema.parse_sql_migration(sql);
    assert_eq!(changes, 0);
    assert!(!schema.table("users").unwrap().has_column("injected"));
}

#[test]
fn test_super_admin_audit_warns_without_explicit_tenant_scope() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
  tenant_id UUID
}
"#,
    )
    .unwrap();

    let source = r#"
fn demo() {
    let _sa = SuperAdminToken::for_system_process("jobs");
    let _q = Qail::get("orders").columns(["id"]);
}
"#;
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before unix epoch")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "qail_build_sa_scope_warn_{}_{}",
        std::process::id(),
        unique
    ));
    std::fs::create_dir_all(&root).expect("create temp root");
    let file = root.join("sa_no_scope.rs");
    std::fs::write(&file, source).expect("write source");
    let usages = scan_source_files(root.to_str().expect("utf8 temp path"));
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);
    let _ = std::fs::remove_file(&file);
    let _ = std::fs::remove_dir_all(&root);

    assert!(diagnostics.iter().any(|d| {
        matches!(d.kind, ValidationDiagnosticKind::RlsWarning)
            && d.message.contains("no explicit tenant scope")
    }));
}

#[test]
fn test_super_admin_audit_accepts_tenant_id_is_null_scope() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
  tenant_id UUID
}
"#,
    )
    .unwrap();

    let source = r#"
fn demo() {
    let _sa = SuperAdminToken::for_system_process("jobs");
    let _q = Qail::get("orders")
        .columns(["id"])
        .is_null("tenant_id");
}
"#;
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before unix epoch")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "qail_build_sa_scope_null_{}_{}",
        std::process::id(),
        unique
    ));
    std::fs::create_dir_all(&root).expect("create temp root");
    let file = root.join("sa_is_null_scope.rs");
    std::fs::write(&file, source).expect("write source");
    let usages = scan_source_files(root.to_str().expect("utf8 temp path"));
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);
    let _ = std::fs::remove_file(&file);
    let _ = std::fs::remove_dir_all(&root);

    assert!(
        !diagnostics
            .iter()
            .any(|d| d.message.contains("no explicit tenant scope"))
    );
}

#[test]
fn test_super_admin_audit_accepts_tenant_id_eq_scope() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
  tenant_id UUID
}
"#,
    )
    .unwrap();

    let source = r#"
fn demo() {
    let _sa = SuperAdminToken::for_system_process("jobs");
    let _q = Qail::get("orders")
        .columns(["id"])
        .eq("tenant_id", "00000000-0000-0000-0000-000000000000");
}
"#;
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before unix epoch")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "qail_build_sa_scope_eq_{}_{}",
        std::process::id(),
        unique
    ));
    std::fs::create_dir_all(&root).expect("create temp root");
    let file = root.join("sa_eq_scope.rs");
    std::fs::write(&file, source).expect("write source");
    let usages = scan_source_files(root.to_str().expect("utf8 temp path"));
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);
    let _ = std::fs::remove_file(&file);
    let _ = std::fs::remove_dir_all(&root);

    assert!(
        !diagnostics
            .iter()
            .any(|d| d.message.contains("no explicit tenant scope"))
    );
}

#[test]
fn test_scan_source_files_uses_semantic_scanner() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before unix epoch")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "qail_build_scan_route_{}_{}",
        std::process::id(),
        unique
    ));
    std::fs::create_dir_all(&root).expect("create temp root");
    let file = root.join("demo.rs");
    let source = r#"
fn demo() {
    let _q = Qail::get("users").eq("id", 1);
}
    "#;
    std::fs::write(&file, source).expect("write demo.rs");

    let scanned = scan_source_files(root.to_str().expect("utf8 temp path"));
    let _ = std::fs::remove_file(&file);
    let _ = std::fs::remove_dir_all(&root);

    assert_eq!(scanned.len(), 1, "expected exactly one scanned usage");
    let usage = &scanned[0];
    assert_eq!(usage.file, file.display().to_string());
    assert_eq!(usage.table, "users");
    assert_eq!(usage.action, "GET");
    assert!(usage.columns.iter().any(|c| c == "id"));
    assert!(!usage.has_rls);
    assert!(!usage.is_cte_ref);
    assert!(!usage.file_uses_super_admin);
}

#[test]
fn test_scan_source_text_scans_in_memory_buffer() {
    let source = r#"
fn demo() {
    let _q = Qail::typed(users::table)
        .column("id")
        .with_rls(&ctx);
}
    "#;

    let scanned = scan_source_text("virtual.rs", source);
    assert_eq!(scanned.len(), 1, "expected exactly one scanned usage");

    let usage = &scanned[0];
    assert_eq!(usage.file, "virtual.rs");
    assert_eq!(usage.table, "users");
    assert_eq!(usage.action, "TYPED");
    assert!(usage.columns.iter().any(|c| c == "id"));
    assert!(usage.has_rls);
}

#[test]
fn test_scan_file_ignores_qail_markers_in_comments_and_strings() {
    let content = r#"
fn demo() {
    let _fake = "Qail::get(\"ghost\").column(\"id\")";
    // let _also_fake = Qail::set("ghost");
    /* Qail::del("ghost"); */
    let _q = Qail::get("users").column("id");
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(
        usages.len(),
        1,
        "expected only real Qail chain to be scanned"
    );
    assert_eq!(usages[0].table, "users");
    assert_eq!(usages[0].action, "GET");
}

#[test]
fn test_extract_columns_ignores_method_markers_inside_string_literals() {
    let line = r#"Qail::get("orders")
        .column("id")
        .eq("note", ".set_value(\"secret\", 1)")
        .filter("status", true)"#;
    let cols = extract_columns(line);

    assert!(cols.contains(&"id".to_string()));
    assert!(cols.contains(&"status".to_string()));
    assert!(
        !cols.contains(&"secret".to_string()),
        "string content must not be parsed as method call"
    );
}

#[test]
fn test_super_admin_allow_comment_in_block_comment_disables_audit_flag() {
    let content = r#"
/*
 qail:allow(super_admin)
*/
fn demo() {
    let _sa = SuperAdminToken::for_system_process("jobs");
    let _q = Qail::get("orders").column("id");
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 1);
    assert!(
        !usages[0].file_uses_super_admin,
        "allow marker in comments should disable super-admin file flag"
    );
}
