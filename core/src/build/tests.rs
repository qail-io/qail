//! Tests for build-time validation.

use super::scanner::*;
use super::schema::*;
#[cfg(feature = "syn-scanner")]
use super::syn_analyzer::*;
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

#[cfg(all(feature = "syn-scanner", not(feature = "analyzer")))]
#[test]
fn test_syn_scanner_nplus1_detector_available_without_analyzer() {
    let source = r#"
async fn demo(ids: Vec<i64>, conn: &Conn, pool: &Pool) {
    for id in ids {
        let q = Qail::get("users").eq("id", id);
        let _ = conn.fetch_all(&q).await;
    }
}
"#;
    let diags = super::syn_nplus1::detect_n_plus_one_in_file("demo.rs", source);
    assert!(!diags.is_empty(), "expected at least one N+1 diagnostic");
}

#[cfg(feature = "syn-scanner")]
#[test]
fn test_syn_extract_join_group_by_having() {
    let source = r#"
fn demo(ctx: &RlsContext) {
    let _q = Qail::get("orders")
        .left_join("customers", "orders.customer_id", "customers.id")
        .group_by(["customer_id"])
        .having_cond(Condition {
            left: Expr::Named("total".into()),
            op: Operator::Eq,
            value: Value::Int(1),
            is_array_unnest: false,
        })
        .with_rls(ctx);
}
"#;

    let parsed = extract_syn_usages_from_source(source);
    let usage = parsed
        .into_iter()
        .find(|u| u.action == "GET" && u.table == "orders")
        .expect("expected syn usage for Qail::get(\"orders\")");

    assert_eq!(usage.cmd.joins.len(), 1);
    assert!(usage
        .cmd
        .cages
        .iter()
        .any(|c| matches!(c.kind, crate::ast::CageKind::Partition)));
    assert_eq!(usage.cmd.having.len(), 1);
    assert!(usage.has_rls);
}

#[cfg(feature = "syn-scanner")]
#[test]
fn test_validate_against_schema_uses_syn_structural_fields() {
    let schema = Schema::parse(
        r#"
table orders {
  id INT
  customer_id INT
  total INT
}

table customers {
  id INT
}
"#,
    )
    .unwrap();

    let content = r#"
fn demo() {
    let _q = Qail::get("orders")
        .left_join("customerz", "orders.customer_id", "customerz.id")
        .group_by(["custmer_id"])
        .having_cond(Condition {
            left: Expr::Named("totl".into()),
            op: Operator::Eq,
            value: Value::Int(1),
            is_array_unnest: false,
        });
}
"#;

    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let test_path = std::env::temp_dir().join(format!(
        "qail_build_syn_structural_{}_{}.rs",
        std::process::id(),
        unique
    ));
    std::fs::write(&test_path, content).unwrap();

    let mut usages = Vec::new();
    scan_file(&test_path.display().to_string(), content, &mut usages);
    let errors = validate_against_schema(&schema, &usages);
    let _ = std::fs::remove_file(&test_path);

    assert!(errors.iter().any(|e: &String| e.contains("customerz")));
    assert!(errors.iter().any(|e: &String| e.contains("custmer_id")));
    assert!(errors.iter().any(|e: &String| e.contains("totl")));
}

#[cfg(feature = "syn-scanner")]
#[test]
fn test_query_ir_ignores_scanner_columns_when_syn_command_exists() {
    let source = r#"
fn demo() {
    let _q = Qail::get("users").eq("id", 1);
}
"#;

    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before unix epoch")
        .as_nanos();
    let test_path = std::env::temp_dir().join(format!(
        "qail_build_syn_query_ir_{}_{}.rs",
        std::process::id(),
        unique
    ));
    std::fs::write(&test_path, source).expect("write source");

    let mut usages = Vec::new();
    scan_file(&test_path.display().to_string(), source, &mut usages);
    assert_eq!(usages.len(), 1, "expected one scanner usage");

    // Simulate scanner drift/noise: this column is not present in the syn AST.
    usages[0]
        .columns
        .push("definitely_not_a_real_column".to_string());

    let query_ir = super::query_ir::build_query_ir(&usages);
    let _ = std::fs::remove_file(&test_path);

    assert_eq!(query_ir.len(), 1);
    let cols = normalized_columns_from_cmd(&query_ir[0].cmd);
    assert!(
        !cols.contains(&"definitely_not_a_real_column".to_string()),
        "scanner-only column noise must be ignored when syn command is available"
    );
    assert!(
        cols.contains(&"id".to_string()),
        "syn-derived command should still include real filter columns"
    );
}

#[cfg(feature = "syn-scanner")]
#[test]
fn test_syn_chain_through_await() {
    let source = r#"
async fn demo(pool: &Pool) {
    let _r = Qail::get("orders")
        .eq("status", "active")
        .fetch_one(&pool)
        .await;
}
"#;
    let parsed = extract_syn_usages_from_source(source);
    let usage = parsed
        .into_iter()
        .find(|u| u.action == "GET" && u.table == "orders")
        .expect("expected syn usage for Qail::get(\"orders\") through .await");

    // The chain should have captured the .eq("status", ...) filter
    assert!(
        usage
            .cmd
            .cages
            .iter()
            .any(|c| matches!(&c.kind, crate::ast::CageKind::Filter)),
        "expected a Filter cage from .eq(\"status\", ...)"
    );
}

#[cfg(feature = "syn-scanner")]
#[test]
fn test_syn_chain_through_try() {
    let source = r#"
fn demo(pool: &Pool) -> Result<(), Box<dyn std::error::Error>> {
    let _r = Qail::get("orders")
        .eq("id", 42)
        .fetch_one(&pool)?;
    Ok(())
}
"#;
    let parsed = extract_syn_usages_from_source(source);
    let usage = parsed
        .into_iter()
        .find(|u| u.action == "GET" && u.table == "orders")
        .expect("expected syn usage for Qail::get(\"orders\") through ?");

    assert!(
        usage
            .cmd
            .cages
            .iter()
            .any(|c| matches!(&c.kind, crate::ast::CageKind::Filter)),
        "expected a Filter cage from .eq(\"id\", ...)"
    );
}

#[cfg(feature = "syn-scanner")]
#[test]
fn test_syn_variable_reassigned_chain() {
    let source = r#"
fn demo() {
    let mut cmd = Qail::get("orders");
    cmd = cmd.eq("status", "active");
    cmd = cmd.order_desc("created_at");
}
"#;
    let parsed = extract_syn_usages_from_source(source);
    // Should find multiple usages — the final one (highest score) should have all 3 columns
    let best = parsed
        .iter()
        .filter(|u| u.action == "GET" && u.table == "orders")
        .max_by_key(|u| u.score)
        .expect("expected syn usage for reassigned Qail chain");

    // The best usage should have both filter and sort cages
    let has_filter = best
        .cmd
        .cages
        .iter()
        .any(|c| matches!(&c.kind, crate::ast::CageKind::Filter));
    let has_sort = best
        .cmd
        .cages
        .iter()
        .any(|c| matches!(&c.kind, crate::ast::CageKind::Sort(_)));

    assert!(
        has_filter,
        "expected a Filter cage from .eq(\"status\", ...)"
    );
    assert!(
        has_sort,
        "expected a Sort cage from .order_desc(\"created_at\")"
    );
}

#[cfg(feature = "syn-scanner")]
#[test]
fn test_syn_closure_chain() {
    let source = r#"
fn demo() {
    let items = vec![1, 2, 3];
    let _results: Vec<_> = items.iter().map(|item| {
        Qail::get("products").eq("product_id", *item)
    }).collect();
}
"#;
    let parsed = extract_syn_usages_from_source(source);
    let usage = parsed
        .into_iter()
        .find(|u| u.action == "GET" && u.table == "products")
        .expect("expected syn usage for Qail::get(\"products\") inside closure");

    assert!(
        usage
            .cmd
            .cages
            .iter()
            .any(|c| matches!(&c.kind, crate::ast::CageKind::Filter)),
        "expected a Filter cage from .eq(\"product_id\", ...)"
    );
}

#[cfg(feature = "syn-scanner")]
#[test]
fn test_syn_async_move_chain() {
    let source = r#"
fn demo() {
    tokio::spawn(async move {
        Qail::set("orders")
            .set_value("status", "completed")
    });
}
"#;
    let parsed = extract_syn_usages_from_source(source);
    let usage = parsed
        .into_iter()
        .find(|u| u.action == "SET" && u.table == "orders")
        .expect("expected syn usage for Qail::set(\"orders\") inside async move block");

    assert!(
        usage
            .cmd
            .cages
            .iter()
            .any(|c| matches!(&c.kind, crate::ast::CageKind::Payload)),
        "expected a Payload cage from .set_value(\"status\", ...)"
    );
}

#[cfg(feature = "syn-scanner")]
fn normalized_columns_from_cmd(cmd: &crate::ast::Qail) -> Vec<String> {
    use crate::ast::Expr;
    let mut out = Vec::<String>::new();
    let mut seen = std::collections::HashSet::<String>::new();
    let mut push = |name: &str| {
        if name.is_empty() || name == "*" || name.contains('.') || name.contains('(') {
            return;
        }
        if seen.insert(name.to_string()) {
            out.push(name.to_string());
        }
    };

    for expr in &cmd.columns {
        match expr {
            Expr::Named(name) => push(name),
            Expr::Aliased { name, .. } => push(name),
            Expr::Aggregate { col, .. } => push(col),
            _ => {}
        }
    }
    for cage in &cmd.cages {
        for cond in &cage.conditions {
            if let Expr::Named(name) = &cond.left {
                push(name);
            }
        }
    }
    for cond in &cmd.having {
        if let Expr::Named(name) = &cond.left {
            push(name);
        }
    }
    if let Some(returning) = &cmd.returning {
        for expr in returning {
            if let Expr::Named(name) = expr {
                push(name);
            }
        }
    }
    out.sort();
    out
}

#[cfg(feature = "syn-scanner")]
fn normalize_usage_rows(
    usages: &[QailUsage],
) -> Vec<(usize, usize, String, String, bool, bool, bool, Vec<String>)> {
    let mut rows = usages
        .iter()
        .map(|u| {
            let mut cols = u.columns.clone();
            cols.sort();
            cols.dedup();
            (
                u.line,
                u.column,
                u.action.clone(),
                u.table.clone(),
                u.has_rls,
                u.is_cte_ref,
                u.file_uses_super_admin,
                cols,
            )
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

#[cfg(feature = "syn-scanner")]
#[test]
fn test_syn_emit_usage_drift_gate() {
    let source = r#"
fn demo(ctx: &RlsContext) {
    let _sa = SuperAdminToken::for_system_process("jobs");
    let _q = Qail::get("orders")
        .columns(["id", "tenant_id", "status"])
        .eq("tenant_id", 7)
        .with_rls(ctx);
    let _cte = Qail::get("orders").columns(["id"]).to_cte("agg");
    let _read = Qail::get("agg").column("id");
}
"#;

    let emitted = emit_qail_usages_from_syn_source("drift.rs", source);
    let parsed = extract_syn_usages_from_source(source);

    let mut best_by_key: std::collections::HashMap<
        (usize, usize, String, String),
        super::syn_analyzer::SynParsedUsage,
    > = std::collections::HashMap::new();
    for parsed_usage in parsed {
        let key = (
            parsed_usage.line,
            parsed_usage.column,
            parsed_usage.action.clone(),
            parsed_usage.table.clone(),
        );
        match best_by_key.get(&key) {
            Some(existing) if existing.score >= parsed_usage.score => {}
            _ => {
                best_by_key.insert(key, parsed_usage);
            }
        }
    }

    let mut expected_rows = best_by_key
        .values()
        .map(|p| {
            (
                p.line,
                p.column,
                p.action.clone(),
                p.table.clone(),
                p.has_rls,
                p.table == "agg",
                true,
                normalized_columns_from_cmd(&p.cmd),
            )
        })
        .collect::<Vec<_>>();
    expected_rows.sort();

    let actual_rows = normalize_usage_rows(&emitted);
    assert_eq!(
        actual_rows, expected_rows,
        "syn parsed usage and emitted QailUsage drifted"
    );
}

#[cfg(feature = "syn-scanner")]
#[test]
fn test_syn_emit_usage_respects_super_admin_allow_comment() {
    let source = r#"
// qail:allow(super_admin)
fn demo() {
    let _sa = SuperAdminToken::for_system_process("jobs");
    let _q = Qail::get("orders").column("id");
}
"#;
    let emitted = emit_qail_usages_from_syn_source("allow.rs", source);
    assert!(!emitted.is_empty());
    assert!(emitted.iter().all(|u| !u.file_uses_super_admin));
}

#[cfg(feature = "syn-scanner")]
#[test]
fn test_syn_emit_usage_allow_comment_applies_to_next_qail_call_only() {
    let source = r#"
fn demo() {
    let _sa = SuperAdminToken::for_system_process("jobs");
    // qail:allow(super_admin)
    let _q1 = Qail::get("orders").column("id");
    let _q2 = Qail::get("orders").column("status");
}
"#;
    let mut emitted = emit_qail_usages_from_syn_source("allow_once.rs", source)
        .into_iter()
        .filter(|u| u.table == "orders")
        .collect::<Vec<_>>();
    emitted.sort_by_key(|u| (u.line, u.column));
    assert!(
        emitted.len() >= 2,
        "expected at least two Qail usages, got {}",
        emitted.len()
    );
    assert!(!emitted[0].file_uses_super_admin);
    assert!(emitted[1].file_uses_super_admin);
}

#[cfg(feature = "syn-scanner")]
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

#[cfg(feature = "syn-scanner")]
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

    assert!(!diagnostics
        .iter()
        .any(|d| d.message.contains("no explicit tenant scope")));
}

#[cfg(feature = "syn-scanner")]
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

    assert!(!diagnostics
        .iter()
        .any(|d| d.message.contains("no explicit tenant scope")));
}

#[cfg(feature = "syn-scanner")]
#[test]
fn test_scan_source_files_routes_to_syn_emitter() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before unix epoch")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "qail_build_syn_route_{}_{}",
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

    let mut scanned = scan_source_files(root.to_str().expect("utf8 temp path"));
    let mut expected = emit_qail_usages_from_syn_source(&file.display().to_string(), source);
    let _ = std::fs::remove_file(&file);
    let _ = std::fs::remove_dir_all(&root);

    scanned.sort_by(|a, b| {
        (
            &a.file,
            a.line,
            a.column,
            &a.action,
            &a.table,
            a.has_rls,
            a.is_cte_ref,
            a.file_uses_super_admin,
        )
            .cmp(&(
                &b.file,
                b.line,
                b.column,
                &b.action,
                &b.table,
                b.has_rls,
                b.is_cte_ref,
                b.file_uses_super_admin,
            ))
    });
    expected.sort_by(|a, b| {
        (
            &a.file,
            a.line,
            a.column,
            &a.action,
            &a.table,
            a.has_rls,
            a.is_cte_ref,
            a.file_uses_super_admin,
        )
            .cmp(&(
                &b.file,
                b.line,
                b.column,
                &b.action,
                &b.table,
                b.has_rls,
                b.is_cte_ref,
                b.file_uses_super_admin,
            ))
    });

    assert_eq!(
        normalize_usage_rows(&scanned),
        normalize_usage_rows(&expected)
    );
}
