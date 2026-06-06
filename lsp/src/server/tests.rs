use super::*;
use std::fs;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tower_lsp::LspService;

fn create_temp_dir(prefix: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    dir.push(format!(
        "qail_lsp_{prefix}_{}_{}",
        std::process::id(),
        nanos
    ));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

#[test]
fn rust_query_span_detection_covers_chain_lines() {
    let src = r#"async fn run(pool: &Pool) {
    let rows = query("SELECT * FROM users")
        .fetch_all(pool)
        .await;
}"#;

    let query = extract_rust_query_at_line(src, 2).expect("query should match span");
    assert_eq!(query.kind, EmbeddedQueryKind::Sql);
    assert_eq!(query.text, "SELECT * FROM users");
}

#[test]
fn rust_query_kind_marks_qail_text() {
    let src = r#"async fn run(pool: &Pool) {
    let rows = query("get users fields id")
        .fetch_all(pool)
        .await;
}"#;

    let query = extract_rust_query_at_line(src, 1).expect("query expected");
    assert_eq!(query.kind, EmbeddedQueryKind::Qail);
}

#[test]
fn rust_query_kind_marks_sql_cte_text() {
    let src = r#"async fn run(pool: &Pool) {
    let rows = query("WITH x AS (SELECT id FROM users) SELECT id FROM x")
        .fetch_all(pool)
        .await;
}"#;

    let query = extract_rust_query_at_line(src, 1).expect("query expected");
    assert_eq!(query.kind, EmbeddedQueryKind::Sql);

    let diags = collect_rust_qail_diagnostics(src);
    assert!(diags.is_empty(), "{diags:?}");
}

#[test]
fn rust_qail_diagnostics_ignore_query_calls_inside_comments_and_literals() {
    let src = r##"async fn run(pool: &Pool) {
    // query("get users fields where").fetch_all(pool).await;
    /*
    query("get orders fields where")
        .fetch_all(pool)
        .await;
    */
    let doc = "query(\"get teams fields where\").fetch_all(pool).await";
    let raw = r#"query("get logs fields where").fetch_all(pool).await"#;
    let rows = query("get users fields id")
        .fetch_all(pool)
        .await;
}"##;

    let diags = collect_rust_qail_diagnostics(src);

    assert!(diags.is_empty(), "{diags:?}");
}

#[test]
fn qail_classifier_rejects_sql_prefix() {
    assert!(!looks_like_qail_query("SELECT id FROM users"));
    assert!(looks_like_qail_query("get users fields id"));
}

#[test]
fn text_query_extraction_supports_multiline_literals() {
    let src = r#"const q = `
get users
fields id, email
where active = true
`;"#;

    let query = extract_text_query_at_line(src, 2).expect("query expected");
    assert_eq!(query.kind, EmbeddedQueryKind::Qail);
    assert_eq!(
        query.text,
        "get users\nfields id, email\nwhere active = true"
    );
}

#[test]
fn text_query_extraction_marks_sql_literals() {
    let src = r#"const sql = "
SELECT id, email
FROM users
WHERE active = true
";"#;

    let query = extract_text_query_at_line(src, 2).expect("sql query expected");
    assert_eq!(query.kind, EmbeddedQueryKind::Sql);
    assert_eq!(
        query.text,
        "SELECT id, email\nFROM users\nWHERE active = true"
    );
}

#[test]
fn text_query_extraction_marks_sql_cte_literals() {
    let src = r#"const sql = "
WITH x AS (SELECT id FROM users)
SELECT id FROM x
";"#;

    let query = extract_text_query_at_line(src, 2).expect("sql query expected");
    assert_eq!(query.kind, EmbeddedQueryKind::Sql);

    let diags = collect_text_qail_diagnostics(src);
    assert!(diags.is_empty(), "{diags:?}");
}

#[test]
fn text_diagnostics_ignore_comment_literals() {
    let src = r#"
// "get users fields id where"
const msg = "hello";
"#;

    let diags = collect_text_qail_diagnostics(src);
    assert!(diags.is_empty(), "{diags:?}");
}

#[test]
fn text_diagnostics_cover_raw_qail_documents() {
    let src = "get users fields id where";
    let diags = collect_text_qail_diagnostics(src);
    assert!(!diags.is_empty(), "raw qail file should be validated");
}

#[test]
fn text_query_extraction_supports_raw_qail_document() {
    let src = "get users fields id where active = true";
    let query = extract_text_query_at_line(src, 0).expect("raw query expected");
    assert_eq!(query.kind, EmbeddedQueryKind::Qail);
    assert_eq!(query.text, "get users fields id where active = true");
}

#[test]
fn rust_semantic_usages_include_qail_builder_chain() {
    let src = r#"fn demo(ctx: &RlsContext) {
    let _q = Qail::get("orders")
        .columns(["id"])
        .with_rls(&ctx);
}"#;

    let (usages, ranges) = collect_rust_document_usages("test.rs", src, false);
    assert_eq!(usages.len(), 1);
    assert_eq!(usages[0].table, "orders");
    assert_eq!(usages[0].action, "GET");
    assert!(usages[0].has_rls);
    assert_eq!(ranges.len(), 1);
}

#[test]
fn rust_semantic_usages_include_typed_builder_chain() {
    let src = r#"fn demo(ctx: &RlsContext) {
    let _q = Qail::typed(users::table)
        .column("id")
        .with_rls(&ctx);
}"#;

    let (usages, _) = collect_rust_document_usages("test.rs", src, false);
    assert_eq!(usages.len(), 1);
    assert_eq!(usages[0].table, "users");
    assert_eq!(usages[0].action, "TYPED");
    assert!(usages[0].columns.iter().any(|c| c == "id"));
}

#[test]
fn parsed_merge_usage_preserves_ir_columns_and_source_table() {
    let cmd = Qail::merge_into("orders")
        .using_table("staging_orders")
        .merge_on_column("id", qail_core::ast::Operator::Eq, "staging.order_id")
        .when_matched_update(&[("status", Expr::Named("staging.status".to_string()))]);

    assert_eq!(action_to_usage_tag(&cmd), "MERGE");

    let columns = collect_usage_columns(&cmd);
    assert!(columns.iter().any(|column| column == "id"));
    assert!(columns.iter().any(|column| column == "status"));

    let related_tables = collect_usage_related_tables(&cmd);
    assert_eq!(related_tables, vec!["staging_orders".to_string()]);
}

#[test]
fn rust_semantic_diagnostics_cover_qail_builder_chain() {
    let schema = BuildSchema::parse(
        r#"
table orders {
  id UUID
  tenant_id UUID
}
"#,
    )
    .expect("schema should parse");
    let src = r#"fn demo() {
    let _q = Qail::get("orders").columns(["id"]);
}"#;

    let diags = collect_semantic_qail_diagnostics(src, "file:///tmp/demo.rs", &schema);
    assert!(
        diags.iter().any(|d| matches!(
            &d.code,
            Some(NumberOrString::String(code)) if code == "QAIL-RLS"
        )),
        "expected RLS warning for builder chain without with_rls: {diags:?}"
    );
}

#[test]
fn rust_embedded_qail_ignores_commented_super_admin_marker() {
    let schema = BuildSchema::parse(
        r#"
table orders {
  id UUID
  tenant_id UUID
}
"#,
    )
    .expect("schema should parse");
    let src = r#"async fn demo(pool: &Pool) {
    // SuperAdminToken::for_system_process("jobs");
    let _rows = query("get orders fields id")
        .fetch_all(pool)
        .await;
}"#;

    let diags = collect_semantic_qail_diagnostics(src, "file:///tmp/demo.rs", &schema);

    assert!(
        !diags
            .iter()
            .any(|d| d.message.contains("for_system_process")),
        "comment-only SuperAdmin marker must not trigger audit warning: {diags:?}"
    );
}

#[test]
fn rust_embedded_qail_string_allow_marker_does_not_disable_super_admin_audit() {
    let schema = BuildSchema::parse(
        r#"
table orders {
  id UUID
  tenant_id UUID
}
"#,
    )
    .expect("schema should parse");
    let src = r#"async fn demo(pool: &Pool) {
    let _sa = SuperAdminToken::for_system_process("jobs");
    let _note = "qail:allow(super_admin)";
    let _rows = query("get orders fields id")
        .fetch_all(pool)
        .await;
}"#;

    let diags = collect_semantic_qail_diagnostics(src, "file:///tmp/demo.rs", &schema);

    assert!(
        diags
            .iter()
            .any(|d| d.message.contains("for_system_process")),
        "allow marker inside a string must not suppress SuperAdmin audit warning: {diags:?}"
    );
}

#[test]
fn rust_embedded_qail_string_with_rls_marker_does_not_suppress_rls_warning() {
    let schema = BuildSchema::parse(
        r#"
table orders {
  id UUID
  tenant_id UUID
}
"#,
    )
    .expect("schema should parse");
    let src = r#"async fn demo(pool: &Pool) {
    let _rows = query("get orders fields id")
        .bind(".with_rls(")
        .fetch_all(pool)
        .await;
}"#;

    let diags = collect_semantic_qail_diagnostics(src, "file:///tmp/demo.rs", &schema);

    assert!(
        diags
            .iter()
            .any(|d| d.message.contains("has no .with_rls()")),
        "string marker must not suppress RLS warning: {diags:?}"
    );
}

#[test]
fn rust_embedded_qail_actual_with_rls_suppresses_rls_warning() {
    let schema = BuildSchema::parse(
        r#"
table orders {
  id UUID
  tenant_id UUID
}
"#,
    )
    .expect("schema should parse");
    let src = r#"async fn demo(pool: &Pool, ctx: &RlsContext) {
    let _rows = query("get orders fields id")
        .with_rls(ctx)
        .fetch_all(pool)
        .await;
}"#;

    let diags = collect_semantic_qail_diagnostics(src, "file:///tmp/demo.rs", &schema);

    assert!(
        !diags
            .iter()
            .any(|d| d.message.contains("has no .with_rls()")),
        "real with_rls call should suppress RLS warning: {diags:?}"
    );
}

#[test]
fn file_uri_maps_to_path() {
    let uri = "file:///tmp/qail/src/main.rs";
    let path = uri_to_file_path(uri).expect("file uri should parse");
    assert_eq!(path, PathBuf::from("/tmp/qail/src/main.rs"));
}

#[test]
fn schema_probe_dirs_walks_upward() {
    let dirs = schema_probe_dirs(Path::new("/tmp/qail/src/main.rs"));
    assert_eq!(dirs.first(), Some(&PathBuf::from("/tmp/qail/src")));
    assert!(dirs.contains(&PathBuf::from("/tmp/qail")));
}

#[test]
fn schema_directory_source_loads_modules_and_reloads_on_module_change() {
    let root = create_temp_dir("schema_dir");
    let schema_dir = root.join("schema");
    fs::create_dir_all(root.join("src")).expect("mkdir src");
    fs::create_dir_all(&schema_dir).expect("mkdir schema");
    fs::write(
        root.join("qail.toml"),
        "[project]\nschema_strict_manifest = false\n",
    )
    .expect("write qail.toml");
    fs::write(schema_dir.join("_order.qail"), "users.qail\norders.qail\n").expect("write order");
    fs::write(
        schema_dir.join("users.qail"),
        r#"
table users {
  id UUID
}
"#,
    )
    .expect("write users");
    fs::write(
        schema_dir.join("orders.qail"),
        r#"
table orders {
  id UUID
}
"#,
    )
    .expect("write orders");

    let uri = Url::from_file_path(root.join("src/main.rs"))
        .expect("uri")
        .to_string();
    let (service, _socket) = LspService::new(QailLanguageServer::new);
    let server = service.inner();

    assert_eq!(server.try_load_schema_from_uri(&uri), Some(root.clone()));

    let mtime_before = {
        let schemas = server.schemas.read().expect("schema cache");
        let cache = schemas.get(&root).expect("root cache");
        assert_eq!(cache.schema_path, schema_dir);
        assert!(
            cache
                .build_schema
                .as_ref()
                .is_some_and(|schema| schema.tables.contains_key("users")
                    && schema.tables.contains_key("orders"))
        );
        assert!(cache.schema_watch_mtimes.iter().any(|(path, _)| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "_order.qail")
        }));
        assert!(cache.schema_watch_mtimes.iter().any(|(path, _)| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "qail.toml")
        }));
        latest_schema_mtime(&cache.schema_watch_mtimes)
    };

    std::thread::sleep(Duration::from_secs(1));
    fs::write(
        schema_dir.join("orders.qail"),
        r#"
table orders {
  id UUID
  tenant_id UUID
}
"#,
    )
    .expect("rewrite orders");

    assert_eq!(server.try_load_schema_from_uri(&uri), Some(root.clone()));

    let mtime_after = {
        let schemas = server.schemas.read().expect("schema cache");
        let cache = schemas.get(&root).expect("root cache");
        assert!(
            cache
                .build_schema
                .as_ref()
                .and_then(|schema| schema.tables.get("orders"))
                .is_some_and(|table| table.columns.contains_key("tenant_id"))
        );
        latest_schema_mtime(&cache.schema_watch_mtimes)
    };

    assert_ne!(
        mtime_before, mtime_after,
        "module mtime changes should invalidate schema cache"
    );

    fs::write(schema_dir.join("_order.qail"), "missing.qail\n").expect("break order");
    assert_eq!(server.try_load_schema_from_uri(&uri), Some(root.clone()));
    let schemas = server.schemas.read().expect("schema cache");
    assert!(
        schemas.get(&root).is_none(),
        "broken schema source should clear stale workspace cache"
    );
    drop(schemas);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn validation_message_parsing_handles_windows_paths() {
    let msg = r"C:\work\qail\src\main.rs:42: Table 'usrs' not found. Did you mean 'users'?";
    assert_eq!(extract_line_from_validation_message(msg), Some(42));
    assert_eq!(
        strip_file_line_prefix(msg),
        "Table 'usrs' not found. Did you mean 'users'?"
    );
}

#[test]
fn validation_message_parsing_handles_colon_in_body() {
    let msg =
        r"C:\work\qail\src\main.rs:17: Invalid operator '=' for column 'id': expected integer type";
    assert_eq!(extract_line_from_validation_message(msg), Some(17));
    assert_eq!(
        strip_file_line_prefix(msg),
        "Invalid operator '=' for column 'id': expected integer type"
    );
}

#[test]
fn embedded_query_contains_position_checks_character_bounds() {
    let query = EmbeddedQuery {
        kind: EmbeddedQueryKind::Qail,
        text: "get users fields id".to_string(),
        start_line: 2,
        start_column: 4,
        end_line: 2,
        end_column: 12,
    };

    assert!(embedded_query_contains_position(
        &query,
        Position {
            line: 2,
            character: 6,
        }
    ));
    assert!(!embedded_query_contains_position(
        &query,
        Position {
            line: 2,
            character: 2,
        }
    ));
    assert!(!embedded_query_contains_position(
        &query,
        Position {
            line: 2,
            character: 20,
        }
    ));
}

#[test]
fn literal_query_span_tracks_utf16_with_unicode_content() {
    let src = r#"const q = "get users fields name where name = '🙂🙂'";"#;
    let literal = extract_text_literals(src)
        .into_iter()
        .next()
        .expect("literal expected");

    let (kind, query_text, start_line, start_col, end_line, end_col) =
        literal_query_span(src, &literal).expect("query span expected");
    assert_eq!(kind, EmbeddedQueryKind::Qail);
    assert_eq!(query_text, "get users fields name where name = '🙂🙂'");
    assert_eq!(start_line, 1);
    assert_eq!(end_line, 1);

    let (trimmed_start, trimmed_end) = trim_query_bounds(&literal.text).expect("trim bounds");
    let literal_abs_start = src.find(&literal.text).expect("literal body offset");
    let index = Utf16Index::new(src);
    let expected_start = index
        .offset_to_position(literal_abs_start + trimmed_start)
        .character
        + 1;
    let expected_end = index
        .offset_to_position(literal_abs_start + trimmed_end)
        .character
        + 1;
    assert_eq!(start_col as u32, expected_start);
    assert_eq!(end_col as u32, expected_end);
}

#[test]
fn schema_cache_is_isolated_per_workspace_and_reloads_on_change() {
    let root = create_temp_dir("schema_multi_root");
    let workspace_a = root.join("workspace_a");
    let workspace_b = root.join("workspace_b");
    fs::create_dir_all(workspace_a.join("src")).expect("workspace A");
    fs::create_dir_all(workspace_b.join("src")).expect("workspace B");

    let schema_a = workspace_a.join("schema.qail");
    let schema_b = workspace_b.join("schema.qail");
    fs::write(
        &schema_a,
        r#"
table users {
  id UUID
}
"#,
    )
    .expect("schema A");
    fs::write(
        &schema_b,
        r#"
table orders {
  id UUID
}
"#,
    )
    .expect("schema B");

    let uri_a = Url::from_file_path(workspace_a.join("src/main.rs"))
        .expect("uri A")
        .to_string();
    let uri_b = Url::from_file_path(workspace_b.join("src/main.rs"))
        .expect("uri B")
        .to_string();

    let (service, _socket) = LspService::new(QailLanguageServer::new);
    let server = service.inner();

    assert_eq!(
        server.try_load_schema_from_uri(&uri_a),
        Some(workspace_a.clone())
    );
    assert_eq!(
        server.try_load_schema_from_uri(&uri_b),
        Some(workspace_b.clone())
    );

    let (mtime_a_before, mtime_b_before) = {
        let schemas = server.schemas.read().expect("schema cache");
        let cache_a = schemas.get(&workspace_a).expect("cache A");
        let cache_b = schemas.get(&workspace_b).expect("cache B");
        (
            latest_schema_mtime(&cache_a.schema_watch_mtimes),
            latest_schema_mtime(&cache_b.schema_watch_mtimes),
        )
    };

    std::thread::sleep(Duration::from_secs(1));
    fs::write(
        &schema_a,
        r#"
table users {
  id UUID
  tenant_id UUID
}
"#,
    )
    .expect("schema A updated");

    assert_eq!(
        server.try_load_schema_from_uri(&uri_a),
        Some(workspace_a.clone())
    );

    let (mtime_a_after, mtime_b_after) = {
        let schemas = server.schemas.read().expect("schema cache");
        let cache_a = schemas.get(&workspace_a).expect("cache A");
        let cache_b = schemas.get(&workspace_b).expect("cache B");
        (
            latest_schema_mtime(&cache_a.schema_watch_mtimes),
            latest_schema_mtime(&cache_b.schema_watch_mtimes),
        )
    };

    assert_ne!(
        mtime_a_before, mtime_a_after,
        "workspace A schema should reload after file change"
    );
    assert_eq!(
        mtime_b_before, mtime_b_after,
        "workspace B cache should remain unchanged"
    );

    let _ = fs::remove_dir_all(root);
}
