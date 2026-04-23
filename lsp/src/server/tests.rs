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
        (cache_a.schema_mtime, cache_b.schema_mtime)
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
        (cache_a.schema_mtime, cache_b.schema_mtime)
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
