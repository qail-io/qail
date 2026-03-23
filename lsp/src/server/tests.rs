use super::*;

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
