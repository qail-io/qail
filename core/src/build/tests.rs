//! Tests for build-time validation.

use super::scanner::*;
use super::schema::*;
use super::validate::*;
use crate::migrate::types::ColumnType;

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
fn test_parse_schema_rejects_duplicate_tables() {
    let content = r#"
table users {
  id UUID
}

table users {
  email TEXT
}
"#;
    let err = Schema::parse(content).expect_err("duplicate table must fail");
    assert!(err.contains("duplicate table declaration 'users'"));
}

#[test]
fn test_parse_schema_rejects_duplicate_columns() {
    let content = r#"
table users {
  id UUID
  id TEXT
}
"#;
    let err = Schema::parse(content).expect_err("duplicate column must fail");
    assert!(err.contains("duplicate column 'id' in table 'users'"));
}

#[test]
fn test_parse_schema_rejects_invalid_column_names() {
    let content = r#"
table users {
  bad-name UUID
}
"#;

    let err = Schema::parse(content).expect_err("invalid column name must fail");
    assert!(err.contains("Invalid column name 'bad-name' in table 'users'"));
}

#[test]
fn test_parse_schema_rejects_column_without_type() {
    let content = r#"
table users {
  id
}
"#;

    let err = Schema::parse(content).expect_err("missing column type must fail");
    assert!(err.contains("Missing type for column 'id' in table 'users'"));
}

#[test]
fn test_parse_schema_rejects_unknown_column_type() {
    let content = r#"
table users {
  id UUUD
}
"#;

    let err = Schema::parse(content).expect_err("unknown column type must fail");
    assert!(err.contains("Unknown column type 'UUUD' for column 'id' in table 'users'"));
}

#[test]
fn test_parse_schema_rejects_unknown_column_option() {
    let content = r#"
table users {
  email TEXT uniq
}
"#;

    let err = Schema::parse(content).expect_err("unknown column option must fail");
    assert!(err.contains("Unknown column option 'uniq' for column 'email' in table 'users'"));
}

#[test]
fn test_parse_schema_rejects_duplicate_column_options() {
    let content = r#"
table users {
  email TEXT unique unique
}
"#;

    let err = Schema::parse(content).expect_err("duplicate column option must fail");
    assert!(err.contains("duplicate column option 'unique' for column 'email' in table 'users'"));
}

#[test]
fn test_parse_schema_rejects_conflicting_nullability_options() {
    let content = r#"
table users {
  email TEXT not_null nullable
}
"#;

    let err = Schema::parse(content).expect_err("conflicting nullability must fail");
    assert!(
        err.contains(
            "conflicting nullability options 'not_null' and 'nullable' for column 'email' in table 'users'"
        ),
        "{err}"
    );
}

#[test]
fn test_parse_schema_rejects_conflicting_generated_options() {
    let content = r#"
table users {
  id UUID generated_identity generated_by_default_identity
}
"#;

    let err = Schema::parse(content).expect_err("conflicting generated options must fail");
    assert!(
        err.contains(
            "conflicting generated options 'generated_identity' and 'generated_by_default_identity' for column 'id' in table 'users'"
        ),
        "{err}"
    );
}

#[test]
fn test_parse_schema_rejects_duplicate_protected_option() {
    let content = r#"
table users {
  password_hash TEXT protected protected
}
"#;

    let err = Schema::parse(content).expect_err("duplicate protected option must fail");
    assert!(err.contains("duplicate protected option for column 'password_hash' in table 'users'"));
}

#[test]
fn test_parse_schema_tracks_references_column_option() {
    let content = r#"
table users {
  id UUID
}

table posts {
  id UUID
  user_id UUID not_null references users(id) on_delete cascade
}
"#;

    let schema = Schema::parse(content).expect("references syntax should parse");
    let posts = schema.table("posts").expect("posts table should exist");
    assert_eq!(posts.foreign_keys.len(), 1);
    assert_eq!(posts.foreign_keys[0].column, "user_id");
    assert_eq!(posts.foreign_keys[0].ref_table, "users");
    assert_eq!(posts.foreign_keys[0].ref_column, "id");
}

#[test]
fn test_parse_schema_rejects_malformed_ref_option() {
    for content in [
        r#"
table posts {
  user_id UUID ref:users
}
"#,
        r#"
table posts {
  user_id UUID ref:.id
}
"#,
        r#"
table posts {
  user_id UUID ref:users.
}
"#,
        r#"
table posts {
  user_id UUID ref:users.id.extra
}
"#,
    ] {
        let err = Schema::parse(content).expect_err("malformed ref option must fail");
        assert!(err.contains("Invalid ref target"));
        assert!(err.contains("column 'user_id' in table 'posts'"));
    }
}

#[test]
fn test_parse_schema_rejects_duplicate_foreign_keys() {
    let content = r#"
table posts {
  user_id UUID ref:users.id references users(id)
}
"#;

    let err = Schema::parse(content).expect_err("duplicate foreign keys must fail");
    assert!(err.contains("duplicate foreign key 'posts.user_id -> users.id'"));
}

#[test]
fn test_parse_schema_rejects_fk_actions_without_reference() {
    let content = r#"
table posts {
  user_id UUID on_delete cascade
}
"#;

    let err = Schema::parse(content).expect_err("fk action without reference must fail");
    assert!(
        err.contains(
            "on_delete requires a preceding foreign key for column 'user_id' in table 'posts'"
        ),
        "{err}"
    );
}

#[test]
fn test_parse_schema_rejects_unknown_fk_action() {
    let content = r#"
table posts {
  user_id UUID references users(id) on_delete cascad
}
"#;

    let err = Schema::parse(content).expect_err("unknown fk action must fail");
    assert!(
        err.contains("unknown foreign key action 'cascad' for column 'user_id' in table 'posts'"),
        "{err}"
    );
}

#[test]
fn test_parse_schema_rejects_duplicate_fk_actions() {
    let content = r#"
table posts {
  user_id UUID references users(id) on_delete cascade on_delete restrict
}
"#;

    let err = Schema::parse(content).expect_err("duplicate fk action must fail");
    assert!(
        err.contains("duplicate on_delete action for column 'user_id' in table 'posts'"),
        "{err}"
    );
}

#[test]
fn test_parse_schema_rejects_malformed_references_target() {
    for content in [
        r#"
table posts {
  user_id UUID references users(id))
}
"#,
        r#"
table posts {
  user_id UUID references users(i-d)
}
"#,
    ] {
        let err = Schema::parse(content).expect_err("malformed references target must fail");
        assert!(err.contains("Invalid foreign key reference target"));
        assert!(err.contains("column 'user_id' in table 'posts'"));
    }
}

#[test]
fn test_parse_schema_allows_default_expression_options() {
    let content = r#"
table users {
  id UUID primary_key default gen_random_uuid()
  created_at timestamptz not_null default now()
}
"#;

    let schema = Schema::parse(content).expect("default expressions should parse");
    assert!(schema.table("users").unwrap().has_column("created_at"));
}

#[test]
fn test_parse_schema_supports_declared_enum_column_type() {
    let content = r#"
enum ticket_status { draft, active, cancelled }

table tickets {
  id UUID
  status ticket_status
}
"#;

    let schema = Schema::parse(content).expect("declared enum type should parse");
    let status = schema
        .table("tickets")
        .and_then(|table| table.column_type("status"))
        .expect("status column should exist");
    let ColumnType::Enum { name, values } = status else {
        panic!("expected enum column type, got {:?}", status);
    };
    assert_eq!(name, "ticket_status");
    assert_eq!(values, &["draft", "active", "cancelled"]);
}

#[test]
fn test_parse_schema_supports_quoted_empty_enum_values() {
    let schema = Schema::parse(
        r#"
enum ticket_status { "", draft }

table tickets {
  status ticket_status
}
"#,
    )
    .expect("quoted empty enum values should parse");
    let status = schema
        .table("tickets")
        .and_then(|table| table.column_type("status"))
        .expect("status column should exist");
    let ColumnType::Enum { values, .. } = status else {
        panic!("expected enum column type, got {:?}", status);
    };
    assert_eq!(values, &["", "draft"]);
}

#[test]
fn test_parse_schema_rejects_invalid_enum_names() {
    let err = Schema::parse("enum bad-name { draft }").expect_err("invalid enum name must fail");
    assert!(err.contains("Invalid enum name 'bad-name'"));

    let schema = Schema::parse(
        r#"
enum app.ticket_status { draft, active }

table tickets {
  status app.ticket_status
}
"#,
    )
    .expect("schema-qualified enum names should parse");
    assert!(schema.table("tickets").unwrap().has_column("status"));
}

#[test]
fn test_parse_schema_rejects_invalid_quoted_enum_value_tokens() {
    let err = Schema::parse(r#"enum status { "draft" "active" }"#)
        .expect_err("quoted enum token with trailing content must fail");
    assert!(err.contains(r#"invalid enum value token '"draft" "active"'"#));
}

#[test]
fn test_parse_schema_rejects_malformed_table_headers() {
    let missing_name = r#"
table {
  id UUID
}
"#;
    let err = Schema::parse(missing_name).expect_err("missing table name must fail");
    assert!(err.contains("Missing name for table declaration"));

    let unknown_option = r#"
table users audit {
  id UUID
}
"#;
    let err = Schema::parse(unknown_option).expect_err("unknown table option must fail");
    assert!(err.contains("Unknown table option 'audit' for 'users'"));

    let duplicate_option = r#"
table users rls rls {
  id UUID
}
"#;
    let err = Schema::parse(duplicate_option).expect_err("duplicate table option must fail");
    assert!(err.contains("Duplicate table option 'rls' for 'users'"));

    let trailing_content = "table users { id UUID }";
    let err = Schema::parse(trailing_content).expect_err("inline table body must fail");
    assert!(err.contains("Trailing content after table opening brace for 'users'"));
}

#[test]
fn test_parse_schema_rejects_invalid_table_names() {
    let content = r#"
table bad-name {
  id UUID
}
"#;

    let err = Schema::parse(content).expect_err("invalid table name must fail");
    assert!(err.contains("Invalid table name 'bad-name'"));

    let schema = Schema::parse(
        r#"
table app.users {
  id UUID
}
"#,
    )
    .expect("schema-qualified table names should parse");
    assert!(schema.has_table("app.users"));
}

#[test]
fn test_parse_schema_rejects_table_before_closing_current_table() {
    let content = r#"
table users {
  id UUID

table posts {
  id UUID
}
"#;

    let err = Schema::parse(content).expect_err("nested table declaration must fail");
    assert!(err.contains("Table declaration encountered before closing table 'users'"));
}

#[test]
fn test_parse_schema_rejects_trailing_table_close_content() {
    let content = r#"
table users {
  id UUID
} trailing
}
"#;

    let err = Schema::parse(content).expect_err("trailing close content must fail");
    assert!(err.contains("Trailing content after table closing brace for 'users'"));
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
fn test_parse_schema_rejects_duplicate_views() {
    let content = r#"
view v_users $$
SELECT 1
$$

materialized view v_users $$
SELECT 2
$$
"#;
    let err = Schema::parse(content).expect_err("duplicate view must fail");
    assert!(err.contains("duplicate view declaration 'v_users'"));
}

#[test]
fn test_parse_schema_rejects_invalid_view_names() {
    let content = "view bad-name $$ SELECT 1 $$";
    let err = Schema::parse(content).expect_err("invalid view name must fail");
    assert!(err.contains("Invalid view name 'bad-name'"));

    let schema = Schema::parse("materialized view reporting.active_users $$ SELECT 1 $$")
        .expect("schema-qualified view names should parse");
    assert!(schema.has_table("reporting.active_users"));
}

#[test]
fn test_parse_schema_consumes_multiline_resource_blocks() {
    let content = r#"
bucket avatars {
  provider aws
  region "ap-southeast-1"
}

table files {
  id UUID
}
"#;

    let schema = Schema::parse(content).unwrap();
    let bucket = schema.resources.get("avatars").expect("bucket missing");
    assert_eq!(bucket.kind, "bucket");
    assert_eq!(bucket.provider.as_deref(), Some("aws"));
    assert_eq!(
        bucket.properties.get("region").map(String::as_str),
        Some("ap-southeast-1")
    );
    assert!(schema.has_table("files"));
}

#[test]
fn test_parse_schema_preserves_quoted_resource_values() {
    let content = r#"
bucket avatars {
  provider aws
  display_name "Profile } Images"
  region 'ap southeast 1'
}
"#;

    let schema = Schema::parse(content).unwrap();
    let bucket = schema.resources.get("avatars").expect("bucket missing");
    assert_eq!(
        bucket.properties.get("display_name").map(String::as_str),
        Some("Profile } Images")
    );
    assert_eq!(
        bucket.properties.get("region").map(String::as_str),
        Some("ap southeast 1")
    );
}

#[test]
fn test_parse_schema_rejects_unclosed_resource_blocks() {
    let content = r#"
queue jobs {
  provider sqs
"#;

    let err = Schema::parse(content).expect_err("unclosed resource must fail");
    assert!(err.contains("Unclosed queue resource definition"));
}

#[test]
fn test_parse_schema_rejects_resource_property_without_value() {
    let content = r#"
bucket avatars {
  provider
}
"#;

    let err = Schema::parse(content).expect_err("missing resource value must fail");
    assert!(err.contains("Resource property 'provider' in 'avatars' requires a value"));
}

#[test]
fn test_parse_schema_rejects_duplicate_resource_properties() {
    let content = r#"
bucket avatars {
  provider aws
  provider gcp
}
"#;

    let err = Schema::parse(content).expect_err("duplicate resource property must fail");
    assert!(err.contains("Duplicate resource property 'provider' in 'avatars'"));
}

#[test]
fn test_parse_schema_rejects_duplicate_resource_names() {
    let content = r#"
bucket notifications { provider s3 }
queue notifications { provider sqs }
"#;

    let err = Schema::parse(content).expect_err("duplicate resource name must fail");
    assert!(err.contains("duplicate resource declaration 'notifications'"));
}

#[test]
fn test_parse_schema_rejects_resource_without_name() {
    let content = r#"
bucket { provider s3 }
"#;

    let err = Schema::parse(content).expect_err("missing resource name must fail");
    assert!(err.contains("Missing name for bucket declaration"));
}

#[test]
fn test_parse_schema_rejects_invalid_resource_names() {
    let content = "bucket bad-name { provider s3 }";

    let err = Schema::parse(content).expect_err("invalid resource name must fail");
    assert!(err.contains("Invalid bucket resource name 'bad-name'"));
}

#[test]
fn test_parse_schema_rejects_trailing_resource_content_without_block() {
    let content = "bucket avatars provider s3";

    let err = Schema::parse(content).expect_err("trailing resource content must fail");
    assert!(err.contains("Trailing content after bucket resource name"));
}

#[test]
fn test_parse_qail_migration_supports_explicit_alter_add_column_lines() {
    let mut schema = Schema::parse(
        r#"
table whatsapp_phone_configs {
  id UUID
}
"#,
    )
    .unwrap();

    let changes = schema
        .parse_qail_migration(
            r#"
alter whatsapp_phone_configs add automation_reply_enabled:boolean:default=true
alter whatsapp_phone_configs add ai_reply_enabled:boolean:default=true
"#,
        )
        .expect("explicit alter add-column migration should merge");

    assert_eq!(changes, 2);
    let table = schema
        .table("whatsapp_phone_configs")
        .expect("whatsapp_phone_configs should still exist");
    assert!(table.has_column("automation_reply_enabled"));
    assert!(table.has_column("ai_reply_enabled"));
}

#[test]
fn test_parse_qail_migration_rejects_invalid_explicit_alter_table_name() {
    let mut schema = Schema::default();
    let err = schema
        .parse_qail_migration("alter bad-name add enabled:boolean")
        .expect_err("invalid alter table name must fail");
    assert!(err.contains("invalid alter table name 'bad-name'"));
}

#[test]
fn test_parse_qail_migration_rejects_unknown_explicit_alter_column_type() {
    let mut schema = Schema::default();
    let err = schema
        .parse_qail_migration("alter users add enabled:booleen")
        .expect_err("unknown explicit alter column type must fail");
    assert!(
        err.contains("unknown column type 'booleen' for column 'enabled' in alter 'users'"),
        "{err}"
    );
}

#[test]
fn test_parse_qail_migration_rejects_conflicting_existing_column_type() {
    let mut schema = Schema::parse(
        r#"
table users {
  id UUID
}
"#,
    )
    .unwrap();

    let err = schema
        .parse_qail_migration(
            r#"
table users {
  id TEXT
}
"#,
        )
        .expect_err("conflicting migration column type must fail");
    assert!(
        err.contains("conflicting column type for 'users.id'"),
        "{err}"
    );
}

#[test]
fn test_parse_qail_migration_rejects_conflicting_explicit_alter_column_type() {
    let mut schema = Schema::parse(
        r#"
table users {
  id UUID
}
"#,
    )
    .unwrap();

    let err = schema
        .parse_qail_migration("alter users add id:text")
        .expect_err("conflicting explicit alter column type must fail");
    assert!(
        err.contains("conflicting column type for 'users.id'"),
        "{err}"
    );
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
fn test_scan_file_const_array_with_inline_comments_preserves_columns() {
    // Regression: inline `//` comments inside const column arrays must not
    // swallow the rest of the statement during scanner pre-processing.
    let content = r#"
const ORDER_COLUMNS: &[&str] = &[
    "id",                 // 0
    "invoice_number",     // 1
    "status",             // 2
    "total_amount",       // 3
    "created_at",         // 4
];

fn list(uid: &str) {
    let _cmd = qail_core::ast::Qail::get("orders")
        .columns(ORDER_COLUMNS)
        .eq("user_id", uid)
        .order_by("created_at", qail_core::ast::SortOrder::Desc);
}
"#;

    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 1);
    assert_eq!(usages[0].table, "orders");
    assert!(usages[0].columns.contains(&"id".to_string()));
    assert!(usages[0].columns.contains(&"invoice_number".to_string()));
    assert!(usages[0].columns.contains(&"status".to_string()));
    assert!(usages[0].columns.contains(&"total_amount".to_string()));
    assert!(usages[0].columns.contains(&"created_at".to_string()));
    assert!(usages[0].columns.contains(&"user_id".to_string()));
}

#[test]
fn test_scan_file_resolves_helper_param_tables_and_columns_from_call_sites() {
    let content = r#"
const USERS_TABLE: &str = "users";
const USERS_COLUMNS: &[&str] = &["id", "email"];
const ORDERS_COLUMNS: &[&str] = &["id", "status"];

async fn fetch_one_by_id(table: &str, columns: &[&str], id: &str) {
    let _cmd = Qail::get(table).columns(columns).eq("id", id).limit(1);
}

async fn demo() {
    fetch_one_by_id(USERS_TABLE, USERS_COLUMNS, "u1").await;
    fetch_one_by_id("orders", ORDERS_COLUMNS, "o1").await;
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 2);
    assert_eq!(usages[0].table, "users");
    assert!(usages[0].columns.contains(&"id".to_string()));
    assert!(usages[0].columns.contains(&"email".to_string()));
    assert_eq!(usages[1].table, "orders");
    assert!(usages[1].columns.contains(&"id".to_string()));
    assert!(usages[1].columns.contains(&"status".to_string()));
}

#[test]
fn test_scan_file_scopes_local_literal_bindings_by_function() {
    let content = r#"
fn users() {
    let table = "users";
    let columns = ["id", "email"];
    let _cmd = Qail::get(table).columns(columns);
}

fn orders() {
    let table = "orders";
    let columns = ["id", "status"];
    let _cmd = Qail::get(table).columns(columns);
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(
        usages.len(),
        2,
        "same local binding names in different functions must not fan out: {usages:?}"
    );
    assert_eq!(usages[0].table, "users");
    assert_eq!(usages[0].columns, vec!["id", "email"]);
    assert_eq!(usages[1].table, "orders");
    assert_eq!(usages[1].columns, vec!["id", "status"]);
}

#[test]
fn test_scan_file_local_literal_shadowing_uses_nearest_binding() {
    let content = r#"
fn demo() {
    let table = "users";
    let _users = Qail::get(table).column("email");

    let table = "orders";
    let _orders = Qail::get(table).column("status");
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(
        usages.len(),
        2,
        "shadowed local table binding must not fan out: {usages:?}"
    );
    assert_eq!(usages[0].table, "users");
    assert_eq!(usages[1].table, "orders");
}

#[test]
fn test_scan_file_inner_block_literal_binding_does_not_escape_scope() {
    let content = r#"
fn demo(table: &str) {
    {
        let table = "users";
        let _users = Qail::get(table).column("email");
    }

    let _dynamic = Qail::get(table).column("id");
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(
        usages.len(),
        1,
        "inner block literal binding must not resolve later dynamic query: {usages:?}"
    );
    assert_eq!(usages[0].table, "users");
}

#[test]
fn test_scan_file_function_local_const_does_not_bleed_between_functions() {
    let content = r#"
fn users() {
    const TABLE: &str = "users";
    let _cmd = Qail::get(TABLE).column("email");
}

fn orders() {
    const TABLE: &str = "orders";
    let _cmd = Qail::get(TABLE).column("status");
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(
        usages.len(),
        2,
        "function-local const table binding must not fan out: {usages:?}"
    );
    assert_eq!(usages[0].table, "users");
    assert_eq!(usages[1].table, "orders");
}

#[test]
fn test_scan_file_ignores_helper_calls_in_comments_for_param_substitution() {
    let content = r#"
async fn fetch_one(table: &str) {
    let _cmd = Qail::get(table).column("id");
}

async fn demo() {
    fetch_one("users").await;
    // fetch_one("ghost").await;
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(
        usages.len(),
        1,
        "commented helper calls must not create phantom table usages: {usages:?}"
    );
    assert_eq!(usages[0].table, "users");
}

#[test]
fn test_helper_rls_detection_ignores_comments_in_helper_body() {
    let content = r#"
async fn exec_without_rls(cmd: Qail, conn: &mut qail_pg::PooledConnection) {
    // conn.fetch_all_uncached(&cmd.with_rls(ctx)).await;
    let _ = conn.fetch_all_uncached(&cmd).await;
}

async fn demo(conn: &mut qail_pg::PooledConnection) {
    let _ = exec_without_rls(Qail::get("orders").columns(["id"]), conn).await;
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 1);
    assert!(
        !usages[0].has_rls,
        "commented cmd.with_rls(...) must not mark helper argument as RLS-scoped"
    );
}

#[test]
fn test_scan_file_resolves_generic_helper_function_calls() {
    let content = r#"
async fn fetch_one<T>(table: &str, columns: &[&str], id: T) {
    let _cmd = Qail::get(table).columns(columns).eq("id", id).limit(1);
}

async fn demo() {
    fetch_one::<uuid::Uuid>("users", &["id", "email"], uuid::Uuid::nil()).await;
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 1, "generic helper should resolve: {usages:?}");
    assert_eq!(usages[0].table, "users");
    assert!(usages[0].columns.contains(&"email".to_string()));
}

#[test]
fn test_scan_file_resolves_multiline_helper_calls_like_charters_admin() {
    let content = r#"
const ALIEUS_COLUMNS: &[&str] = &["id", "name"];
const DIATHESI_TEMPLATE_TABLE: &str = "charters_diathesi_templates";
const DIATHESI_TEMPLATE_COLUMNS: &[&str] = &["id", "tenant_id", "drasimos_id"];

async fn fetch_rows(_state: &AppState, _claims: &AdminClaims, _cmd: Qail, _table: &str) {}
async fn execute_write(_state: &AppState, _claims: &AdminClaims, _cmd: Qail, _ctx: &str) {}

async fn fetch_one_by_id(
    state: &AppState,
    claims: &AdminClaims,
    table: &str,
    columns: &[&str],
    id: &str,
) {
    let _ = fetch_rows(
        state,
        claims,
        Qail::get(table).columns(columns).eq("id", id).limit(1),
        table,
    )
    .await;
}

async fn delete_by_id(
    state: &AppState,
    claims: &AdminClaims,
    table: &str,
    id: &str,
) {
    let _ = execute_write(
        state,
        claims,
        Qail::del(table).eq("id", id),
        "delete",
    )
    .await;
}

async fn get_alieus(state: &AppState, claims: &AdminClaims, id: &str) {
    let _ = fetch_one_by_id(state, claims, "charters_alieus", ALIEUS_COLUMNS, id).await;
}

async fn get_template(state: &AppState, claims: &AdminClaims, id: &str) {
    let _ = fetch_one_by_id(
        state,
        claims,
        DIATHESI_TEMPLATE_TABLE,
        DIATHESI_TEMPLATE_COLUMNS,
        id,
    )
    .await;
    let _ = delete_by_id(state, claims, DIATHESI_TEMPLATE_TABLE, id).await;
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert!(
        usages
            .iter()
            .any(|usage| usage.table == "charters_alieus"
                && usage.columns.contains(&"name".to_string())),
        "expected helper call-site substitution to resolve charters_alieus columns, got: {:?}",
        usages
            .iter()
            .map(|usage| (&usage.table, &usage.columns))
            .collect::<Vec<_>>()
    );
    assert!(
        usages
            .iter()
            .any(|usage| usage.table == "charters_diathesi_templates"
                && usage.columns.contains(&"tenant_id".to_string())
                && usage.action == "GET"),
        "expected helper call-site substitution to resolve template GET columns, got: {:?}",
        usages
            .iter()
            .map(|usage| (&usage.table, &usage.columns, &usage.action))
            .collect::<Vec<_>>()
    );
    assert!(
        usages
            .iter()
            .any(|usage| usage.table == "charters_diathesi_templates" && usage.action == "DEL"),
        "expected helper call-site substitution to resolve template delete helper, got: {:?}",
        usages
            .iter()
            .map(|usage| (&usage.table, &usage.action))
            .collect::<Vec<_>>()
    );
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
fn test_scan_typed_api_columns_and_filters() {
    let content = r#"
let q = Qail::typed(orders::table)
    .typed_column(orders::id())
    .typed_columns([orders::status(), orders::tenant_id()])
    .typed_eq(orders::tenant_id(), tenant_id)
    .typed_filter(orders::status(), Operator::Eq, "paid");
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 1);
    assert_eq!(usages[0].table, "orders");
    assert_eq!(usages[0].action, "TYPED");
    for expected in ["id", "status", "tenant_id"] {
        assert!(
            usages[0].columns.contains(&expected.to_string()),
            "typed API column {expected} should be scanned: {:?}",
            usages[0]
        );
    }
    assert!(
        usages[0].has_explicit_tenant_scope,
        "typed tenant_id filter should count as explicit tenant scope: {:?}",
        usages[0]
    );
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
    let line = r#"Qail::set("orders")
        .set_value("status", "Paid")
        .set_opt("amount", maybe_amount)
        .set_coalesce("notes", "default")"#;
    let cols = extract_columns(line);
    assert!(cols.contains(&"status".to_string()));
    assert!(cols.contains(&"amount".to_string()));
    assert!(cols.contains(&"notes".to_string()));
}

#[test]
fn test_extract_columns_query_builder_surface_methods() {
    let line = r#"Qail::get("orders")
        .or_filter("status", Operator::Eq, "paid")
        .array_elem_contained_in_text("tags", "urgent")
        .group_by(["tenant_id", "status"])
        .distinct_on(["tenant_id"])
        .left_join("order_items", "orders.id", "order_items.order_id")"#;
    let cols = extract_columns(line);

    for expected in [
        "status",
        "tags",
        "tenant_id",
        "orders.id",
        "order_items.order_id",
    ] {
        assert!(
            cols.contains(&expected.to_string()),
            "expected {expected} in extracted columns: {cols:?}"
        );
    }
}

#[test]
fn test_extract_columns_condition_builder_surface_methods() {
    let line = r#"Qail::get("orders")
        .filter_cond(eq("status", "paid"))
        .having_cond(gte("total", 10))
        .inner_join_conds(
            "order_items",
            vec![eq("orders.id", Value::Column("order_items.order_id".to_string()))],
        )"#;
    let cols = extract_columns(line);

    for expected in ["status", "total", "orders.id", "order_items.order_id"] {
        assert!(
            cols.contains(&expected.to_string()),
            "expected {expected} in extracted condition columns: {cols:?}"
        );
    }
}

#[test]
fn test_extract_columns_expression_projection_surface_methods() {
    let line = r#"Qail::get("orders")
        .column_expr(col("status").with_alias("order_status"))
        .select_expr(sum("total").alias("total_sum"))
        .columns_expr(vec![
            Expr::Aliased { name: "users.email".to_string(), alias: "user_email".to_string() },
            count_filter(vec![eq("payment_status", "paid")]).alias("paid_count"),
        ])"#;
    let cols = extract_columns(line);

    for expected in ["status", "total", "users.email", "payment_status"] {
        assert!(
            cols.contains(&expected.to_string()),
            "expected {expected} in extracted expression columns: {cols:?}"
        );
    }
}

#[test]
fn test_extract_columns_order_by_expr_and_condition_helpers() {
    let line = r#"Qail::get("orders")
        .order_by_expr(col("created_at"), SortOrder::Desc)
        .filter_cond(key_exists("metadata", "priority"))
        .filter_cond(cond(col("status"), Operator::Eq, "paid"))"#;
    let cols = extract_columns(line);

    for expected in ["created_at", "metadata", "status"] {
        assert!(
            cols.contains(&expected.to_string()),
            "expected {expected} in expression/condition columns: {cols:?}"
        );
    }
}

#[test]
fn test_extract_columns_ast_native_helper_builders() {
    let line = r#"Qail::get("orders")
        .column_expr(json("metadata", "priority").alias("priority"))
        .column_expr(string_agg("customer_name", ", ").alias("customers"))
        .column_expr(percentage("delivered_count", "sent_count").alias("delivery_rate"))
        .filter_cond(recent_col("updated_at", "7 days"))
        .filter_cond(in_list("status", ["paid", "refunded"]))
        .set_value("total_quantity", inc("usage_daily_rollups.total_quantity", 1))"#;
    let cols = extract_columns(line);

    for expected in [
        "metadata",
        "customer_name",
        "delivered_count",
        "sent_count",
        "updated_at",
        "status",
        "usage_daily_rollups.total_quantity",
    ] {
        assert!(
            cols.contains(&expected.to_string()),
            "expected {expected} from AST-native helper builders: {cols:?}"
        );
    }
}

#[test]
fn test_extract_columns_string_backed_expr_helpers() {
    let line = r#"Qail::get("orders")
        .column_expr(cast("created_at", "text").alias("created_text"))
        .column_expr(binary("total", BinaryOp::Add, "fee").alias("gross_total"))
        .column_expr(replace("phone", text("+"), text("")).alias("normalized_phone"))
        .column_expr(concat(["first_name", "last_name"]).alias("full_name"))
        .column_expr(coalesce(["nickname", "display_name"]).alias("preferred_name"))
        .column_expr("email".lower().with_alias("email_lower"))"#;
    let cols = extract_columns(line);

    for expected in [
        "created_at",
        "total",
        "fee",
        "phone",
        "first_name",
        "last_name",
        "nickname",
        "display_name",
        "email",
    ] {
        assert!(
            cols.contains(&expected.to_string()),
            "expected {expected} from string-backed expression helpers: {cols:?}"
        );
    }
    assert!(
        !cols.contains(&"+".to_string()),
        "text literal arguments should not be extracted as columns: {cols:?}"
    );
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
    assert!(cols.contains(&"name".to_string()));
}

#[test]
fn test_extract_columns_merge_actions() {
    let line = r#"Qail::merge_into("orders")
        .merge_on_column("id", Operator::Eq, "source.order_id")
        .when_matched_update(&[("status", Expr::Named("source.status".into()))])
        .when_not_matched_insert(&["id", "status"], &[Expr::Named("source.order_id".into()), Expr::Named("source.status".into())])"#;
    let cols = extract_columns(line);

    assert!(cols.contains(&"id".to_string()));
    assert!(cols.contains(&"status".to_string()));
}

#[test]
fn test_scan_file_extracts_related_tables_from_builder_surfaces() {
    let content = r#"
let q = Qail::get("orders")
    .left_join("order_items items", "orders.id", "items.order_id")
    .join_conds(JoinKind::Left, "shipments", vec![])
    .join_on_optional("tenants")
    .update_from(["accounts"]);
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 1);
    for expected in ["order_items", "shipments", "tenants", "accounts"] {
        assert!(
            usages[0].related_tables.contains(&expected.to_string()),
            "expected related table {expected}: {:?}",
            usages[0].related_tables
        );
    }
}

#[test]
fn test_schema_validation_covers_set_opt_column_typos() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
  status TEXT
}
"#,
    )
    .unwrap();

    let content = r#"
let q = Qail::set("orders")
    .set_opt("statuz", Some("paid"))
    .eq("id", order_id);
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    assert!(
        diagnostics.iter().any(|d| d.message.contains("statuz")),
        "set_opt column typo should be schema-validated: {:?}",
        diagnostics
    );
}

#[test]
fn test_schema_validation_covers_join_related_table_typos() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
}
"#,
    )
    .unwrap();

    let content = r#"
let q = Qail::get("orders")
    .left_join("order_itemz", "orders.id", "order_itemz.order_id");
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    assert!(
        diagnostics
            .iter()
            .any(|d| d.message.contains("order_itemz")),
        "join related table typo should be schema-validated: {:?}",
        diagnostics
    );
}

#[test]
fn test_schema_validation_covers_join_conds_related_table_typos() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
}
"#,
    )
    .unwrap();

    let content = r#"
let q = Qail::get("orders")
    .join_conds(JoinKind::Left, "shipmentz", vec![]);
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    assert!(
        diagnostics.iter().any(|d| d.message.contains("shipmentz")),
        "join_conds related table typo should be schema-validated: {:?}",
        diagnostics
    );
}

#[test]
fn test_schema_validation_resolves_primary_table_alias_columns() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
  status TEXT
}
"#,
    )
    .unwrap();

    let content = r#"
let q = Qail::get("orders")
    .table_alias("o")
    .column("o.statuz");
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    assert!(
        diagnostics.iter().any(|d| d.message.contains("statuz")),
        "primary table alias columns should validate against the aliased table: {:?}",
        diagnostics
    );
}

#[test]
fn test_schema_validation_resolves_join_alias_columns() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
  user_id UUID
}

table users {
  id UUID
  email TEXT
}
"#,
    )
    .unwrap();

    let content = r#"
let q = Qail::get("orders")
    .left_join_as("users", "u", "orders.user_id", "u.id")
    .column("u.emial");
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    assert!(
        diagnostics.iter().any(|d| d.message.contains("emial")),
        "join alias columns should validate against the joined table: {:?}",
        diagnostics
    );
}

#[test]
fn test_schema_validation_covers_column_expr_typos() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
  status TEXT
  total INT
}
"#,
    )
    .unwrap();

    let content = r#"
let q = Qail::get("orders")
    .column_expr(col("statuz").with_alias("order_status"))
    .column_expr(sum("totl").alias("sum_total"));
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    assert!(
        diagnostics.iter().any(|d| d.message.contains("statuz")),
        "column_expr col() typo should be schema-validated: {:?}",
        diagnostics
    );
    assert!(
        diagnostics.iter().any(|d| d.message.contains("totl")),
        "column_expr aggregate typo should be schema-validated: {:?}",
        diagnostics
    );
}

#[test]
fn test_schema_validation_covers_expr_aliased_projection_typos() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
  status TEXT
}
"#,
    )
    .unwrap();

    let content = r#"
let q = Qail::get("orders")
    .column_expr(Expr::Aliased {
        name: "statuz".to_string(),
        alias: "order_status".to_string(),
    });
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    assert!(
        diagnostics.iter().any(|d| d.message.contains("statuz")),
        "Expr::Aliased projection name should be schema-validated: {:?}",
        diagnostics
    );
}

#[test]
fn test_schema_validation_resolves_join_alias_expression_columns() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
  user_id UUID
}

table users {
  id UUID
  email TEXT
}
"#,
    )
    .unwrap();

    let content = r#"
let q = Qail::get("orders")
    .left_join_as("users", "u", "orders.user_id", "u.id")
    .column_expr(col("u.emial").with_alias("user_email"));
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    assert!(
        diagnostics.iter().any(|d| d.message.contains("emial")),
        "join alias expression columns should validate against the joined table: {:?}",
        diagnostics
    );
}

#[test]
fn test_schema_validation_covers_order_by_expr_typos() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
  created_at TIMESTAMP
}
"#,
    )
    .unwrap();

    let content = r#"
let q = Qail::get("orders")
    .order_by_expr(col("cretaed_at"), SortOrder::Desc);
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    assert!(
        diagnostics.iter().any(|d| d.message.contains("cretaed_at")),
        "order_by_expr column typo should be schema-validated: {:?}",
        diagnostics
    );
}

#[test]
fn test_schema_validation_covers_ast_native_helper_builder_typos() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
  metadata JSONB
  customer_name TEXT
  delivered_count INT
  sent_count INT
  updated_at TIMESTAMP
  status TEXT
  total_quantity INT
}
"#,
    )
    .unwrap();

    let content = r#"
let q = Qail::get("orders")
    .column_expr(json("metdata", "priority").alias("priority"))
    .column_expr(string_agg("customer_nmae", ", ").alias("customers"))
    .column_expr(percentage("delivred_count", "sent_cout").alias("delivery_rate"))
    .filter_cond(recent_col("updted_at", "7 days"))
    .filter_cond(in_list("statuz", ["paid", "refunded"]))
    .set_value("total_quantity", inc("total_quanity", 1));
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    for expected in [
        "metdata",
        "customer_nmae",
        "delivred_count",
        "sent_cout",
        "updted_at",
        "statuz",
        "total_quanity",
    ] {
        assert!(
            diagnostics.iter().any(|d| d.message.contains(expected)),
            "AST-native helper typo {expected} should be schema-validated: {:?}",
            diagnostics
        );
    }
}

#[test]
fn test_schema_validation_covers_string_backed_expr_helper_typos() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
  created_at TIMESTAMP
  total INT
  fee INT
  phone TEXT
  first_name TEXT
  last_name TEXT
  nickname TEXT
  display_name TEXT
  email TEXT
}
"#,
    )
    .unwrap();

    let content = r#"
let q = Qail::get("orders")
    .column_expr(cast("cretaed_at", "text").alias("created_text"))
    .column_expr(binary("totl", BinaryOp::Add, "feee").alias("gross_total"))
    .column_expr(replace("phoen", text("+"), text("")).alias("normalized_phone"))
    .column_expr(concat(["first_nmae", "last_nmae"]).alias("full_name"))
    .column_expr(coalesce(["nicknmae", "display_nmae"]).alias("preferred_name"))
    .column_expr("emial".lower().with_alias("email_lower"));
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    for expected in [
        "cretaed_at",
        "totl",
        "feee",
        "phoen",
        "first_nmae",
        "last_nmae",
        "nicknmae",
        "display_nmae",
        "emial",
    ] {
        assert!(
            diagnostics.iter().any(|d| d.message.contains(expected)),
            "string-backed expression helper typo {expected} should be schema-validated: {:?}",
            diagnostics
        );
    }
}

#[test]
fn test_schema_validation_covers_on_conflict_update_typos() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
  status TEXT
  total INT
}
"#,
    )
    .unwrap();

    let content = r#"
let q = Qail::put("orders")
    .on_conflict_update(
        &["id"],
        &[("statuz", Expr::Named("orders.totl".into()))],
    );
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    for expected in ["statuz", "totl"] {
        assert!(
            diagnostics.iter().any(|d| d.message.contains(expected)),
            "on_conflict_update typo {expected} should be schema-validated: {:?}",
            diagnostics
        );
    }
}

#[test]
fn test_schema_validation_covers_chained_expression_builder_typos() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
  status TEXT
  total INT
  payment_status TEXT
  amount INT
  refunded_at TIMESTAMP
}
"#,
    )
    .unwrap();

    let content = r#"
let q = Qail::get("orders")
    .column_expr(
        case_when(eq("status", "paid"), col("total"))
            .when(eq("payment_statuz", "refunded"), col("amunt"))
            .otherwise("refundd_at")
            .alias("status_amount")
    )
    .column_expr(count().filter(vec![eq("paymnt_status", "paid")]).alias("paid_count"));
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    for expected in ["payment_statuz", "amunt", "refundd_at", "paymnt_status"] {
        assert!(
            diagnostics.iter().any(|d| d.message.contains(expected)),
            "chained expression typo {expected} should be schema-validated: {:?}",
            diagnostics
        );
    }
}

#[test]
fn test_schema_validation_covers_merge_condition_typos() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
  status TEXT
}

table stage_orders {
  order_id UUID
  status TEXT
  active BOOL
}
"#,
    )
    .unwrap();

    let content = r#"
let q = Qail::merge_into("orders")
    .using_table_as("stage_orders", "s")
    .merge_on_condition(Condition {
        left: Expr::Named("orders.idd".to_string()),
        op: Operator::Eq,
        value: Value::Column("s.order_idd".to_string()),
        is_array_unnest: false,
    })
    .when_matched_update_if(
        vec![eq("s.actve", true)],
        &[("status", Expr::Named("s.status".to_string()))],
    );
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    for expected in ["idd", "order_idd", "actve"] {
        assert!(
            diagnostics.iter().any(|d| d.message.contains(expected)),
            "MERGE condition typo {expected} should be schema-validated: {:?}",
            diagnostics
        );
    }
}

#[test]
fn test_schema_validation_covers_merge_on_column_alias_typos() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
  status TEXT
}

table stage_orders {
  order_id UUID
  status TEXT
}
"#,
    )
    .unwrap();

    let content = r#"
let q = Qail::merge_into("orders")
    .target_alias("o")
    .using_table_as("stage_orders", "s")
    .merge_on_column("o.idd", Operator::Eq, "s.order_idd");
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    for expected in ["idd", "order_idd"] {
        assert!(
            diagnostics.iter().any(|d| d.message.contains(expected)),
            "merge_on_column alias typo {expected} should be schema-validated: {:?}",
            diagnostics
        );
    }
}

#[test]
fn test_schema_validation_covers_filter_cond_condition_builder_typos() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
  status TEXT
}
"#,
    )
    .unwrap();

    let content = r#"
let q = Qail::get("orders")
    .filter_cond(eq("statuz", "paid"));
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    assert!(
        diagnostics.iter().any(|d| d.message.contains("statuz")),
        "filter_cond condition-builder column typo should be schema-validated: {:?}",
        diagnostics
    );
}

#[test]
fn test_schema_validation_covers_direct_expr_string_left_typos() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
  status TEXT
  payment_status TEXT
}
"#,
    )
    .unwrap();

    let content = r#"
let q = Qail::get("orders")
    .filter_cond(cond("statuz".into(), Operator::Eq, "paid"))
    .filter_cond(Condition {
        left: "paymnt_status".into(),
        op: Operator::Eq,
        value: Value::String("paid".to_string()),
        is_array_unnest: false,
    });
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    for expected in ["statuz", "paymnt_status"] {
        assert!(
            diagnostics.iter().any(|d| d.message.contains(expected)),
            "direct Expr string-left typo {expected} should be schema-validated: {:?}",
            diagnostics
        );
    }
}

#[test]
fn test_schema_validation_does_not_treat_expression_col_helpers_as_schema_columns() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
  sales_start_date DATE
}
"#,
    )
    .unwrap();

    let content = r#"
let q = Qail::get("orders")
    .filter_cond(Condition {
        left: Expr::FunctionCall {
            name: "COALESCE".to_string(),
            args: vec![col("sales_start_date"), col("CURRENT_DATE")],
            alias: None,
        },
        op: Operator::Lte,
        value: Value::Expr(Box::new(col("CURRENT_DATE"))),
        is_array_unnest: false,
    });
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    assert!(
        !diagnostics
            .iter()
            .any(|d| d.message.contains("CURRENT_DATE")),
        "SQL expression helper names must not be treated as schema columns: {:?}",
        diagnostics
    );
}

#[test]
fn test_schema_validation_does_not_treat_set_value_string_literals_as_columns() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
  status TEXT
}
"#,
    )
    .unwrap();

    let content = r#"
let q = Qail::set("orders")
    .set_value("status", "paid")
    .eq("id", order_id);
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    assert!(
        diagnostics.is_empty(),
        "set_value string literals should remain values, not schema columns: {:?}",
        diagnostics
    );
}

#[test]
fn test_validate_against_schema_diagnostics_casted_column_no_false_positive() {
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
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);
    assert!(
        diagnostics.is_empty(),
        "casted column should not produce schema error: {:?}",
        diagnostics
    );
}

#[test]
fn test_validate_against_schema_diagnostics_view_table_name_is_allowed() {
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
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    assert!(
        diagnostics.is_empty(),
        "view-backed query should not fail table validation: {:?}",
        diagnostics
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

    // "results" is the main table; the nested CTE source query should also
    // be represented so its table/columns are schema-validated.
    assert_eq!(usages.len(), 2);
    // It should NOT be a CTE ref since "results" != "agg"
    assert!(!usages[0].is_cte_ref);
    assert_eq!(usages[1].table, "orders");
    assert!(!usages[1].is_cte_ref);
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
fn test_cte_alias_does_not_bleed_across_functions() {
    let content = r#"
fn define_cte() {
    let _cte = Qail::get("orders").columns(["total"]).to_cte("agg");
}

fn read_real_table() {
    let _read = Qail::get("agg").column("id");
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 2);
    assert!(
        !usages[1].is_cte_ref,
        "CTE aliases from another function must not hide real table validation"
    );
}

#[test]
fn test_bound_cte_source_does_not_cross_nested_duplicate_function_name() {
    let content = r#"
fn demo() {
    let source = Qail::get("orders").columns(["total"]);

    fn demo() {
        let _main = Qail::get("results").with("agg", source);
        let _read = Qail::get("agg").column("total");
    }
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 3);
    assert!(
        !usages[2].is_cte_ref,
        "bound CTE source variables must not cross into a different nested function"
    );
}

#[test]
fn test_cte_alias_does_not_apply_before_definition() {
    let content = r#"
fn demo() {
    let _read = Qail::get("agg").column("id");
    let _cte = Qail::get("orders").columns(["total"]).to_cte("agg");
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 2);
    assert!(
        !usages[0].is_cte_ref,
        "later CTE aliases must not mark earlier table refs as CTE refs"
    );
}

#[test]
fn test_cte_alias_with_bound_qail_source_is_detected() {
    let content = r#"
fn demo() {
    let source = Qail::get("orders").columns(["total"]);
    let _main = Qail::get("results").with("agg", source);
    let _read = Qail::get("agg").column("total");
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 3);
    assert!(
        usages[2].is_cte_ref,
        "with(alias, bound_qail_source) should mark later alias refs as CTE refs"
    );
}

#[test]
fn test_schema_validation_catches_nested_cte_source_table_typos() {
    let schema = Schema::parse(
        r#"
table results {
  id UUID
}

table orders {
  id UUID
}
"#,
    )
    .unwrap();

    let content = r#"
fn demo() {
    let _q = Qail::get("results")
        .with("agg", Qail::get("ordres").column("id"));
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);
    assert!(
        diagnostics
            .iter()
            .any(|d| d.message.contains("Table 'ordres' not found")),
        "nested Qail CTE source queries should be represented in IR: {:?}",
        diagnostics
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
        related_tables: Vec::new(),
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
        related_tables: Vec::new(),
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
fn test_schema_validation_catches_qualified_column_typos() {
    let schema = Schema::parse(
        r#"
table users {
  id UUID
  email TEXT
}
"#,
    )
    .unwrap();

    let content = r#"
fn demo() {
    let _q = Qail::get("users").column("users.emial");
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);
    assert!(
        diagnostics.iter().any(|d| d
            .message
            .contains("Column 'emial' not found in table 'users'")),
        "qualified column typos should remain visible to schema validation: {:?}",
        diagnostics
    );
}

#[test]
fn test_schema_validation_catches_qualified_filter_column_typos() {
    let schema = Schema::parse(
        r#"
table users {
  id UUID
  email TEXT
}
"#,
    )
    .unwrap();

    let content = r#"
fn demo() {
    let _q = Qail::get("users").eq("users.emial", "x");
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);
    assert!(
        diagnostics.iter().any(|d| d
            .message
            .contains("Column 'emial' not found in table 'users'")),
        "qualified filter column typos should remain visible to schema validation: {:?}",
        diagnostics
    );
}

#[test]
fn test_schema_validation_accepts_public_qualified_source_table_alias() {
    let schema = Schema::parse(
        r#"
table orders rls {
  id UUID
  tenant_id UUID
}
"#,
    )
    .unwrap();

    let content = r#"
fn demo() {
    let _q = Qail::get("public.orders")
        .column("public.orders.id")
        .with_rls(&ctx);
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);
    assert!(
        diagnostics.is_empty(),
        "public-qualified source tables should resolve to bare public schema tables: {:?}",
        diagnostics
    );
}

#[test]
fn test_schema_validation_catches_public_qualified_column_typos() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
  status TEXT
}
"#,
    )
    .unwrap();

    let content = r#"
fn demo() {
    let _q = Qail::get("public.orders").column("public.orders.statuz");
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);
    assert!(
        diagnostics.iter().any(|d| d
            .message
            .contains("Column 'statuz' not found in table 'orders'")),
        "public-qualified column typos should be validated against the public table: {:?}",
        diagnostics
    );
}

#[test]
fn test_schema_validation_accepts_bare_source_table_for_public_qualified_schema() {
    let schema = Schema::parse(
        r#"
table public.orders rls {
  id UUID
  tenant_id UUID
}
"#,
    )
    .unwrap();

    let content = r#"
fn demo() {
    let _q = Qail::get("orders").column("id");
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);
    assert!(
        diagnostics
            .iter()
            .any(|d| matches!(d.kind, ValidationDiagnosticKind::RlsWarning)),
        "bare source tables should inherit RLS metadata from public-qualified schema tables: {:?}",
        diagnostics
    );
    assert!(
        !diagnostics
            .iter()
            .any(|d| matches!(d.kind, ValidationDiagnosticKind::SchemaError)),
        "bare source tables should not fail against public-qualified schema tables: {:?}",
        diagnostics
    );
}

#[test]
fn test_source_audit_warns_on_unscoped_export_rls_table() {
    let schema = Schema::parse(
        r#"
table orders rls {
  id UUID
  tenant_id UUID
}
"#,
    )
    .unwrap();

    let content = r#"
fn demo() {
    let _q = Qail::export("orders").column("id");
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    assert!(
        diagnostics
            .iter()
            .any(|d| matches!(d.kind, ValidationDiagnosticKind::RlsWarning)),
        "export on RLS table should be scanned and warned when unscoped: {:?}",
        diagnostics
    );
}

#[test]
fn test_source_audit_validates_existing_table_constructor_typos() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
}
"#,
    )
    .unwrap();

    let content = r#"
fn demo() {
    let _q = Qail::truncate("ordres");
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    assert!(
        diagnostics.iter().any(|d| {
            matches!(d.kind, ValidationDiagnosticKind::SchemaError) && d.message.contains("ordres")
        }),
        "existing-table constructors should be scanned for table typos: {:?}",
        diagnostics
    );
}

#[test]
fn test_schema_validation_catches_bare_column_typos_for_public_qualified_schema() {
    let schema = Schema::parse(
        r#"
table public.orders {
  id UUID
  status TEXT
}
"#,
    )
    .unwrap();

    let content = r#"
fn demo() {
    let _q = Qail::get("orders").column("orders.statuz");
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);
    assert!(
        diagnostics.iter().any(|d| d
            .message
            .contains("Column 'statuz' not found in table 'public.orders'")),
        "bare column prefixes should validate against public-qualified schema tables: {:?}",
        diagnostics
    );
}

#[test]
fn test_schema_validation_catches_merge_action_column_typos() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
  status TEXT
}

table staging_orders {
  order_id UUID
  status TEXT
}
"#,
    )
    .unwrap();

    let content = r#"
fn demo() {
    let _q = Qail::merge_into("orders")
        .using_table("staging_orders")
        .merge_on_column("id", Operator::Eq, "staging.order_id")
        .when_matched_update(&[("statuz", Expr::Named("staging.status".into()))]);
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);
    assert!(
        diagnostics.iter().any(|d| d
            .message
            .contains("Column 'statuz' not found in table 'orders'")),
        "MERGE target action columns should be represented in IR: {:?}",
        diagnostics
    );
}

#[test]
fn test_schema_validation_catches_merge_source_table_typos() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
}

table staging_orders {
  order_id UUID
}
"#,
    )
    .unwrap();

    let content = r#"
fn demo() {
    let _q = Qail::merge_into("orders")
        .using_table("stagin_orders")
        .merge_on_column("id", Operator::Eq, "staging.order_id")
        .when_matched_do_nothing();
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);
    assert!(
        diagnostics
            .iter()
            .any(|d| d.message.contains("Table 'stagin_orders' not found")),
        "MERGE source tables should be represented in IR: {:?}",
        diagnostics
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
        related_tables: Vec::new(),
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
fn test_explicit_tenant_scope_detects_payload_setters() {
    let content = r#"
let q = Qail::add("usage_ledger")
    .set_value("tenant_id", tenant_id)
    .set_opt("tenant_id", Some(tenant_id))
    .set_value("metric", "waba_messages");
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 1);
    assert!(
        usages[0].has_explicit_tenant_scope,
        "tenant_id payload setters should count as explicit tenant scope"
    );
}

#[test]
fn test_explicit_tenant_scope_ignores_update_payload_setters() {
    let content = r#"
let q = Qail::set("orders")
    .set_opt("tenant_id", Some(tenant_id))
    .eq("id", order_id);
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 1);
    assert!(
        !usages[0].has_explicit_tenant_scope,
        "tenant_id update payload setters are not tenant filters"
    );
}

#[test]
fn test_rls_detection_late_with_rls_on_bound_query_var() {
    let content = r#"
async fn demo(conn: &mut qail_pg::PooledConnection, ctx: &qail_core::rls::RlsContext) {
    let cmd = Qail::get("orders")
        .columns(["id"])
        .limit(1);

    let _ = conn.fetch_all_uncached(&cmd.with_rls(ctx)).await;
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 1);
    assert!(
        usages[0].has_rls,
        "execution-site cmd.with_rls(...) should mark the bound query as RLS-scoped"
    );
}

#[test]
fn test_rls_detection_late_with_rls_does_not_bleed_across_same_var_name() {
    let content = r#"
async fn scoped(conn: &mut qail_pg::PooledConnection, ctx: &qail_core::rls::RlsContext) {
    let cmd = Qail::get("orders").columns(["id"]);
    let _ = conn.fetch_all_uncached(&cmd.with_rls(ctx)).await;
}

async fn unscoped(conn: &mut qail_pg::PooledConnection) {
    let cmd = Qail::get("orders").columns(["id"]);
    let _ = conn.fetch_all_uncached(&cmd).await;
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 2);
    assert!(usages[0].has_rls);
    assert!(
        !usages[1].has_rls,
        "late with_rls on a previous binding must not suppress warnings for a later same-name binding"
    );
}

#[test]
fn test_rls_detection_helper_param_with_rls_on_inline_qail_arg() {
    let content = r#"
async fn exec_with_rls(
    cmd: Qail,
    conn: &mut qail_pg::PooledConnection,
    ctx: &qail_core::rls::RlsContext,
) {
    let _ = conn.fetch_all_uncached(&cmd.with_rls(ctx)).await;
}

async fn demo(
    conn: &mut qail_pg::PooledConnection,
    ctx: &qail_core::rls::RlsContext,
) {
    let _ = exec_with_rls(Qail::get("orders").columns(["id"]).limit(1), conn, ctx).await;
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 1);
    assert!(
        usages[0].has_rls,
        "passing a Qail chain into a helper that applies cmd.with_rls(...) should count as RLS-scoped"
    );
}

#[test]
fn test_rls_detection_helper_param_with_rls_on_bound_query_var() {
    let content = r#"
async fn exec_with_rls(
    cmd: Qail,
    conn: &mut qail_pg::PooledConnection,
    ctx: &qail_core::rls::RlsContext,
) {
    let _ = conn.fetch_all_uncached(&cmd.with_rls(ctx)).await;
}

async fn scoped(
    conn: &mut qail_pg::PooledConnection,
    ctx: &qail_core::rls::RlsContext,
) {
    let cmd = Qail::get("orders").columns(["id"]).limit(1);
    let _ = exec_with_rls(cmd, conn, ctx).await;
}

async fn unscoped(conn: &mut qail_pg::PooledConnection) {
    let cmd = Qail::get("orders").columns(["id"]).limit(1);
    let _ = consume(cmd).await;
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", content, &mut usages);

    assert_eq!(usages.len(), 2);
    assert!(usages[0].has_rls);
    assert!(
        !usages[1].has_rls,
        "helper-param RLS detection must stay scoped to the call that passes the query into the RLS helper"
    );
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
fn test_super_admin_audit_accepts_typed_tenant_id_eq_scope() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
  tenant_id UUID
  status TEXT
}
"#,
    )
    .unwrap();

    let source = r#"
fn demo(tenant_id: uuid::Uuid) {
    let _sa = SuperAdminToken::for_system_process("jobs");
    let _q = Qail::typed(orders::table)
        .typed_eq(orders::tenant_id(), tenant_id)
        .typed_filter(orders::status(), Operator::Eq, "paid");
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", source, &mut usages);
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    assert!(
        !diagnostics
            .iter()
            .any(|d| d.message.contains("no explicit tenant scope")),
        "typed tenant_id equality should count as explicit tenant scope: {:?}",
        diagnostics
    );
}

#[test]
fn test_super_admin_audit_accepts_const_tenant_id_eq_scope() {
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
const TENANT_COL: &str = "tenant_id";

fn demo(tenant_id: uuid::Uuid) {
    let _sa = SuperAdminToken::for_system_process("jobs");
    let _q = Qail::get("orders")
        .columns(["id"])
        .eq(TENANT_COL, tenant_id);
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", source, &mut usages);
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    assert!(
        !diagnostics
            .iter()
            .any(|d| d.message.contains("no explicit tenant scope")),
        "tenant_id constants should count as explicit tenant scope: {:?}",
        diagnostics
    );
}

#[test]
fn test_super_admin_audit_warns_when_update_only_sets_tenant_id() {
    let schema = Schema::parse(
        r#"
table orders {
  id UUID
  tenant_id UUID
  status TEXT
}
"#,
    )
    .unwrap();

    let source = r#"
fn demo(order_id: uuid::Uuid, tenant_id: uuid::Uuid) {
    let _sa = SuperAdminToken::for_system_process("jobs");
    let _q = Qail::set("orders")
        .set_value("tenant_id", tenant_id)
        .set_value("status", "paid")
        .eq("id", order_id);
}
"#;
    let mut usages = Vec::new();
    scan_file("test.rs", source, &mut usages);
    let diagnostics = validate_against_schema_diagnostics(&schema, &usages);

    assert!(
        diagnostics.iter().any(|d| {
            matches!(d.kind, ValidationDiagnosticKind::RlsWarning)
                && d.message.contains("no explicit tenant scope")
        }),
        "tenant_id update payload must not silence SuperAdmin tenant-scope warning: {:?}",
        diagnostics
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
