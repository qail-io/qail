use super::*;
use qail_core::Qail;

#[test]
fn test_qail_schema_loading() {
    let schema_str = r#"
table users {
id uuid primary_key default gen_random_uuid()
email text not_null unique
name text nullable
created_at timestamptz default now()
}

table orders {
id uuid primary_key default gen_random_uuid()
user_id uuid not_null references users(id)
total decimal not_null
status text not_null default 'pending'
created_at timestamptz default now()
}
    "#;

    let mut registry = SchemaRegistry::new();
    registry.load_from_qail_str(schema_str).unwrap();

    // Check tables loaded
    assert!(registry.table_exists("users"));
    assert!(registry.table_exists("orders"));
    assert!(!registry.table_exists("nonexistent"));

    // Check users table
    let users = registry.table("users").unwrap();
    assert_eq!(users.primary_key, Some("id".to_string()));
    assert_eq!(users.columns.len(), 4);

    let email = users.columns.iter().find(|c| c.name == "email").unwrap();
    assert!(!email.nullable);
    assert!(email.unique);
    assert_eq!(email.pg_type, "TEXT");

    // Check orders table FK
    let orders = registry.table("orders").unwrap();
    let user_id = orders.columns.iter().find(|c| c.name == "user_id").unwrap();
    assert!(user_id.foreign_key.is_some());
    let fk = user_id.foreign_key.as_ref().unwrap();
    assert_eq!(fk.ref_table, "users");
    assert_eq!(fk.ref_column, "id");

    // Check insertable columns (should skip auto-generated PKs)
    let insertable = users.insertable_columns();
    assert!(
        insertable
            .iter()
            .all(|c| c.name != "id" || c.pg_type != "SERIAL")
    );

    // Check FKs
    let fks = orders.foreign_keys();
    assert_eq!(fks.len(), 1);
    assert_eq!(fks[0].0, "user_id");
}

#[test]
fn test_schema_validation() {
    let mut registry = SchemaRegistry::new();
    registry
        .load_from_qail_str(
            r#"
table users {
id uuid primary_key
name text
}
    "#,
        )
        .unwrap();

    let cmd = Qail::get("users").columns(["id", "name"]);
    assert!(registry.validate(&cmd).is_ok());

    let cmd = Qail::get("users").columns(["id", "invalid_col"]);
    assert!(registry.validate(&cmd).is_err());

    let cmd = Qail::get("nonexistent");
    assert!(registry.validate(&cmd).is_err());
}
