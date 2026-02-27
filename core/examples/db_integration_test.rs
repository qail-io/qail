/// Real Database Integration Test for QAIL Typed Codegen
/// Run with: cargo run --example db_integration_test
use qail_core::Qail;
use qail_core::transpiler::ToSql;
use qail_core::typed::TypedColumn;

fn main() {
    println!("=== QAIL Real Database Integration Test ===\n");

    // Simulate typed columns (these would come from generated schema_gen.rs)
    const USERS_ID: TypedColumn<uuid::Uuid> = TypedColumn::new("users", "id");
    const USERS_EMAIL: TypedColumn<String> = TypedColumn::new("users", "email");
    const USERS_FIRST_NAME: TypedColumn<String> = TypedColumn::new("users", "first_name");

    println!("1. Testing Qail::get() with typed table marker...");

    // Build query using typed columns
    let query = Qail::get("users")
        .columns([
            USERS_ID.as_ref(),
            USERS_EMAIL.as_ref(),
            USERS_FIRST_NAME.as_ref(),
        ])
        .limit(5);

    let sql = query.to_sql();
    println!("   Generated SQL: {}\n", sql);

    // Verify the SQL structure
    assert!(sql.contains("SELECT"), "Missing SELECT");
    assert!(sql.contains("id"), "Missing id column");
    assert!(sql.contains("email"), "Missing email column");
    assert!(sql.contains("first_name"), "Missing first_name column");
    assert!(sql.contains("FROM users"), "Missing FROM");
    assert!(sql.contains("LIMIT 5"), "Missing LIMIT");

    println!("✓ SQL structure verified\n");

    println!("2. Testing typed column in WHERE clause...");

    let query2 = Qail::get("orders")
        .column("id")
        .column("status")
        .eq("status", "confirmed")
        .limit(10);

    let sql2 = query2.to_sql();
    println!("   Generated SQL: {}\n", sql2);

    assert!(
        sql2.contains("status = 'confirmed'"),
        "Missing WHERE condition"
    );
    println!("✓ WHERE clause verified\n");

    println!("✅ All database integration tests passed!");
    println!("\n(Note: No actual DB connection in this test - verifying SQL generation)");
}
