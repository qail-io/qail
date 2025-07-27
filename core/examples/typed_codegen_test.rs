/// Integration test for typed codegen
/// Run with: cargo run --example typed_codegen_test

// Include the generated schema (would normally be from build.rs)
use qail_core::build::{Schema, generate_schema_code};

fn main() {
    // Test schema with protected column
    let schema_content = r#"
table users {
    id UUID primary_key
    email TEXT not_null
    password_hash TEXT protected
}

table orders {
    id UUID primary_key
    user_id UUID ref:users.id
    status TEXT
}
"#;

    println!("=== QAIL Typed Codegen Integration Test ===\n");
    
    // Parse schema
    let schema = Schema::parse(schema_content).expect("Failed to parse schema");
    println!("✓ Parsed schema with {} tables", schema.tables.len());
    
    // Generate code
    let code = generate_schema_code(&schema);
    println!("✓ Generated {} bytes of Rust code\n", code.len());
    
    // Show generated code
    println!("=== Generated Code ===\n");
    println!("{}", code);
    
    // Verify key features
    println!("\n=== Verification ===\n");
    
    // 1. TypedColumn with Policy
    assert!(code.contains("TypedColumn<String, Public>"), "Missing Public column");
    println!("✓ TypedColumn<T, Public> generated");
    
    assert!(code.contains("TypedColumn<String, Protected>"), "Missing Protected column");
    println!("✓ TypedColumn<T, Protected> generated for password_hash");
    
    // 2. RelatedTo impls
    assert!(code.contains("impl RelatedTo<users::Users> for orders::Orders"), "Missing FK relation");
    println!("✓ RelatedTo<users::Users> for orders::Orders generated");
    
    assert!(code.contains("impl RelatedTo<orders::Orders> for users::Users"), "Missing reverse relation");
    println!("✓ RelatedTo<orders::Orders> for users::Users generated (reverse)");
    
    println!("\n✅ All typed codegen tests passed!");
}
