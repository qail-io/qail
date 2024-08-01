//! Test the qail! macro with real schema validation

// This test requires qail.schema.json to exist in the project root
// Run: cargo run --bin qail -- pull "postgresql://..."

#[cfg(test)]
mod tests {
    // Uncomment when macro is ready for integration testing
    // use qail_macros::qail;
    
    #[test]
    fn test_schema_loaded() {
        // Verify schema file exists and can be loaded
        let content = std::fs::read_to_string("qail.schema.json")
            .expect("qail.schema.json should exist - run 'cargo run --bin qail -- pull <url>'");
        
        let schema: serde_json::Value = serde_json::from_str(&content)
            .expect("Schema should be valid JSON");
        
        let tables = schema["tables"].as_array().expect("Should have tables");
        assert!(tables.len() > 0, "Should have at least one table");
        
        // Check for whatsapp_messages table
        let has_whatsapp = tables.iter().any(|t| {
            t["name"].as_str() == Some("whatsapp_messages")
        });
        assert!(has_whatsapp, "Should have whatsapp_messages table");
        
        println!("✓ Schema loaded with {} tables", tables.len());
    }

    // Future test: This should FAIL to compile because 'invalid_table' doesn't exist
    // #[test]
    // fn test_invalid_table() {
    //     let pool = todo!();
    //     qail!(pool, User, "get invalid_table where id = :id", id: 1);
    // }

    // Future test: This should FAIL to compile because 'invalid_column' doesn't exist
    // #[test]
    // fn test_invalid_column() {
    //     let pool = todo!();
    //     qail!(pool, User, "get whatsapp_messages where invalid_column = :id", id: 1);
    // }
}
