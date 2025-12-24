//! Test String Functions

use qail_core::parse;
use qail_core::transpiler::ToSql;

fn main() {
    println!("=== Testing String Functions ===\n");
    
    // Test 1: SUBSTRING
    let query1 = "get users fields substring(phone from 2) as phone_trimmed";
    println!("Query 1 (SUBSTRING):");
    match parse(query1) {
        Ok(cmd) => println!("  SQL: {}\n", cmd.to_sql()),
        Err(e) => println!("  Error: {}\n", e),
    }
    
    // Test 2: String concatenation
    let query2 = "get users fields first_name || ' ' || last_name as full_name";
    println!("Query 2 (Concat ||):");
    match parse(query2) {
        Ok(cmd) => println!("  SQL: {}\n", cmd.to_sql()),
        Err(e) => println!("  Error: {}\n", e),
    }
    
    // Test 2b: Simple function with one arg
    let query2b = "get x fields upper(name)";
    println!("Query 2b (UPPER - simple):");
    match parse(query2b) {
        Ok(cmd) => println!("  SQL: {}\n", cmd.to_sql()),
        Err(e) => println!("  Error: {}\n", e),
    }
    
    // Test 2c: Function with two identifier args
    let query2c = "get x fields coalesce(a, b)";
    println!("Query 2c (COALESCE - two idents):");
    match parse(query2c) {
        Ok(cmd) => println!("  SQL: {}\n", cmd.to_sql()),
        Err(e) => println!("  Error: {}\n", e),
    }
    
    // Test 2d: Function with string literal arg
    let query2d = "get x fields coalesce(name, 'Unknown')";
    println!("Query 2d (COALESCE - with string literal):");
    match parse(query2d) {
        Ok(cmd) => println!("  SQL: {}\n", cmd.to_sql()),
        Err(e) => println!("  Error: {}\n", e),
    }
    
    // Test 3: REPLACE with only identifiers
    let query3a = "get users fields replace(phone, old, new)";
    println!("Query 3a (REPLACE - identifiers only):");
    match parse(query3a) {
        Ok(cmd) => println!("  SQL: {}\n", cmd.to_sql()),
        Err(e) => println!("  Error: {}\n", e),
    }
    
    // Test 3b: REPLACE with string literals
    let query3 = "get users fields replace(phone, '+', '')";
    println!("Query 3 (REPLACE - with strings):");
    match parse(query3) {
        Ok(cmd) => println!("  SQL: {}\n", cmd.to_sql()),
        Err(e) => println!("  Error: {}\n", e),
    }
}
