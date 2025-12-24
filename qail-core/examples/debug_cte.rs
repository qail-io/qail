//! Debug NULL binding

use qail_core::parse;
use qail_core::transpiler::ToSqlParameterized;

fn main() {
    println!("=== Debug NULL Binding ===\n");
    
    // Simulate the INSERT
    let q = r#"add whatsapp_messages 
        fields phone_number, order_id
        values :phone_number, :order_id"#;
    
    let result = parse(q).map(|c| c.to_sql_parameterized());
    
    match result {
        Ok(r) => {
            println!("SQL: {}\n", r.sql);
        },
        Err(e) => println!("Error: {}", e),
    }
}
