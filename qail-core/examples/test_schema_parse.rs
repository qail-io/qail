//! Test native .qail file parsing

use qail_core::parser::schema::Schema;

fn main() {
    let schema_content = r#"
-- Test Schema
table users (
    id uuid primary_key,
    email text not null,
    name varchar(255),
    tags text[],
    created_at timestamp
)

table orders (
    id serial primary_key,
    user_id uuid not null references users(id),
    total decimal(10,2),
    status text check(status in ('pending', 'completed'))
)
"#;

    println!("Testing schema parsing...\n");
    
    match Schema::parse(schema_content) {
        Ok(schema) => {
            println!("✓ Parsed {} tables", schema.tables.len());
            
            for table in &schema.tables {
                println!("\nTable: {}", table.name);
                for col in &table.columns {
                    print!("  - {} {}", col.name, col.typ);
                    if col.primary_key { print!(" PK"); }
                    if !col.nullable { print!(" NOT NULL"); }
                    if col.is_array { print!(" ARRAY"); }
                    if col.is_serial { print!(" SERIAL"); }
                    if let Some(ref check) = col.check { print!(" CHECK({})", check); }
                    if let Some(ref refs) = col.references { print!(" -> {}", refs); }
                    if let Some(ref params) = col.type_params { print!(" ({})", params.join(",")); }
                    println!();
                }
            }
            
            println!("\n✓ JSON export:");
            match schema.to_json() {
                Ok(json) => println!("{}", &json[..300.min(json.len())]),
                Err(e) => println!("JSON error: {}", e),
            }
        }
        Err(e) => {
            eprintln!("✗ Parse error: {}", e);
            std::process::exit(1);
        }
    }
}
