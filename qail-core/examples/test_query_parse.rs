//! Test query file parsing

use qail_core::parser::query_file::QueryFile;

fn main() {
    let queries_content = r#"
-- User queries
query find_user(id: Uuid) -> User:
  get users where id = :id

query list_users() -> Vec<User>:
  get users order by created_at desc

query find_by_email(email: String) -> Option<User>:
  get users where email = :email limit 1

execute create_user(email: String, name: String):
  add::users : email, name [ :email, :name ]

execute delete_user(id: Uuid):
  del::users where id = :id
"#;

    println!("Testing query file parsing...\n");
    
    match QueryFile::parse(queries_content) {
        Ok(qf) => {
            println!("✓ Parsed {} queries\n", qf.queries.len());
            
            for q in &qf.queries {
                let kind = if q.is_execute { "execute" } else { "query" };
                let params: Vec<_> = q.params.iter()
                    .map(|p| format!("{}: {}", p.name, p.typ))
                    .collect();
                
                let return_type = match &q.return_type {
                    Some(qail_core::parser::query_file::ReturnType::Single(t)) => format!("-> {}", t),
                    Some(qail_core::parser::query_file::ReturnType::Vec(t)) => format!("-> Vec<{}>", t),
                    Some(qail_core::parser::query_file::ReturnType::Option(t)) => format!("-> Option<{}>", t),
                    None => String::new(),
                };
                
                println!("{} {}({}) {}", kind, q.name, params.join(", "), return_type);
                println!("  body: {}", q.body.lines().next().unwrap_or(""));
                println!();
            }
            
            println!("✓ All query parsing tests passed!");
        }
        Err(e) => {
            eprintln!("✗ Parse error: {}", e);
            std::process::exit(1);
        }
    }
}
