//! Test Multiple CTEs

use qail_core::parse;
use qail_core::transpiler::ToSql;

fn main() {
    println!("=== Testing Multiple CTEs ===\n");
    
    // Test 1: Multiple CTEs with raw SQL bodies
    let query1 = r#"with 
        latest_msgs as (select distinct on (phone) phone, content from messages order by phone, created_at desc),
        unread as (select phone, count(*) as cnt from messages where status = 'received' group by phone)
        get latest_msgs"#;
    
    println!("Query 1 (Multi-CTE with raw SQL):");
    match parse(query1) {
        Ok(cmd) => {
            println!("  CTEs count: {}", cmd.ctes.len());
            for cte in &cmd.ctes {
                println!("    - {}", cte.name);
            }
            println!("  SQL: {}\n", cmd.to_sql());
        }
        Err(e) => println!("  Error: {}\n", e),
    }
    
    // Test 2: Multiple CTEs with QAIL syntax inside
    let query2 = r#"with 
        latest as (get messages fields phone, content order by created_at desc limit 1),
        counts as (get messages fields phone, count(*) as cnt group by phone)
        get latest"#;
    
    println!("Query 2 (Multi-CTE with QAIL syntax):");
    match parse(query2) {
        Ok(cmd) => {
            println!("  CTEs count: {}", cmd.ctes.len());
            println!("  SQL: {}\n", cmd.to_sql());
        }
        Err(e) => println!("  Error: {}\n", e),
    }
    
    // Test 3: CTE with JOINs in final query
    let query3 = r#"with 
        a as (get users fields id, name),
        b as (get orders fields user_id, total)
        get a left join b on a.id = b.user_id fields a.name, b.total"#;
    
    println!("Query 3 (CTE with JOIN in final):");
    match parse(query3) {
        Ok(cmd) => {
            println!("  CTEs: {:?}", cmd.ctes.iter().map(|c| &c.name).collect::<Vec<_>>());
            println!("  Final table: {}", cmd.table);
            println!("  Joins: {}", cmd.joins.len());
            println!("  SQL: {}\n", cmd.to_sql());
        }
        Err(e) => println!("  Error: {}\n", e),
    }
}
