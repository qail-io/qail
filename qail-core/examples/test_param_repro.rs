use qail_core::{parse, ast::Column, transpiler::{ToSqlParameterized, TranspileResult}};

fn main() {
    let query = "get::t 'coalesce(uc.unread_count, 0)@unread_count";
    
    match parse(query) {
        Ok(cmd) => {
            let res = cmd.to_sql_parameterized();
            println!("SQL: {}", res.sql);
            if res.sql.contains("coalesce(uc.unread_count, 0) AS \"unread_count\"") || res.sql.contains("COALESCE(uc.unread_count, 0) AS \"unread_count\"") {
                println!("SUCCESS: Alias found correctly.");
            } else {
                println!("FAILURE: Alias missing in parameterized SQL.");
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("Parse error: {}", e);
            std::process::exit(1);
        }
    }
}
