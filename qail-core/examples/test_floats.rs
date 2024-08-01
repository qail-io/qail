/// Test float literal parsing
use qail_core::parser::parse;
use qail_core::transpiler::ToSql;

fn main() {
    let tests = [
        ("Simple float", "get stats fields 100.0 as val"),
        ("Zero float", "get stats fields 0.0 as val"),
        ("Pi", "get stats fields 3.14 as val"),
        ("CASE with floats", "get stats fields case when x > 0 then 100.0 else 0.0 end as rate"),
    ];
    
    for (name, test) in tests {
        println!("{}:", name);
        match parse(test) {
            Ok(cmd) => println!("  ✅ {}\n", cmd.to_sql()),
            Err(e) => println!("  ❌ {}\n", e),
        }
    }
}
