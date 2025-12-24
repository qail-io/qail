#![allow(unused)]
use qail_core::parse;
use qail_core::transpiler::ToSql;

fn main() {
    let q2 = "get a left join b on b.x = a.x left join c on c.y = a.y fields a.id";
    println!("2 JOINs: {}", parse(q2).map(|_| "OK".to_string()).unwrap_or_else(|e| e.to_string()));
    
    let q3 = "get a left join b on b.x = a.x left join c on c.y = a.y left join d on d.z = a.z fields a.id";
    println!("3 JOINs: {}", parse(q3).map(|_| "OK".to_string()).unwrap_or_else(|e| e.to_string()));
    
    let q7 = "get a left join b on b.x = a.x left join c on c.y = a.y left join d on d.z = a.z left join e on e.w = a.w left join f on f.v = a.v left join g on g.u = a.u left join h on h.t = a.t fields a.id";
    println!("7 JOINs: {}", parse(q7).map(|c| format!("OK - {} joins", c.joins.len())).unwrap_or_else(|e| e.to_string()));
}
