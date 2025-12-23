use crate::ast::*;

pub trait ToRedis {
    fn to_redis_search(&self) -> String;
}

impl ToRedis for QailCmd {
    fn to_redis_search(&self) -> String {
        match self.action {
            Action::Get => build_search(self),
            Action::Put | Action::Add | Action::Set => build_hset(self),
            Action::Del => build_del(self),
            Action::TxnStart => "MULTI".to_string(),
            Action::TxnCommit => "EXEC".to_string(),
            Action::TxnRollback => "DISCARD".to_string(),
            _ => format!("-- Action {:?} not supported for RediSearch. Use standard Redis commands for SET/DEL.", self.action),
        }
    }
}

fn build_hset(cmd: &QailCmd) -> String {
    // let mut parts = Vec::new(); // Unused
    let key = if !cmd.table.is_empty() { format!(":{}", cmd.table) } else { "".to_string() };
    
    // Key Logic: usually `table:id`. We need to extract ID from cages?
    // Or just use table as key prefix?
    // Extract unique key from filter or payload cages.
    // For simplicity: We construct HSET key field val field val
    // But HSET needs a specific KEY.
    // If user says `put::users:id` -> key `users:id`?
    // Use the first cage filter on 'id' or 'key' as the Redis Key suffix.
    let mut redis_key = format!("entity{}", key); // Default
    let mut fields = Vec::new();
    
    for cage in &cmd.cages {
        match cage.kind {
            CageKind::Filter | CageKind::Payload => {
                for cond in &cage.conditions {
                    if let Expr::Named(name) = &cond.left {
                        if name == "id" || name == "key" {
                            redis_key = format!("{}{}", key, value_to_string(&cond.value));
                        } else {
                            fields.push(format!("{} {}", name, escape_redis_val(&value_to_string(&cond.value))));
                        }
                    }
                }
            },
            _ => {}
        }
    }
    
    // If no fields, maybe it's just a key set?
    if fields.is_empty() {
        return format!("-- ERROR: No fields to HSET for key {}", redis_key);
    }
    
    format!("HSET {} {}", redis_key, fields.join(" "))
}

fn build_del(cmd: &QailCmd) -> String {
    // Similar key logic
    let key = if !cmd.table.is_empty() { format!(":{}", cmd.table) } else { "".to_string() };
    let mut redis_key = format!("entity{}", key);
    
    for cage in &cmd.cages {
         if let CageKind::Filter = cage.kind {
             for cond in &cage.conditions {
                 if let Expr::Named(name) = &cond.left {
                     if name == "id" || name == "key" {
                         redis_key = format!("{}{}", key, value_to_string(&cond.value));
                     }
                 }
             }
         }
    }
    format!("DEL {}", redis_key)
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Int(n) => n.to_string(),
        Value::Float(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => "unknown".to_string(),
    }
}

fn build_search(cmd: &QailCmd) -> String {
    let index_name = format!("idx:{}", cmd.table);
    let mut query_parts = Vec::new();

    for cage in &cmd.cages {
        if let CageKind::Filter = cage.kind {
             for cond in &cage.conditions {
                 let col_name = match &cond.left {
                     Expr::Named(name) => name.clone(),
                     expr => expr.to_string(),
                 };
                 let field = format!("@{}", col_name);
                 let val = match &cond.value {
                     Value::Int(n) => n.to_string(),
                     Value::Float(n) => n.to_string(),
                     Value::String(s) => s.clone(), // Start with raw string
                     Value::Bool(b) => b.to_string(),
                     _ => "unknown".to_string(),
                 };
                 
                 // Handle Operators for RediSearch Syntax
                 let expr = match cond.op {
                     Operator::Eq => format!("{}:{}", field, escape_redis_val(&val)),
                     // Numeric Ranges: [min max]
                     // > 10  -> [(10 +inf]
                     // >= 10 -> [10 +inf]
                     // < 10  -> [-inf (10]
                     // <= 10 -> [-inf 10]
                     Operator::Gt => format!("{}:[({} +inf]", field, val),
                     Operator::Gte => format!("{}:[{} +inf]", field, val),
                     Operator::Lt => format!("{}:[-inf ({}]", field, val),
                     Operator::Lte => format!("{}:[-inf {}]", field, val),
                     Operator::Ne => format!("-({}:{})", field, escape_redis_val(&val)), // Negation
                     Operator::Fuzzy => format!("{}:{}{}{}", field, "%", val, "%"), // Suffix matching? incomplete support in redis
                     _ => format!("{}:{}", field, val),
                 };
                 query_parts.push(expr);
             }
        }
    }
    
    let query_str = if query_parts.is_empty() {
        "*".to_string() // Match all
    } else {
        query_parts.join(" ") // Implicit AND
    };
    
    let mut limit_clause = String::new();
    let mut offset = 0;
    let mut has_limit = false;
    let mut limit = 10; // Default if offset exists without limit? 
    
    for cage in &cmd.cages {
         match cage.kind {
             CageKind::Limit(n) => { limit = n; has_limit = true; },
             CageKind::Offset(n) => { offset = n; },
             _ => {}
         }
    }
    
    if has_limit || offset > 0 {
        limit_clause = format!(" LIMIT {} {}", offset, limit);
    }
    
    // Return: if columns specified, RETURN N field1 field2
    let mut return_clause = String::new();
    if !cmd.columns.is_empty() {
         let mut cols = Vec::new();
         for c in &cmd.columns {
             if let Expr::Named(n) = c {
                 cols.push(n.clone());
             }
         }
         return_clause = format!(" RETURN {} {}", cols.len(), cols.join(" "));
    }

    format!("FT.SEARCH {} \"{}\"{}{}", index_name, query_str, return_clause, limit_clause)
}

fn escape_redis_val(s: &str) -> String {
    // If it contains spaces or special chars, maybe quote it?
    // Redis query syntax is sensitive.
    if s.contains(' ') {
        format!("\"{}\"", s)
    } else {
        s.to_string()
    }
}
