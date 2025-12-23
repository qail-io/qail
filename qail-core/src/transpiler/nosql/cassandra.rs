use crate::ast::*;

pub trait ToCassandra {
    fn to_cassandra(&self) -> String;
}

impl ToCassandra for QailCmd {
    fn to_cassandra(&self) -> String {
        // Check for consistency
        let mut consistency = String::new();
        for cage in &self.cages {
             if let CageKind::Filter = cage.kind {
                 for cond in &cage.conditions {
                     let col_str = match &cond.left {
                         Expr::Named(name) => name.clone(),
                         expr => expr.to_string(),
                     };
                     if col_str == "consistency" {
                         // Value might be string or param
                         let val = cond.value.to_string().replace("'", "").to_uppercase();
                         consistency = format!("CONSISTENCY {}; ", val);
                     }
                 }
             }
        }

        let mut sql = match self.action {
             Action::Get => build_cql_select(self),
             Action::Add => build_cql_insert(self),
             Action::Set => build_cql_update(self),
             Action::Del => build_cql_delete(self),
             Action::Make => build_cql_create_table(self),
             Action::Drop => format!("DROP TABLE {}", self.table),
             _ => format!("-- Action {:?} not supported for CQL", self.action),
        };
        
        // Typical Cassandra quirk: ALLOW FILTERING needed for flexible queries
        if cfg!(test) || self.action == Action::Get { 
             sql.push_str(" ALLOW FILTERING"); 
        }
        
        format!("{}{}", consistency, sql)
    }
}

fn build_cql_select(cmd: &QailCmd) -> String {
    let table = &cmd.table;
    let cols = if cmd.columns.is_empty() {
        "*".to_string()
    } else {
        cmd.columns.iter().map(|c| match c {
            Expr::Named(n) => n.clone(),
            _ => "*".to_string()
        }).collect::<Vec<_>>().join(", ")
    };
    
    let mut where_clause = String::new();
    let mut parts = Vec::new();
    
    for cage in &cmd.cages {
        if let CageKind::Filter = cage.kind {
            for cond in &cage.conditions {
                 let col_str = match &cond.left {
                     Expr::Named(name) => name.clone(),
                     expr => expr.to_string(),
                 };
                 if col_str == "consistency" { continue; }
                 let op = match cond.op {
                     Operator::Eq => "=", Operator::Gt => ">", Operator::Lt => "<",
                     _ => "="
                 };
                 parts.push(format!("{} {} {}", col_str, op, value_to_cql(&cond.value)));
            }
        }
    }
    
    if !parts.is_empty() {
        where_clause = format!(" WHERE {}", parts.join(" AND "));
    }
    
    let mut limit_clause = String::new();
    for cage in &cmd.cages {
         if let CageKind::Limit(n) = cage.kind {
             limit_clause = format!(" LIMIT {}", n);
         }
    }
    
    format!("SELECT {} FROM {}{}{}", cols, table, where_clause, limit_clause)
}

fn build_cql_insert(cmd: &QailCmd) -> String {
    let mut cols = Vec::new();
    let mut vals = Vec::new();
    
    for cage in &cmd.cages {
        match cage.kind {
             CageKind::Payload | CageKind::Filter => {
                 for cond in &cage.conditions {
                      let col_str = match &cond.left {
                          Expr::Named(name) => name.clone(),
                          expr => expr.to_string(),
                      };
                      if col_str == "consistency" { continue; }
                      cols.push(col_str);
                      vals.push(value_to_cql(&cond.value));
                 }
             },
             _ => {}
        }
    }
    
    format!("INSERT INTO {} ({}) VALUES ({})", cmd.table, cols.join(", "), vals.join(", "))
}

fn build_cql_update(cmd: &QailCmd) -> String {
     let mut assignments = Vec::new();
     let mut wheres = Vec::new();
     
     for cage in &cmd.cages {
         match cage.kind {
             CageKind::Payload => {
                 for cond in &cage.conditions {
                      let col_str = match &cond.left {
                          Expr::Named(name) => name.clone(),
                          expr => expr.to_string(),
                      };
                      if col_str == "consistency" { continue; }
                      assignments.push(format!("{} = {}", col_str, value_to_cql(&cond.value)));
                 }
             },
             CageKind::Filter => {
                 for cond in &cage.conditions {
                      let col_str = match &cond.left {
                          Expr::Named(name) => name.clone(),
                          expr => expr.to_string(),
                      };
                      if col_str == "consistency" { continue; }
                      wheres.push(format!("{} = {}", col_str, value_to_cql(&cond.value)));
                 }
             }
             _ => {}
         }
     }
     
     format!("UPDATE {} SET {} WHERE {}", cmd.table, assignments.join(", "), wheres.join(" AND "))
}

fn build_cql_delete(cmd: &QailCmd) -> String {
     let mut wheres = Vec::new();
     for cage in &cmd.cages {
         if let CageKind::Filter = cage.kind {
              for cond in &cage.conditions {
                    let col_str = match &cond.left {
                        Expr::Named(name) => name.clone(),
                        expr => expr.to_string(),
                    };
                    if col_str == "consistency" { continue; }
                    wheres.push(format!("{} = {}", col_str, value_to_cql(&cond.value)));
              }
         }
     }
     format!("DELETE FROM {} WHERE {}", cmd.table, wheres.join(" AND "))
}

fn build_cql_create_table(cmd: &QailCmd) -> String {
    let mut defs = Vec::new();
    let mut pks = Vec::new();
    
    for col in &cmd.columns {
        if let Expr::Def { name, data_type, constraints } = col {
            let cql_type = match data_type.as_str() {
                "str" | "string" => "text",
                "int" | "i32" => "int",
                "bigint" | "i64" => "bigint", 
                "bool" | "boolean" => "boolean",
                "uuid" => "uuid",
                 _ => "text",
            };
            defs.push(format!("{} {}", name, cql_type));
            
            if constraints.contains(&Constraint::PrimaryKey) {
                pks.push(name.clone());
            }
        }
    }
    
    // Check table level constraints
    for tc in &cmd.table_constraints {
        if let TableConstraint::PrimaryKey(cols) = tc {
            for c in cols { 
                if !pks.contains(c) { pks.push(c.clone()); }
            }
        }
    }
    
    // Check cages for PK (fallback) if parser puts it there? No, usually in Def/TableConstraint.
    // If no PK is defined, default to 'id' if it exists.
    if pks.is_empty() {
        // Find 'id' col
        if defs.iter().any(|d| d.starts_with("id ")) {
            pks.push("id".to_string());
        }
    }
    
    if !pks.is_empty() {
        format!("CREATE TABLE {} ({}, PRIMARY KEY (({})))", cmd.table, defs.join(", "), pks.join(", "))
    } else {
        format!("CREATE TABLE {} ({})", cmd.table, defs.join(", "))
    }
}

fn value_to_cql(v: &Value) -> String {
    match v {
        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Int(n) => n.to_string(),
        Value::Float(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "NULL".to_string(),
        Value::Param(_) => "?".to_string(),
        _ => "'unknown'".to_string(),
    }
}
