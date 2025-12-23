use crate::ast::*;

pub trait ToNeo4j {
    fn to_cypher(&self) -> String;
}

impl ToNeo4j for QailCmd {
    fn to_cypher(&self) -> String {
        match self.action {
            Action::Get => build_cypher_match(self),
            _ => format!("// Action {:?} not supported for Cypher yet", self.action),
        }
    }
}

fn build_cypher_match(cmd: &QailCmd) -> String {
    // Basic: MATCH (n:Table) WHERE n.prop = val RETURN n
    // Joins: MATCH (n:Table)-[:REL]->(m:Table2)
    
    // For now simple single node match
    let label = &cmd.table;
    let alias = "n";
    
    let mut parts = Vec::new();
    parts.push(format!("MATCH ({}:{})", alias, label));
    
    let where_clause = build_where(cmd, alias);
    if !where_clause.is_empty() {
        parts.push(format!("WHERE {}", where_clause));
    }
    
    let return_clause = if cmd.columns.is_empty() {
        format!("RETURN {}", alias)
    } else {
        let cols: Vec<String> = cmd.columns.iter().map(|c| match c {
            Expr::Named(n) => format!("{}.{}", alias, n),
            _ => "".to_string()
        }).collect();
        format!("RETURN {}", cols.join(", "))
    };
    parts.push(return_clause);
    
    if let Some(limit) = get_cage_val(cmd, CageKind::Limit(0)) {
        parts.push(format!("LIMIT {}", limit));
    }
    if let Some(offset) = get_cage_val(cmd, CageKind::Offset(0)) {
        parts.push(format!("SKIP {}", offset)); // Cypher uses SKIP, not OFFSET
    }

    parts.join(" ")
}

fn build_where(cmd: &QailCmd, alias: &str) -> String {
    let mut conds = Vec::new();
    for cage in &cmd.cages {
        if let CageKind::Filter = cage.kind {
            for cond in &cage.conditions {
                 let op = match cond.op {
                     Operator::Eq => "=", Operator::Gt => ">", Operator::Lt => "<",
                     Operator::Gte => ">=", Operator::Lte => "<=", Operator::Ne => "<>",
                     Operator::Fuzzy => "=~", // RegEx match in Cypher
                     _ => "="
                 };
                 let val = value_to_cypher(&cond.value);
                 let col_str = match &cond.left {
                     Expr::Named(name) => name.clone(),
                     expr => expr.to_string(),
                 };
                 conds.push(format!("{}.{} {} {}", alias, col_str, op, val));
            }
        }
    }
    conds.join(" AND ")
}


fn get_cage_val(cmd: &QailCmd, kind_example: CageKind) -> Option<usize> {
    for cage in &cmd.cages {
        match (&cage.kind, &kind_example) {
            (CageKind::Limit(n), CageKind::Limit(_)) => return Some(*n),
            (CageKind::Offset(n), CageKind::Offset(_)) => return Some(*n),
            _ => {}
        }
    }
    None
}

fn value_to_cypher(v: &Value) -> String {
    match v {
        Value::String(s) => format!("'{}'", s),
        Value::Int(n) => n.to_string(),
        Value::Float(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => "null".to_string(),
    }
}
