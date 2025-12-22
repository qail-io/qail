use crate::ast::*;

pub trait ToElastic {
    fn to_elastic(&self) -> String;
}

impl ToElastic for QailCmd {
    fn to_elastic(&self) -> String {
        match self.action {
            Action::Get => build_elastic_search(self),
            _ => format!("{{ \"error\": \"Action {:?} not supported for Elastic DSL\" }}", self.action),
        }
    }
}

fn build_elastic_search(cmd: &QailCmd) -> String {
    // Basic structure: { "query": { "bool": { "must": [...] } } }
    let mut parts = Vec::new();

    // Filtering
    let filter = build_filter(cmd);
    if !filter.is_empty() {
        parts.push(format!("\"query\": {}", filter));
    } else {
        parts.push("\"query\": { \"match_all\": {} }".to_string());
    }

    // Pagination
    if let Some(limit) = get_cage_val(cmd, CageKind::Limit(0)) {
        parts.push(format!("\"size\": {}", limit));
    }
    if let Some(offset) = get_cage_val(cmd, CageKind::Offset(0)) {
        parts.push(format!("\"from\": {}", offset));
    }
    
    // Projections (Source filtering)
    if !cmd.columns.is_empty() {
         let cols: Vec<String> = cmd.columns.iter().map(|c| match c {
             Column::Named(n) => format!("\"{}\"", n),
             _ => "".to_string()
         }).collect();
         parts.push(format!("\"_source\": [{}]", cols.join(", ")));
    }

    format!("{{ {} }}", parts.join(", "))
}

fn build_filter(cmd: &QailCmd) -> String {
    let mut musts = Vec::new();
    
    for cage in &cmd.cages {
        if let CageKind::Filter = cage.kind {
            for cond in &cage.conditions {
                 let val = value_to_json(&cond.value);
                 let term = match cond.op {
                     Operator::Eq => format!("{{ \"term\": {{ \"{}\": {} }} }}", cond.column, val),
                     Operator::Ne => format!("{{ \"bool\": {{ \"must_not\": {{ \"term\": {{ \"{}\": {} }} }} }} }}", cond.column, val),
                     Operator::Gt => format!("{{ \"range\": {{ \"{}\": {{ \"gt\": {} }} }} }}", cond.column, val),
                     Operator::Gte => format!("{{ \"range\": {{ \"{}\": {{ \"gte\": {} }} }} }}", cond.column, val),
                     Operator::Lt => format!("{{ \"range\": {{ \"{}\": {{ \"lt\": {} }} }} }}", cond.column, val),
                     Operator::Lte => format!("{{ \"range\": {{ \"{}\": {{ \"lte\": {} }} }} }}", cond.column, val),
                     Operator::Fuzzy => format!("{{ \"match\": {{ \"{}\": {{ \"query\": {}, \"fuzziness\": \"AUTO\" }} }} }}", cond.column, val),
                     _ => format!("{{ \"match\": {{ \"{}\": {} }} }}", cond.column, val),
                 };
                 musts.push(term);
            }
        }
    }
    
    if musts.is_empty() {
        return String::new();
    }
    
    format!("{{ \"bool\": {{ \"must\": [{}] }} }}", musts.join(", "))
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

fn value_to_json(v: &Value) -> String {
    match v {
        Value::String(s) => format!("\"{}\"", s),
        Value::Int(n) => n.to_string(),
        Value::Float(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => "null".to_string(),
    }
}
