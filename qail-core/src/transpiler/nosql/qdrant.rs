use crate::ast::*;

pub trait ToQdrant {
    fn to_qdrant_search(&self) -> String;
}

impl ToQdrant for QailCmd {
    fn to_qdrant_search(&self) -> String {
        match self.action {
            Action::Get => build_qdrant_search(self),
            Action::Put | Action::Add => build_qdrant_upsert(self),
            Action::Del => build_qdrant_delete(self),
            _ => format!("{{ \"error\": \"Action {:?} not supported for Qdrant\" }}", self.action),
        }
    }
}

fn build_qdrant_upsert(cmd: &QailCmd) -> String {
    // POST /collections/{name}/points?wait=true
    // Body: { "points": [ { "id": 1, "vector": [...], "payload": {...} } ] }
    // let mut points = Vec::new(); // Unused
    
    // Single point upsert from payload/filter cages.
    let mut point_id = "0".to_string(); // Default ID?
    let mut vector = "[0.0]".to_string();
    let mut payload_parts = Vec::new();
    
    for cage in &cmd.cages {
        match cage.kind {
            CageKind::Payload | CageKind::Filter => {
                for cond in &cage.conditions {
                    if cond.column == "id" {
                         point_id = value_to_json(&cond.value);
                    } else if cond.column == "vector" {
                         vector = value_to_json(&cond.value);
                    } else {
                         payload_parts.push(format!("\"{}\": {}", cond.column, value_to_json(&cond.value)));
                    }
                }
            }
            _ => {}
        }
    }
    
    let payload_json = if payload_parts.is_empty() { "{}".to_string() } else { format!("{{ {} }}", payload_parts.join(", ")) };
    
    // Construct single point
    let point = format!("{{ \"id\": {}, \"vector\": {}, \"payload\": {} }}", point_id, vector, payload_json);
    
    format!("{{ \"points\": [{}] }}", point)
}

fn build_qdrant_delete(cmd: &QailCmd) -> String {
    // POST /collections/{name}/points/delete
    // Body: { "points": [1, 2, 3] } OR { "filter": ... }
    
    // If ID specified, delete by ID. Else delete by filter.
    let mut ids = Vec::new();
    
    for cage in &cmd.cages {
        if let CageKind::Filter = cage.kind {
            for cond in &cage.conditions {
                 if cond.column == "id" {
                     ids.push(value_to_json(&cond.value));
                 }
            }
        }
    }
    
    if !ids.is_empty() {
        format!("{{ \"points\": [{}] }}", ids.join(", "))
    } else {
        // Delete by filter
        let filter = build_filter(cmd);
        format!("{{ \"filter\": {} }}", filter)
    }
}

fn build_qdrant_search(cmd: &QailCmd) -> String {
    // Target endpoint: POST /collections/{collection_name}/points/search
    // Output: JSON Body
    
    let mut parts = Vec::new();

    // 1. Vector handling
    // We look for a condition with the key "vector" or similar, usage: [vector~[0.1, 0.2]]
    // Any array value with a Fuzzy match (~) is treated as the query vector.
    let mut vector_found = false;
    
    for cage in &cmd.cages {
        if let CageKind::Filter = cage.kind {
            for cond in &cage.conditions {
                 if cond.op == Operator::Fuzzy {
                     // Vector Query found.
                     // Case 1: [vector~[0.1, 0.2]] -> Explicit Vector (Already handled by Value::Array)
                     // Case 2: [vector~"cute cat"] -> Semantic Search Intent
                     match &cond.value {
                         Value::String(s) => {
                             // Output Placeholder for Runtime Resolution
                             // e.g. {{EMBED:cute cat}}
                             parts.push(format!("\"vector\": \"{{{{EMBED:{}}}}}\"", s));
                         },
                         _ => {
                            parts.push(format!("\"vector\": {}", value_to_json(&cond.value)));
                         }
                     }
                     vector_found = true;
                     break; 
                 }
            }
        }
        if vector_found { break; }
    }
    
    if !vector_found {
        // Fallback: If no vector specified, Qdrant can effectively do a Scroll (listing), but "search" needs vector?
        // Actually, Qdrant supports Scroll API separate from Search. 
        // Default to zero vector for search if none specified.
        parts.push("\"vector\": [0.0]".to_string()); // Dummy vector or error? Let's use dummy to show intent.
    }

    // 2. Filters (Hybrid Search)
    let filter = build_filter(cmd);
    if !filter.is_empty() {
        parts.push(format!("\"filter\": {}", filter));
    }

    // 3. Limit
    let mut limit = 10;
    if let Some(l) = get_cage_val(cmd, CageKind::Limit(0)) {
        limit = l;
    }
    parts.push(format!("\"limit\": {}", limit));
    
    // 4. With Payload (Projections)
    if !cmd.columns.is_empty() {
         let mut incl = Vec::new();
         for c in &cmd.columns {
             if let Column::Named(n) = c {
                 incl.push(format!("\"{}\"", n));
             }
         }
         parts.push(format!("\"with_payload\": {{ \"include\": [{}] }}", incl.join(", ")));
    } else {
         parts.push("\"with_payload\": true".to_string());
    }

    format!("{{ {} }}", parts.join(", "))
}

fn build_filter(cmd: &QailCmd) -> String {
    // Qdrant Filter structure: { "must": [ { "key": "city", "match": { "value": "London" } } ] }
    let mut musts = Vec::new();
    
    for cage in &cmd.cages {
        if let CageKind::Filter = cage.kind {
            for cond in &cage.conditions {
                 // Skip the vector query itself
                 if cond.op == Operator::Fuzzy { continue; }
                 
                 let val = value_to_json(&cond.value); 
                 
                 let clause = match cond.op {
                     Operator::Eq => format!("{{ \"key\": \"{}\", \"match\": {{ \"value\": {} }} }}", cond.column, val),
                     // Qdrant range: { "key": "price", "range": { "gt": 10.0 } }
                     Operator::Gt => format!("{{ \"key\": \"{}\", \"range\": {{ \"gt\": {} }} }}", cond.column, val),
                     Operator::Gte => format!("{{ \"key\": \"{}\", \"range\": {{ \"gte\": {} }} }}", cond.column, val),
                     Operator::Lt => format!("{{ \"key\": \"{}\", \"range\": {{ \"lt\": {} }} }}", cond.column, val),
                     Operator::Lte => format!("{{ \"key\": \"{}\", \"range\": {{ \"lte\": {} }} }}", cond.column, val),
                     Operator::Ne => format!("{{ \"must_not\": [{{ \"key\": \"{}\", \"match\": {{ \"value\": {} }} }}] }}", cond.column, val), // This needs wrapping?
                     _ => format!("{{ \"key\": \"{}\", \"match\": {{ \"value\": {} }} }}", cond.column, val),
                 };
                 musts.push(clause);
            }
        }
    }
    
    if musts.is_empty() {
        return String::new();
    }
    
    format!("{{ \"must\": [{}] }}", musts.join(", "))
}

fn get_cage_val(cmd: &QailCmd, kind_example: CageKind) -> Option<usize> {
    for cage in &cmd.cages {
        match (&cage.kind, &kind_example) {
            (CageKind::Limit(n), CageKind::Limit(_)) => return Some(*n),
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
        // Handle array for vector
         Value::Array(arr) => {
            let elems: Vec<String> = arr.iter().map(|e| match e {
                Value::Int(i) => i.to_string(),
                Value::Float(f) => f.to_string(),
                _ => "0.0".to_string(),
            }).collect();
            format!("[{}]", elems.join(", "))
        },
        _ => "null".to_string(),
    }
}
