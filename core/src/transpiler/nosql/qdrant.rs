use crate::ast::*;

fn json_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_string())
}

/// Trait for converting QAIL AST to Qdrant vector-search JSON.
pub trait ToQdrant {
    /// Convert a QAIL query into a Qdrant search/upsert/delete JSON body.
    fn to_qdrant_search(&self) -> String;
}

impl ToQdrant for Qail {
    fn to_qdrant_search(&self) -> String {
        let result = match self.action {
            Action::Get => build_qdrant_search(self),
            Action::Put | Action::Add => build_qdrant_upsert(self),
            Action::Del => build_qdrant_delete(self),
            _ => {
                return format!(
                    "{{ \"error\": \"Action {:?} not supported for Qdrant\" }}",
                    self.action
                );
            }
        };

        result.unwrap_or_else(|err| qdrant_error(&err))
    }
}

fn qdrant_error(message: &str) -> String {
    format!("{{ \"error\": {} }}", json_string(message))
}

fn build_qdrant_upsert(cmd: &Qail) -> Result<String, String> {
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
                    if let Expr::Named(name) = &cond.left {
                        if name == "id" {
                            point_id = value_to_json(&cond.value)?;
                        } else if name == "vector" {
                            vector = vector_to_json(&cond.value)?;
                        } else {
                            payload_parts.push(format!(
                                "{}: {}",
                                json_string(name),
                                value_to_json(&cond.value)?
                            ));
                        }
                    }
                }
            }
            _ => {}
        }
    }

    let payload_json = if payload_parts.is_empty() {
        "{}".to_string()
    } else {
        format!("{{ {} }}", payload_parts.join(", "))
    };

    // Construct single point
    let point = format!(
        "{{ \"id\": {}, \"vector\": {}, \"payload\": {} }}",
        point_id, vector, payload_json
    );

    Ok(format!("{{ \"points\": [{}] }}", point))
}

fn build_qdrant_delete(cmd: &Qail) -> Result<String, String> {
    // POST /collections/{name}/points/delete
    // Body: { "points": [1, 2, 3] } OR { "filter": ... }

    // If ID specified, delete by ID. Else delete by filter.
    let mut ids = Vec::new();

    for cage in &cmd.cages {
        if let CageKind::Filter = cage.kind {
            for cond in &cage.conditions {
                if let Expr::Named(name) = &cond.left
                    && name == "id"
                {
                    ids.push(value_to_json(&cond.value)?);
                }
            }
        }
    }

    if !ids.is_empty() {
        Ok(format!("{{ \"points\": [{}] }}", ids.join(", ")))
    } else {
        // Delete by filter
        let filter = build_filter(cmd)?;
        if filter.is_empty() {
            return Err("Qdrant delete requires an id or filter condition".to_string());
        }
        Ok(format!("{{ \"filter\": {} }}", filter))
    }
}

fn build_qdrant_search(cmd: &Qail) -> Result<String, String> {
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
                            parts.push(format!(
                                "\"vector\": {}",
                                json_string(&format!("{{{{EMBED:{}}}}}", s))
                            ));
                        }
                        _ => {
                            parts.push(format!("\"vector\": {}", vector_to_json(&cond.value)?));
                        }
                    }
                    vector_found = true;
                    break;
                }
            }
        }
        if vector_found {
            break;
        }
    }

    if !vector_found {
        // Actually, Qdrant supports Scroll API separate from Search.
        parts.push("\"vector\": [0.0]".to_string()); // Dummy vector or error? Let's use dummy to show intent.
    }

    // 2. Filters (Hybrid Search)
    let filter = build_filter(cmd)?;
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
            if let Expr::Named(n) = c {
                incl.push(json_string(n));
            }
        }
        parts.push(format!(
            "\"with_payload\": {{ \"include\": [{}] }}",
            incl.join(", ")
        ));
    } else {
        parts.push("\"with_payload\": true".to_string());
    }

    Ok(format!("{{ {} }}", parts.join(", ")))
}

fn build_filter(cmd: &Qail) -> Result<String, String> {
    // Qdrant Filter structure: { "must": [ { "key": "city", "match": { "value": "London" } } ] }
    let mut musts = Vec::new();
    let mut should_groups: Vec<Vec<String>> = Vec::new();

    for cage in &cmd.cages {
        if let CageKind::Filter = cage.kind {
            let mut cage_clauses = Vec::new();
            for cond in &cage.conditions {
                // Skip the vector query itself
                if cond.op == Operator::Fuzzy {
                    continue;
                }

                let val = value_to_json(&cond.value)?;
                let col_str = match &cond.left {
                    Expr::Named(name) => name.clone(),
                    expr => {
                        return Err(format!(
                            "Qdrant filters require named fields, got expression `{expr}`"
                        ));
                    }
                };

                let clause = match cond.op {
                    Operator::Eq => format!(
                        "{{ \"key\": {}, \"match\": {{ \"value\": {} }} }}",
                        json_string(&col_str),
                        val
                    ),
                    // Qdrant range: { "key": "price", "range": { "gt": 10.0 } }
                    Operator::Gt => format!(
                        "{{ \"key\": {}, \"range\": {{ \"gt\": {} }} }}",
                        json_string(&col_str),
                        val
                    ),
                    Operator::Gte => format!(
                        "{{ \"key\": {}, \"range\": {{ \"gte\": {} }} }}",
                        json_string(&col_str),
                        val
                    ),
                    Operator::Lt => format!(
                        "{{ \"key\": {}, \"range\": {{ \"lt\": {} }} }}",
                        json_string(&col_str),
                        val
                    ),
                    Operator::Lte => format!(
                        "{{ \"key\": {}, \"range\": {{ \"lte\": {} }} }}",
                        json_string(&col_str),
                        val
                    ),
                    Operator::Ne => format!(
                        "{{ \"must_not\": [{{ \"key\": {}, \"match\": {{ \"value\": {} }} }}] }}",
                        json_string(&col_str),
                        val
                    ), // This needs wrapping?
                    _ => return Err(format!("unsupported Qdrant filter operator {:?}", cond.op)),
                };
                cage_clauses.push(clause);
            }

            if cage_clauses.is_empty() {
                continue;
            }

            match cage.logical_op {
                LogicalOp::And => musts.extend(cage_clauses),
                LogicalOp::Or => should_groups.push(cage_clauses),
            }
        }
    }

    for group in should_groups {
        if group.len() == 1 {
            musts.push(group[0].clone());
        } else {
            musts.push(format!("{{ \"should\": [{}] }}", group.join(", ")));
        }
    }

    if musts.is_empty() {
        return Ok(String::new());
    }

    let mut parts = Vec::new();
    if !musts.is_empty() {
        parts.push(format!("\"must\": [{}]", musts.join(", ")));
    }
    Ok(format!("{{ {} }}", parts.join(", ")))
}

fn get_cage_val(cmd: &Qail, kind_example: CageKind) -> Option<usize> {
    for cage in &cmd.cages {
        if let (CageKind::Limit(n), CageKind::Limit(_)) = (&cage.kind, &kind_example) {
            return Some(*n);
        }
    }
    None
}

fn value_to_json(v: &Value) -> Result<String, String> {
    match v {
        Value::Null | Value::NullUuid => Ok("null".to_string()),
        Value::String(s) => Ok(json_string(s)),
        Value::Int(n) => Ok(n.to_string()),
        Value::Float(n) if n.is_finite() => Ok(n.to_string()),
        Value::Float(_) => Err("non-finite floats cannot be encoded as Qdrant JSON".to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Uuid(u) => Ok(json_string(&u.to_string())),
        Value::Timestamp(ts) => Ok(json_string(ts)),
        Value::Array(arr) => {
            let elems: Result<Vec<String>, String> = arr.iter().map(value_to_json).collect();
            Ok(format!("[{}]", elems?.join(", ")))
        }
        Value::Vector(values) => {
            let elems: Result<Vec<String>, String> = values
                .iter()
                .map(|value| {
                    if value.is_finite() {
                        Ok(value.to_string())
                    } else {
                        Err("non-finite vector values cannot be encoded as Qdrant JSON".to_string())
                    }
                })
                .collect();
            Ok(format!("[{}]", elems?.join(", ")))
        }
        Value::Json(json) => serde_json::from_str::<serde_json::Value>(json)
            .map(|value| value.to_string())
            .map_err(|err| format!("invalid JSON value for Qdrant payload: {err}")),
        other => Err(format!("unsupported Qdrant JSON value: {other}")),
    }
}

fn vector_to_json(v: &Value) -> Result<String, String> {
    let elems: Result<Vec<String>, String> = match v {
        Value::Vector(values) => values
            .iter()
            .map(|value| {
                if value.is_finite() {
                    Ok(value.to_string())
                } else {
                    Err("Qdrant vector values must be finite numbers".to_string())
                }
            })
            .collect(),
        Value::Array(values) => values
            .iter()
            .map(|value| match value {
                Value::Int(n) => Ok(n.to_string()),
                Value::Float(n) if n.is_finite() => Ok(n.to_string()),
                Value::Float(_) => Err("Qdrant vector values must be finite numbers".to_string()),
                other => Err(format!("Qdrant vector values must be numeric, got {other}")),
            })
            .collect(),
        other => return Err(format!("Qdrant vector must be an array, got {other}")),
    };

    let elems = elems?;
    if elems.is_empty() {
        return Err("Qdrant vector cannot be empty".to_string());
    }
    Ok(format!("[{}]", elems.join(", ")))
}
