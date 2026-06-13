use crate::ast::*;

const ORIGINAL_POINT_ID_PAYLOAD_KEY: &str = "_qail_original_point_id";

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

fn normalize_qdrant_field(raw: &str) -> &str {
    raw.trim().trim_matches('"').trim()
}

fn qdrant_reserved_field_matches(raw: &str, reserved: &str) -> bool {
    normalize_qdrant_field(raw).eq_ignore_ascii_case(normalize_qdrant_field(reserved))
}

fn qdrant_projection_is_wildcard(raw: &str, table: &str) -> bool {
    let trimmed = raw.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        return false;
    }
    trimmed == "*" || trimmed.strip_prefix(table).is_some_and(|rest| rest == ".*")
}

fn raw_named_qdrant_field(expr: &Expr) -> Result<&str, String> {
    let raw = match expr {
        Expr::Named(name) | Expr::Aliased { name, .. } => name.as_str(),
        other => {
            return Err(format!(
                "Qdrant fields must be named, got expression `{other}`"
            ));
        }
    };
    let field = normalize_qdrant_field(raw);
    if field.is_empty() {
        return Err("Qdrant field name cannot be empty".to_string());
    }
    Ok(raw)
}

fn named_qdrant_field(expr: &Expr) -> Result<&str, String> {
    Ok(normalize_qdrant_field(raw_named_qdrant_field(expr)?))
}

fn validate_json_payload_value(value: &serde_json::Value) -> Result<(), String> {
    match value {
        serde_json::Value::Object(map) => {
            for (key, value) in map {
                if key.trim().is_empty() {
                    return Err("Qdrant JSON payload object keys cannot be empty".to_string());
                }
                validate_json_payload_value(value)?;
            }
            Ok(())
        }
        serde_json::Value::Array(items) => {
            for item in items {
                validate_json_payload_value(item)?;
            }
            Ok(())
        }
        serde_json::Value::Number(number) => {
            if let Some(value) = number.as_u64()
                && value > i64::MAX as u64
            {
                return Err(
                    "Qdrant JSON integer payload values must fit in signed 64-bit range"
                        .to_string(),
                );
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn point_id_to_json(value: &Value) -> Result<String, String> {
    match value {
        Value::Int(n) if *n >= 0 => Ok(n.to_string()),
        Value::String(s) if !s.trim().is_empty() => Ok(json_string(s)),
        Value::Uuid(u) => Ok(json_string(&u.to_string())),
        _ => Err(
            "Qdrant point id must be a non-negative integer, non-empty string, or UUID".to_string(),
        ),
    }
}

fn qdrant_limit(cmd: &Qail) -> Result<usize, String> {
    let mut limit = None;
    for cage in &cmd.cages {
        if let CageKind::Limit(n) = cage.kind {
            if n == 0 {
                return Err("Qdrant limit must be greater than zero".to_string());
            }
            if limit.replace(n).is_some() {
                return Err("Duplicate Qdrant LIMIT clauses are not supported".to_string());
            }
        }
    }
    Ok(limit.unwrap_or(10))
}

fn build_qdrant_upsert(cmd: &Qail) -> Result<String, String> {
    // POST /collections/{name}/points?wait=true
    // Body: { "points": [ { "id": 1, "vector": [...], "payload": {...} } ] }

    // Single point upsert from payload/filter cages.
    let mut point_id = None;
    let mut vector = cmd
        .vector
        .as_ref()
        .map(|values| vector_to_json(&Value::Vector(values.clone())))
        .transpose()?;
    let mut payload_parts = Vec::new();
    let mut payload_fields = std::collections::HashSet::new();

    for cage in &cmd.cages {
        match cage.kind {
            CageKind::Payload => {
                for cond in &cage.conditions {
                    if cond.op != Operator::Eq {
                        return Err(
                            "Qdrant upsert payload fields require equality values".to_string()
                        );
                    }
                    let name = named_qdrant_field(&cond.left)?;
                    if qdrant_reserved_field_matches(name, "id") {
                        if point_id.replace(point_id_to_json(&cond.value)?).is_some() {
                            return Err(
                                "Duplicate Qdrant upsert id fields are not supported".to_string()
                            );
                        }
                    } else if qdrant_reserved_field_matches(name, "vector") {
                        if vector.replace(vector_to_json(&cond.value)?).is_some() {
                            return Err("Duplicate Qdrant upsert vector fields are not supported"
                                .to_string());
                        }
                    } else if qdrant_reserved_field_matches(name, ORIGINAL_POINT_ID_PAYLOAD_KEY) {
                        return Err(format!(
                            "Qdrant upsert payload field `{ORIGINAL_POINT_ID_PAYLOAD_KEY}` is reserved"
                        ));
                    } else {
                        if !payload_fields.insert(name.to_string()) {
                            return Err(format!(
                                "Duplicate Qdrant upsert payload field `{name}` is not supported"
                            ));
                        }
                        payload_parts.push(format!(
                            "{}: {}",
                            json_string(name),
                            value_to_json(&cond.value)?
                        ));
                    }
                }
            }
            CageKind::Filter => {
                let can_infer_identity =
                    matches!(cage.logical_op, LogicalOp::And) || cage.conditions.len() == 1;
                for cond in &cage.conditions {
                    let name = named_qdrant_field(&cond.left)?;
                    if cond.op != Operator::Eq {
                        return Err(
                            "Qdrant upsert filter fallbacks require equality values".to_string()
                        );
                    }
                    if qdrant_reserved_field_matches(name, "id") {
                        if !can_infer_identity {
                            if point_id.is_none() {
                                return Err(
                                    "Qdrant upsert id cannot be inferred from a multi-condition OR filter"
                                        .to_string(),
                                );
                            }
                            continue;
                        }
                        let next = point_id_to_json(&cond.value)?;
                        if point_id.as_ref().is_some_and(|existing| existing != &next) {
                            return Err(
                                "Qdrant upsert filter id conflicts with payload id".to_string()
                            );
                        }
                        point_id = Some(next);
                    } else if qdrant_reserved_field_matches(name, "vector") {
                        if !can_infer_identity {
                            if vector.is_none() {
                                return Err(
                                    "Qdrant upsert vector cannot be inferred from a multi-condition OR filter"
                                        .to_string(),
                                );
                            }
                            continue;
                        }
                        let next = vector_to_json(&cond.value)?;
                        if vector.as_ref().is_some_and(|existing| existing != &next) {
                            return Err(
                                "Qdrant upsert filter vector conflicts with payload vector"
                                    .to_string(),
                            );
                        }
                        vector = Some(next);
                    } else {
                        return Err(format!(
                            "Qdrant upsert filters cannot be encoded as conditional writes: `{name}`"
                        ));
                    }
                }
            }
            _ => {}
        }
    }

    let point_id =
        point_id.ok_or_else(|| "Qdrant upsert requires payload/filter field `id`".to_string())?;
    let vector = vector.ok_or_else(|| {
        "Qdrant upsert requires payload/filter field `vector` or cmd.vector".to_string()
    })?;

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
                let field = named_qdrant_field(&cond.left)?;
                if qdrant_reserved_field_matches(field, "id") {
                    if cond.op != Operator::Eq {
                        return Err("Qdrant delete id filters support only equality".to_string());
                    }
                    ids.push(point_id_to_json(&cond.value)?);
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
    let mut vector_json = cmd
        .vector
        .as_ref()
        .map(|values| vector_to_json(&Value::Vector(values.clone())))
        .transpose()?;

    for cage in &cmd.cages {
        if let CageKind::Filter = cage.kind {
            for cond in &cage.conditions {
                if cond.op == Operator::Fuzzy {
                    let field = named_qdrant_field(&cond.left)?;
                    if !qdrant_reserved_field_matches(field, "vector") {
                        return Err(
                            "Qdrant fuzzy search is only supported on the vector field".to_string()
                        );
                    }
                    if vector_json.is_some() {
                        return Err("Duplicate Qdrant search vectors are not supported".to_string());
                    }
                    // Vector Query found.
                    // Case 1: [vector~[0.1, 0.2]] -> Explicit Vector (Already handled by Value::Array)
                    // Case 2: [vector~"cute cat"] -> Semantic Search Intent
                    let encoded = match &cond.value {
                        Value::String(s) => {
                            if s.trim().is_empty() {
                                return Err(
                                    "Qdrant semantic vector prompt cannot be empty".to_string()
                                );
                            }
                            // Output Placeholder for Runtime Resolution
                            // e.g. {{EMBED:cute cat}}
                            json_string(&format!("{{{{EMBED:{}}}}}", s))
                        }
                        _ => vector_to_json(&cond.value)?,
                    };
                    vector_json = Some(encoded);
                }
            }
        }
    }

    let vector_json = vector_json
        .ok_or_else(|| "Qdrant search requires cmd.vector or a fuzzy vector filter".to_string())?;
    parts.push(format!("\"vector\": {vector_json}"));

    if let Some(threshold) = cmd.score_threshold {
        if !threshold.is_finite() {
            return Err("Qdrant score threshold must be finite".to_string());
        }
        parts.push(format!("\"score_threshold\": {threshold}"));
    }

    if let Some(vector_name) = &cmd.vector_name {
        if vector_name.trim().is_empty() {
            return Err("Qdrant vector name cannot be empty".to_string());
        }
        return Err(
            "Qdrant JSON transpiler does not support named vector searches; use the qail-qdrant driver"
                .to_string(),
        );
    }

    // 2. Filters (Hybrid Search)
    let filter = build_filter(cmd)?;
    if !filter.is_empty() {
        parts.push(format!("\"filter\": {}", filter));
    }

    // 3. Limit
    let limit = qdrant_limit(cmd)?;
    parts.push(format!("\"limit\": {}", limit));

    // 4. With Payload (Projections)
    let mut wants_vector = cmd.with_vector;
    if !cmd.columns.is_empty() {
        let mut payload_includes = Vec::new();
        let mut has_wildcard = false;
        for c in &cmd.columns {
            let raw_field = raw_named_qdrant_field(c)?;
            let field = normalize_qdrant_field(raw_field);
            if qdrant_projection_is_wildcard(raw_field, &cmd.table) {
                has_wildcard = true;
                continue;
            }
            if qdrant_reserved_field_matches(field, "vector") {
                wants_vector = true;
                continue;
            }
            if qdrant_reserved_field_matches(field, "id")
                || qdrant_reserved_field_matches(field, "score")
            {
                continue;
            }
            payload_includes.push(json_string(field));
        }
        if has_wildcard {
            parts.push("\"with_payload\": true".to_string());
        } else if payload_includes.is_empty() {
            parts.push("\"with_payload\": false".to_string());
        } else {
            parts.push(format!(
                "\"with_payload\": {{ \"include\": [{}] }}",
                payload_includes.join(", ")
            ));
        }
    } else {
        parts.push("\"with_payload\": true".to_string());
    }
    if wants_vector {
        parts.push("\"with_vector\": true".to_string());
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
                let col_str = named_qdrant_field(&cond.left)?;

                if qdrant_reserved_field_matches(col_str, "id") {
                    if cond.op != Operator::Eq {
                        return Err(
                            "Qdrant id filters support only equality against integer, string, or UUID values"
                                .to_string(),
                        );
                    }
                    cage_clauses.push(format!(
                        "{{ \"has_id\": [{}] }}",
                        point_id_to_json(&cond.value)?
                    ));
                    continue;
                }

                let clause = match cond.op {
                    Operator::Eq => format!(
                        "{{ \"key\": {}, \"match\": {{ \"value\": {} }} }}",
                        json_string(col_str),
                        filter_match_value_to_json(&cond.value)?
                    ),
                    Operator::In => format!(
                        "{{ \"key\": {}, \"match\": {{ \"any\": [{}] }} }}",
                        json_string(col_str),
                        filter_array_values_to_json(&cond.value)?
                    ),
                    // Qdrant range: { "key": "price", "range": { "gt": 10.0 } }
                    Operator::Gt => format!(
                        "{{ \"key\": {}, \"range\": {{ \"gt\": {} }} }}",
                        json_string(col_str),
                        numeric_filter_value(&cond.value)?
                    ),
                    Operator::Gte => format!(
                        "{{ \"key\": {}, \"range\": {{ \"gte\": {} }} }}",
                        json_string(col_str),
                        numeric_filter_value(&cond.value)?
                    ),
                    Operator::Lt => format!(
                        "{{ \"key\": {}, \"range\": {{ \"lt\": {} }} }}",
                        json_string(col_str),
                        numeric_filter_value(&cond.value)?
                    ),
                    Operator::Lte => format!(
                        "{{ \"key\": {}, \"range\": {{ \"lte\": {} }} }}",
                        json_string(col_str),
                        numeric_filter_value(&cond.value)?
                    ),
                    Operator::IsNull => {
                        if !matches!(cond.value, Value::Null | Value::NullUuid) {
                            return Err("Qdrant IS NULL filters require a null value".to_string());
                        }
                        format!("{{ \"is_null\": {{ \"key\": {} }} }}", json_string(col_str))
                    }
                    Operator::Fuzzy => {
                        if qdrant_reserved_field_matches(col_str, "vector") {
                            continue;
                        }
                        return Err(
                            "Qdrant fuzzy filters are only supported on the vector field"
                                .to_string(),
                        );
                    }
                    Operator::Contains | Operator::Like => {
                        let Value::String(value) = &cond.value else {
                            return Err("Qdrant text filters require a string value".to_string());
                        };
                        if value.trim().is_empty() {
                            return Err("Qdrant text filter value cannot be empty".to_string());
                        }
                        format!(
                            "{{ \"key\": {}, \"match\": {{ \"text\": {} }} }}",
                            json_string(col_str),
                            json_string(value)
                        )
                    }
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

fn filter_match_value_to_json(v: &Value) -> Result<String, String> {
    match v {
        Value::String(s) => Ok(json_string(s)),
        Value::Int(n) => Ok(n.to_string()),
        Value::Float(n) if n.is_finite() => Ok(n.to_string()),
        Value::Float(_) => Err("Qdrant equality filter values must be finite numbers".to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Null | Value::NullUuid => {
            Err("Qdrant equality filters cannot match null; use IS NULL".to_string())
        }
        other => Err(format!(
            "Qdrant equality filters support only string, integer, finite float, or bool values, got {other}"
        )),
    }
}

fn filter_array_values_to_json(v: &Value) -> Result<String, String> {
    let Value::Array(values) = v else {
        return Err("Qdrant IN filters require an array value".to_string());
    };
    if values.is_empty() {
        return Err("Qdrant IN filters require at least one value".to_string());
    }
    let values = values
        .iter()
        .map(filter_match_value_to_json)
        .map(|result| result.map_err(|err| format!("Qdrant IN filters value is invalid: {err}")))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(values.join(", "))
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
        Value::Json(json) => {
            let value = serde_json::from_str::<serde_json::Value>(json)
                .map_err(|err| format!("invalid JSON value for Qdrant payload: {err}"))?;
            validate_json_payload_value(&value)?;
            Ok(value.to_string())
        }
        other => Err(format!("unsupported Qdrant JSON value: {other}")),
    }
}

fn numeric_filter_value(v: &Value) -> Result<String, String> {
    match v {
        Value::Int(n) => Ok(n.to_string()),
        Value::Float(n) if n.is_finite() => Ok(n.to_string()),
        Value::Float(_) => Err("Qdrant range filter values must be finite numbers".to_string()),
        other => Err(format!(
            "Qdrant range filter values must be numeric, got {other}"
        )),
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
