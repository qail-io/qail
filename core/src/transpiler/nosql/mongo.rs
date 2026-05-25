use crate::ast::*;

fn js_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_string())
}

fn is_js_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };

    if !(first == '_' || first == '$' || first.is_ascii_alphabetic()) {
        return false;
    }

    chars.all(|c| c == '_' || c == '$' || c.is_ascii_alphanumeric())
}

fn mongo_collection(name: &str) -> String {
    if is_js_identifier(name) {
        format!("db.{name}")
    } else {
        format!("db.getCollection({})", js_string(name))
    }
}

/// Trait for converting QAIL AST to MongoDB shell commands.
pub trait ToMongo {
    /// Convert a QAIL query into a MongoDB shell command string.
    fn to_mongo(&self) -> String;
}

impl ToMongo for Qail {
    fn to_mongo(&self) -> String {
        let result = match self.action {
            Action::Get => {
                if !self.joins.is_empty() {
                    build_aggregate(self)
                } else {
                    build_find(self)
                }
            }
            Action::Set => build_update(self),
            Action::Add => build_insert(self),
            Action::Put => build_upsert(self),
            Action::Del => build_delete(self),
            Action::Make => Ok(format!("db.createCollection({})", js_string(&self.table))),
            Action::Drop => Ok(format!("{}.drop()", mongo_collection(&self.table))),
            Action::TxnStart => Ok("session.startTransaction()".to_string()),
            Action::TxnCommit => Ok("session.commitTransaction()".to_string()),
            Action::TxnRollback => Ok("session.abortTransaction()".to_string()),
            _ => {
                return mongo_error(&format!(
                    "Action {:?} not supported for MongoDB",
                    self.action
                ));
            }
        };

        result.unwrap_or_else(|err| mongo_error(&err))
    }
}

fn mongo_error(message: &str) -> String {
    format!("throw new Error({})", js_string(message))
}

fn build_aggregate(cmd: &Qail) -> Result<String, String> {
    let mut stages = Vec::new();

    // 1. $match
    let filter = build_query_filter(cmd)?;
    if filter != "{}" {
        stages.push(format!("{{ \"$match\": {} }}", filter));
    }

    // 2. $lookup
    for join in &cmd.joins {
        let target = &join.table;
        let source_singular = cmd.table.trim_end_matches('s');
        let pk = format!("{}_id", source_singular); // users -> user_id

        // from: orders, localField: _id, foreignField: user_id, as: orders
        let lookup = format!(
            "{{ \"$lookup\": {{ \"from\": {}, \"localField\": \"_id\", \"foreignField\": {}, \"as\": {} }} }}",
            js_string(target),
            js_string(&pk),
            js_string(target)
        );
        stages.push(lookup);
    }

    // 3. $project & Add Fields logic if needed?
    // For now simple projection if columns exist
    let proj = build_projection(cmd)?;
    if proj != "{}" {
        stages.push(format!("{{ \"$project\": {} }}", proj));
    }

    // 4. Sort, Skip, Limit
    for cage in &cmd.cages {
        match &cage.kind {
            CageKind::Sort(order) => {
                let val = match order {
                    SortOrder::Asc | SortOrder::AscNullsFirst | SortOrder::AscNullsLast => 1,
                    SortOrder::Desc | SortOrder::DescNullsFirst | SortOrder::DescNullsLast => -1,
                };
                if let Some(cond) = cage.conditions.first() {
                    let col_str = match &cond.left {
                        Expr::Named(name) => name.clone(),
                        expr => {
                            return Err(format!(
                                "MongoDB sort fields must be named, got expression `{expr}`"
                            ));
                        }
                    };
                    stages.push(format!(
                        "{{ \"$sort\": {{ {}: {} }} }}",
                        js_string(&col_str),
                        val
                    ));
                }
            }
            CageKind::Offset(n) => stages.push(format!("{{ \"$skip\": {} }}", n)),
            CageKind::Limit(n) => stages.push(format!("{{ \"$limit\": {} }}", n)),
            _ => {}
        }
    }

    Ok(format!(
        "{}.aggregate([{}])",
        mongo_collection(&cmd.table),
        stages.join(", ")
    ))
}

fn build_find(cmd: &Qail) -> Result<String, String> {
    let query = build_query_filter(cmd)?;
    let projection = build_projection(cmd)?;

    // Base: db.collection.find(query, projection)
    let mut mongo = format!(
        "{}.find({}, {})",
        mongo_collection(&cmd.table),
        query,
        projection
    );

    // Sort, Limit, Skip logic
    for cage in &cmd.cages {
        match &cage.kind {
            CageKind::Limit(n) => mongo.push_str(&format!(".limit({})", n)),
            CageKind::Offset(n) => mongo.push_str(&format!(".skip({})", n)),
            CageKind::Sort(order) => {
                let val = match order {
                    SortOrder::Asc | SortOrder::AscNullsFirst | SortOrder::AscNullsLast => 1,
                    SortOrder::Desc | SortOrder::DescNullsFirst | SortOrder::DescNullsLast => -1,
                };
                if let Some(cond) = cage.conditions.first() {
                    let col_str = match &cond.left {
                        Expr::Named(name) => name.clone(),
                        expr => {
                            return Err(format!(
                                "MongoDB sort fields must be named, got expression `{expr}`"
                            ));
                        }
                    };
                    mongo.push_str(&format!(".sort({{ {}: {} }})", js_string(&col_str), val));
                }
            }
            _ => {}
        }
    }

    Ok(mongo)
}

fn build_update(cmd: &Qail) -> Result<String, String> {
    let query = build_query_filter(cmd)?;
    // Payload logic for $set would go here
    let mut update_doc = String::from("{ $set: { ");
    let mut first = true;

    for cage in &cmd.cages {
        if let CageKind::Payload = cage.kind {
            for cond in &cage.conditions {
                if !first {
                    update_doc.push_str(", ");
                }
                let col_str = match &cond.left {
                    Expr::Named(name) => name.clone(),
                    expr => {
                        return Err(format!(
                            "MongoDB update fields must be named, got expression `{expr}`"
                        ));
                    }
                };
                update_doc.push_str(&format!(
                    "{}: {}",
                    js_string(&col_str),
                    value_to_json(&cond.value)?
                ));
                first = false;
            }
        }
    }
    if first {
        return Err("MongoDB update requires at least one update field".to_string());
    }
    update_doc.push_str(" } }");

    Ok(format!(
        "{}.updateMany({}, {})",
        mongo_collection(&cmd.table),
        query,
        update_doc
    ))
}

fn build_insert(cmd: &Qail) -> Result<String, String> {
    let mut doc = String::from("{ ");
    let mut first = true;

    for cage in &cmd.cages {
        if let CageKind::Payload = cage.kind {
            for cond in &cage.conditions {
                if !first {
                    doc.push_str(", ");
                }
                let col_str = match &cond.left {
                    Expr::Named(name) => name.clone(),
                    expr => {
                        return Err(format!(
                            "MongoDB insert fields must be named, got expression `{expr}`"
                        ));
                    }
                };
                doc.push_str(&format!(
                    "{}: {}",
                    js_string(&col_str),
                    value_to_json(&cond.value)?
                ));
                first = false;
            }
        }
    }
    if first {
        return Err("MongoDB insert requires at least one document field".to_string());
    }
    doc.push_str(" }");

    Ok(format!(
        "{}.insertOne({})",
        mongo_collection(&cmd.table),
        doc
    ))
}

fn build_upsert(cmd: &Qail) -> Result<String, String> {
    // Similar to update but with upsert: true
    let query = build_query_filter(cmd)?;

    // Payload logic for $set
    let mut update_doc = String::from("{ $set: { ");
    let mut first = true;

    for cage in &cmd.cages {
        if let CageKind::Payload = cage.kind {
            for cond in &cage.conditions {
                if !first {
                    update_doc.push_str(", ");
                }
                let col_str = match &cond.left {
                    Expr::Named(name) => name.clone(),
                    expr => {
                        return Err(format!(
                            "MongoDB upsert fields must be named, got expression `{expr}`"
                        ));
                    }
                };
                update_doc.push_str(&format!(
                    "{}: {}",
                    js_string(&col_str),
                    value_to_json(&cond.value)?
                ));
                first = false;
            }
        }
    }
    if first {
        return Err("MongoDB upsert requires at least one update field".to_string());
    }
    update_doc.push_str(" } }");

    Ok(format!(
        "{}.updateOne({}, {}, {{ \"upsert\": true }})",
        mongo_collection(&cmd.table),
        query,
        update_doc
    ))
}

fn build_delete(cmd: &Qail) -> Result<String, String> {
    let query = build_query_filter(cmd)?;
    if query == "{}" {
        return Err("MongoDB delete requires at least one filter condition".to_string());
    }
    Ok(format!(
        "{}.deleteMany({})",
        mongo_collection(&cmd.table),
        query
    ))
}

fn build_query_filter(cmd: &Qail) -> Result<String, String> {
    let mut and_clauses = Vec::new();

    for cage in &cmd.cages {
        if let CageKind::Filter = cage.kind {
            let mut cage_clauses = Vec::new();
            for cond in &cage.conditions {
                cage_clauses.push(mongo_condition_clause(cond)?);
            }

            if cage_clauses.is_empty() {
                continue;
            }

            match cage.logical_op {
                LogicalOp::And => and_clauses.extend(cage_clauses),
                LogicalOp::Or => {
                    if cage_clauses.len() == 1 {
                        and_clauses.push(cage_clauses[0].clone());
                    } else {
                        and_clauses.push(format!("{{ \"$or\": [{}] }}", cage_clauses.join(", ")));
                    }
                }
            }
        }
    }

    match and_clauses.len() {
        0 => Ok("{}".to_string()),
        1 => Ok(and_clauses.remove(0)),
        _ => Ok(format!("{{ \"$and\": [{}] }}", and_clauses.join(", "))),
    }
}

fn mongo_condition_clause(cond: &Condition) -> Result<String, String> {
    let op = match cond.op {
        Operator::Eq => "$eq",
        Operator::Ne => "$ne",
        Operator::Gt => "$gt",
        Operator::Lt => "$lt",
        Operator::Gte => "$gte",
        Operator::Lte => "$lte",
        _ => return Err(format!("unsupported MongoDB filter operator {:?}", cond.op)),
    };

    let col_str = match &cond.left {
        Expr::Named(name) => name.clone(),
        expr => {
            return Err(format!(
                "MongoDB filters require named fields, got expression `{expr}`"
            ));
        }
    };

    if let Operator::Eq = cond.op {
        Ok(format!(
            "{{ {}: {} }}",
            js_string(&col_str),
            value_to_json(&cond.value)?
        ))
    } else {
        Ok(format!(
            "{{ {}: {{ \"{}\": {} }} }}",
            js_string(&col_str),
            op,
            value_to_json(&cond.value)?
        ))
    }
}

fn build_projection(cmd: &Qail) -> Result<String, String> {
    if cmd.columns.is_empty() {
        return Ok("{}".to_string());
    }

    let mut proj = String::from("{ ");
    for (i, col) in cmd.columns.iter().enumerate() {
        if i > 0 {
            proj.push_str(", ");
        }
        let Expr::Named(name) = col else {
            return Err(format!(
                "MongoDB projections require named fields, got expression `{col}`"
            ));
        };
        proj.push_str(&format!("{}: 1", js_string(name)));
    }
    proj.push_str(" }");
    Ok(proj)
}

fn value_to_json(v: &Value) -> Result<String, String> {
    match v {
        Value::Null | Value::NullUuid => Ok("null".to_string()),
        Value::String(s) => Ok(js_string(s)),
        Value::Int(n) => Ok(n.to_string()),
        Value::Float(n) if n.is_finite() => Ok(n.to_string()),
        Value::Float(_) => Err("non-finite floats cannot be encoded as MongoDB JSON".to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Uuid(uuid) => Ok(js_string(&uuid.to_string())),
        Value::Timestamp(ts) => Ok(js_string(ts)),
        Value::Array(values) => {
            let values: Result<Vec<String>, String> = values.iter().map(value_to_json).collect();
            Ok(format!("[{}]", values?.join(", ")))
        }
        Value::Vector(values) => {
            let values: Result<Vec<String>, String> = values
                .iter()
                .map(|value| {
                    if value.is_finite() {
                        Ok(value.to_string())
                    } else {
                        Err("non-finite vector values cannot be encoded as MongoDB JSON"
                            .to_string())
                    }
                })
                .collect();
            Ok(format!("[{}]", values?.join(", ")))
        }
        Value::Json(json) => serde_json::from_str::<serde_json::Value>(json)
            .map(|value| value.to_string())
            .map_err(|err| format!("invalid JSON value for MongoDB document: {err}")),
        other => Err(format!("unsupported MongoDB value: {other}")),
    }
}
