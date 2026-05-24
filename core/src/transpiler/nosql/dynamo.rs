use crate::ast::*;

fn json_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_string())
}

#[derive(Default)]
struct DynamoExpression {
    expression: String,
    values: String,
    names: Vec<(String, String)>,
}

fn attribute_names_json(names: &[(String, String)]) -> String {
    names
        .iter()
        .map(|(placeholder, name)| format!("{}: {}", json_string(placeholder), json_string(name)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Trait for converting QAIL AST to DynamoDB JSON.
pub trait ToDynamo {
    /// Convert a QAIL query into a DynamoDB request JSON body.
    fn to_dynamo(&self) -> String;
}

impl ToDynamo for Qail {
    fn to_dynamo(&self) -> String {
        let result = match self.action {
            Action::Get => build_get_item(self),
            Action::Add | Action::Put => build_put_item(self),
            Action::Set => build_update_item(self),
            Action::Del => build_delete_item(self),
            Action::Make => Ok(build_create_table(self)),
            Action::Drop => Ok(format!("{{ \"TableName\": {} }}", json_string(&self.table))), // DeleteTable input
            _ => {
                return format!(
                    "{{ \"error\": {} }}",
                    json_string(&format!("Action {:?} not supported", self.action))
                );
            }
        };

        result.unwrap_or_else(|err| dynamo_error(&err))
    }
}

fn dynamo_error(message: &str) -> String {
    format!("{{ \"error\": {} }}", json_string(message))
}

fn build_get_item(cmd: &Qail) -> Result<String, String> {
    let mut parts = Vec::new();
    parts.push(format!("\"TableName\": {}", json_string(&cmd.table)));

    let mut filter = build_expression(cmd)?;
    if !filter.expression.is_empty() {
        parts.push(format!(
            "\"FilterExpression\": {}",
            json_string(&filter.expression)
        ));
        parts.push(format!(
            "\"ExpressionAttributeValues\": {{ {} }}",
            filter.values
        ));
    }

    for cage in &cmd.cages {
        if let CageKind::Filter = cage.kind {
            for cond in &cage.conditions {
                if let Expr::Named(name) = &cond.left {
                    match name.as_str() {
                        "gsi" | "index" => {
                            let index_name = match &cond.value {
                                Value::String(s) => s.clone(),
                                _ => {
                                    return Err("DynamoDB index name must be provided as a string"
                                        .to_string());
                                }
                            };
                            parts.push(format!("\"IndexName\": {}", json_string(&index_name)));
                        }
                        "consistency" | "consistent" => {
                            if consistent_read_value(&cond.value)? {
                                parts.push("\"ConsistentRead\": true".to_string());
                            } else {
                                parts.push("\"ConsistentRead\": false".to_string());
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    if !cmd.columns.is_empty() {
        let mut cols = Vec::new();
        for (idx, col) in cmd.columns.iter().enumerate() {
            if let Expr::Named(n) = col {
                let placeholder = format!("#p{}", idx + 1);
                cols.push(placeholder.clone());
                filter.names.push((placeholder, n.clone()));
            }
        }
        if !cols.is_empty() {
            parts.push(format!(
                "\"ProjectionExpression\": {}",
                json_string(&cols.join(", "))
            ));
        }
    }

    if !filter.names.is_empty() {
        parts.push(format!(
            "\"ExpressionAttributeNames\": {{ {} }}",
            attribute_names_json(&filter.names)
        ));
    }

    if let Some(n) = get_limit(cmd) {
        parts.push(format!("\"Limit\": {}", n))
    }

    Ok(format!("{{ {} }}", parts.join(", ")))
}

fn build_put_item(cmd: &Qail) -> Result<String, String> {
    let mut parts = Vec::new();
    parts.push(format!("\"TableName\": {}", json_string(&cmd.table)));

    let item = build_item_json(cmd)?;
    parts.push(format!("\"Item\": {{ {} }}", item));

    Ok(format!("{{ {} }}", parts.join(", ")))
}

fn build_update_item(cmd: &Qail) -> Result<String, String> {
    let mut parts = Vec::new();
    parts.push(format!("\"TableName\": {}", json_string(&cmd.table)));

    let key = build_key_from_filter(cmd)?;
    parts.push(format!("\"Key\": {{ {} }}", key));

    let update = build_update_expression(cmd)?;
    parts.push(format!(
        "\"UpdateExpression\": {}",
        json_string(&update.expression)
    ));
    parts.push(format!(
        "\"ExpressionAttributeValues\": {{ {} }}",
        update.values
    ));
    if !update.names.is_empty() {
        parts.push(format!(
            "\"ExpressionAttributeNames\": {{ {} }}",
            attribute_names_json(&update.names)
        ));
    }

    Ok(format!("{{ {} }}", parts.join(", ")))
}

fn build_delete_item(cmd: &Qail) -> Result<String, String> {
    let mut parts = Vec::new();
    parts.push(format!("\"TableName\": {}", json_string(&cmd.table)));

    // Key logic
    let key = build_key_from_filter(cmd)?;
    parts.push(format!("\"Key\": {{ {} }}", key));

    Ok(format!("{{ {} }}", parts.join(", ")))
}

fn build_expression(cmd: &Qail) -> Result<DynamoExpression, String> {
    let mut expr_parts = Vec::new();
    let mut values_parts = Vec::new();
    let mut names = Vec::new();
    let mut counter = 0;

    for cage in &cmd.cages {
        if let CageKind::Filter = cage.kind {
            for cond in &cage.conditions {
                let Expr::Named(name) = &cond.left else {
                    return Err(format!(
                        "DynamoDB filters require named fields, got expression `{}`",
                        cond.left
                    ));
                };

                if matches!(
                    name.as_str(),
                    "gsi" | "index" | "consistency" | "consistent"
                ) {
                    continue;
                }

                counter += 1;
                let placeholder = format!(":v{}", counter);
                let name_placeholder = format!("#f{}", counter);
                let op = match cond.op {
                    Operator::Eq => "=",
                    Operator::Ne => "<>",
                    Operator::Gt => ">",
                    Operator::Lt => "<",
                    Operator::Gte => ">=",
                    Operator::Lte => "<=",
                    _ => {
                        return Err(format!(
                            "unsupported DynamoDB filter operator {:?}",
                            cond.op
                        ));
                    }
                };

                expr_parts.push(format!("{} {} {}", name_placeholder, op, placeholder));
                names.push((name_placeholder, name.clone()));

                let val_json = value_to_dynamo(&cond.value)?;
                values_parts.push(format!("{}: {}", json_string(&placeholder), val_json));
            }
        }
    }

    Ok(DynamoExpression {
        expression: expr_parts.join(" AND "),
        values: values_parts.join(", "),
        names,
    })
}

fn build_item_json(cmd: &Qail) -> Result<String, String> {
    let mut parts = Vec::new();
    for cage in &cmd.cages {
        match cage.kind {
            CageKind::Payload | CageKind::Filter => {
                for cond in &cage.conditions {
                    let val = value_to_dynamo(&cond.value)?;
                    let Expr::Named(name) = &cond.left else {
                        return Err(format!(
                            "DynamoDB item fields must be named, got expression `{}`",
                            cond.left
                        ));
                    };
                    parts.push(format!("{}: {}", json_string(name), val));
                }
            }
            _ => {}
        }
    }

    if parts.is_empty() {
        return Err("DynamoDB put item requires at least one item field".to_string());
    }

    Ok(parts.join(", "))
}

fn build_key_from_filter(cmd: &Qail) -> Result<String, String> {
    for cage in &cmd.cages {
        if let CageKind::Filter = cage.kind {
            for cond in &cage.conditions {
                let Expr::Named(name) = &cond.left else {
                    return Err(format!(
                        "DynamoDB key fields must be named, got expression `{}`",
                        cond.left
                    ));
                };
                if matches!(
                    name.as_str(),
                    "gsi" | "index" | "consistency" | "consistent"
                ) {
                    continue;
                }
                if cond.op != Operator::Eq {
                    return Err("DynamoDB key filters must use equality".to_string());
                }
                let val = value_to_dynamo(&cond.value)?;
                return Ok(format!("{}: {}", json_string(name), val));
            }
        }
    }
    Err("DynamoDB update/delete requires an equality key filter".to_string())
}

fn build_update_expression(cmd: &Qail) -> Result<DynamoExpression, String> {
    let mut sets = Vec::new();
    let mut vals = Vec::new();
    let mut names = Vec::new();
    let mut counter = 100; // Offset to avoid collision with filters

    for cage in &cmd.cages {
        if let CageKind::Payload = cage.kind {
            for cond in &cage.conditions {
                counter += 1;
                let placeholder = format!(":u{}", counter);
                let Expr::Named(name) = &cond.left else {
                    return Err(format!(
                        "DynamoDB update fields must be named, got expression `{}`",
                        cond.left
                    ));
                };
                let name_placeholder = format!("#u{}", counter);
                sets.push(format!("{} = {}", name_placeholder, placeholder));
                names.push((name_placeholder, name.clone()));

                let val = value_to_dynamo(&cond.value)?;
                vals.push(format!("{}: {}", json_string(&placeholder), val));
            }
        }
    }

    if sets.is_empty() {
        return Err("DynamoDB update requires at least one payload field".to_string());
    }

    Ok(DynamoExpression {
        expression: format!("SET {}", sets.join(", ")),
        values: vals.join(", "),
        names,
    })
}

fn get_limit(cmd: &Qail) -> Option<usize> {
    for cage in &cmd.cages {
        if let CageKind::Limit(n) = cage.kind {
            return Some(n);
        }
    }
    None
}

fn build_create_table(cmd: &Qail) -> String {
    let mut attr_defs = Vec::new();
    let mut key_schema = Vec::new();

    for col in &cmd.columns {
        if let Expr::Def {
            name,
            data_type,
            constraints,
        } = col
            && constraints.contains(&Constraint::PrimaryKey)
        {
            let dtype = match data_type.as_str() {
                "int" | "i32" | "float" => "N",
                _ => "S",
            };
            attr_defs.push(format!(
                "{{ \"AttributeName\": {}, \"AttributeType\": {} }}",
                json_string(name),
                json_string(dtype)
            ));
            key_schema.push(format!(
                "{{ \"AttributeName\": {}, \"KeyType\": \"HASH\" }}",
                json_string(name)
            ));
        }
    }

    if key_schema.is_empty() {
        attr_defs.push("{ \"AttributeName\": \"id\", \"AttributeType\": \"S\" }".to_string());
        key_schema.push("{ \"AttributeName\": \"id\", \"KeyType\": \"HASH\" }".to_string());
    }

    format!(
        "{{ \"TableName\": {}, \"KeySchema\": [{}], \"AttributeDefinitions\": [{}], \"BillingMode\": \"PAY_PER_REQUEST\" }}",
        json_string(&cmd.table),
        key_schema.join(", "),
        attr_defs.join(", ")
    )
}

fn consistent_read_value(value: &Value) -> Result<bool, String> {
    match value {
        Value::Bool(value) => Ok(*value),
        Value::String(value) => match value.to_ascii_uppercase().as_str() {
            "STRONG" | "TRUE" => Ok(true),
            "EVENTUAL" | "FALSE" => Ok(false),
            _ => Err("DynamoDB consistency must be STRONG, EVENTUAL, true, or false".to_string()),
        },
        other => Err(format!(
            "DynamoDB consistency must be a bool or string, got {other}"
        )),
    }
}

fn value_to_dynamo(v: &Value) -> Result<String, String> {
    match v {
        Value::String(s) => Ok(format!("{{ \"S\": {} }}", json_string(s))),
        Value::Int(n) => Ok(format!("{{ \"N\": \"{}\" }}", n)),
        Value::Float(n) if n.is_finite() => Ok(format!("{{ \"N\": \"{}\" }}", n)),
        Value::Float(_) => {
            Err("non-finite floats cannot be encoded as DynamoDB numbers".to_string())
        }
        Value::Bool(b) => Ok(format!("{{ \"BOOL\": {} }}", b)),
        Value::Null | Value::NullUuid => Ok("{ \"NULL\": true }".to_string()),
        Value::Uuid(uuid) => Ok(format!("{{ \"S\": {} }}", json_string(&uuid.to_string()))),
        Value::Timestamp(ts) => Ok(format!("{{ \"S\": {} }}", json_string(ts))),
        Value::Array(values) => {
            let values: Result<Vec<String>, String> = values.iter().map(value_to_dynamo).collect();
            Ok(format!("{{ \"L\": [{}] }}", values?.join(", ")))
        }
        Value::Vector(values) => {
            let values: Result<Vec<String>, String> = values
                .iter()
                .map(|value| {
                    if value.is_finite() {
                        Ok(format!("{{ \"N\": \"{}\" }}", value))
                    } else {
                        Err(
                            "non-finite vector values cannot be encoded as DynamoDB numbers"
                                .to_string(),
                        )
                    }
                })
                .collect();
            Ok(format!("{{ \"L\": [{}] }}", values?.join(", ")))
        }
        Value::Json(json) => serde_json::from_str::<serde_json::Value>(json)
            .map_err(|err| format!("invalid JSON value for DynamoDB attribute: {err}"))
            .and_then(|value| json_value_to_dynamo(&value)),
        other => Err(format!("unsupported DynamoDB attribute value: {other}")),
    }
}

fn json_value_to_dynamo(value: &serde_json::Value) -> Result<String, String> {
    match value {
        serde_json::Value::Null => Ok("{ \"NULL\": true }".to_string()),
        serde_json::Value::Bool(value) => Ok(format!("{{ \"BOOL\": {} }}", value)),
        serde_json::Value::Number(value) => {
            Ok(format!("{{ \"N\": {} }}", json_string(&value.to_string())))
        }
        serde_json::Value::String(value) => Ok(format!("{{ \"S\": {} }}", json_string(value))),
        serde_json::Value::Array(values) => {
            let values: Result<Vec<String>, String> =
                values.iter().map(json_value_to_dynamo).collect();
            Ok(format!("{{ \"L\": [{}] }}", values?.join(", ")))
        }
        serde_json::Value::Object(values) => {
            let values: Result<Vec<String>, String> = values
                .iter()
                .map(|(key, value)| {
                    Ok(format!(
                        "{}: {}",
                        json_string(key),
                        json_value_to_dynamo(value)?
                    ))
                })
                .collect();
            Ok(format!("{{ \"M\": {{ {} }} }}", values?.join(", ")))
        }
    }
}
