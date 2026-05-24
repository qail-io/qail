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
        match self.action {
            Action::Get => build_get_item(self),
            Action::Add | Action::Put => build_put_item(self),
            Action::Set => build_update_item(self),
            Action::Del => build_delete_item(self),
            Action::Make => build_create_table(self),
            Action::Drop => format!("{{ \"TableName\": {} }}", json_string(&self.table)), // DeleteTable input
            _ => format!(
                "{{ \"error\": {} }}",
                json_string(&format!("Action {:?} not supported", self.action))
            ),
        }
    }
}

fn build_get_item(cmd: &Qail) -> String {
    let mut parts = Vec::new();
    parts.push(format!("\"TableName\": {}", json_string(&cmd.table)));

    let mut filter = build_expression(cmd);
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
                                _ => cond.value.to_string().replace("'", ""),
                            };
                            parts.push(format!("\"IndexName\": {}", json_string(&index_name)));
                        }
                        "consistency" | "consistent" => {
                            // STRONG -> true. EVENTUAL -> false.
                            let val = cond.value.to_string().to_uppercase();
                            if val.contains("STRONG") || val.contains("TRUE") {
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

    format!("{{ {} }}", parts.join(", "))
}

fn build_put_item(cmd: &Qail) -> String {
    let mut parts = Vec::new();
    parts.push(format!("\"TableName\": {}", json_string(&cmd.table)));

    let item = build_item_json(cmd);
    parts.push(format!("\"Item\": {{ {} }}", item));

    format!("{{ {} }}", parts.join(", "))
}

fn build_update_item(cmd: &Qail) -> String {
    let mut parts = Vec::new();
    parts.push(format!("\"TableName\": {}", json_string(&cmd.table)));

    let key = build_key_from_filter(cmd);
    parts.push(format!("\"Key\": {{ {} }}", key));

    let update = build_update_expression(cmd);
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

    format!("{{ {} }}", parts.join(", "))
}

fn build_delete_item(cmd: &Qail) -> String {
    let mut parts = Vec::new();
    parts.push(format!("\"TableName\": {}", json_string(&cmd.table)));

    // Key logic
    let key = build_key_from_filter(cmd);
    parts.push(format!("\"Key\": {{ {} }}", key));

    format!("{{ {} }}", parts.join(", "))
}

fn build_expression(cmd: &Qail) -> DynamoExpression {
    let mut expr_parts = Vec::new();
    let mut values_parts = Vec::new();
    let mut names = Vec::new();
    let mut counter = 0;

    for cage in &cmd.cages {
        if let CageKind::Filter = cage.kind {
            for cond in &cage.conditions {
                let col_name = match &cond.left {
                    Expr::Named(name) => name.clone(),
                    expr => expr.to_string(),
                };

                if matches!(
                    col_name.as_str(),
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
                    _ => "=",
                };

                expr_parts.push(format!("{} {} {}", name_placeholder, op, placeholder));
                names.push((name_placeholder, col_name));

                let val_json = value_to_dynamo(&cond.value);
                values_parts.push(format!("{}: {}", json_string(&placeholder), val_json));
            }
        }
    }

    DynamoExpression {
        expression: expr_parts.join(" AND "),
        values: values_parts.join(", "),
        names,
    }
}

fn build_item_json(cmd: &Qail) -> String {
    let mut parts = Vec::new();
    for cage in &cmd.cages {
        match cage.kind {
            CageKind::Payload | CageKind::Filter => {
                for cond in &cage.conditions {
                    let val = value_to_dynamo(&cond.value);
                    let col_str = match &cond.left {
                        Expr::Named(name) => name.clone(),
                        expr => expr.to_string(),
                    };
                    parts.push(format!("{}: {}", json_string(&col_str), val));
                }
            }
            _ => {}
        }
    }
    parts.join(", ")
}

fn build_key_from_filter(cmd: &Qail) -> String {
    for cage in &cmd.cages {
        if let CageKind::Filter = cage.kind
            && let Some(cond) = cage.conditions.first()
        {
            let val = value_to_dynamo(&cond.value);
            let col_str = match &cond.left {
                Expr::Named(name) => name.clone(),
                expr => expr.to_string(),
            };
            return format!("{}: {}", json_string(&col_str), val);
        }
    }
    "\"pk\": { \"S\": \"unknown\" }".to_string()
}

fn build_update_expression(cmd: &Qail) -> DynamoExpression {
    let mut sets = Vec::new();
    let mut vals = Vec::new();
    let mut names = Vec::new();
    let mut counter = 100; // Offset to avoid collision with filters

    for cage in &cmd.cages {
        if let CageKind::Payload = cage.kind {
            for cond in &cage.conditions {
                counter += 1;
                let placeholder = format!(":u{}", counter);
                let col_str = match &cond.left {
                    Expr::Named(name) => name.clone(),
                    expr => expr.to_string(),
                };
                let name_placeholder = format!("#u{}", counter);
                sets.push(format!("{} = {}", name_placeholder, placeholder));
                names.push((name_placeholder, col_str));

                let val = value_to_dynamo(&cond.value);
                vals.push(format!("{}: {}", json_string(&placeholder), val));
            }
        }
    }

    DynamoExpression {
        expression: format!("SET {}", sets.join(", ")),
        values: vals.join(", "),
        names,
    }
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

fn value_to_dynamo(v: &Value) -> String {
    match v {
        Value::String(s) => format!("{{ \"S\": {} }}", json_string(s)),
        Value::Int(n) => format!("{{ \"N\": \"{}\" }}", n),
        Value::Float(n) => format!("{{ \"N\": \"{}\" }}", n),
        Value::Bool(b) => format!("{{ \"BOOL\": {} }}", b),
        Value::Null => "{ \"NULL\": true }".to_string(),
        _ => "{ \"S\": \"unknown\" }".to_string(),
    }
}
