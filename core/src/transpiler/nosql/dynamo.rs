use crate::ast::*;

pub trait ToDynamo {
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
            Action::Drop => format!("{{ \"TableName\": \"{}\" }}", self.table), // DeleteTable input
            _ => format!(
                "{{ \"error\": \"Action {:?} not supported\" }}",
                self.action
            ),
        }
    }
}

fn build_get_item(cmd: &Qail) -> String {

    let mut parts = Vec::new();
    parts.push(format!("\"TableName\": \"{}\"", cmd.table));

    let filter = build_expression(cmd);
    if !filter.0.is_empty() {
        parts.push(format!("\"FilterExpression\": \"{}\"", filter.0));
        parts.push(format!("\"ExpressionAttributeValues\": {{ {} }}", filter.1));
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
                            parts.push(format!("\"IndexName\": \"{}\"", index_name));
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
        let cols: Vec<String> = cmd
            .columns
            .iter()
            .map(|c| match c {
                Expr::Named(n) => n.clone(),
                _ => "".to_string(),
            })
            .collect();
        parts.push(format!("\"ProjectionExpression\": \"{}\"", cols.join(", ")));
    }

    if let Some(n) = get_limit(cmd) {
        parts.push(format!("\"Limit\": {}", n))
    }

    format!("{{ {} }}", parts.join(", "))
}

fn build_put_item(cmd: &Qail) -> String {
    let mut parts = Vec::new();
    parts.push(format!("\"TableName\": \"{}\"", cmd.table));

    let item = build_item_json(cmd);
    parts.push(format!("\"Item\": {{ {} }}", item));

    format!("{{ {} }}", parts.join(", "))
}

fn build_update_item(cmd: &Qail) -> String {
    let mut parts = Vec::new();
    parts.push(format!("\"TableName\": \"{}\"", cmd.table));

    let key = build_key_from_filter(cmd);
    parts.push(format!("\"Key\": {{ {} }}", key));

    let update = build_update_expression(cmd);
    parts.push(format!("\"UpdateExpression\": \"{}\"", update.0));
    parts.push(format!("\"ExpressionAttributeValues\": {{ {} }}", update.1));

    format!("{{ {} }}", parts.join(", "))
}

fn build_delete_item(cmd: &Qail) -> String {
    let mut parts = Vec::new();
    parts.push(format!("\"TableName\": \"{}\"", cmd.table));

    // Key logic
    let key = build_key_from_filter(cmd);
    parts.push(format!("\"Key\": {{ {} }}", key));

    format!("{{ {} }}", parts.join(", "))
}

fn build_expression(cmd: &Qail) -> (String, String) {
    let mut expr_parts = Vec::new();
    let mut values_parts = Vec::new();
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
                let op = match cond.op {
                    Operator::Eq => "=",
                    Operator::Ne => "<>",
                    Operator::Gt => ">",
                    Operator::Lt => "<",
                    Operator::Gte => ">=",
                    Operator::Lte => "<=",
                    _ => "=",
                };

                expr_parts.push(format!("{} {} {}", col_name, op, placeholder));

                let val_json = value_to_dynamo(&cond.value);
                values_parts.push(format!("\"{}\": {}", placeholder, val_json));
            }
        }
    }

    (expr_parts.join(" AND "), values_parts.join(", "))
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
                    parts.push(format!("\"{}\": {}", col_str, val));
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
            return format!("\"{}\": {}", col_str, val);
        }
    }
    "\"pk\": { \"S\": \"unknown\" }".to_string()
}

fn build_update_expression(cmd: &Qail) -> (String, String) {
    let mut sets = Vec::new();
    let mut vals = Vec::new();
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
                sets.push(format!("{} = {}", col_str, placeholder));

                let val = value_to_dynamo(&cond.value);
                vals.push(format!("\"{}\": {}", placeholder, val));
            }
        }
    }

    (format!("SET {}", sets.join(", ")), vals.join(", "))
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
                "{{ \"AttributeName\": \"{}\", \"AttributeType\": \"{}\" }}",
                name, dtype
            ));
            key_schema.push(format!(
                "{{ \"AttributeName\": \"{}\", \"KeyType\": \"HASH\" }}",
                name
            ));
        }
    }

    if key_schema.is_empty() {
        attr_defs.push("{ \"AttributeName\": \"id\", \"AttributeType\": \"S\" }".to_string());
        key_schema.push("{ \"AttributeName\": \"id\", \"KeyType\": \"HASH\" }".to_string());
    }

    format!(
        "{{ \"TableName\": \"{}\", \"KeySchema\": [{}], \"AttributeDefinitions\": [{}], \"BillingMode\": \"PAY_PER_REQUEST\" }}",
        cmd.table,
        key_schema.join(", "),
        attr_defs.join(", ")
    )
}

fn value_to_dynamo(v: &Value) -> String {
    match v {
        Value::String(s) => format!("{{ \"S\": \"{}\" }}", s),
        Value::Int(n) => format!("{{ \"N\": \"{}\" }}", n),
        Value::Float(n) => format!("{{ \"N\": \"{}\" }}", n),
        Value::Bool(b) => format!("{{ \"BOOL\": {} }}", b),
        Value::Null => "{ \"NULL\": true }".to_string(),
        _ => "{ \"S\": \"unknown\" }".to_string(),
    }
}
