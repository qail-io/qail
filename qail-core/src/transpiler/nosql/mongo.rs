use crate::ast::*;

pub trait ToMongo {
    fn to_mongo(&self) -> String;
}

impl ToMongo for QailCmd {
    fn to_mongo(&self) -> String {
        match self.action {
            Action::Get => {
                if !self.joins.is_empty() {
                    build_aggregate(self)
                } else {
                    build_find(self)
                }
            },
            Action::Set => build_update(self),
            Action::Add => build_insert(self),
            Action::Put => build_upsert(self),
            Action::Del => build_delete(self),
            Action::Make => format!("db.createCollection(\"{}\")", self.table),
            Action::Drop => format!("db.{}.drop()", self.table),
            Action::TxnStart => "session.startTransaction()".to_string(),
            Action::TxnCommit => "session.commitTransaction()".to_string(),
            Action::TxnRollback => "session.abortTransaction()".to_string(),
            _ => format!("// Action {:?} not supported for MongoDB yet", self.action),
        }
    }
}


fn build_aggregate(cmd: &QailCmd) -> String {
    let mut stages = Vec::new();
    
    // 1. $match
    let filter = build_query_filter(cmd);
    if filter != "{}" {
        stages.push(format!("{{ \"$match\": {} }}", filter));
    }
    
    // 2. $lookup
    for join in &cmd.joins {
         let target = &join.table;
         let source_singular = cmd.table.trim_end_matches('s');
         let pk = format!("{}_id", source_singular); // users -> user_id
         
         // from: orders, localField: _id, foreignField: user_id, as: orders
         let lookup = format!("{{ \"$lookup\": {{ \"from\": \"{}\", \"localField\": \"_id\", \"foreignField\": \"{}\", \"as\": \"{}\" }} }}",
             target, pk, target);
         stages.push(lookup);
    }
    
    // 3. $project & Add Fields logic if needed? 
    // For now simple projection if columns exist
    let proj = build_projection(cmd);
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
                     stages.push(format!("{{ \"$sort\": {{ \"{}\": {} }} }}", cond.column, val));
                 }
             },
             CageKind::Offset(n) => stages.push(format!("{{ \"$skip\": {} }}", n)),
             CageKind::Limit(n) => stages.push(format!("{{ \"$limit\": {} }}", n)),
             _ => {}
        }
    }
    
    format!("db.{}.aggregate([{}])", cmd.table, stages.join(", "))
}

fn build_find(cmd: &QailCmd) -> String {
    let query = build_query_filter(cmd);
    let projection = build_projection(cmd);
    
    // Base: db.collection.find(query, projection)
    let mut mongo = format!("db.{}.find({}, {})", cmd.table, query, projection);

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
                 // Extract sort field from condition.
                 if let Some(cond) = cage.conditions.first() {
                     mongo.push_str(&format!(".sort({{ \"{}\": {} }})", cond.column, val));
                 }
            },
            _ => {}
        }
    }
    
    mongo
}

fn build_update(cmd: &QailCmd) -> String {
    let query = build_query_filter(cmd);
    // Payload logic for $set would go here
    let mut update_doc = String::from("{ $set: { ");
    let mut first = true;
    
    for cage in &cmd.cages {
        // In current parser, [key=val] updates come as Filter cages
        match cage.kind {
             CageKind::Payload | CageKind::Filter => {
                for cond in &cage.conditions {
                     if !first { update_doc.push_str(", "); }
                     update_doc.push_str(&format!("\"{}\": {}", cond.column, value_to_json(&cond.value)));
                     first = false;
                }
            },
            _ => {}
        }
    }
    update_doc.push_str(" } }");

    format!("db.{}.updateMany({}, {})", cmd.table, query, update_doc)
}

fn build_insert(cmd: &QailCmd) -> String {
    let mut doc = String::from("{ ");
    let mut first = true;
    
    // Assuming cages contain the payload for insert
    for cage in &cmd.cages {
        // In current parser, [key=val] inserts come as Filter cages
         match cage.kind {
            CageKind::Payload | CageKind::Filter => {
                for cond in &cage.conditions {
                     if !first { doc.push_str(", "); }
                     doc.push_str(&format!("\"{}\": {}", cond.column, value_to_json(&cond.value)));
                     first = false;
                }
            },
            _ => {}
        }
    }
    doc.push_str(" }");
    
    format!("db.{}.insertOne({})", cmd.table, doc)
}

fn build_upsert(cmd: &QailCmd) -> String {
    // Similar to update but with upsert: true
    let query = build_query_filter(cmd);
    
    // Payload logic for $set
    let mut update_doc = String::from("{ $set: { ");
    let mut first = true;
    
    for cage in &cmd.cages {
        match cage.kind {
             CageKind::Payload | CageKind::Filter => {
                for cond in &cage.conditions {
                     if !first { update_doc.push_str(", "); }
                     update_doc.push_str(&format!("\"{}\": {}", cond.column, value_to_json(&cond.value)));
                     first = false;
                }
            },
            _ => {}
        }
    }
    update_doc.push_str(" } }");

    format!("db.{}.updateOne({}, {}, {{ \"upsert\": true }})", cmd.table, query, update_doc)
}

fn build_delete(cmd: &QailCmd) -> String {
    let query = build_query_filter(cmd);
    format!("db.{}.deleteMany({})", cmd.table, query)
}

fn build_query_filter(cmd: &QailCmd) -> String {
    let mut query_parts = Vec::new();

    for cage in &cmd.cages {
        if let CageKind::Filter = cage.kind {
             for cond in &cage.conditions {
                 let op = match cond.op {
                     Operator::Eq => "$eq",
                     Operator::Ne => "$ne",
                     Operator::Gt => "$gt",
                     Operator::Lt => "$lt",
                     Operator::Gte => "$gte",
                     Operator::Lte => "$lte",
                     _ => "$eq" // Fallback
                 };
                 
                 // If simple equality, clean syntax { key: val }
                 if let Operator::Eq = cond.op {
                      query_parts.push(format!("\"{}\": {}", cond.column, value_to_json(&cond.value)));
                 } else {
                      query_parts.push(format!("\"{}\": {{ \"{}\": {} }}", cond.column, op, value_to_json(&cond.value)));
                 }
             }
        }
    }

    if query_parts.is_empty() {
        return "{}".to_string();
    }
    
    format!("{{ {} }}", query_parts.join(", "))
}

fn build_projection(cmd: &QailCmd) -> String {
    if cmd.columns.is_empty() {
        return "{}".to_string();
    }
    
    let mut proj = String::from("{ ");
    for (i, col) in cmd.columns.iter().enumerate() {
        if i > 0 { proj.push_str(", "); }
        if let Column::Named(name) = col {
            proj.push_str(&format!("\"{}\": 1", name));
        }
    }
    proj.push_str(" }");
    proj
}

fn value_to_json(v: &Value) -> String {
    match v {
        Value::String(s) => format!("\"{}\"", s),
        Value::Int(n) => n.to_string(),
        Value::Float(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        Value::Param(i) => format!("\"$param{}\"", i),
        _ => "\"unknown\"".to_string(),
    }
}
