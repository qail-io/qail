use crate::parser::parse;
use crate::transpiler::nosql::dynamo::ToDynamo;
use crate::transpiler::nosql::mongo::ToMongo;
use crate::transpiler::nosql::qdrant::ToQdrant;

#[test]
fn test_mongo_output() {
    let cmd = parse("get users fields name, age where active = true and age > 20 limit 5").unwrap();

    let mongo = cmd.to_mongo();
    assert!(mongo.contains("db.users.find("));
    assert!(mongo.contains("\"active\": true"));
    assert!(mongo.contains("\"age\": { \"$gt\": 20 }"));
    assert!(mongo.contains(".limit(5)"));
}

#[test]
fn test_mongo_insert() {
    use crate::ast::*;
    // For INSERT, use manual construction since v2 ADD syntax isn't fully implemented
    let mut cmd = Qail::add("users");
    cmd.cages.push(Cage {
        kind: CageKind::Payload,
        conditions: vec![
            Condition {
                left: Expr::Named("name".to_string()),
                op: Operator::Eq,
                value: Value::String("John".to_string()),
                is_array_unnest: false,
            },
            Condition {
                left: Expr::Named("age".to_string()),
                op: Operator::Eq,
                value: Value::Int(30),
                is_array_unnest: false,
            },
        ],
        logical_op: LogicalOp::And,
    });
    let mongo = cmd.to_mongo();
    assert!(mongo.contains("db.users.insertOne("));
    assert!(mongo.contains("\"name\": \"John\""));
}

// Mongo join using manual Qail construction
#[test]
fn test_mongo_join() {
    use crate::ast::*;
    let mut cmd = Qail::get("users");
    cmd.columns.push(Expr::Named("name".to_string()));
    cmd.columns.push(Expr::Named("email".to_string()));
    cmd.joins.push(Join {
        table: "orders".to_string(),
        kind: JoinKind::Left,
        on: None,
        on_true: false,
    });

    let mongo = cmd.to_mongo();
    println!("Mongo $lookup: {}", mongo);
    assert!(mongo.contains("$lookup"));
    assert!(mongo.contains("orders"));
}

#[test]
fn test_dynamo_output() {
    let cmd = parse("get users fields name, age where active = true and age > 20").unwrap();
    let dynamo = cmd.to_dynamo();

    assert!(dynamo.contains("\"TableName\": \"users\""));
    assert!(dynamo.contains("active = :v"));
    assert!(dynamo.contains("ProjectionExpression"));
}

#[test]
fn test_dynamo_gsi() {
    use crate::ast::*;
    // Use manual construction for meta params like index/consistency
    let mut cmd = Qail::get("users");
    cmd.columns.push(Expr::Named("email".to_string()));
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![
            Condition {
                left: Expr::Named("index".to_string()),
                op: Operator::Eq,
                value: Value::String("email_gsi".to_string()),
                is_array_unnest: false,
            },
            Condition {
                left: Expr::Named("consistency".to_string()),
                op: Operator::Eq,
                value: Value::String("strong".to_string()),
                is_array_unnest: false,
            },
        ],
        logical_op: LogicalOp::And,
    });
    let dynamo = cmd.to_dynamo();
    println!("Dynamo GSI: {}", dynamo);
    assert!(dynamo.contains("email_gsi") || dynamo.contains("IndexName"));
}

#[test]
fn test_qdrant_search() {
    use crate::ast::*;
    // Qdrant with vector search uses special syntax, use manual construction
    let mut cmd = Qail::get("points");
    cmd.columns.push(Expr::Named("id".to_string()));
    cmd.columns.push(Expr::Named("score".to_string()));
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![
            Condition {
                left: Expr::Named("vector".to_string()),
                op: Operator::Fuzzy,
                value: Value::String("cute cat".to_string()),
                is_array_unnest: false,
            },
            Condition {
                left: Expr::Named("city".to_string()),
                op: Operator::Eq,
                value: Value::String("London".to_string()),
                is_array_unnest: false,
            },
        ],
        logical_op: LogicalOp::And,
    });
    cmd.cages.push(Cage {
        kind: CageKind::Limit(10),
        conditions: vec![],
        logical_op: LogicalOp::And,
    });
    let qdrant = cmd.to_qdrant_search();

    assert!(qdrant.contains("{{EMBED:cute cat}}"));
    assert!(qdrant.contains("\"filter\": { \"must\": ["));
    assert!(qdrant.contains("\"key\": \"city\", \"match\": { \"value\": \"London\" }"));
}
