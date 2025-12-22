//! NoSQL transpiler tests (MongoDB, DynamoDB, Redis, Cassandra, Elasticsearch, Neo4j, Qdrant).

use crate::parser::parse;
use crate::transpiler::nosql::mongo::ToMongo;
use crate::transpiler::nosql::dynamo::ToDynamo;
use crate::transpiler::nosql::cassandra::ToCassandra;
use crate::transpiler::nosql::redis::ToRedis;
use crate::transpiler::nosql::elastic::ToElastic;
use crate::transpiler::nosql::neo4j::ToNeo4j;
use crate::transpiler::nosql::qdrant::ToQdrant;

#[test]
fn test_mongo_output() {
    let cmd = parse("get::users:'name'age[active=true][age>20][lim=5]").unwrap();
    
    let mongo = cmd.to_mongo();
    assert!(mongo.contains("db.users.find("));
    assert!(mongo.contains("\"active\": true"));
    assert!(mongo.contains("\"age\": { \"$gt\": 20 }"));
    assert!(mongo.contains(".limit(5)"));
}

#[test]
fn test_mongo_insert() {
    let cmd = parse("add::users:[name=\"John\"][age=30]").unwrap();
    let mongo = cmd.to_mongo();
    assert!(mongo.contains("db.users.insertOne("));
    assert!(mongo.contains("\"name\": \"John\""));
}

// Mongo join using manual QailCmd construction
#[test]
fn test_mongo_join() {
    use crate::ast::*;
    let mut cmd = QailCmd::get("users");
    cmd.columns.push(Column::Named("name".to_string()));
    cmd.columns.push(Column::Named("email".to_string()));
    cmd.joins.push(Join { table: "orders".to_string(), kind: JoinKind::Left, on: None });
    
    let mongo = cmd.to_mongo();
    println!("Mongo $lookup: {}", mongo);
    assert!(mongo.contains("$lookup"));
    assert!(mongo.contains("orders"));
}

#[test]
fn test_dynamo_output() {
    let cmd = parse("get::users:'name'age[active=true][age>20]").unwrap();
    let dynamo = cmd.to_dynamo();
    
    assert!(dynamo.contains("\"TableName\": \"users\""));
    assert!(dynamo.contains("active = :v")); 
    assert!(dynamo.contains("ProjectionExpression"));
}

#[test]
fn test_dynamo_gsi() {
    let cmd = parse("get::users:'email[index=email_gsi][consistency=strong]").unwrap();
    let dynamo = cmd.to_dynamo();
    println!("Dynamo GSI: {}", dynamo);
    assert!(dynamo.contains("email_gsi") || dynamo.contains("IndexName"));
}

#[test]
fn test_cassandra_output() {
    let cmd = parse("get::users:'name'age[active=true][age>20][lim=5]").unwrap();
    let cql = cmd.to_cassandra();
    
    assert!(cql.contains("SELECT name, age FROM users"));
    assert!(cql.contains("active = true"));
    assert!(cql.contains("age > 20"));
    assert!(cql.contains("LIMIT 5"));
    assert!(cql.contains("ALLOW FILTERING"));
}

#[test]
fn test_cassandra_consistency() {
    let cmd = parse("get::users:'_[consistency=quorum]").unwrap();
    let cql = cmd.to_cassandra();
    println!("Cassandra CQL: {}", cql);
    assert!(cql.contains("CONSISTENCY QUORUM") || cql.contains("quorum"));
}

#[test]
fn test_redis_search() {
    let cmd = parse("get::users:'name'age[active=true][age>20][lim=5]").unwrap();
    let redis = cmd.to_redis_search();
    
    assert!(redis.contains("FT.SEARCH idx:users"));
    assert!(redis.contains("@active:true"));
    assert!(redis.contains("@age:["));
    assert!(redis.contains("LIMIT 0 5"));
    assert!(redis.contains("RETURN 2 name age"));
}

#[test]
fn test_redis_complex_operators() {
    let cmd = parse("get::users:'_[role!=\"admin\"][name~\"john\"][age<=30]").unwrap();
    let redis = cmd.to_redis_search();
    
    assert!(redis.contains("-(@role:admin)"));
    assert!(redis.contains("@name:%john%"));
    assert!(redis.contains("@age:[-inf 30]"));
}

#[test]
fn test_elastic_dsl() {
    let cmd = parse("get::logs:'message'level[level=\"error\"][count>10][lim=50]").unwrap();
    let elastic = cmd.to_elastic();
    
    assert!(elastic.contains("\"query\": { \"bool\": { \"must\": ["));
    assert!(elastic.contains("\"term\": { \"level\": \"error\" }"));
    assert!(elastic.contains("\"range\": { \"count\": { \"gt\": 10 } }"));
    assert!(elastic.contains("\"size\": 50"));
}

#[test]
fn test_neo4j_cypher() {
    let cmd = parse("get::users:'name'age[active=true][age>20][lim=5]").unwrap();
    let cypher = cmd.to_cypher();
    
    assert!(cypher.contains("MATCH (n:users)"));
    assert!(cypher.contains("WHERE n.active = true AND n.age > 20"));
    assert!(cypher.contains("RETURN n.name, n.age"));
    assert!(cypher.contains("LIMIT 5"));
}

#[test]
fn test_qdrant_search() {
    let cmd = parse("get::points:'id'score[vector~\"cute cat\"][city=\"London\"][lim=10]").unwrap();
    let qdrant = cmd.to_qdrant_search();
    
    assert!(qdrant.contains("{{EMBED:cute cat}}"));
    assert!(qdrant.contains("\"filter\": { \"must\": ["));
    assert!(qdrant.contains("\"key\": \"city\", \"match\": { \"value\": \"London\" }"));
}
