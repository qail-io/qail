//! Integration tests for QAIL with real SQLite database.
//!
//! These tests verify the complete flow from QAIL parsing to SQL execution.

use qail_core::{parse, transpiler::ToSql};
use sqlx::sqlite::SqlitePool;

/// Setup test database with sample data
async fn setup_test_db() -> SqlitePool {
    let pool = SqlitePool::connect(":memory:").await.unwrap();
    
    // Create tables
    sqlx::query(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            role TEXT DEFAULT 'user',
            active INTEGER DEFAULT 1
        )"
    )
    .execute(&pool)
    .await
    .unwrap();
    
    sqlx::query(
        "CREATE TABLE profiles (
            id INTEGER PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            avatar TEXT,
            bio TEXT
        )"
    )
    .execute(&pool)
    .await
    .unwrap();
    
    sqlx::query(
        "CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            total REAL,
            status TEXT
        )"
    )
    .execute(&pool)
    .await
    .unwrap();
    
    // Insert sample data
    sqlx::query("INSERT INTO users (name, email, role, active) VALUES ('Alice', 'alice@example.com', 'admin', 1)")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO users (name, email, role, active) VALUES ('Bob', 'bob@example.com', 'user', 1)")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO users (name, email, role, active) VALUES ('Charlie', 'charlie@example.com', 'user', 0)")
        .execute(&pool).await.unwrap();
    
    sqlx::query("INSERT INTO profiles (user_id, avatar, bio) VALUES (1, 'alice.jpg', 'Admin user')")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO profiles (user_id, avatar, bio) VALUES (2, 'bob.jpg', 'Regular user')")
        .execute(&pool).await.unwrap();
    
    sqlx::query("INSERT INTO orders (user_id, total, status) VALUES (1, 100.50, 'completed')")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO orders (user_id, total, status) VALUES (1, 200.00, 'pending')")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO orders (user_id, total, status) VALUES (2, 50.00, 'completed')")
        .execute(&pool).await.unwrap();
    
    pool
}

#[tokio::test]
async fn test_simple_select() {
    let pool = setup_test_db().await;
    
    let cmd = parse("get::users•@*").unwrap();
    let sql = cmd.to_sql();
    
    let rows: Vec<(i64, String, String)> = sqlx::query_as(&sql)
        .fetch_all(&pool)
        .await
        .unwrap();
    
    assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn test_select_columns() {
    let pool = setup_test_db().await;
    
    let cmd = parse("get::users•@name@email").unwrap();
    let sql = cmd.to_sql();
    
    let rows: Vec<(String, String)> = sqlx::query_as(&sql)
        .fetch_all(&pool)
        .await
        .unwrap();
    
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, "Alice");
}

#[tokio::test]
async fn test_filtered_query() {
    let pool = setup_test_db().await;
    
    let cmd = parse("get::users•@name[active=1]").unwrap();
    let sql = cmd.to_sql();
    
    let rows: Vec<(String,)> = sqlx::query_as(&sql)
        .fetch_all(&pool)
        .await
        .unwrap();
    
    assert_eq!(rows.len(), 2); // Alice and Bob are active
}

#[tokio::test]
async fn test_limit() {
    let pool = setup_test_db().await;
    
    let cmd = parse("get::users•@name[lim=2]").unwrap();
    let sql = cmd.to_sql();
    
    let rows: Vec<(String,)> = sqlx::query_as(&sql)
        .fetch_all(&pool)
        .await
        .unwrap();
    
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_offset_pagination() {
    let pool = setup_test_db().await;
    
    let cmd = parse("get::users•@name[lim=2][off=1]").unwrap();
    let sql = cmd.to_sql();
    
    let rows: Vec<(String,)> = sqlx::query_as(&sql)
        .fetch_all(&pool)
        .await
        .unwrap();
    
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, "Bob"); // Skipped Alice
}

#[tokio::test]
async fn test_order_by_asc() {
    let pool = setup_test_db().await;
    
    let cmd = parse("get::users•@name[^name]").unwrap();
    let sql = cmd.to_sql();
    
    let rows: Vec<(String,)> = sqlx::query_as(&sql)
        .fetch_all(&pool)
        .await
        .unwrap();
    
    assert_eq!(rows[0].0, "Alice");
    assert_eq!(rows[1].0, "Bob");
    assert_eq!(rows[2].0, "Charlie");
}

#[tokio::test]
async fn test_order_by_desc() {
    let pool = setup_test_db().await;
    
    let cmd = parse("get::users•@name[^!name]").unwrap();
    let sql = cmd.to_sql();
    
    let rows: Vec<(String,)> = sqlx::query_as(&sql)
        .fetch_all(&pool)
        .await
        .unwrap();
    
    assert_eq!(rows[0].0, "Charlie");
    assert_eq!(rows[2].0, "Alice");
}

#[tokio::test]
async fn test_distinct() {
    let pool = setup_test_db().await;
    
    let cmd = parse("get!::users•@role").unwrap();
    let sql = cmd.to_sql();
    
    let rows: Vec<(String,)> = sqlx::query_as(&sql)
        .fetch_all(&pool)
        .await
        .unwrap();
    
    assert_eq!(rows.len(), 2); // 'admin' and 'user' (distinct)
}

#[tokio::test]
async fn test_aggregate_count() {
    let pool = setup_test_db().await;
    
    let cmd = parse("get::users•@id#count").unwrap();
    let sql = cmd.to_sql();
    
    let row: (i64,) = sqlx::query_as(&sql)
        .fetch_one(&pool)
        .await
        .unwrap();
    
    assert_eq!(row.0, 3);
}

#[tokio::test]
async fn test_aggregate_sum() {
    let pool = setup_test_db().await;
    
    let cmd = parse("get::orders•@total#sum[status=completed]").unwrap();
    let sql = cmd.to_sql();
    
    let row: (f64,) = sqlx::query_as(&sql)
        .fetch_one(&pool)
        .await
        .unwrap();
    
    assert!((row.0 - 150.50).abs() < 0.01); // 100.50 + 50.00
}

#[tokio::test]
async fn test_inner_join() {
    let pool = setup_test_db().await;
    
    let cmd = parse("get::users->profiles•@name").unwrap();
    let sql = cmd.to_sql();
    
    // Should only return users with profiles (Alice and Bob)
    let rows: Vec<(String,)> = sqlx::query_as(&sql)
        .fetch_all(&pool)
        .await
        .unwrap();
    
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_left_join() {
    let pool = setup_test_db().await;
    
    let cmd = parse("get::users<-profiles•@name").unwrap();
    let sql = cmd.to_sql();
    
    // Should return all users (LEFT JOIN includes users without profiles)
    let rows: Vec<(String,)> = sqlx::query_as(&sql)
        .fetch_all(&pool)
        .await
        .unwrap();
    
    assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn test_complex_query() {
    let pool = setup_test_db().await;
    
    let cmd = parse("get::users•@name@role[active=1][^!name][lim=1]").unwrap();
    let sql = cmd.to_sql();
    
    let rows: Vec<(String, String)> = sqlx::query_as(&sql)
        .fetch_all(&pool)
        .await
        .unwrap();
    
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "Bob"); // Active, sorted by name DESC, limited to 1
}
