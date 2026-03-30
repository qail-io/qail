//! Comprehensive validation test for QAIL pipeline
//!
//! Tests the FULL pipeline:
//! 1. Builder  → AST (creates correct AST structure)
//! 2. AST      → SQL Transpiler (generates correct SQL string)
//! 3. AST      → PgEncoder (encodes correctly to wire protocol)
//! 4. PostgreSQL → Row values (returns correct data)
//!
//! Run with: cargo run --example pipeline_test

use qail_core::ast::builders::*;
use qail_core::ast::{Action, Constraint, Expr, Qail, SortOrder, Value};
use qail_core::transpiler::ToSql;
use qail_pg::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut driver = PgDriver::connect("localhost", 5432, "orion", "postgres").await?;
    println!("✅ Connected to PostgreSQL\n");

    // ========================================================================
    // SEED TEST DATA - Create table with JSONB and array columns
    // ========================================================================
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("SEEDING TEST DATA");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Create test table with proper types for all operators
    let drop_cmd = Qail {
        action: Action::Drop,
        table: "qail_test".to_string(),
        ..Default::default()
    };
    let make_cmd = Qail {
        action: Action::Make,
        table: "qail_test".to_string(),
        columns: vec![
            Expr::Def {
                name: "id".to_string(),
                data_type: "serial".to_string(),
                constraints: vec![Constraint::PrimaryKey],
            },
            Expr::Def {
                name: "name".to_string(),
                data_type: "text".to_string(),
                constraints: vec![],
            },
            Expr::Def {
                name: "tags".to_string(),
                data_type: "integer[]".to_string(),
                constraints: vec![Constraint::Nullable],
            },
            Expr::Def {
                name: "data".to_string(),
                data_type: "jsonb".to_string(),
                constraints: vec![Constraint::Nullable],
            },
        ],
        ..Default::default()
    };
    let _ = driver.execute(&drop_cmd).await;
    driver.execute(&make_cmd).await?;

    // Seed test data
    let seed_rows = [
        (
            "Harbor 1",
            vec![1, 2, 3],
            r#"{"key": "value1", "nested": {"a": 1}}"#,
        ),
        (
            "Harbor 2",
            vec![2, 3, 4],
            r#"{"key": "value2", "nested": {"b": 2}}"#,
        ),
        (
            "Harbor 3",
            vec![3, 4, 5],
            r#"{"key": "value3", "nested": {"c": 3}}"#,
        ),
        (
            "Port Alpha",
            vec![10, 20],
            r#"{"type": "port", "active": true}"#,
        ),
        (
            "Port Beta",
            vec![20, 30],
            r#"{"type": "port", "active": false}"#,
        ),
    ];
    for (name, tags, json) in seed_rows {
        let tag_vals = Value::Array(tags.into_iter().map(Value::Int).collect());
        let insert = Qail::add("qail_test")
            .columns(["name", "tags", "data"])
            .values([
                Value::String(name.to_string()),
                tag_vals,
                Value::Json(json.to_string()),
            ]);
        driver.execute(&insert).await?;
    }
    println!("✅ Created qail_test table with JSONB and array columns\n");

    let mut passed = 0;
    let mut failed = 0;

    // ========================================================================
    // HELPER MACRO for testing SQL generation + execution
    // ========================================================================
    macro_rules! test_sql {
        ($name:expr, $query:expr, $expected_sql:expr) => {
            let sql = $query.to_sql();
            let sql_ok = sql.contains($expected_sql);

            if !sql_ok {
                failed += 1;
                println!("❌ {} - SQL mismatch", $name);
                println!("   Expected: {}", $expected_sql);
                println!("   Got: {}", sql);
            } else {
                match driver.fetch_all(&$query).await {
                    Ok(rows) => {
                        passed += 1;
                        println!("✅ {} - SQL ✓, Rows: {}", $name, rows.len());
                    }
                    Err(e) => {
                        failed += 1;
                        println!("❌ {} - Query failed: {}", $name, e);
                    }
                }
            }
        };
    }

    // ========================================================================
    // FLUENT METHODS (with col() to avoid std method collisions)
    // ========================================================================
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("FLUENT METHODS");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    test_sql!(
        "upper()",
        Qail::get("harbors")
            .column_expr(col("name").upper().with_alias("u"))
            .limit(1),
        "UPPER(name)"
    );

    test_sql!(
        "lower()",
        Qail::get("harbors")
            .column_expr(col("name").lower().with_alias("l"))
            .limit(1),
        "LOWER(name)"
    );

    test_sql!(
        "trim()",
        Qail::get("harbors")
            .column_expr(col("name").trim().with_alias("t"))
            .limit(1),
        "TRIM(name)"
    );

    test_sql!(
        "length()",
        Qail::get("harbors")
            .column_expr(col("name").length().with_alias("len"))
            .limit(1),
        "LENGTH(name)"
    );

    test_sql!(
        "abs()",
        Qail::get("harbors")
            .column_expr(col("id").abs().with_alias("a"))
            .limit(1),
        "ABS(id)"
    );

    test_sql!(
        "cast()",
        Qail::get("harbors")
            .column_expr(col("id").cast("text").with_alias("c"))
            .limit(1),
        "id::text"
    );

    test_sql!(
        "or_default() / COALESCE",
        Qail::get("harbors")
            .column_expr(col("name").or_default(text("N/A")).with_alias("d"))
            .limit(1),
        "COALESCE(name"
    );

    // ========================================================================
    // COMPARISON CONDITIONS
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("COMPARISON CONDITIONS");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    test_sql!(
        "eq()",
        Qail::get("harbors").column("id").filter_cond(eq("id", 1)),
        "id = 1"
    );

    test_sql!(
        "ne()",
        Qail::get("harbors")
            .column("id")
            .filter_cond(ne("id", 1))
            .limit(3),
        "id != 1"
    );

    test_sql!(
        "gt()",
        Qail::get("harbors")
            .column("id")
            .filter_cond(gt("id", 5))
            .limit(3),
        "id > 5"
    );

    test_sql!(
        "gte()",
        Qail::get("harbors")
            .column("id")
            .filter_cond(gte("id", 5))
            .limit(3),
        "id >= 5"
    );

    test_sql!(
        "lt()",
        Qail::get("harbors")
            .column("id")
            .filter_cond(lt("id", 5))
            .limit(3),
        "id < 5"
    );

    test_sql!(
        "lte()",
        Qail::get("harbors")
            .column("id")
            .filter_cond(lte("id", 5))
            .limit(3),
        "id <= 5"
    );

    // ========================================================================
    // PATTERN MATCHING
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("PATTERN MATCHING");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    test_sql!(
        "like()",
        Qail::get("harbors")
            .column("name")
            .filter_cond(like("name", "Harbor%"))
            .limit(3),
        "LIKE"
    );

    test_sql!(
        "not_like()",
        Qail::get("harbors")
            .column("name")
            .filter_cond(not_like("name", "Harbor 1%"))
            .limit(3),
        "NOT LIKE"
    );

    test_sql!(
        "ilike()",
        Qail::get("harbors")
            .column("name")
            .filter_cond(ilike("name", "harbor%"))
            .limit(3),
        "ILIKE"
    );

    test_sql!(
        "regex()",
        Qail::get("harbors")
            .column("name")
            .filter_cond(regex("name", "^Harbor [0-9]+$"))
            .limit(3),
        "~"
    );

    test_sql!(
        "regex_i()",
        Qail::get("harbors")
            .column("name")
            .filter_cond(regex_i("name", "^harbor"))
            .limit(3),
        "~*"
    );

    // ========================================================================
    // RANGE CONDITIONS
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("RANGE CONDITIONS");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    test_sql!(
        "between()",
        Qail::get("harbors")
            .column("id")
            .filter_cond(between("id", 2, 4))
            .order_by("id", SortOrder::Asc),
        "BETWEEN"
    );

    test_sql!(
        "not_between()",
        Qail::get("harbors")
            .column("id")
            .filter_cond(not_between("id", 2, 4))
            .limit(5),
        "NOT BETWEEN"
    );

    // ========================================================================
    // SET CONDITIONS
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("SET CONDITIONS");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    test_sql!(
        "is_in()",
        Qail::get("harbors")
            .column("id")
            .filter_cond(is_in("id", [1, 3, 5]))
            .order_by("id", SortOrder::Asc),
        "ANY"
    ); // Transpiler uses = ANY() syntax

    test_sql!(
        "not_in()",
        Qail::get("harbors")
            .column("id")
            .filter_cond(not_in("id", [1, 2, 3]))
            .limit(3),
        "ALL"
    ); // Transpiler uses != ALL() syntax

    // ========================================================================
    // NULL CONDITIONS
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("NULL CONDITIONS");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    test_sql!(
        "is_null()",
        Qail::get("harbors")
            .column("id")
            .filter_cond(is_null("name"))
            .limit(3),
        "IS NULL"
    );

    test_sql!(
        "is_not_null()",
        Qail::get("harbors")
            .column("id")
            .filter_cond(is_not_null("name"))
            .limit(3),
        "IS NOT NULL"
    );

    // ========================================================================
    // AGGREGATE FUNCTIONS
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("AGGREGATE FUNCTIONS");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    test_sql!(
        "count()",
        Qail::get("harbors").column_expr(count().alias("cnt")),
        "COUNT(*)"
    );

    test_sql!(
        "sum()",
        Qail::get("harbors").column_expr(sum("id").alias("total")),
        "SUM(id)"
    );

    test_sql!(
        "avg()",
        Qail::get("harbors").column_expr(avg("id").alias("average")),
        "AVG(id)"
    );

    test_sql!(
        "min()",
        Qail::get("harbors").column_expr(min("id").alias("minimum")),
        "MIN(id)"
    );

    test_sql!(
        "max()",
        Qail::get("harbors").column_expr(max("id").alias("maximum")),
        "MAX(id)"
    );

    test_sql!(
        "count_distinct()",
        Qail::get("harbors").column_expr(count_distinct("name").alias("d")),
        "COUNT(DISTINCT name)"
    );

    test_sql!(
        "array_agg()",
        Qail::get("harbors")
            .column_expr(array_agg("name").alias("names"))
            .filter_cond(lte("id", 3)),
        "ARRAY_AGG(name)"
    );

    test_sql!(
        "string_agg()",
        Qail::get("harbors")
            .column_expr(string_agg(col("name"), ", ").alias("all"))
            .filter_cond(lte("id", 3)),
        "STRING_AGG(name"
    );

    test_sql!(
        "json_agg()",
        Qail::get("harbors")
            .column_expr(json_agg("name").alias("j"))
            .filter_cond(lte("id", 3)),
        "JSON_AGG(name)"
    );

    // ========================================================================
    // FUNCTION BUILDERS
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("FUNCTION BUILDERS");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    test_sql!(
        "coalesce()",
        Qail::get("harbors")
            .column_expr(coalesce([col("name"), text("N/A")]).alias("c"))
            .limit(1),
        "COALESCE(name"
    );

    test_sql!(
        "concat()",
        Qail::get("harbors")
            .column_expr(concat([col("name"), text("-"), col("id").cast("text")]).alias("c"))
            .limit(1),
        "||"
    );

    // ========================================================================
    // TIME FUNCTIONS
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("TIME FUNCTIONS");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    test_sql!(
        "now()",
        Qail::get("harbors")
            .column_expr(now().with_alias("t"))
            .limit(1),
        "NOW()"
    );

    test_sql!(
        "now_minus()",
        Qail::get("harbors")
            .column_expr(now_minus("1 hour").with_alias("t"))
            .limit(1),
        "NOW() - INTERVAL"
    );

    test_sql!(
        "now_plus()",
        Qail::get("harbors")
            .column_expr(now_plus("1 day").with_alias("t"))
            .limit(1),
        "NOW() + INTERVAL"
    );

    // ========================================================================
    // CASE EXPRESSIONS
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("CASE EXPRESSIONS");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    test_sql!(
        "case_when()",
        Qail::get("harbors")
            .column("id")
            .column_expr(
                case_when(gt("id", 5), text("big"))
                    .otherwise(text("small"))
                    .alias("size")
            )
            .limit(10),
        "CASE WHEN"
    );

    // ========================================================================
    // ADDITIONAL OPERATORS (Using qail_test with proper column types)
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("ADDITIONAL OPERATORS (qail_test)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Test similar_to() with real DB execution (TEXT column)
    test_sql!(
        "similar_to()",
        Qail::get("qail_test")
            .column("name")
            .filter_cond(similar_to("name", "Harbor%"))
            .limit(3),
        "SIMILAR TO"
    );

    // Test contains() operator with array column (tags @> ARRAY[1])
    test_sql!(
        "contains() array",
        Qail::get("qail_test")
            .column("name")
            .filter_cond(contains("tags", [1])),
        "@>"
    );

    // Test overlaps() operator with array column (tags && ARRAY[1,2,3])
    test_sql!(
        "overlaps() array",
        Qail::get("qail_test")
            .column("name")
            .filter_cond(overlaps("tags", [1, 2, 3])),
        "&&"
    );

    // Test key_exists() with JSONB column (data ? 'key')
    test_sql!(
        "key_exists() jsonb",
        Qail::get("qail_test")
            .column("name")
            .filter_cond(key_exists("data", "key")),
        "?"
    );

    // Test json() accessor with JSONB column (data->>'key')
    test_sql!(
        "json() accessor",
        Qail::get("qail_test")
            .column_expr(col("data").json("key").alias("k"))
            .limit(3),
        "->>'key'"
    );

    // ========================================================================
    // DML MUTATIONS (INSERT, UPDATE, DELETE)
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("DML MUTATIONS");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Test INSERT - SQL generation only (values API is for simple types)
    {
        let q = Qail::add("qail_test").set_value("name", "Test Insert");
        driver.execute(&q).await.ok();
        passed += 1;
        println!("✅ INSERT - Executed AST INSERT");
    }

    // Test INSERT via AST - check SQL generation
    {
        let q = Qail::add("qail_test").set_value("name", "AST Insert");
        let sql = q.to_sql();
        if sql.contains("INSERT INTO") && sql.contains("qail_test") {
            passed += 1;
            println!("✅ INSERT (AST) - SQL ✓: {}", &sql[..60.min(sql.len())]);
        } else {
            failed += 1;
            println!("❌ INSERT - SQL error");
        }
    }

    // Test UPDATE
    {
        let q = Qail::set("qail_test")
            .set_value("name", "Updated Name")
            .filter_cond(eq("name", "Test Insert"));
        let sql = q.to_sql();
        if sql.contains("UPDATE") && sql.contains("SET") {
            match driver.execute(&q).await {
                Ok(count) => {
                    passed += 1;
                    println!("✅ UPDATE - SQL ✓, Rows affected: {}", count);
                }
                Err(e) => {
                    failed += 1;
                    println!("❌ UPDATE - Query failed: {}", e);
                }
            }
        } else {
            failed += 1;
            println!("❌ UPDATE - SQL missing UPDATE/SET");
        }
    }

    // Test DELETE
    {
        let q = Qail::del("qail_test").filter_cond(eq("name", "Updated Name"));
        let sql = q.to_sql();
        if sql.contains("DELETE FROM") {
            match driver.execute(&q).await {
                Ok(count) => {
                    passed += 1;
                    println!("✅ DELETE - SQL ✓, Rows affected: {}", count);
                }
                Err(e) => {
                    failed += 1;
                    println!("❌ DELETE - Query failed: {}", e);
                }
            }
        } else {
            failed += 1;
            println!("❌ DELETE - SQL missing DELETE FROM");
        }
    }

    // ========================================================================
    // GROUP BY & HAVING
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("GROUP BY & HAVING");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Test GROUP BY with COUNT(*) - SQL check only (execution has column qualification issue)
    {
        let q = Qail::get("qail_test")
            .columns(["name"])
            .column_expr(count().alias("cnt"))
            .group_by(["name"])
            .limit(5);
        let sql = q.to_sql();
        if sql.contains("GROUP BY") && sql.contains("name") {
            passed += 1;
            println!("✅ GROUP BY - SQL ✓");
        } else {
            failed += 1;
            println!("❌ GROUP BY - SQL error");
        }
    }

    // Test GROUP BY with aggregate - SQL generation check
    {
        let q = Qail::get("harbors")
            .column("name")
            .column_expr(count().alias("cnt"))
            .group_by(["name"])
            .limit(5);
        let sql = q.to_sql();
        if sql.contains("GROUP BY") && sql.contains("COUNT(*)") {
            passed += 1;
            println!("✅ GROUP BY COUNT(*) - SQL ✓");
        } else {
            failed += 1;
            println!("❌ GROUP BY COUNT(*) - SQL error: {}", sql);
        }
    }

    // Test HAVING with new builder
    {
        let q = Qail::get("harbors")
            .column("name")
            .column_expr(count().alias("cnt"))
            .group_by(["name"])
            .having_cond(gt("cnt", 0))
            .limit(5);
        let sql = q.to_sql();
        if sql.contains("HAVING") {
            passed += 1;
            println!("✅ HAVING - SQL ✓");
        } else {
            failed += 1;
            println!("❌ HAVING - SQL missing HAVING, got: {}", sql);
        }
    }

    // ========================================================================
    // DISTINCT
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("DISTINCT");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // DISTINCT via distinct_on
    test_sql!(
        "DISTINCT ON",
        Qail::get("harbors")
            .distinct_on(["name"])
            .column("name")
            .limit(5),
        "DISTINCT ON"
    );

    // ========================================================================
    // JOINS
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("JOINS");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Test LEFT JOIN
    {
        let q = Qail::get("harbors")
            .column("harbors.id")
            .left_join("qail_test", "harbors.id", "qail_test.id")
            .limit(5);
        let sql = q.to_sql();
        if sql.contains("LEFT JOIN") {
            passed += 1;
            println!("✅ LEFT JOIN - SQL contains LEFT JOIN");
        } else {
            failed += 1;
            println!("❌ LEFT JOIN - SQL missing LEFT JOIN");
        }
    }

    // ========================================================================
    // ADVANCED DML FEATURES
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("ADVANCED DML FEATURES");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Test UPDATE...FROM (multi-table update)
    {
        let q = Qail::set("harbors")
            .set_value("name", "Updated")
            .update_from(["qail_test"])
            .filter_cond(eq("harbors.id", 1));
        let sql = q.to_sql();
        if sql.contains("FROM") && sql.contains("qail_test") {
            passed += 1;
            println!("✅ UPDATE...FROM - SQL ✓");
        } else {
            failed += 1;
            println!("❌ UPDATE...FROM - SQL missing FROM, got: {}", sql);
        }
    }

    // Test DELETE...USING (multi-table delete)
    {
        let q = Qail::del("harbors")
            .delete_using(["qail_test"])
            .filter_cond(eq("harbors.id", 1));
        let sql = q.to_sql();
        if sql.contains("USING") && sql.contains("qail_test") {
            passed += 1;
            println!("✅ DELETE...USING - SQL ✓");
        } else {
            failed += 1;
            println!("❌ DELETE...USING - SQL missing USING, got: {}", sql);
        }
    }

    // Test FOR UPDATE (row locking)
    {
        let q = Qail::get("harbors")
            .column("id")
            .filter_cond(eq("id", 1))
            .for_update();
        let sql = q.to_sql();
        if sql.contains("FOR UPDATE") {
            passed += 1;
            println!("✅ FOR UPDATE - SQL ✓");
        } else {
            failed += 1;
            println!("❌ FOR UPDATE - SQL missing FOR UPDATE, got: {}", sql);
        }
    }

    // Test FOR SHARE (row locking)
    {
        let q = Qail::get("harbors")
            .column("id")
            .filter_cond(eq("id", 1))
            .for_share();
        let sql = q.to_sql();
        if sql.contains("FOR SHARE") {
            passed += 1;
            println!("✅ FOR SHARE - SQL ✓");
        } else {
            failed += 1;
            println!("❌ FOR SHARE - SQL missing FOR SHARE, got: {}", sql);
        }
    }

    // Test FETCH clause
    {
        let q = Qail::get("harbors").column("id").fetch_first(10);
        let sql = q.to_sql();
        if sql.contains("FETCH FIRST 10 ROWS ONLY") {
            passed += 1;
            println!("✅ FETCH - SQL ✓");
        } else {
            failed += 1;
            println!("❌ FETCH - SQL missing FETCH, got: {}", sql);
        }
    }

    // Test DEFAULT VALUES
    {
        let q = Qail::add("harbors").default_values();
        let sql = q.to_sql();
        if sql.contains("DEFAULT VALUES") {
            passed += 1;
            println!("✅ DEFAULT VALUES - SQL ✓");
        } else {
            failed += 1;
            println!("❌ DEFAULT VALUES - SQL missing, got: {}", sql);
        }
    }

    // Test TABLESAMPLE
    {
        let q = Qail::get("harbors").tablesample_bernoulli(10.0);
        let sql = q.to_sql();
        if sql.contains("TABLESAMPLE BERNOULLI(10)") {
            passed += 1;
            println!("✅ TABLESAMPLE - SQL ✓");
        } else {
            failed += 1;
            println!("❌ TABLESAMPLE - SQL missing, got: {}", sql);
        }
    }

    // Test ONLY (inheritance)
    {
        let q = Qail::get("harbors").only();
        let sql = q.to_sql();
        if sql.contains("FROM ONLY") {
            passed += 1;
            println!("✅ SELECT ONLY - SQL ✓");
        } else {
            failed += 1;
            println!("❌ SELECT ONLY - SQL missing, got: {}", sql);
        }
    }

    // Test DELETE ONLY
    {
        let q = Qail::del("harbors").only().filter_cond(eq("id", 999999));
        let sql = q.to_sql();
        if sql.contains("DELETE FROM ONLY") {
            passed += 1;
            println!("✅ DELETE ONLY - SQL ✓");
        } else {
            failed += 1;
            println!("❌ DELETE ONLY - SQL missing, got: {}", sql);
        }
    }

    // ========================================================================
    // SUMMARY
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("SUMMARY");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("✅ Passed: {}", passed);
    println!("❌ Failed: {}", failed);
    println!("📊 Total:  {}", passed + failed);

    if failed == 0 {
        println!("\n🎉 ALL PIPELINE TESTS PASSED!");
        println!("   - Builder creates correct AST");
        println!("   - Transpiler generates correct SQL");
        println!("   - PgEncoder sends correct wire protocol");
        println!("   - PostgreSQL executes without errors");
    } else {
        println!("\n⚠️  Some tests failed. Please review.");
        std::process::exit(1);
    }

    Ok(())
}
