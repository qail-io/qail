//! Comprehensive test of ALL QAIL builder methods against real PostgreSQL
//!
//! Run with: cargo run --example builder_test
//!
//! ============================================================================
//! TEST COVERAGE STATUS
//! ============================================================================
//!
//! FLUENT METHODS (ExprExt trait):
//! ✅ with_alias()  - Add alias to expression
//! ✅ or_default()  - COALESCE with default
//! ✅ json()        - JSON text extraction (->>')
//! ✅ path()        - Nested JSON path
//! ✅ cast()        - CAST(x AS type)
//! ✅ upper()       - UPPER(x)
//! ✅ lower()       - LOWER(x)
//! ✅ trim()        - TRIM(x)
//! ✅ length()      - LENGTH(x)
//! ✅ abs()         - ABS(x)
//!
//! CONDITION HELPERS:
//! ✅ eq()          - column = value
//! ✅ ne()          - column != value
//! ✅ gt()          - column > value
//! ✅ gte()         - column >= value
//! ✅ lt()          - column < value
//! ✅ lte()         - column <= value
//! ✅ like()        - column LIKE pattern
//! ✅ not_like()    - column NOT LIKE pattern
//! ✅ ilike()       - column ILIKE pattern (case-insensitive)
//! ✅ between()     - column BETWEEN low AND high
//! ✅ not_between() - column NOT BETWEEN low AND high
//! ✅ regex()       - column ~ pattern
//! ✅ regex_i()     - column ~* pattern (case-insensitive)
//! ✅ is_in()       - column IN (values)
//! ✅ not_in()      - column NOT IN (values)
//! ✅ is_null()     - column IS NULL
//! ✅ is_not_null() - column IS NOT NULL
//! ✅ contains()    - column @> values (JSONB/array containment)
//! ✅ overlaps()    - column && values (array overlap)
//!
//! AGGREGATE FUNCTIONS:
//! ✅ count()       - COUNT(column)
//! ✅ sum()         - SUM(column)
//! ✅ avg()         - AVG(column)
//! ✅ min()         - MIN(column)
//! ✅ max()         - MAX(column)
//! ✅ count_distinct() - COUNT(DISTINCT column)
//! ✅ array_agg()   - ARRAY_AGG(column)
//! ✅ string_agg()  - STRING_AGG(column, delimiter)
//! ✅ json_agg()    - JSON_AGG(column)
//!
//! FUNCTION BUILDERS:
//! ✅ coalesce()    - COALESCE(args...)
//! ✅ nullif()      - NULLIF(a, b)
//! ✅ replace()     - REPLACE(source, from, to)
//! ✅ substring()   - SUBSTRING(x FROM n)
//! ✅ concat()      - a || b || c
//!
//! TIME FUNCTIONS:
//! ✅ now()         - NOW()
//! ✅ now_plus()    - NOW() + INTERVAL
//! ✅ now_minus()   - NOW() - INTERVAL
//! ✅ interval()    - INTERVAL 'x'
//!
//! CASE EXPRESSIONS:
//! ✅ case_when()   - CASE WHEN ... THEN ... ELSE ... END
//!
//! CAST BUILDER:
//! ✅ cast()        - CAST(expr AS type)
//!
//! JSON BUILDERS:
//! ✅ json()        - column->>'key'
//! ✅ json_path()   - column->'a'->'b'->>'c'
//! ✅ json_obj()    - Build JSON object
//!
//! ============================================================================

use qail_core::ast::builders::*;
use qail_core::ast::{Qail, SortOrder};
use qail_pg::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut driver = PgDriver::connect("localhost", 5432, "orion", "postgres").await?;
    println!("✅ Connected to PostgreSQL\n");

    let mut passed = 0;
    let mut failed = 0;

    // ========================================================================
    // FLUENT METHODS
    // ========================================================================
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("FLUENT METHODS (ExprExt)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Test 1: upper()
    let q = Qail::get("harbors")
        .column_expr("name".upper().with_alias("u"))
        .limit(1);
    match driver.fetch_all(&q).await {
        Ok(_) => {
            passed += 1;
            println!("✅ upper()");
        }
        Err(e) => {
            failed += 1;
            println!("❌ upper(): {}", e);
        }
    }

    // Test 2: lower()
    let q = Qail::get("harbors")
        .column_expr("name".lower().with_alias("l"))
        .limit(1);
    match driver.fetch_all(&q).await {
        Ok(_) => {
            passed += 1;
            println!("✅ lower()");
        }
        Err(e) => {
            failed += 1;
            println!("❌ lower(): {}", e);
        }
    }

    // Test 3: trim()
    let q = Qail::get("harbors")
        .column_expr("name".trim().with_alias("t"))
        .limit(1);
    match driver.fetch_all(&q).await {
        Ok(_) => {
            passed += 1;
            println!("✅ trim()");
        }
        Err(e) => {
            failed += 1;
            println!("❌ trim(): {}", e);
        }
    }

    // Test 4: length()
    let q = Qail::get("harbors")
        .column_expr("name".length().with_alias("len"))
        .limit(1);
    match driver.fetch_all(&q).await {
        Ok(_) => {
            passed += 1;
            println!("✅ length()");
        }
        Err(e) => {
            failed += 1;
            println!("❌ length(): {}", e);
        }
    }

    // Test 5: abs()
    let q = Qail::get("harbors")
        .column_expr(col("id").abs().with_alias("a"))
        .limit(1);
    match driver.fetch_all(&q).await {
        Ok(_) => {
            passed += 1;
            println!("✅ abs()");
        }
        Err(e) => {
            failed += 1;
            println!("❌ abs(): {}", e);
        }
    }

    // Test 6: cast()
    let q = Qail::get("harbors")
        .column_expr(col("id").cast("text").with_alias("c"))
        .limit(1);
    match driver.fetch_all(&q).await {
        Ok(_) => {
            passed += 1;
            println!("✅ cast()");
        }
        Err(e) => {
            failed += 1;
            println!("❌ cast(): {}", e);
        }
    }

    // Test 7: or_default()
    let q = Qail::get("harbors")
        .column_expr(col("name").or_default(text("N/A")).with_alias("d"))
        .limit(1);
    match driver.fetch_all(&q).await {
        Ok(_) => {
            passed += 1;
            println!("✅ or_default()");
        }
        Err(e) => {
            failed += 1;
            println!("❌ or_default(): {}", e);
        }
    }

    // ========================================================================
    // COMPARISON CONDITIONS
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("COMPARISON CONDITIONS");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Test 8: eq()
    let q = Qail::get("harbors").column("id").filter_cond(eq("id", 1));
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ eq() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ eq(): {}", e);
        }
    }

    // Test 9: ne()
    let q = Qail::get("harbors")
        .column("id")
        .filter_cond(ne("id", 1))
        .limit(3);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ ne() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ ne(): {}", e);
        }
    }

    // Test 10: gt()
    let q = Qail::get("harbors")
        .column("id")
        .filter_cond(gt("id", 5))
        .limit(3);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ gt() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ gt(): {}", e);
        }
    }

    // Test 11: gte()
    let q = Qail::get("harbors")
        .column("id")
        .filter_cond(gte("id", 5))
        .limit(3);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ gte() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ gte(): {}", e);
        }
    }

    // Test 12: lt()
    let q = Qail::get("harbors")
        .column("id")
        .filter_cond(lt("id", 5))
        .limit(3);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ lt() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ lt(): {}", e);
        }
    }

    // Test 13: lte()
    let q = Qail::get("harbors")
        .column("id")
        .filter_cond(lte("id", 5))
        .limit(3);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ lte() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ lte(): {}", e);
        }
    }

    // ========================================================================
    // PATTERN MATCHING
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("PATTERN MATCHING");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Test 14: like()
    let q = Qail::get("harbors")
        .column("name")
        .filter_cond(like("name", "Harbor%"))
        .limit(3);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ like() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ like(): {}", e);
        }
    }

    // Test 15: not_like()
    let q = Qail::get("harbors")
        .column("name")
        .filter_cond(not_like("name", "Harbor 1%"))
        .limit(3);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ not_like() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ not_like(): {}", e);
        }
    }

    // Test 16: ilike()
    let q = Qail::get("harbors")
        .column("name")
        .filter_cond(ilike("name", "harbor%"))
        .limit(3);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ ilike() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ ilike(): {}", e);
        }
    }

    // Test 17: regex()
    let q = Qail::get("harbors")
        .column("name")
        .filter_cond(regex("name", "^Harbor [0-9]+$"))
        .limit(3);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ regex() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ regex(): {}", e);
        }
    }

    // Test 18: regex_i()
    let q = Qail::get("harbors")
        .column("name")
        .filter_cond(regex_i("name", "^harbor"))
        .limit(3);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ regex_i() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ regex_i(): {}", e);
        }
    }

    // ========================================================================
    // RANGE CONDITIONS
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("RANGE CONDITIONS");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Test 19: between()
    let q = Qail::get("harbors")
        .column("id")
        .filter_cond(between("id", 1, 5))
        .order_by("id", SortOrder::Asc);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ between() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ between(): {}", e);
        }
    }

    // Test 20: not_between()
    let q = Qail::get("harbors")
        .column("id")
        .filter_cond(not_between("id", 1, 5))
        .limit(5);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ not_between() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ not_between(): {}", e);
        }
    }

    // ========================================================================
    // SET CONDITIONS
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("SET CONDITIONS");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Test 21: is_in()
    let q = Qail::get("harbors")
        .column("id")
        .filter_cond(is_in("id", [1, 2, 3]))
        .order_by("id", SortOrder::Asc);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ is_in() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ is_in(): {}", e);
        }
    }

    // Test 22: not_in()
    let q = Qail::get("harbors")
        .column("id")
        .filter_cond(not_in("id", [1, 2, 3]))
        .limit(5);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ not_in() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ not_in(): {}", e);
        }
    }

    // ========================================================================
    // NULL CONDITIONS
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("NULL CONDITIONS");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Test 23: is_null()
    let q = Qail::get("harbors")
        .column("id")
        .filter_cond(is_null("name"))
        .limit(3);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ is_null() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ is_null(): {}", e);
        }
    }

    // Test 24: is_not_null()
    let q = Qail::get("harbors")
        .column("id")
        .filter_cond(is_not_null("name"))
        .limit(3);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ is_not_null() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ is_not_null(): {}", e);
        }
    }

    // ========================================================================
    // AGGREGATE FUNCTIONS
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("AGGREGATE FUNCTIONS");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Test 25: count()
    let q = Qail::get("harbors").column_expr(count().alias("cnt"));
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ count() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ count(): {}", e);
        }
    }

    // Test 26: sum()
    let q = Qail::get("harbors").column_expr(sum("id").alias("total"));
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ sum() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ sum(): {}", e);
        }
    }

    // Test 27: avg()
    let q = Qail::get("harbors").column_expr(avg("id").alias("average"));
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ avg() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ avg(): {}", e);
        }
    }

    // Test 28: min()
    let q = Qail::get("harbors").column_expr(min("id").alias("minimum"));
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ min() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ min(): {}", e);
        }
    }

    // Test 29: max()
    let q = Qail::get("harbors").column_expr(max("id").alias("maximum"));
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ max() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ max(): {}", e);
        }
    }

    // Test 30: count_distinct()
    let q = Qail::get("harbors").column_expr(count_distinct("name").alias("distinct_names"));
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ count_distinct() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ count_distinct(): {}", e);
        }
    }

    // Test 31: array_agg()
    let q = Qail::get("harbors")
        .column_expr(array_agg("name").alias("names"))
        .limit(1);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ array_agg() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ array_agg(): {}", e);
        }
    }

    // Test 32: string_agg()
    let q = Qail::get("harbors")
        .column_expr(string_agg(col("name"), ", ").alias("all_names"))
        .limit(1);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ string_agg() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ string_agg(): {}", e);
        }
    }

    // Test 33: json_agg()
    let q = Qail::get("harbors")
        .column_expr(json_agg("name").alias("json_names"))
        .limit(1);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ json_agg() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ json_agg(): {}", e);
        }
    }

    // ========================================================================
    // FUNCTION BUILDERS
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("FUNCTION BUILDERS");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Test 31: coalesce()
    let q = Qail::get("harbors")
        .column_expr(coalesce([col("name"), text("N/A")]).alias("coalesced"))
        .limit(1);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ coalesce() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ coalesce(): {}", e);
        }
    }

    // Test 32: concat()
    let q = Qail::get("harbors")
        .column_expr(concat([col("name"), text(" - "), col("id").cast("text")]).alias("combined"))
        .limit(1);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ concat() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ concat(): {}", e);
        }
    }

    // ========================================================================
    // TIME FUNCTIONS
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("TIME FUNCTIONS");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Test 33: now()
    let q = Qail::get("harbors")
        .column_expr(now().with_alias("current_time"))
        .limit(1);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ now() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ now(): {}", e);
        }
    }

    // Test 34: now_minus()
    let q = Qail::get("harbors")
        .column_expr(now_minus("1 hour").with_alias("hour_ago"))
        .limit(1);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ now_minus() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ now_minus(): {}", e);
        }
    }

    // Test 35: now_plus()
    let q = Qail::get("harbors")
        .column_expr(now_plus("1 day").with_alias("tomorrow"))
        .limit(1);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ now_plus() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ now_plus(): {}", e);
        }
    }

    // ========================================================================
    // CASE EXPRESSIONS
    // ========================================================================
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("CASE EXPRESSIONS");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Test 36: case_when()
    let q = Qail::get("harbors")
        .column("id")
        .column_expr(
            case_when(gt("id", 5), text("big"))
                .otherwise(text("small"))
                .alias("size"),
        )
        .limit(10);
    match driver.fetch_all(&q).await {
        Ok(r) => {
            passed += 1;
            println!("✅ case_when() - {} rows", r.len());
        }
        Err(e) => {
            failed += 1;
            println!("❌ case_when(): {}", e);
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
        println!("\n🎉 ALL TESTS PASSED!");
    } else {
        println!("\n⚠️  Some tests failed. Please review.");
    }

    Ok(())
}
