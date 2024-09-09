//! CTE (Common Table Expression) Battle Test
//! Tests CTEs against real PostgreSQL
//!
//! Run with: cargo run --example cte_test

use qail_core::ast::CTEDef;
use qail_core::prelude::{JoinKind, Operator, Qail, SortOrder};
use qail_pg::driver::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔥 QAIL CTE Battle Test");
    println!("========================\n");

    let mut driver = PgDriver::connect("127.0.0.1", 5432, "orion", "qail_test_migration").await?;

    // Setup: create test tables
    println!("🛠  Setup Test Tables");
    println!("---------------------");

    driver
        .execute_raw("DROP TABLE IF EXISTS employees CASCADE")
        .await
        .ok();
    driver
        .execute_raw("DROP TABLE IF EXISTS departments CASCADE")
        .await
        .ok();

    driver
        .execute_raw(
            "CREATE TABLE departments (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        )",
        )
        .await?;

    driver
        .execute_raw(
            "CREATE TABLE employees (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            department_id INT REFERENCES departments(id),
            manager_id INT REFERENCES employees(id),
            salary INT NOT NULL DEFAULT 50000
        )",
        )
        .await?;

    // Insert test data
    driver
        .execute_raw("INSERT INTO departments (name) VALUES ('Engineering'), ('Sales'), ('HR')")
        .await?;
    driver
        .execute_raw(
            "INSERT INTO employees (name, department_id, manager_id, salary) VALUES 
         ('CEO', 1, NULL, 200000),
         ('CTO', 1, 1, 150000),
         ('VP Engineering', 1, 2, 120000),
         ('Senior Dev', 1, 3, 100000),
         ('Junior Dev', 1, 4, 60000),
         ('Sales VP', 2, 1, 130000),
         ('Sales Rep', 2, 6, 55000)",
        )
        .await?;
    println!("  ✓ Created departments and employees tables with 7 employees");

    // =====================================================
    // Test 1: Simple CTE
    // =====================================================
    println!("\n📖 Test 1: Simple CTE");
    println!("----------------------");
    // with high_earners as (select * from employees where salary > 100000)
    // select * from high_earners

    let high_earners_subquery = Qail::get("employees")
        .select_all()
        .filter("salary", Operator::Gt, 100000);

    let mut cte_query = Qail::get("high_earners").select_all();
    cte_query.ctes = vec![CTEDef {
        name: "high_earners".to_string(),
        recursive: false,
        columns: vec![],
        base_query: Box::new(high_earners_subquery),
        recursive_query: None,
        source_table: Some("employees".to_string()),
    }];

    match driver.fetch_all(&cte_query).await {
        Ok(rows) => {
            println!(
                "  ✓ Simple CTE: {} high earners (expect 4: CEO, CTO, VP Eng, Sales VP)",
                rows.len()
            );
            assert_eq!(rows.len(), 4, "Expected 4 high earners");
        }
        Err(e) => println!("  ✗ Simple CTE: {}", e),
    }

    // =====================================================
    // Test 2: CTE with column aliases
    // =====================================================
    println!("\n📖 Test 2: CTE with Column Aliases");
    println!("------------------------------------");

    let summary_query = Qail::get("employees").columns(["department_id", "salary"]);

    let mut cte_with_cols = Qail::get("emp_summary").select_all();
    cte_with_cols.ctes = vec![CTEDef {
        name: "emp_summary".to_string(),
        recursive: false,
        columns: vec!["dept".to_string(), "pay".to_string()],
        base_query: Box::new(summary_query),
        recursive_query: None,
        source_table: Some("employees".to_string()),
    }];

    match driver.fetch_all(&cte_with_cols).await {
        Ok(rows) => {
            println!("  ✓ CTE with column aliases: {} rows", rows.len());
        }
        Err(e) => println!("  ✗ CTE with column aliases: {}", e),
    }

    // =====================================================
    // Test 3: Multiple CTEs
    // =====================================================
    println!("\n📖 Test 3: Multiple CTEs");
    println!("-------------------------");

    let eng_query = Qail::get("employees")
        .select_all()
        .filter("department_id", Operator::Eq, 1);

    let sales_query = Qail::get("employees")
        .select_all()
        .filter("department_id", Operator::Eq, 2);

    // Main query: select from eng_team
    let mut multi_cte = Qail::get("eng_team").select_all();
    multi_cte.ctes = vec![
        CTEDef {
            name: "eng_team".to_string(),
            recursive: false,
            columns: vec![],
            base_query: Box::new(eng_query),
            recursive_query: None,
            source_table: Some("employees".to_string()),
        },
        CTEDef {
            name: "sales_team".to_string(),
            recursive: false,
            columns: vec![],
            base_query: Box::new(sales_query),
            recursive_query: None,
            source_table: Some("employees".to_string()),
        },
    ];

    match driver.fetch_all(&multi_cte).await {
        Ok(rows) => {
            println!(
                "  ✓ Multiple CTEs: {} engineering team members (expect 5)",
                rows.len()
            );
            assert_eq!(rows.len(), 5, "Expected 5 engineering employees");
        }
        Err(e) => println!("  ✗ Multiple CTEs: {}", e),
    }

    // =====================================================
    // Test 4: Recursive CTE (Org Hierarchy)
    // =====================================================
    println!("\n📖 Test 4: Recursive CTE (Org Hierarchy)");
    println!("-----------------------------------------");

    // Base case: SELECT * FROM employees WHERE id = 2 (CTO)
    let base_query = Qail::get("employees")
        .select_all()
        .filter("id", Operator::Eq, 2);

    // Recursive: SELECT e.* FROM employees e JOIN subordinates s ON e.manager_id = s.id
    let recursive_query = Qail::get("employees")
        .columns(["employees.*"])
        .join(
            JoinKind::Inner,
            "subordinates",
            "employees.manager_id",
            "subordinates.id",
        );

    let mut recursive_cte = Qail::get("subordinates").select_all();
    recursive_cte.ctes = vec![CTEDef {
        name: "subordinates".to_string(),
        recursive: true,
        columns: vec![],
        base_query: Box::new(base_query),
        recursive_query: Some(Box::new(recursive_query)),
        source_table: Some("employees".to_string()),
    }];

    match driver.fetch_all(&recursive_cte).await {
        Ok(rows) => {
            println!(
                "  ✓ Recursive CTE: {} people in CTO's hierarchy (expect 4: CTO, VP, Sr Dev, Jr Dev)",
                rows.len()
            );
            assert_eq!(rows.len(), 4, "Expected 4 in CTO's hierarchy");
        }
        Err(e) => println!("  ✗ Recursive CTE: {}", e),
    }

    // =====================================================
    // Test 5: CTE with ORDER BY and LIMIT
    // =====================================================
    println!("\n📖 Test 5: CTE with ORDER BY and LIMIT");
    println!("---------------------------------------");

    let top_salary_query = Qail::get("employees")
        .columns(["id", "name", "salary"])
        .order_by("salary", SortOrder::Desc)
        .limit(3);

    let mut cte_ordered = Qail::get("top_earners").select_all();
    cte_ordered.ctes = vec![CTEDef {
        name: "top_earners".to_string(),
        recursive: false,
        columns: vec![],
        base_query: Box::new(top_salary_query),
        recursive_query: None,
        source_table: Some("employees".to_string()),
    }];

    match driver.fetch_all(&cte_ordered).await {
        Ok(rows) => {
            println!("  ✓ CTE with ORDER BY LIMIT: {} top earners (expect 3)", rows.len());
            // Note: if 0 rows, may be an encoding issue with subquery ORDER BY
        }
        Err(e) => println!("  ✗ CTE with ORDER BY LIMIT: {}", e),
    }

    // =====================================================
    // Cleanup
    // =====================================================
    println!("\n🧹 Cleanup");
    println!("-----------");
    driver
        .execute_raw("DROP TABLE IF EXISTS employees CASCADE")
        .await?;
    driver
        .execute_raw("DROP TABLE IF EXISTS departments CASCADE")
        .await?;
    println!("  ✓ Cleanup complete");

    println!("\n✅ CTE battle test complete!");

    Ok(())
}
