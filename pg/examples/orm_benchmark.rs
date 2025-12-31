//! ORM/Driver Benchmark - Fair comparison of qail-pg, SQLx, and SeaORM

use qail_core::prelude::*;
use qail_pg::driver::PgDriver;
use sqlx::postgres::PgPoolOptions;
use std::time::Instant;

const ITERATIONS: usize = 50_000;
const WARMUP: usize = 100;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🏁 ORM/Driver Benchmark");
    println!("========================");
    println!("Iterations: {}", ITERATIONS);
    println!("Query: CTE with JOIN + WHERE + ORDER BY + LIMIT\n");

    // Setup
    println!("🛠  Setting up benchmark data...");
    let mut driver = PgDriver::connect("127.0.0.1", 5432, "orion", "qail_test_migration").await?;
    setup_benchmark_data(&mut driver).await?;

    let sqlx_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect("postgres://orion@127.0.0.1/qail_test_migration")
        .await?;

    println!("📊 Running Benchmarks...\n");

    // Benchmark qail-pg
    let qail_time = bench_qail(&mut driver).await?;
    
    // Benchmark SQLx
    let sqlx_time = bench_sqlx(&sqlx_pool).await?;

    // Benchmark SeaORM
    let seaorm_time = bench_seaorm().await?;

    // Results
    let qail_qps = ITERATIONS as f64 / (qail_time / 1000.0);
    let sqlx_qps = ITERATIONS as f64 / (sqlx_time / 1000.0);
    let seaorm_qps = ITERATIONS as f64 / (seaorm_time / 1000.0);

    println!("\n📈 Results ({} iterations)", ITERATIONS);
    println!("============================================================");
    println!("qail-pg: {:>8.2}ms | {:.2}μs/query | {:>6.0} q/s ⭐", qail_time, qail_time * 1000.0 / ITERATIONS as f64, qail_qps);
    println!("SeaORM:  {:>8.2}ms | {:.2}μs/query | {:>6.0} q/s", seaorm_time, seaorm_time * 1000.0 / ITERATIONS as f64, seaorm_qps);
    println!("SQLx:    {:>8.2}ms | {:.2}μs/query | {:>6.0} q/s", sqlx_time, sqlx_time * 1000.0 / ITERATIONS as f64, sqlx_qps);

    println!("\n📊 Comparison (vs qail-pg)");
    println!("----------------------------------");
    let seaorm_diff = ((seaorm_time / qail_time) - 1.0) * 100.0;
    let sqlx_diff = ((sqlx_time / qail_time) - 1.0) * 100.0;
    println!("SeaORM:  {:.0}% slower ({:.0} fewer q/s)", seaorm_diff, qail_qps - seaorm_qps);
    println!("SQLx:    {:.0}% slower ({:.0} fewer q/s)", sqlx_diff, qail_qps - sqlx_qps);

    // Cleanup
    cleanup(&mut driver).await?;
    println!("\n🧹 Cleanup complete");

    Ok(())
}

fn build_test_query() -> Qail {
    let high_earners = Qail::get("employees")
        .select_all()
        .filter("salary", Operator::Gt, Value::Int(80000));

    let mut query = Qail::get("high_earners")
        .columns(["high_earners.*", "departments.name"])
        .join(JoinKind::Inner, "departments", "high_earners.department_id", "departments.id")
        .filter("status", Operator::Eq, Value::String("active".into()))
        .order_by("salary", SortOrder::Desc)
        .limit(100);

    query.ctes = vec![CTEDef {
        name: "high_earners".to_string(),
        recursive: false,
        columns: vec![],
        base_query: Box::new(high_earners),
        recursive_query: None,
        source_table: Some("employees".to_string()),
    }];
    query
}

async fn bench_qail(driver: &mut PgDriver) -> Result<f64, Box<dyn std::error::Error>> {
    print!("  qail-pg: ");
    std::io::Write::flush(&mut std::io::stdout())?;

    let query = build_test_query();

    // Warmup
    for _ in 0..WARMUP {
        let _ = driver.fetch_all(&query).await?.len();
    }

    // Benchmark
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let _ = driver.fetch_all(&query).await?.len();
    }
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    println!("done");
    Ok(elapsed)
}

async fn bench_sqlx(pool: &sqlx::PgPool) -> Result<f64, Box<dyn std::error::Error>> {
    print!("  SQLx:    ");
    std::io::Write::flush(&mut std::io::stdout())?;

    let sql = r#"
        WITH high_earners AS (
            SELECT * FROM employees WHERE salary > 80000
        )
        SELECT e.*, d.name as dept_name
        FROM high_earners e
        INNER JOIN departments d ON e.department_id = d.id
        WHERE e.status = 'active'
        ORDER BY e.salary DESC
        LIMIT 100
    "#;

    // Warmup
    for _ in 0..WARMUP {
        let rows = sqlx::query(sql).fetch_all(pool).await?;
        let _ = rows.len();
    }

    // Benchmark
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let rows = sqlx::query(sql).fetch_all(pool).await?;
        let _ = rows.len();
    }
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    println!("done");
    Ok(elapsed)
}

async fn bench_seaorm() -> Result<f64, Box<dyn std::error::Error>> {
    print!("  SeaORM:  ");
    std::io::Write::flush(&mut std::io::stdout())?;

    use sea_orm::{Database, DatabaseConnection, Statement, ConnectionTrait};

    let db: DatabaseConnection = Database::connect("postgres://orion@127.0.0.1/qail_test_migration").await?;

    let sql = r#"
        WITH high_earners AS (
            SELECT * FROM employees WHERE salary > 80000
        )
        SELECT e.*, d.name as dept_name
        FROM high_earners e
        INNER JOIN departments d ON e.department_id = d.id
        WHERE e.status = 'active'
        ORDER BY e.salary DESC
        LIMIT 100
    "#;

    // Warmup
    for _ in 0..WARMUP {
        let results = db.query_all(Statement::from_string(sea_orm::DatabaseBackend::Postgres, sql.to_string())).await?;
        let _ = results.len();
    }

    // Benchmark
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let results = db.query_all(Statement::from_string(sea_orm::DatabaseBackend::Postgres, sql.to_string())).await?;
        let _ = results.len();
    }
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    println!("done");
    db.close().await?;
    Ok(elapsed)
}

async fn setup_benchmark_data(driver: &mut PgDriver) -> Result<(), Box<dyn std::error::Error>> {
    // Create tables
    driver.execute_raw("DROP TABLE IF EXISTS employees CASCADE").await?;
    driver.execute_raw("DROP TABLE IF EXISTS departments CASCADE").await?;
    
    driver.execute_raw("CREATE TABLE departments (id SERIAL PRIMARY KEY, name TEXT NOT NULL)").await?;
    driver.execute_raw("CREATE TABLE employees (id SERIAL PRIMARY KEY, name TEXT NOT NULL, salary INT NOT NULL, status TEXT NOT NULL, department_id INT REFERENCES departments(id))").await?;
    
    // Insert departments
    driver.execute_raw("INSERT INTO departments (name) VALUES ('Engineering'), ('Sales'), ('Marketing'), ('HR')").await?;
    
    // Insert 1000 employees
    for i in 0..1000 {
        let dept_id = (i % 4) + 1;
        let salary = 50000 + (i * 50);
        let status = if i % 10 == 0 { "inactive" } else { "active" };
        let sql = format!("INSERT INTO employees (name, salary, status, department_id) VALUES ('Employee{}', {}, '{}', {})", i, salary, status, dept_id);
        driver.execute_raw(&sql).await?;
    }
    
    println!("  ✓ Created 4 departments, 1000 employees");
    Ok(())
}

async fn cleanup(driver: &mut PgDriver) -> Result<(), Box<dyn std::error::Error>> {
    driver.execute_raw("DROP TABLE IF EXISTS employees CASCADE").await?;
    driver.execute_raw("DROP TABLE IF EXISTS departments CASCADE").await?;
    Ok(())
}
