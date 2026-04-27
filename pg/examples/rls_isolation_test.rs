//! Multi-Tenant RLS Isolation Chaos Test
//!
//! Proves ZERO cross-tenant data leakage under concurrent load.
//!
//! 1. Creates a temporary RLS-protected table with `tenant_id` column
//! 2. Inserts data for N distinct tenant IDs
//! 3. Spawns N concurrent workers, each with a different `app.current_tenant_id` session var
//! 4. Each worker runs many SELECTs, asserting ALL returned rows belong to their tenant
//!
//! Pass criteria: Zero cross-tenant rows across all queries, zero errors.
//!
//! Run:
//!   DATABASE_URL=postgresql://qail_user@localhost:5432/qail_test \
//!     cargo run -p qail-pg --example rls_isolation_test --features chrono,uuid --release

use qail_core::ast::{Action, Constraint, Expr, Qail};
use qail_core::migrate::policy::{RlsPolicy, tenant_check};
use qail_pg::PgDriver;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::Barrier;
use uuid::Uuid;

const NUM_TENANTS: usize = 10;
const ROWS_PER_TENANT: usize = 50;
const QUERIES_PER_WORKER: usize = 100;
const TABLE: &str = "_rls_chaos_test";

fn drop_table_cmd(table: &str) -> Qail {
    Qail {
        action: Action::Drop,
        table: table.to_string(),
        ..Default::default()
    }
}

fn make_rls_table_cmd() -> Qail {
    Qail::make(TABLE).columns_expr(vec![
        Expr::Def {
            name: "id".into(),
            data_type: "UUID".into(),
            constraints: vec![
                Constraint::PrimaryKey,
                Constraint::Default("gen_random_uuid()".into()),
            ],
        },
        Expr::Def {
            name: "tenant_id".into(),
            data_type: "UUID".into(),
            constraints: vec![],
        },
        Expr::Def {
            name: "data".into(),
            data_type: "TEXT".into(),
            constraints: vec![],
        },
        Expr::Def {
            name: "created_at".into(),
            data_type: "TIMESTAMPTZ".into(),
            constraints: vec![Constraint::Default("NOW()".into())],
        },
    ])
}

fn alter_enable_rls_cmd(table: &str) -> Qail {
    Qail {
        action: Action::AlterEnableRls,
        table: table.to_string(),
        ..Default::default()
    }
}

fn alter_force_rls_cmd(table: &str) -> Qail {
    Qail {
        action: Action::AlterForceRls,
        table: table.to_string(),
        ..Default::default()
    }
}

fn create_tenant_policy_cmd() -> Qail {
    let policy = RlsPolicy::create("tenant_isolation", TABLE)
        .for_all()
        .using(tenant_check("tenant_id", "app.current_tenant_id", "uuid"))
        .with_check(tenant_check("tenant_id", "app.current_tenant_id", "uuid"));

    Qail {
        action: Action::CreatePolicy,
        table: TABLE.to_string(),
        policy_def: Some(policy),
        ..Default::default()
    }
}

#[tokio::main]
async fn main() {
    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║       RLS ISOLATION CHAOS TEST — Multi-Tenant Safety           ║");
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!("║  Tenants:          {:<46}║", NUM_TENANTS);
    println!("║  Rows per tenant:  {:<46}║", ROWS_PER_TENANT);
    println!("║  Queries/worker:   {:<46}║", QUERIES_PER_WORKER);
    println!(
        "║  Total queries:    {:<46}║",
        NUM_TENANTS * QUERIES_PER_WORKER
    );
    println!("╚══════════════════════════════════════════════════════════════════╝");

    // Generate tenant IDs
    let tenant_ids: Vec<Uuid> = (0..NUM_TENANTS).map(|_| Uuid::new_v4()).collect();

    // =========================================================================
    // Setup: Create temp table with RLS
    // =========================================================================
    println!("\n⏳ Setting up RLS-protected table...");
    {
        let mut driver = PgDriver::connect_url(&db_url)
            .await
            .expect("Failed to connect");

        // Drop if exists from previous run
        driver.execute(&drop_table_cmd(TABLE)).await.ok();

        // Create table
        driver
            .execute(&make_rls_table_cmd())
            .await
            .expect("Failed to create table");

        // Enable RLS
        driver
            .execute(&alter_enable_rls_cmd(TABLE))
            .await
            .expect("Failed to enable RLS");
        driver
            .execute(&alter_force_rls_cmd(TABLE))
            .await
            .expect("Failed to force RLS");

        // Create policy: users can only see their own tenant_id rows
        driver
            .execute(&create_tenant_policy_cmd())
            .await
            .expect("Failed to create policy");

        // Insert test data for each tenant
        for (i, tenant_id) in tenant_ids.iter().enumerate() {
            driver
                .execute(&Qail::session_set(
                    "app.current_tenant_id",
                    tenant_id.to_string(),
                ))
                .await
                .expect("Failed to set tenant_id for insert");

            for j in 0..ROWS_PER_TENANT {
                driver
                    .execute(
                        &Qail::add(TABLE)
                            .columns(["tenant_id", "data"])
                            .values([tenant_id.to_string(), format!("tenant_{}_row_{}", i, j)]),
                    )
                    .await
                    .expect("Failed to insert test data");
            }
        }
        println!(
            "  ✅ Created table with {} rows ({} tenants × {} rows)",
            NUM_TENANTS * ROWS_PER_TENANT,
            NUM_TENANTS,
            ROWS_PER_TENANT
        );
    }

    // =========================================================================
    // Chaos: Concurrent tenant queries
    // =========================================================================
    println!("\n🔥 Starting isolation attack...\n");

    let barrier = Arc::new(Barrier::new(NUM_TENANTS));
    let violations = Arc::new(AtomicU64::new(0));
    let total_rows_checked = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let mut handles = Vec::new();

    for (tenant_idx, tenant_id) in tenant_ids.iter().enumerate() {
        let db_url = db_url.clone();
        let tenant_id = *tenant_id;
        let barrier = Arc::clone(&barrier);
        let violations = Arc::clone(&violations);
        let total_rows = Arc::clone(&total_rows_checked);
        let errors = Arc::clone(&errors);

        handles.push(tokio::spawn(async move {
            let mut driver = PgDriver::connect_url(&db_url)
                .await
                .expect("Worker failed to connect");

            driver
                .execute(&Qail::session_set(
                    "app.current_tenant_id",
                    tenant_id.to_string(),
                ))
                .await
                .expect("Failed to set tenant_id");

            // Wait for all workers to be ready
            barrier.wait().await;

            let mut local_violations = 0u64;
            let mut local_rows = 0u64;
            let mut local_errors = 0u64;

            for _ in 0..QUERIES_PER_WORKER {
                // Build query using Qail AST
                let cmd = Qail::get(TABLE).columns(["id", "tenant_id", "data"]);

                match driver.fetch_all_cached(&cmd).await {
                    Ok(rows) => {
                        for row in &rows {
                            local_rows += 1;
                            // CRITICAL CHECK: every row must belong to our tenant
                            let col_idx = row
                                .column_index("tenant_id")
                                .expect("missing tenant_id column");
                            let row_tenant_id: String = row.text(col_idx);
                            let expected = tenant_id.to_string();
                            if row_tenant_id != expected {
                                local_violations += 1;
                                eprintln!(
                                    "  ❌ VIOLATION! Tenant {} saw row belonging to {}",
                                    expected, row_tenant_id
                                );
                            }
                        }

                        // Verify correct count
                        if rows.len() != ROWS_PER_TENANT {
                            eprintln!(
                                "  ⚠️  Tenant {} got {} rows (expected {})",
                                tenant_idx,
                                rows.len(),
                                ROWS_PER_TENANT
                            );
                        }
                    }
                    Err(e) => {
                        local_errors += 1;
                        if local_errors <= 3 {
                            eprintln!("  ⚠️  Tenant {} query error: {}", tenant_idx, e);
                        }
                    }
                }
            }

            violations.fetch_add(local_violations, Ordering::Relaxed);
            total_rows.fetch_add(local_rows, Ordering::Relaxed);
            errors.fetch_add(local_errors, Ordering::Relaxed);
        }));
    }

    // Wait for all workers
    for h in handles {
        h.await.expect("Worker panicked");
    }
    let elapsed = start.elapsed();

    // =========================================================================
    // Cleanup
    // =========================================================================
    {
        let mut driver = PgDriver::connect_url(&db_url)
            .await
            .expect("Failed to connect for cleanup");
        driver.execute(&drop_table_cmd(TABLE)).await.ok();
    }

    // =========================================================================
    // Results
    // =========================================================================
    let total_violations = violations.load(Ordering::Relaxed);
    let total_checked = total_rows_checked.load(Ordering::Relaxed);
    let total_errors = errors.load(Ordering::Relaxed);

    println!("\n╔══════════════════════════════════════════════════════════════════╗");
    println!("║  RLS ISOLATION RESULTS                                         ║");
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!(
        "║  Total queries:    {:<46}║",
        NUM_TENANTS * QUERIES_PER_WORKER
    );
    println!("║  Total rows checked: {:<44}║", total_checked);
    println!(
        "║  Cross-tenant violations: {:<39}║",
        if total_violations == 0 {
            "✅ ZERO".to_string()
        } else {
            format!("❌ {} VIOLATIONS", total_violations)
        }
    );
    println!("║  Query errors:     {:<46}║", total_errors);
    println!(
        "║  Elapsed:          {:<46}║",
        format!("{:.2}s", elapsed.as_secs_f64())
    );
    println!(
        "║  QPS:              {:<46}║",
        format!(
            "{:.0}",
            (NUM_TENANTS * QUERIES_PER_WORKER) as f64 / elapsed.as_secs_f64()
        )
    );
    println!("╚══════════════════════════════════════════════════════════════════╝");

    if total_violations > 0 {
        eprintln!(
            "\n🚨 CRITICAL: {} cross-tenant data leaks detected!",
            total_violations
        );
        std::process::exit(1);
    }
    if total_errors > 0 {
        eprintln!(
            "\n⚠️  {} query errors occurred (check logs above)",
            total_errors
        );
        std::process::exit(1);
    }
    println!(
        "\n✅ RLS isolation verified: {} rows checked, ZERO leaks\n",
        total_checked
    );
}
