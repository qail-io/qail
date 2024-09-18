//! Shadow Database (Blue-Green) Migrations
//!
//! Provides zero-downtime migration capabilities by:
//! 1. Creating a shadow database with new schema
//! 2. Syncing data from primary to shadow
//! 3. Validating shadow before switch
//! 4. Promoting shadow to primary or aborting
//!
//! This is Phase 3 of the data-safe migration system.

use anyhow::{Result, anyhow};
use colored::*;
use qail_core::ast::Qail;
use qail_pg::driver::PgDriver;

use crate::util::parse_pg_url;

/// Shadow database state
#[derive(Debug, Clone)]
pub struct ShadowState {
    /// Primary database URL
    pub primary_url: String,
    /// Shadow database name (derived from primary)
    pub shadow_name: String,
    /// Shadow database URL
    pub shadow_url: String,
    pub is_ready: bool,
    pub tables_synced: u64,
    pub rows_synced: u64,
}

impl ShadowState {
    pub fn new(primary_url: &str) -> Result<Self> {
        let (host, port, user, password, database) = parse_pg_url(primary_url)?;
        let shadow_name = format!("{}_shadow", database);

        let shadow_url = if let Some(pwd) = &password {
            format!(
                "postgres://{}:{}@{}:{}/{}",
                user, pwd, host, port, shadow_name
            )
        } else {
            format!("postgres://{}@{}:{}/{}", user, host, port, shadow_name)
        };

        Ok(Self {
            primary_url: primary_url.to_string(),
            shadow_name,
            shadow_url,
            is_ready: false,
            tables_synced: 0,
            rows_synced: 0,
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Shadow State Persistence
// ─────────────────────────────────────────────────────────────────────────────

/// Ensure _qail_shadow_state table exists in primary database
async fn ensure_shadow_state_table(driver: &mut PgDriver) -> Result<()> {
    let sql = r#"
        CREATE TABLE IF NOT EXISTS _qail_shadow_state (
            id SERIAL PRIMARY KEY,
            shadow_name TEXT NOT NULL,
            primary_url TEXT NOT NULL,
            diff_cmds TEXT NOT NULL,
            old_schema_path TEXT,
            new_schema_path TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            status TEXT DEFAULT 'pending'
        )
    "#;
    driver.execute_raw(sql).await
        .map_err(|e| anyhow!("Failed to create shadow state table: {}", e))?;
    Ok(())
}

/// Save shadow state to _qail_shadow_state table (for promote/abort recovery)
async fn save_shadow_state(
    driver: &mut PgDriver,
    state: &ShadowState,
    diff_cmds: &[Qail],
    old_path: &str,
    new_path: &str,
) -> Result<()> {
    ensure_shadow_state_table(driver).await?;
    
    // Serialize diff commands as JSON
    let diff_json = serde_json::to_string(diff_cmds)
        .map_err(|e| anyhow!("Failed to serialize diff commands: {}", e))?;
    
    // Clear any existing pending state
    let clear_sql = "DELETE FROM _qail_shadow_state WHERE status = 'pending'";
    let _ = driver.execute_raw(clear_sql).await;
    
    // Insert new state
    let insert_sql = format!(
        "INSERT INTO _qail_shadow_state (shadow_name, primary_url, diff_cmds, old_schema_path, new_schema_path, status) VALUES ('{}', '{}', '{}', '{}', '{}', 'pending')",
        state.shadow_name,
        state.primary_url.replace('\'', "''"),
        diff_json.replace('\'', "''"),
        old_path.replace('\'', "''"),
        new_path.replace('\'', "''")
    );
    driver.execute_raw(&insert_sql).await
        .map_err(|e| anyhow!("Failed to save shadow state: {}", e))?;
    
    Ok(())
}

/// Load pending shadow state from _qail_shadow_state table
async fn load_shadow_state(driver: &mut PgDriver) -> Result<Option<(ShadowState, Vec<Qail>)>> {
    ensure_shadow_state_table(driver).await?;
    
    let cmd = Qail::get("_qail_shadow_state")
        .columns(["shadow_name", "primary_url", "diff_cmds"])
        .filter("status", qail_core::ast::Operator::Eq, "pending")
        .limit(1);
    
    let rows = driver.fetch_all(&cmd).await
        .map_err(|e| anyhow!("Failed to load shadow state: {}", e))?;
    
    if rows.is_empty() {
        return Ok(None);
    }
    
    let row = &rows[0];
    let shadow_name = row.get_string(0).ok_or_else(|| anyhow!("Missing shadow_name"))?;
    let primary_url = row.get_string(1).ok_or_else(|| anyhow!("Missing primary_url"))?;
    let diff_json = row.get_string(2).ok_or_else(|| anyhow!("Missing diff_cmds"))?;
    
    let diff_cmds: Vec<Qail> = serde_json::from_str(&diff_json)
        .map_err(|e| anyhow!("Failed to deserialize diff commands: {}", e))?;
    
    let state = ShadowState {
        primary_url: primary_url.clone(),
        shadow_name,
        shadow_url: String::new(), // Will be reconstructed
        is_ready: true,
        tables_synced: 0,
        rows_synced: 0,
    };
    
    Ok(Some((state, diff_cmds)))
}

/// Update shadow state status (pending → promoted/aborted)
async fn update_shadow_state_status(driver: &mut PgDriver, new_status: &str) -> Result<()> {
    let sql = format!(
        "UPDATE _qail_shadow_state SET status = '{}' WHERE status = 'pending'",
        new_status
    );
    driver.execute_raw(&sql).await
        .map_err(|e| anyhow!("Failed to update shadow state: {}", e))?;
    Ok(())
}

/// Create a shadow database for blue-green migration
pub async fn create_shadow_database(primary_url: &str) -> Result<ShadowState> {
    println!();
    println!("{}", "🔄 Shadow Migration Mode".cyan().bold());
    println!("{}", "━".repeat(40).dimmed());

    let state = ShadowState::new(primary_url)?;

    println!(
        "  {} Creating shadow database: {}",
        "[1/4]".cyan(),
        state.shadow_name.yellow()
    );

    // Connect to postgres database (not the target) to create new database
    let (host, port, user, password, _database) = parse_pg_url(primary_url)?;

    let mut admin_driver = if let Some(pwd) = password.clone() {
        PgDriver::connect_with_password(&host, port, &user, "postgres", &pwd)
            .await
            .map_err(|e| anyhow!("Failed to connect to postgres: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, "postgres")
            .await
            .map_err(|e| anyhow!("Failed to connect to postgres: {}", e))?
    };

    let check_cmd = Qail::get("pg_database")
        .column("datname")
        .where_eq("datname", state.shadow_name.clone());

    let existing = admin_driver
        .fetch_all(&check_cmd)
        .await
        .map_err(|e| anyhow!("Failed to check existing database: {}", e))?;

    if !existing.is_empty() {
        println!("    {} Shadow database already exists", "⚠".yellow());
    } else {
        // Note: CREATE DATABASE cannot be in a transaction, using bootstrap DDL
        let create_ddl = format!("CREATE DATABASE {}", state.shadow_name);
        admin_driver
            .execute_raw(&create_ddl)
            .await
            .map_err(|e| anyhow!("Failed to create shadow database: {}", e))?;

        println!("    {} Created", "✓".green());
    }

    Ok(state)
}

/// Apply migrations to shadow database
pub async fn apply_migrations_to_shadow(state: &mut ShadowState, cmds: &[Qail]) -> Result<()> {
    println!("  {} Applying migration to shadow...", "[2/4]".cyan());

    let (host, port, user, password, _) = parse_pg_url(&state.primary_url)?;

    let mut shadow_driver = if let Some(pwd) = password {
        PgDriver::connect_with_password(&host, port, &user, &state.shadow_name, &pwd)
            .await
            .map_err(|e| anyhow!("Failed to connect to shadow: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, &state.shadow_name)
            .await
            .map_err(|e| anyhow!("Failed to connect to shadow: {}", e))?
    };

    for (i, cmd) in cmds.iter().enumerate() {
        shadow_driver
            .execute(cmd)
            .await
            .map_err(|e| anyhow!("Migration {} failed on shadow: {}", i + 1, e))?;
    }

    println!("    {} {} migrations applied", "✓".green(), cmds.len());

    Ok(())
}

/// Sync data from primary to shadow using COPY streaming (zero-dependency).
/// Uses COPY TO STDOUT → raw bytes → COPY FROM STDIN for maximum performance.
pub async fn sync_data_to_shadow(state: &mut ShadowState) -> Result<()> {
    println!(
        "  {} Syncing data from primary to shadow...",
        "[3/4]".cyan()
    );

    let (host, port, user, password, database) = parse_pg_url(&state.primary_url)?;

    // Connect to primary
    let mut primary_driver = if let Some(pwd) = password.clone() {
        PgDriver::connect_with_password(&host, port, &user, &database, &pwd)
            .await
            .map_err(|e| anyhow!("Failed to connect to primary: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, &database)
            .await
            .map_err(|e| anyhow!("Failed to connect to primary: {}", e))?
    };

    // Connect to shadow
    let mut shadow_driver = if let Some(pwd) = password {
        PgDriver::connect_with_password(&host, port, &user, &state.shadow_name, &pwd)
            .await
            .map_err(|e| anyhow!("Failed to connect to shadow: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, &state.shadow_name)
            .await
            .map_err(|e| anyhow!("Failed to connect to shadow: {}", e))?
    };

    // Get list of tables in SHADOW (not primary, since shadow may have different schema)
    use qail_core::ast::Operator;
    let tables_cmd = Qail::get("information_schema.tables")
        .column("table_name")
        .filter("table_schema", Operator::Eq, "public")
        .filter("table_type", Operator::Eq, "BASE TABLE");

    let table_rows = shadow_driver
        .fetch_all(&tables_cmd)
        .await
        .map_err(|e| anyhow!("Failed to list shadow tables: {}", e))?;

    let tables: Vec<String> = table_rows
        .iter()
        .filter_map(|r| r.get_string(0))
        .filter(|t| !t.starts_with("_qail")) // Skip internal tables
        .collect();

    state.tables_synced = tables.len() as u64;

    for table in &tables {
        // Get column names for this table in shadow
        let cols_cmd = Qail::get("information_schema.columns")
            .column("column_name")
            .filter("table_schema", Operator::Eq, "public")
            .filter("table_name", Operator::Eq, table.clone());
        
        let col_rows = shadow_driver
            .fetch_all(&cols_cmd)
            .await
            .map_err(|e| anyhow!("Failed to get columns for {}: {}", table, e))?;
        
        let shadow_columns: Vec<String> = col_rows
            .iter()
            .filter_map(|r| r.get_string(0))
            .collect();
        
        if shadow_columns.is_empty() {
            continue;
        }
        
        // Check if table exists in primary (it might not after migration diff)
        let check_cmd = Qail::get("information_schema.tables")
            .column("table_name")
            .filter("table_schema", Operator::Eq, "public")
            .filter("table_name", Operator::Eq, table.clone());
        
        let exists = primary_driver
            .fetch_all(&check_cmd)
            .await
            .map_err(|e| anyhow!("Failed to check table {} in primary: {}", table, e))?;
        
        if exists.is_empty() {
            // Table doesn't exist in primary (new table in migration)
            println!("    {} {} (new table, no data)", "⊕".blue(), table.cyan());
            continue;
        }
        
        // Get columns that exist in PRIMARY to find intersection
        let primary_cols_cmd = Qail::get("information_schema.columns")
            .column("column_name")
            .filter("table_schema", Operator::Eq, "public")
            .filter("table_name", Operator::Eq, table.clone());
        
        let primary_col_rows = primary_driver
            .fetch_all(&primary_cols_cmd)
            .await
            .map_err(|e| anyhow!("Failed to get primary columns for {}: {}", table, e))?;
        
        let primary_columns: std::collections::HashSet<String> = primary_col_rows
            .iter()
            .filter_map(|r| r.get_string(0))
            .collect();
        
        // Use intersection: columns that exist in BOTH shadow AND primary
        let columns: Vec<String> = shadow_columns
            .into_iter()
            .filter(|c| primary_columns.contains(c))
            .collect();
        
        if columns.is_empty() {
            println!("    {} {} (no common columns)", "⊕".blue(), table.cyan());
            continue;
        }
        
        // Use COPY streaming: export from primary, import to shadow
        let copy_data = primary_driver
            .copy_export_table(table, &columns)
            .await
            .map_err(|e| anyhow!("Failed to export {}: {}", table, e))?;
        
        let row_count = copy_data.iter().filter(|&&b| b == b'\n').count();
        
        if !copy_data.is_empty() {
            // Build Qail::Add for copy_bulk_bytes
            let mut add_cmd = Qail::add(table);
            for col in &columns {
                add_cmd = add_cmd.column(col);
            }
            
            shadow_driver
                .copy_bulk_bytes(&add_cmd, &copy_data)
                .await
                .map_err(|e| anyhow!("Failed to import {}: {}", table, e))?;
        }
        
        state.rows_synced += row_count as u64;
        println!("    {} {} ({} rows)", "✓".green(), table.cyan(), row_count);
    }

    println!(
        "    {} Synced {} tables, {} rows",
        "✓".green().bold(),
        state.tables_synced,
        state.rows_synced
    );

    Ok(())
}

/// Display shadow status and available commands
pub fn display_shadow_status(state: &ShadowState) {
    println!("  {} Shadow ready for validation", "[4/4]".cyan());
    println!();
    println!("{}", "━".repeat(40).dimmed());
    println!("  Shadow URL: {}", state.shadow_url.yellow());
    println!(
        "  Tables: {}, Rows: {}",
        state.tables_synced.to_string().cyan(),
        state.rows_synced.to_string().cyan()
    );
    println!();
    println!("  {}", "Available Commands:".bold());
    println!(
        "    {} → Run tests against shadow",
        "qail shadow test".green()
    );
    println!(
        "    {} → Switch traffic to shadow",
        "qail shadow promote".green().bold()
    );
    println!(
        "    {} → Drop shadow, keep primary",
        "qail shadow abort".red()
    );
    println!();
}

/// Promote shadow to primary (Option B: apply migration to primary, then cleanup)
/// 
/// Workflow:
/// 1. Load diff commands from _qail_shadow_state table
/// 2. Apply migration to PRIMARY database (not swap!)
/// 3. Drop shadow database
/// 4. Update state: status = 'promoted'
pub async fn promote_shadow(primary_url: &str) -> Result<()> {
    let state = ShadowState::new(primary_url)?;

    println!();
    println!("{}", "🚀 Promoting Shadow to Primary".green().bold());
    println!("{}", "━".repeat(40).dimmed());

    let (host, port, user, password, database) = parse_pg_url(primary_url)?;

    // Connect to primary to load state
    let mut primary_driver = if let Some(pwd) = password.clone() {
        PgDriver::connect_with_password(&host, port, &user, &database, &pwd)
            .await
            .map_err(|e| anyhow!("Failed to connect to primary: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, &database)
            .await
            .map_err(|e| anyhow!("Failed to connect to primary: {}", e))?
    };

    // Load stored state (diff commands)
    println!("  [1/4] Loading migration state...");
    let state_option = load_shadow_state(&mut primary_driver).await?;
    
    let (_, diff_cmds) = state_option
        .ok_or_else(|| anyhow!("No pending shadow migration found. Run 'qail migrate shadow' first."))?;
    
    println!("    {} {} migration commands loaded", "✓".green(), diff_cmds.len());

    // Data Drift Warning (documented edge case)
    println!();
    println!("    {} Changes on primary since shadow sync may cause failure.", "⚠️".yellow());
    println!();
    
    // Apply migration to PRIMARY
    println!("  [2/4] Applying migration to primary...");
    for (i, cmd) in diff_cmds.iter().enumerate() {
        primary_driver
            .execute(cmd)
            .await
            .map_err(|e| anyhow!("Migration {} failed on primary: {} (cmd: {:?})", i + 1, e, cmd.action))?;
    }
    println!("    {} {} migrations applied to primary", "✓".green(), diff_cmds.len());

    // Drop shadow database
    println!("  [3/4] Dropping shadow database...");
    let mut admin_driver = if let Some(pwd) = password {
        PgDriver::connect_with_password(&host, port, &user, "postgres", &pwd)
            .await
            .map_err(|e| anyhow!("Failed to connect to postgres: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, "postgres")
            .await
            .map_err(|e| anyhow!("Failed to connect to postgres: {}", e))?
    };

    let drop_ddl = format!("DROP DATABASE IF EXISTS {}", state.shadow_name);
    admin_driver
        .execute_raw(&drop_ddl)
        .await
        .map_err(|e| anyhow!("Failed to drop shadow: {}", e))?;
    println!("    {} Shadow database dropped", "✓".green());

    // Update state: promoted
    println!("  [4/4] Updating migration status...");
    update_shadow_state_status(&mut primary_driver, "promoted").await?;
    println!("    {} Status: promoted", "✓".green());

    println!();
    println!("{}", "✓ Shadow promoted successfully!".green().bold());
    println!("  Migration applied to: {}", database.cyan());
    println!("  Shadow {} dropped", state.shadow_name.dimmed());

    Ok(())
}

/// Abort shadow migration (drop shadow database)
pub async fn abort_shadow(primary_url: &str) -> Result<()> {
    let state = ShadowState::new(primary_url)?;

    println!();
    println!("{}", "🛑 Aborting Shadow Migration".red().bold());
    println!("{}", "━".repeat(40).dimmed());

    let (host, port, user, password, database) = parse_pg_url(primary_url)?;

    // Connect to postgres for admin operations
    let mut admin_driver = if let Some(pwd) = password.clone() {
        PgDriver::connect_with_password(&host, port, &user, "postgres", &pwd)
            .await
            .map_err(|e| anyhow!("Failed to connect to postgres: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, "postgres")
            .await
            .map_err(|e| anyhow!("Failed to connect to postgres: {}", e))?
    };

    println!("  Dropping shadow database: {}", state.shadow_name.yellow());

    let drop_ddl = format!("DROP DATABASE IF EXISTS {}", state.shadow_name);
    admin_driver
        .execute_raw(&drop_ddl)
        .await
        .map_err(|e| anyhow!("Failed to drop shadow: {}", e))?;

    // Update state: aborted
    let mut primary_driver = if let Some(pwd) = password {
        PgDriver::connect_with_password(&host, port, &user, &database, &pwd)
            .await
            .map_err(|e| anyhow!("Failed to connect to primary: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, &database)
            .await
            .map_err(|e| anyhow!("Failed to connect to primary: {}", e))?
    };
    
    let _ = update_shadow_state_status(&mut primary_driver, "aborted").await;

    println!(
        "{}",
        "✓ Shadow database dropped. Primary unchanged.".green()
    );

    Ok(())
}

pub async fn run_shadow_migration(
    primary_url: &str, 
    old_cmds: &[Qail], 
    diff_cmds: &[Qail],
    old_path: &str,
    new_path: &str,
) -> Result<ShadowState> {
    let mut state = create_shadow_database(primary_url).await?;

    // Step 1: Apply OLD schema to create base tables
    apply_base_schema_to_shadow(&mut state, old_cmds).await?;
    
    // Step 2: Apply DIFF commands (migrations)
    apply_migrations_to_shadow(&mut state, diff_cmds).await?;

    sync_data_to_shadow(&mut state).await?;

    // Step 3: Save state for promote/abort (Enterprise feature)
    let (host, port, user, password, database) = parse_pg_url(primary_url)?;
    let mut primary_driver = if let Some(pwd) = password {
        PgDriver::connect_with_password(&host, port, &user, &database, &pwd)
            .await
            .map_err(|e| anyhow!("Failed to connect to primary: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, &database)
            .await
            .map_err(|e| anyhow!("Failed to connect to primary: {}", e))?
    };
    
    save_shadow_state(&mut primary_driver, &state, diff_cmds, old_path, new_path).await?;

    state.is_ready = true;

    display_shadow_status(&state);

    Ok(state)
}

/// Apply base schema to shadow (CREATE TABLEs from old.qail)
async fn apply_base_schema_to_shadow(state: &mut ShadowState, cmds: &[Qail]) -> Result<()> {
    println!("  {} Applying base schema to shadow...", "[1.5/4]".cyan());

    let (host, port, user, password, _) = parse_pg_url(&state.primary_url)?;

    let mut shadow_driver = if let Some(pwd) = password {
        PgDriver::connect_with_password(&host, port, &user, &state.shadow_name, &pwd)
            .await
            .map_err(|e| anyhow!("Failed to connect to shadow: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, &state.shadow_name)
            .await
            .map_err(|e| anyhow!("Failed to connect to shadow: {}", e))?
    };

    for (i, cmd) in cmds.iter().enumerate() {
        shadow_driver
            .execute(cmd)
            .await
            .map_err(|e| anyhow!("Base schema {} failed on shadow: {}", i + 1, e))?;
    }

    println!("    {} {} tables/indexes created", "✓".green(), cmds.len());

    Ok(())
}
