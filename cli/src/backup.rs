//! Backup utilities for safe migrations.
//!
//! Provides pre-migration impact analysis and snapshot creation.

use crate::colors::*;
use anyhow::{Result, anyhow};
use qail_core::ast::{Action, Expr, Operator, Qail};
use qail_pg::driver::PgDriver;
use std::path::PathBuf;

use crate::migrations::types::is_safe_cast;

/// Impact analysis result for a migration command
#[derive(Debug, Default)]
pub struct MigrationImpact {
    pub table: String,
    /// Type of operation
    pub operation: String,
    pub rows_affected: u64,
    /// Columns being dropped (if any)
    pub dropped_columns: Vec<String>,
    pub is_destructive: bool,
}

/// Analyze the impact of a migration command
pub async fn analyze_impact(driver: &mut PgDriver, cmd: &Qail) -> Result<MigrationImpact> {
    let mut impact = MigrationImpact {
        table: cmd.table.clone(),
        operation: format!("{:?}", cmd.action),
        ..Default::default()
    };

    match cmd.action {
        Action::Drop => {
            // DROP TABLE - count all rows
            impact.operation = "DROP TABLE".to_string();
            impact.is_destructive = true;
            impact.rows_affected = count_table_rows(driver, &cmd.table).await?;
        }
        Action::AlterDrop => {
            // DROP COLUMN - count rows with non-null values
            impact.operation = "DROP COLUMN".to_string();
            impact.is_destructive = true;

            for col in &cmd.columns {
                if let Expr::Named(name) = col {
                    impact.dropped_columns.push(name.clone());
                    impact.rows_affected += count_column_values(driver, &cmd.table, name).await?;
                }
            }
        }
        Action::AlterType => {
            impact.operation = "ALTER TYPE".to_string();

            if let Some((column, target_type)) = alter_type_target(cmd)
                && let Some(source_type) = column_data_type(driver, &cmd.table, &column).await?
                    && is_narrowing_type_change(&source_type, &target_type)
                {
                    impact.operation =
                        format!("ALTER TYPE (narrowing {} -> {})", source_type, target_type);
                    impact.is_destructive = true;
                    impact.rows_affected = count_table_rows(driver, &cmd.table).await?;
                }
        }
        Action::AlterSetNotNull => {
            impact.operation = "ALTER SET NOT NULL".to_string();
            let table_rows = count_table_rows(driver, &cmd.table).await?;
            if table_rows > 0 {
                impact.is_destructive = true;
                impact.rows_affected = table_rows;
            }
        }
        Action::Alter => {
            // ALTER TABLE (add column is usually safe)
            impact.operation = "ALTER TABLE".to_string();
            impact.is_destructive = false;
        }
        Action::Make => {
            // CREATE TABLE is safe
            impact.operation = "CREATE TABLE".to_string();
            impact.is_destructive = false;
        }
        _ => {}
    }

    Ok(impact)
}

/// Count rows in a table using AST-native query
async fn count_table_rows(driver: &mut PgDriver, table: &str) -> Result<u64> {
    // SELECT COUNT(*) FROM table (using AST)
    let cmd = Qail::get(table).column("count(*)");

    let rows = driver
        .fetch_all(&cmd)
        .await
        .map_err(|e| anyhow!("Failed to count rows: {}", e))?;

    if let Some(row) = rows.first()
        && let Some(count_str) = row.get_string(0)
    {
        return Ok(count_str.parse().unwrap_or(0));
    }

    Ok(0)
}

/// Count non-null values in a column using AST-native query
async fn count_column_values(driver: &mut PgDriver, table: &str, column: &str) -> Result<u64> {
    // SELECT COUNT(column) FROM table WHERE column IS NOT NULL
    let cmd = Qail::get(table).column(format!("count({})", column));

    let rows = driver
        .fetch_all(&cmd)
        .await
        .map_err(|e| anyhow!("Failed to count column values: {}", e))?;

    if let Some(row) = rows.first()
        && let Some(count_str) = row.get_string(0)
    {
        return Ok(count_str.parse().unwrap_or(0));
    }

    Ok(0)
}

fn alter_type_target(cmd: &Qail) -> Option<(String, String)> {
    match cmd.columns.first() {
        Some(Expr::Def {
            name,
            data_type,
            constraints: _,
        }) => Some((name.clone(), normalize_type_for_cast(data_type))),
        _ => None,
    }
}

fn normalize_type_for_cast(raw: &str) -> String {
    match raw.trim().to_ascii_lowercase().as_str() {
        "character varying" => "VARCHAR".to_string(),
        "character" => "CHAR".to_string(),
        "timestamp with time zone" => "TIMESTAMPTZ".to_string(),
        "timestamp without time zone" => "TIMESTAMP".to_string(),
        "double precision" => "DOUBLE PRECISION".to_string(),
        "boolean" => "BOOLEAN".to_string(),
        "integer" => "INT".to_string(),
        "bigint" => "BIGINT".to_string(),
        "smallint" => "SMALLINT".to_string(),
        "numeric" => "NUMERIC".to_string(),
        "uuid" => "UUID".to_string(),
        "text" => "TEXT".to_string(),
        "date" => "DATE".to_string(),
        "time without time zone" => "TIME".to_string(),
        "time with time zone" => "TIMETZ".to_string(),
        other => other.to_ascii_uppercase(),
    }
}

fn is_narrowing_type_change(source: &str, target: &str) -> bool {
    !is_safe_cast(source, target)
}

async fn column_data_type(
    driver: &mut PgDriver,
    table: &str,
    column: &str,
) -> Result<Option<String>> {
    let cmd = Qail::get("information_schema.columns")
        .column("data_type")
        .filter("table_schema", Operator::Eq, "public")
        .filter("table_name", Operator::Eq, table.to_string())
        .filter("column_name", Operator::Eq, column.to_string())
        .limit(1);
    let rows = driver
        .fetch_all(&cmd)
        .await
        .map_err(|e| anyhow!("Failed to inspect type for {}.{}: {}", table, column, e))?;

    Ok(rows
        .first()
        .and_then(|row| row.get_string(0))
        .map(|raw| normalize_type_for_cast(&raw)))
}

/// Display impact analysis to user
pub fn display_impact(impacts: &[MigrationImpact]) {
    let destructive: Vec<_> = impacts.iter().filter(|i| i.is_destructive).collect();

    if destructive.is_empty() {
        println!("{}", "✓ No destructive operations detected".green());
        return;
    }

    println!();
    println!("{}", "🚨 Migration Impact Analysis".red().bold());
    println!("{}", "━".repeat(40).dimmed());

    let mut total_rows = 0u64;

    for impact in &destructive {
        let op_colored = if impact.operation == "DROP TABLE" {
            impact.operation.red().bold()
        } else if impact.operation == "DROP COLUMN"
            || impact.operation == "ALTER SET NOT NULL"
            || impact.operation.starts_with("ALTER TYPE (narrowing")
        {
            impact.operation.yellow().bold()
        } else {
            Painted {
                text: impact.operation.clone(),
                prefix: String::new(),
            }
        };

        if !impact.dropped_columns.is_empty() {
            for col in &impact.dropped_columns {
                println!(
                    "  {} {}.{} → {} values at risk",
                    op_colored,
                    impact.table.cyan(),
                    col.yellow(),
                    impact.rows_affected.to_string().red().bold()
                );
            }
        } else {
            println!(
                "  {} {} → {} rows affected",
                op_colored,
                impact.table.cyan(),
                impact.rows_affected.to_string().red().bold()
            );
        }

        total_rows += impact.rows_affected;
    }

    println!("{}", "━".repeat(40).dimmed());
    println!(
        "  Total: {} records at risk",
        total_rows.to_string().red().bold()
    );
    println!();
}

/// User choice for migration
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MigrationChoice {
    Proceed,
    BackupToFile,
    BackupToDatabase,
    Cancel,
}

/// Prompt user for migration choice
pub fn prompt_migration_choice() -> MigrationChoice {
    println!("Choose an option:");
    println!("  {} Proceed (I have my own backup)", "[1]".cyan());
    println!("  {} Backup to files (_qail_snapshots/)", "[2]".green());
    println!(
        "  {} Backup to database (with rollback support)",
        "[3]".green().bold()
    );
    println!("  {} Cancel migration", "[4]".red());
    print!("> ");

    // Flush stdout
    use std::io::Write;
    std::io::stdout().flush().ok();

    let mut input = String::new();
    if std::io::stdin().read_line(&mut input).is_ok() {
        match input.trim() {
            "1" => return MigrationChoice::Proceed,
            "2" => return MigrationChoice::BackupToFile,
            "3" => return MigrationChoice::BackupToDatabase,
            "4" | "" => return MigrationChoice::Cancel,
            _ => {}
        }
    }

    MigrationChoice::Cancel
}

/// Create snapshot directory
fn ensure_snapshot_dir() -> Result<PathBuf> {
    let dir = PathBuf::from("_qail_snapshots");
    if !dir.exists() {
        std::fs::create_dir_all(&dir)?;
    }
    Ok(dir)
}

/// Backup a table to CSV file using COPY protocol (AST-native)
pub async fn backup_table(driver: &mut PgDriver, table: &str) -> Result<PathBuf> {
    let snapshot_dir = ensure_snapshot_dir()?;
    let timestamp = crate::time::timestamp_filename();
    let filename = format!("{}_{}.csv", timestamp, table);
    let path = snapshot_dir.join(&filename);

    // Use fetch_all for backup
    let cmd = Qail::get(table);

    let rows = driver
        .fetch_all(&cmd)
        .await
        .map_err(|e| anyhow!("Failed to export table {}: {}", table, e))?;

    // Write to file as TSV
    let mut content = String::new();
    for row in rows {
        let line: Vec<String> = (0..10) // Assume max 10 columns
            .filter_map(|i| row.get_string(i))
            .collect();
        if !line.is_empty() {
            content.push_str(&line.join("\t"));
            content.push('\n');
        }
    }

    std::fs::write(&path, content)?;

    Ok(path)
}

/// Backup specific columns from a table
pub async fn backup_columns(
    driver: &mut PgDriver,
    table: &str,
    columns: &[String],
) -> Result<PathBuf> {
    let snapshot_dir = ensure_snapshot_dir()?;
    let timestamp = crate::time::timestamp_filename();
    let col_names = columns.join("_");
    let filename = format!("{}_{}_{}.csv", timestamp, table, col_names);
    let path = snapshot_dir.join(&filename);

    // Assuming 'id' is common primary key - this is a simplification
    let mut cols: Vec<&str> = vec!["id"];
    cols.extend(columns.iter().map(|s| s.as_str()));

    let cols_len = cols.len();
    let cmd = Qail::get(table).columns(cols);

    let rows = driver
        .fetch_all(&cmd)
        .await
        .map_err(|e| anyhow!("Failed to export columns from {}: {}", table, e))?;

    // Write to file as TSV
    let mut content = String::new();
    for row in rows {
        let line: Vec<String> = (0..cols_len).filter_map(|i| row.get_string(i)).collect();
        if !line.is_empty() {
            content.push_str(&line.join("\t"));
            content.push('\n');
        }
    }

    std::fs::write(&path, content)?;

    Ok(path)
}

/// Create snapshots for all destructive operations
pub async fn create_snapshots(
    driver: &mut PgDriver,
    impacts: &[MigrationImpact],
) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();

    println!();
    println!("{}", "📦 Creating snapshots...".cyan().bold());

    for impact in impacts {
        if !impact.is_destructive {
            continue;
        }

        let path = if impact.operation == "DROP TABLE" {
            backup_table(driver, &impact.table).await?
        } else if !impact.dropped_columns.is_empty() {
            backup_columns(driver, &impact.table, &impact.dropped_columns).await?
        } else {
            continue;
        };

        println!(
            "  {} {} → {}",
            "✓".green(),
            format!("{}.{}", impact.table, impact.dropped_columns.join(",")).cyan(),
            path.display().to_string().dimmed()
        );

        paths.push(path);
    }

    println!("  {}", "Done".green().bold());
    println!();

    Ok(paths)
}

// =============================================================================
// Phase 2: Database-Stored Snapshots with JSONB
// =============================================================================

/// Schema for data snapshots table (QAIL format)
pub const DATA_SNAPSHOTS_SCHEMA: &str = r#"
table _qail_data_snapshots (
    id serial primary_key,
    migration_version varchar(255) not null,
    table_name varchar(255) not null,
    column_name varchar(255),
    row_id text not null,
    value_json jsonb not null,
    snapshot_type varchar(50) not null,
    created_at timestamptz default NOW()
)
"#;

/// Generate data snapshots table DDL
pub fn data_snapshots_ddl() -> String {
    use qail_core::parser::schema::Schema;
    Schema::parse(DATA_SNAPSHOTS_SCHEMA)
        .expect("Invalid data snapshots schema")
        .tables
        .first()
        .expect("No table in snapshots schema")
        .to_ddl()
}

/// Ensure data snapshots table exists
pub async fn ensure_snapshots_table(driver: &mut PgDriver) -> Result<()> {
    driver
        .execute_raw(&data_snapshots_ddl())
        .await
        .map_err(|e| anyhow!("Failed to create data snapshots table: {}", e))?;
    Ok(())
}

/// Snapshot type for different backup scenarios
#[derive(Debug, Clone, Copy)]
pub enum SnapshotType {
    DropTable,
    DropColumn,
    AlterColumn,
}

impl std::fmt::Display for SnapshotType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotType::DropTable => write!(f, "DROP_TABLE"),
            SnapshotType::DropColumn => write!(f, "DROP_COLUMN"),
            SnapshotType::AlterColumn => write!(f, "ALTER_COLUMN"),
        }
    }
}

/// Create database-stored snapshot for a dropped column (Phase 2)
pub async fn snapshot_column_to_db(
    driver: &mut PgDriver,
    migration_version: &str,
    table: &str,
    column: &str,
) -> Result<u64> {
    // Ensure snapshots table exists
    ensure_snapshots_table(driver).await?;

    // Fetch all rows with id and column value
    let cmd = Qail::get(table).columns(["id", column]);
    let rows = driver
        .fetch_all(&cmd)
        .await
        .map_err(|e| anyhow!("Failed to fetch column data: {}", e))?;

    let mut saved = 0u64;

    for row in rows {
        let row_id = row.get_string(0).unwrap_or_default();
        let value = row.get_string(1);

        if let Some(val) = value {
            // Insert snapshot record
            let snapshot_cmd = Qail::add("_qail_data_snapshots")
                .columns([
                    "migration_version",
                    "table_name",
                    "column_name",
                    "row_id",
                    "value_json",
                    "snapshot_type",
                ])
                .values([
                    migration_version.to_string(),
                    table.to_string(),
                    column.to_string(),
                    row_id,
                    format!("\"{}\"", val.replace('"', "\\\"")), // JSON string
                    SnapshotType::DropColumn.to_string(),
                ]);

            driver
                .execute(&snapshot_cmd)
                .await
                .map_err(|e| anyhow!("Failed to save snapshot: {}", e))?;

            saved += 1;
        }
    }

    Ok(saved)
}

/// Create database-stored snapshot for a dropped table (Phase 2)
pub async fn snapshot_table_to_db(
    driver: &mut PgDriver,
    migration_version: &str,
    table: &str,
) -> Result<u64> {
    // Ensure snapshots table exists
    ensure_snapshots_table(driver).await?;

    // Fetch all rows as JSON
    let cmd = Qail::get(table);
    let rows = driver
        .fetch_all(&cmd)
        .await
        .map_err(|e| anyhow!("Failed to fetch table data: {}", e))?;

    let mut saved = 0u64;

    for (idx, row) in rows.iter().enumerate() {
        // Try to get row ID from first column, or use index
        let row_id = row.get_string(0).unwrap_or_else(|| idx.to_string());

        let mut json_parts = Vec::new();
        for i in 0..20 {
            // Max 20 columns
            if let Some(val) = row.get_string(i) {
                json_parts.push(format!("\"col_{}\": \"{}\"", i, val.replace('"', "\\\"")));
            }
        }
        let value_json = format!("{{{}}}", json_parts.join(", "));

        // Insert snapshot record
        let snapshot_cmd = Qail::add("_qail_data_snapshots")
            .columns([
                "migration_version",
                "table_name",
                "row_id",
                "value_json",
                "snapshot_type",
            ])
            .values([
                migration_version.to_string(),
                table.to_string(),
                row_id,
                value_json,
                SnapshotType::DropTable.to_string(),
            ]);

        driver
            .execute(&snapshot_cmd)
            .await
            .map_err(|e| anyhow!("Failed to save table snapshot: {}", e))?;

        saved += 1;
    }

    Ok(saved)
}

/// Create database snapshots for all destructive operations (Phase 2)
pub async fn create_db_snapshots(
    driver: &mut PgDriver,
    migration_version: &str,
    impacts: &[MigrationImpact],
) -> Result<u64> {
    let mut total_saved = 0u64;

    println!();
    println!(
        "{}",
        "💾 Creating database snapshots (Phase 2)...".cyan().bold()
    );

    for impact in impacts {
        if !impact.is_destructive {
            continue;
        }

        let saved = if impact.operation == "DROP TABLE" {
            let count = snapshot_table_to_db(driver, migration_version, &impact.table).await?;
            println!(
                "  {} {} → {} rows saved to _qail_data_snapshots",
                "✓".green(),
                impact.table.cyan(),
                count.to_string().green()
            );
            count
        } else if !impact.dropped_columns.is_empty() {
            let mut col_saved = 0u64;
            for col in &impact.dropped_columns {
                let count =
                    snapshot_column_to_db(driver, migration_version, &impact.table, col).await?;
                println!(
                    "  {} {}.{} → {} values saved",
                    "✓".green(),
                    impact.table.cyan(),
                    col.yellow(),
                    count.to_string().green()
                );
                col_saved += count;
            }
            col_saved
        } else {
            0
        };

        total_saved += saved;
    }

    println!(
        "  {} Total: {} records backed up to database",
        "✓".green().bold(),
        total_saved.to_string().cyan()
    );
    println!();

    Ok(total_saved)
}

/// Restore column data from database snapshot
pub async fn restore_column_from_db(
    driver: &mut PgDriver,
    migration_version: &str,
    table: &str,
    column: &str,
) -> Result<u64> {
    use qail_core::ast::Operator;

    // Query snapshots for this migration/table/column
    let query_cmd = Qail::get("_qail_data_snapshots")
        .columns(["row_id", "value_json"])
        .filter("migration_version", Operator::Eq, migration_version)
        .filter("table_name", Operator::Eq, table)
        .filter("column_name", Operator::Eq, column);

    let rows = driver
        .fetch_all(&query_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query snapshots: {}", e))?;

    let mut restored = 0u64;

    for row in rows {
        let row_id = row.get_string(0).unwrap_or_default();
        let value_json = row.get_string(1).unwrap_or_default();

        let value = value_json.trim_matches('"').replace("\\\"", "\"");

        // Update the row
        let update_cmd = Qail::set(table)
            .set_value(column, value)
            .where_eq("id", row_id);

        if driver.execute(&update_cmd).await.is_ok() {
            restored += 1;
        }
    }

    Ok(restored)
}

/// List available snapshots for a migration version
pub async fn list_snapshots(
    driver: &mut PgDriver,
    migration_version: Option<&str>,
) -> Result<Vec<(String, String, String, u64)>> {
    use qail_core::ast::Operator;

    let mut cmd = Qail::get("_qail_data_snapshots").columns([
        "migration_version",
        "table_name",
        "column_name",
        "count(*)",
    ]);

    if let Some(version) = migration_version {
        cmd = cmd.filter("migration_version", Operator::Eq, version);
    }

    cmd = cmd.group_by(["migration_version", "table_name", "column_name"]);

    let rows = driver
        .fetch_all(&cmd)
        .await
        .map_err(|e| anyhow!("Failed to list snapshots: {}", e))?;

    let mut results = Vec::new();

    for row in rows {
        let version = row.get_string(0).unwrap_or_default();
        let table = row.get_string(1).unwrap_or_default();
        let column = row.get_string(2).unwrap_or_default();
        let count: u64 = row.get_string(3).unwrap_or_default().parse().unwrap_or(0);

        results.push((version, table, column, count));
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::{is_narrowing_type_change, normalize_type_for_cast};

    #[test]
    fn normalize_type_for_cast_maps_information_schema_names() {
        assert_eq!(normalize_type_for_cast("character varying"), "VARCHAR");
        assert_eq!(
            normalize_type_for_cast("timestamp with time zone"),
            "TIMESTAMPTZ"
        );
        assert_eq!(normalize_type_for_cast("integer"), "INT");
        assert_eq!(normalize_type_for_cast("text"), "TEXT");
    }

    #[test]
    fn narrowing_type_change_detection_uses_cast_safety() {
        assert!(is_narrowing_type_change("TEXT", "INT"));
        assert!(!is_narrowing_type_change("INT", "BIGINT"));
        assert!(!is_narrowing_type_change("INT", "TEXT"));
    }
}
