//! Migration DOWN (rollback) operations

use anyhow::Result;
use colored::*;
use qail_core::migrate::{diff_schemas, parse_qail};
use qail_core::prelude::{Action, Expr};
use qail_pg::driver::PgDriver;

use super::types::is_narrowing_type;
use crate::util::parse_pg_url;

/// Rollback migrations using qail-pg native driver.
pub async fn migrate_down(schema_diff_path: &str, url: &str) -> Result<()> {
    println!("{} {}", "Migrating DOWN:".cyan().bold(), url.yellow());

    // For rollback, user provides: current_schema:target_schema
    let cmds = if schema_diff_path.contains(':') && !schema_diff_path.starts_with("postgres") {
        let parts: Vec<&str> = schema_diff_path.splitn(2, ':').collect();
        let current_path = parts[0];
        let target_path = parts[1];

        let current_content = std::fs::read_to_string(current_path)
            .map_err(|e| anyhow::anyhow!("Failed to read current schema: {}", e))?;
        let target_content = std::fs::read_to_string(target_path)
            .map_err(|e| anyhow::anyhow!("Failed to read target schema: {}", e))?;

        let current_schema = parse_qail(&current_content)
            .map_err(|e| anyhow::anyhow!("Failed to parse current schema: {}", e))?;
        let target_schema = parse_qail(&target_content)
            .map_err(|e| anyhow::anyhow!("Failed to parse target schema: {}", e))?;

        diff_schemas(&current_schema, &target_schema)
    } else {
        println!("{}", "Warning: Rollback requires two .qail files".yellow());
        println!("  Use format: qail migrate down current.qail:target.qail <url>");
        return Ok(());
    };

    if cmds.is_empty() {
        println!("{}", "No rollbacks to apply.".green());
        return Ok(());
    }

    // Check for unsafe type casts (e.g., TEXT -> INT)
    let unsafe_type_changes: Vec<_> = cmds
        .iter()
        .filter(|cmd| cmd.action == Action::AlterType)
        .filter_map(|cmd| {
            if let Some(Expr::Def { name, data_type, .. }) = cmd.columns.first() {
                let target = data_type.as_str();
                if is_narrowing_type(target) {
                    Some(format!("{}.{} → {}", cmd.table, name, target))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    if !unsafe_type_changes.is_empty() {
        println!();
        println!(
            "{} {}",
            "⚠️ Unsafe type changes detected!".yellow().bold(),
            "Rollback may fail.".dimmed()
        );
        println!("{}", "━".repeat(50).dimmed());
        for change in &unsafe_type_changes {
            println!("  {} {}", "•".red(), change.yellow());
        }
        println!("{}", "━".repeat(50).dimmed());
        println!(
            "{}",
            "These type narrowing operations require explicit USING clause.".dimmed()
        );
        println!(
            "{}",
            "PostgreSQL cannot automatically cast TEXT → INT.".dimmed()
        );
        println!();
        print!("Continue anyway? [y/N] ");
        std::io::Write::flush(&mut std::io::stdout())?;

        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("{}", "Rollback cancelled.".yellow());
            return Ok(());
        }
    }

    println!("{} {} rollback(s) to apply", "Found:".cyan(), cmds.len());

    let (host, port, user, password, database) = parse_pg_url(url)?;
    let mut driver = if let Some(pwd) = password {
        PgDriver::connect_with_password(&host, port, &user, &database, &pwd)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, &database)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect: {}", e))?
    };

    // Begin transaction for atomic rollback
    println!("{}", "Starting transaction...".dimmed());
    driver
        .begin()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start transaction: {}", e))?;

    let mut applied = 0;
    for (i, cmd) in cmds.iter().enumerate() {
        println!(
            "  {} {} {}",
            format!("[{}/{}]", i + 1, cmds.len()).cyan(),
            format!("{}", cmd.action).yellow(),
            &cmd.table
        );

        if let Err(e) = driver.execute(cmd).await {
            println!("{}", "Rolling back transaction...".red());
            let _ = driver.rollback().await;
            return Err(anyhow::anyhow!(
                "Rollback failed at step {}/{}: {}\nTransaction rolled back - database unchanged.",
                i + 1,
                cmds.len(),
                e
            ));
        }
        applied += 1;
    }

    // Commit transaction
    driver
        .commit()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to commit transaction: {}", e))?;

    println!(
        "{}",
        format!("✓ {} rollbacks applied successfully (atomic)", applied)
            .green()
            .bold()
    );
    Ok(())
}
