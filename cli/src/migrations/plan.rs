//! Migration plan (dry-run)

use anyhow::Result;
use colored::*;
use qail_core::migrate::{diff_schemas, parse_qail};

use crate::sql_gen::{cmd_to_sql, generate_rollback_sql};

/// Preview migration SQL without executing (dry-run).
pub fn migrate_plan(schema_diff_path: &str, output: Option<&str>) -> Result<()> {
    println!("{}", "📋 Migration Plan (dry-run)".cyan().bold());
    println!();

    let cmds = if schema_diff_path.contains(':') && !schema_diff_path.starts_with("postgres") {
        let parts: Vec<&str> = schema_diff_path.splitn(2, ':').collect();
        let old_path = parts[0];
        let new_path = parts[1];

        println!("  {} → {}", old_path.yellow(), new_path.yellow());
        println!();

        let old_content = std::fs::read_to_string(old_path)
            .map_err(|e| anyhow::anyhow!("Failed to read old schema: {}", e))?;
        let new_content = std::fs::read_to_string(new_path)
            .map_err(|e| anyhow::anyhow!("Failed to read new schema: {}", e))?;

        let old_schema = parse_qail(&old_content)
            .map_err(|e| anyhow::anyhow!("Failed to parse old schema: {}", e))?;
        let new_schema = parse_qail(&new_content)
            .map_err(|e| anyhow::anyhow!("Failed to parse new schema: {}", e))?;

        diff_schemas(&old_schema, &new_schema)
    } else {
        return Err(anyhow::anyhow!(
            "Please provide two .qail files: old.qail:new.qail"
        ));
    };

    if cmds.is_empty() {
        println!(
            "{}",
            "✓ No migrations needed - schemas are identical".green()
        );
        return Ok(());
    }

    let mut up_sql = Vec::new();
    let mut down_sql = Vec::new();

    println!(
        "┌─ {} ({} operations) ─────────────────────────────────┐",
        "UP".green().bold(),
        cmds.len()
    );
    for (i, cmd) in cmds.iter().enumerate() {
        let sql = cmd_to_sql(cmd);
        println!("│ {}. {}", i + 1, sql.cyan());
        up_sql.push(format!("{}. {}", i + 1, sql));

        let rollback = generate_rollback_sql(cmd);
        down_sql.push(format!("{}. {}", i + 1, rollback));
    }
    println!("└──────────────────────────────────────────────────────────────┘");
    println!();

    println!(
        "┌─ {} ({} operations) ──────────────────────────────┐",
        "DOWN".yellow().bold(),
        cmds.len()
    );
    for sql in &down_sql {
        println!("│ {}", sql.yellow());
    }
    println!("└──────────────────────────────────────────────────────────────┘");

    if let Some(path) = output {
        let mut content = String::new();
        content.push_str("-- Migration UP\n");
        for cmd in &cmds {
            content.push_str(&format!("{};\n", cmd_to_sql(cmd)));
        }
        content.push_str("\n-- Migration DOWN (rollback)\n");
        for (i, cmd) in cmds.iter().enumerate() {
            content.push_str(&format!("-- {}. {};\n", i + 1, generate_rollback_sql(cmd)));
        }
        std::fs::write(path, &content)?;
        println!();
        println!("{} {}", "Saved to:".green(), path);
    }

    println!();
    println!(
        "{} Run 'qail migrate up old.qail:new.qail <URL>' to apply",
        "💡".yellow()
    );

    Ok(())
}
