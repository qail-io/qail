//! Exec module - Execute QAIL AST for seeding/admin tasks
//!
//! Type-safe execution using native QAIL AST - no raw SQL.
//!
//! # Example
//! ```bash
//! qail exec "add::users" --url postgres://...
//! qail exec -f seed.qail --url postgres://... --tx
//! qail exec "get::users" --dry-run
//! ```

use anyhow::Result;
use colored::*;
use qail_core::prelude::*;
use qail_core::transpiler::ToSql;
use qail_pg::PgDriver;

/// Configuration for exec command
pub struct ExecConfig {
    pub query: Option<String>,
    pub file: Option<String>,
    pub url: Option<String>,
    pub tx: bool,
    pub dry_run: bool,
}

/// Run the exec command (type-safe QAIL AST only)
pub async fn run_exec(config: ExecConfig) -> Result<()> {
    // Get content from file or inline
    let content = if let Some(file) = &config.file {
        std::fs::read_to_string(file)
            .map_err(|e| anyhow::anyhow!("Failed to read file '{}': {}", file, e))?
    } else if let Some(query) = &config.query {
        query.clone()
    } else {
        anyhow::bail!("Either QAIL query or --file must be provided");
    };

    // Parse QAIL statements (one per line)
    let lines: Vec<&str> = content
        .lines()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty() && !s.starts_with('#') && !s.starts_with("--"))
        .collect();

    if lines.is_empty() {
        println!("{}", "No QAIL statements to execute.".yellow());
        return Ok(());
    }

    // Parse all QAIL statements into ASTs
    let mut statements: Vec<Qail> = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let ast = qail_core::parse(line)
            .map_err(|e| anyhow::anyhow!("Parse error at line {}: {} in '{}'", i + 1, e, line))?;
        statements.push(ast);
    }

    println!(
        "{} Parsed {} QAIL statement(s)",
        "📋".cyan(),
        statements.len().to_string().green()
    );

    // Dry-run mode: show generated SQL
    if config.dry_run {
        println!("\n{}", "🔍 DRY-RUN MODE - Generated SQL:".yellow().bold());
        for (i, ast) in statements.iter().enumerate() {
            let sql = ast.to_sql();
            println!("\n{}{}:", "Statement ".dimmed(), (i + 1).to_string().cyan());
            println!("  {}", sql.white());
        }
        println!("\n{}", "No changes made.".yellow());
        return Ok(());
    }

    // Get database URL
    let db_url = if let Some(url) = &config.url {
        url.clone()
    } else {
        let config_path = std::path::Path::new("qail.toml");
        if config_path.exists() {
            let content = std::fs::read_to_string(config_path)?;
            let toml_config: toml::Value = toml::from_str(&content)?;
            toml_config
                .get("postgres")
                .and_then(|p| p.get("url"))
                .and_then(|u| u.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| anyhow::anyhow!("No postgres.url in qail.toml"))?
        } else {
            anyhow::bail!("No URL provided and qail.toml not found. Use --url or create qail.toml");
        }
    };

    // Connect to database
    println!("{} Connecting to database...", "🔌".cyan());
    let mut driver = PgDriver::connect_url(&db_url).await
        .map_err(|e| anyhow::anyhow!("Connection failed: {}", e))?;

    // Execute statements using type-safe AST
    let mut success_count = 0;
    let mut error_count = 0;

    if config.tx {
        println!("{} Starting transaction...", "🔒".cyan());
        driver.begin().await.map_err(|e| anyhow::anyhow!("BEGIN failed: {}", e))?;
    }

    for (i, ast) in statements.iter().enumerate() {
        let stmt_num = i + 1;
        print!("  {} Executing statement {}... ", "→".dimmed(), stmt_num);

        match driver.execute(ast).await {
            Ok(_) => {
                println!("{}", "✓".green());
                success_count += 1;
            }
            Err(e) => {
                println!("{} {}", "✗".red(), e.to_string().red());
                error_count += 1;

                if config.tx {
                    println!("{} Rolling back transaction...", "⚠️".yellow());
                    let _ = driver.rollback().await;
                    anyhow::bail!("Execution failed at statement {}: {}", stmt_num, e);
                }
            }
        }
    }

    if config.tx {
        println!("{} Committing transaction...", "🔓".cyan());
        driver.commit().await.map_err(|e| anyhow::anyhow!("COMMIT failed: {}", e))?;
    }

    // Summary
    println!();
    if error_count == 0 {
        println!(
            "{} All {} statement(s) executed successfully!",
            "✅".green(),
            success_count.to_string().green()
        );
    } else {
        println!(
            "{} {} succeeded, {} failed",
            "⚠️".yellow(),
            success_count.to_string().green(),
            error_count.to_string().red()
        );
    }

    Ok(())
}
