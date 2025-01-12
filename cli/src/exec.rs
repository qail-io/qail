//! Exec module - Execute QAIL AST for seeding/admin tasks
//!
//! Type-safe execution using native QAIL AST - no raw SQL.
//!
//! # Example
//! ```bash
//! qail exec "add users fields name, email values 'Alice', 'a@test.com'" --url postgres://...
//! qail exec -f seed.qail --url postgres://... --tx
//! qail exec "get users" --dry-run
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
    pub ssh: Option<String>,
    pub tx: bool,
    pub dry_run: bool,
}

/// SSH tunnel wrapper - kills tunnel on drop
struct SshTunnel {
    child: std::process::Child,
    local_port: u16,
}

impl SshTunnel {
    /// Create an SSH tunnel to a remote host
    /// Forwards local_port -> remote_host:remote_port via ssh_host
    async fn new(ssh_host: &str, remote_host: &str, remote_port: u16) -> Result<Self> {
        use std::process::{Command, Stdio};
        
        // Find available local port
        let local_port = Self::find_available_port()?;
        
        // Construct SSH tunnel command
        // ssh -N -L local_port:remote_host:remote_port ssh_host
        let child = Command::new("ssh")
            .args([
                "-N",  // No remote command
                "-L", &format!("{}:{}:{}", local_port, remote_host, remote_port),
                ssh_host,
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| anyhow::anyhow!("Failed to spawn SSH tunnel: {}", e))?;
        
        // Wait a moment for tunnel to establish
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        
        Ok(Self { child, local_port })
    }
    
    fn find_available_port() -> Result<u16> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        drop(listener);
        Ok(port)
    }
    
    fn local_port(&self) -> u16 {
        self.local_port
    }
}

impl Drop for SshTunnel {
    fn drop(&mut self) {
        // Kill the SSH tunnel process
        let _ = self.child.kill();
    }
}

fn split_qail_statements(content: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_triple_single = false;
    let mut in_triple_double = false;
    let mut chars = content.chars().peekable();
    
    while let Some(c) = chars.next() {
        // Check for triple quotes
        if c == '\'' && !in_triple_double {
            if chars.peek() == Some(&'\'') {
                chars.next();
                if chars.peek() == Some(&'\'') {
                    chars.next();
                    current.push_str("'''");
                    in_triple_single = !in_triple_single;
                    continue;
                } else {
                    current.push_str("''");
                    continue;
                }
            }
        } else if c == '"' && !in_triple_single {
            if chars.peek() == Some(&'"') {
                chars.next();
                if chars.peek() == Some(&'"') {
                    chars.next();
                    current.push_str("\"\"\"");
                    in_triple_double = !in_triple_double;
                    continue;
                } else {
                    current.push_str("\"\"");
                    continue;
                }
            }
        }
        
        // Handle newlines - statement boundary if not in multi-line string
        if c == '\n' && !in_triple_single && !in_triple_double {
            let trimmed = current.trim();
            if !trimmed.is_empty() && !trimmed.starts_with('#') && !trimmed.starts_with("--") {
                statements.push(current.trim().to_string());
            }
            current.clear();
            continue;
        }
        
        current.push(c);
    }
    
    // Don't forget the last statement
    let trimmed = current.trim();
    if !trimmed.is_empty() && !trimmed.starts_with('#') && !trimmed.starts_with("--") {
        statements.push(current.trim().to_string());
    }
    
    statements
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

    // Split into statements, handling multi-line strings
    let statements_str = split_qail_statements(&content);
    
    if statements_str.is_empty() {
        println!("{}", "No QAIL statements to execute.".yellow());
        return Ok(());
    }

    // Parse all QAIL statements into ASTs
    let mut statements: Vec<Qail> = Vec::new();
    for (i, stmt) in statements_str.iter().enumerate() {
        let ast = qail_core::parse(stmt)
            .map_err(|e| anyhow::anyhow!("Parse error at statement {}: {}", i + 1, e))?;
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

    // Set up SSH tunnel if requested
    let _tunnel: Option<SshTunnel>;
    let connect_url = if let Some(ssh_host) = &config.ssh {
        println!("{} Opening SSH tunnel to {}...", "🔐".cyan(), ssh_host.green());
        
        // Parse the URL to extract host and port
        let parsed = url::Url::parse(&db_url)
            .map_err(|e| anyhow::anyhow!("Invalid database URL: {}", e))?;
        
        let remote_host = parsed.host_str().unwrap_or("localhost");
        let remote_port = parsed.port().unwrap_or(5432);
        
        // Create tunnel
        let tunnel = SshTunnel::new(ssh_host, remote_host, remote_port).await?;
        let local_port = tunnel.local_port();
        
        // Rewrite URL to use tunnel
        let mut tunneled_url = parsed.clone();
        tunneled_url.set_host(Some("127.0.0.1")).ok();
        tunneled_url.set_port(Some(local_port)).ok();
        
        println!("{} Tunnel established: localhost:{} -> {}:{}", 
            "✓".green(), local_port, remote_host, remote_port);
        
        _tunnel = Some(tunnel);
        tunneled_url.to_string()
    } else {
        _tunnel = None;
        db_url
    };

    // Connect to database
    println!("{} Connecting to database...", "🔌".cyan());
    let mut driver = PgDriver::connect_url(&connect_url).await
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
