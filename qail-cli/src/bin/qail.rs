//! qail — The QAIL CLI
//!
//! A blazing fast CLI for parsing and transpiling QAIL queries.
//!
//! # Usage
//!
//! ```bash
//! # Parse and transpile a query
//! qail "get::users:'_[active=true][lim=10]"
//!
//! # Interactive REPL mode
//! qail repl
//! ```

use clap::{Parser, Subcommand, ValueEnum};
use colored::*;
use qail_core::prelude::*;
use qail_core::transpiler::{ToSql, Dialect};
use qail_core::fmt::Formatter;
use qail_core::migrate::{diff_schemas, parse_qail};
use qail_pg::driver::PgDriver;
use anyhow::Result;

#[derive(Parser)]
#[command(name = "qail")]
#[command(author = "QAIL Contributors")]
#[command(version = "0.5.0")]
#[command(about = "🪝 The Horizontal Query Language CLI", long_about = None)]
#[command(after_help = "EXAMPLES:
    qail \"get::users:'_[active=true]\"
    qail \"get::orders:'id'total[user_id=$1][lim=10]\"
    qail repl")]
struct Cli {
    /// The QAIL query to transpile
    query: Option<String>,

    /// Output format
    #[arg(short, long, value_enum, default_value = "sql")]
    format: OutputFormat,

    /// Target SQL dialect
    #[arg(short, long, value_enum, default_value = "postgres")]
    dialect: CliDialect,

    /// Verbose output (show AST)
    #[arg(short, long)]
    verbose: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Clone, ValueEnum)]
enum OutputFormat {
    Sql,
    Json,
    Pretty,
}

#[derive(Clone, ValueEnum)]
enum CliDialect {
    Postgres,
    Mysql,
    Sqlite,
    Sqlserver,
}

#[derive(Clone, ValueEnum, Default)]
enum SchemaFormat {
    #[default]
    Qail,
}

impl From<CliDialect> for Dialect {
    fn from(val: CliDialect) -> Self {
        match val {
            CliDialect::Postgres => Dialect::Postgres,
            CliDialect::Mysql => Dialect::MySQL,
            CliDialect::Sqlite => Dialect::SQLite,
            CliDialect::Sqlserver => Dialect::SqlServer,
        }
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Parse and explain a QAIL query
    Explain {
        query: String,
    },
    /// Interactive REPL mode
    Repl,
    /// Show the symbol reference
    Symbols,
    /// Generate a migration file
    Mig {
        /// The QAIL migration command (e.g., make::users...)
        query: String,
        
        /// Optional name for the migration
        #[arg(short, long)]
        name: Option<String>,
    },
    /// Introspect database schema and output schema.qail
    Pull {
        /// Database connection URL (postgres:// or mysql://)
        url: String,
    },
    /// Format a QAIL query to canonical v2 syntax
    Fmt {
        /// The QAIL query to format
        query: String,
    },
    /// Diff two schema files and show migration AST
    Diff {
        /// Old schema JSON file
        old: String,
        /// New schema JSON file
        new: String,
        /// Output format (sql or json)
        #[arg(short, long, value_enum, default_value = "sql")]
        format: OutputFormat,
    },
    /// Apply migrations from schema diff
    Migrate {
        #[command(subcommand)]
        action: MigrateAction,
    },
}

#[derive(Subcommand, Clone)]
enum MigrateAction {
    /// Apply migrations (forward)
    Up {
        /// Schema diff file or inline diff
        schema_diff: String,
        /// Database URL
        url: String,
    },
    /// Rollback migrations
    Down {
        /// Schema diff file or inline diff
        schema_diff: String,
        /// Database URL
        url: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Explain { query }) => explain_query(query),
        Some(Commands::Repl) => run_repl(),
        Some(Commands::Symbols) => show_symbols(),
        Some(Commands::Mig { query, name }) => {
            generate_migration(query, name.clone())?;
        },
        Some(Commands::Pull { url }) => {
            qail::introspection::pull_schema(url, qail::introspection::SchemaOutputFormat::Qail).await?;
        },
        Some(Commands::Fmt { query }) => {
            format_query(query)?;
        },
        Some(Commands::Diff { old, new, format }) => {
            diff_schemas_cmd(old, new, format.clone(), &cli)?;
        },
        Some(Commands::Migrate { action }) => {
            match action {
                MigrateAction::Up { schema_diff, url } => {
                    migrate_up(schema_diff, url).await?;
                },
                MigrateAction::Down { schema_diff, url } => {
                    migrate_down(schema_diff, url).await?;
                },
            }
        },
        None => {
            if let Some(query) = &cli.query {
                transpile_query(query, &cli)?;
            } else {
                println!("{}", "🪝 QAIL — The Horizontal Query Language".cyan().bold());
                println!();
                println!("Usage: qail <QUERY> [OPTIONS]");
                println!();
                println!("Try: qail --help");
            }
        }
    }

    Ok(())
}

fn transpile_query(query: &str, cli: &Cli) -> Result<()> {
    if cli.verbose {
        println!("{} {}", "Input:".dimmed(), query.yellow());
        println!();
    }

    // Parse the query
    let cmd = qail_core::parse(query).map_err(|e| anyhow::anyhow!("Parse error: {}", e))?;
    let dialect: Dialect = cli.dialect.clone().into();
    
    match cli.format {
        OutputFormat::Sql => {
            println!("{}", cmd.to_sql_with_dialect(dialect));
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&cmd)?);
        }
        OutputFormat::Pretty => {
            println!("{}", "Generated SQL:".green().bold());
            println!("{}", cmd.to_sql_with_dialect(dialect).white());
        }
    }

    Ok(())
}

fn format_query(query: &str) -> Result<()> {
    let cmd = qail_core::parse(query).map_err(|e| anyhow::anyhow!("Parse error: {}", e))?;
    let formatter = Formatter::new();
    let formatted = formatter.format(&cmd).map_err(|e| anyhow::anyhow!("Format error: {}", e))?;
    println!("{}", formatted);
    Ok(())
}

fn generate_migration(query: &str, name_override: Option<String>) -> Result<()> {
    let cmd = qail_core::parse(query).map_err(|e| anyhow::anyhow!("Parse error: {}", e))?;
    
    // Validate action
    if !matches!(cmd.action, Action::Make | Action::Mod) {
         anyhow::bail!("Only 'make' and 'mod' actions are supported for migrations. Got: {}", cmd.action);
    }

    let up_sql = cmd.to_sql();
    let down_sql = generate_down_sql(&cmd);

    // Generate filename
    let timestamp = chrono::Local::now().format("%Y%m%d%H%M%S");
    let action_name = match cmd.action {
         Action::Make => format!("create_{}", cmd.table),
         Action::Mod => format!("alter_{}", cmd.table),
         _ => "migration".to_string(),
    };
    let name = name_override.unwrap_or(action_name);
    let base_filename = format!("{}_{}", timestamp, name);

    // Ensure migrations directory exists
    std::fs::create_dir_all("migrations")?;

    let up_path = format!("migrations/{}.up.sql", base_filename);
    let down_path = format!("migrations/{}.down.sql", base_filename);

    std::fs::write(&up_path, up_sql)?;
    std::fs::write(&down_path, down_sql)?;

    println!("{} Created migration files:", "✓".green());
    println!("   {} {}", "UP:".cyan(), up_path);
    println!("   {} {}", "DOWN:".cyan(), down_path);

    Ok(())
}

fn generate_down_sql(cmd: &QailCmd) -> String {
    match cmd.action {
        Action::Make => format!("DROP TABLE IF EXISTS {};", cmd.table),
        Action::Mod => {
            let mut stmts = Vec::new();
            for col in &cmd.columns {
                 if let Expr::Mod { kind, col } = col {
                     match kind {
                         ModKind::Add => {
                             if let Expr::Def { name, .. } = col.as_ref() {
                                 stmts.push(format!("ALTER TABLE {} DROP COLUMN {}", cmd.table, name));
                             }
                         }
                         ModKind::Drop => {
                             if let Expr::Named(name) = col.as_ref() {
                                 stmts.push(format!("-- TODO: Re-add dropped column '{}' (type unknown)", name));
                             }
                         }
                     }
                 }
            }
            stmts.join(";\n")
        }
        _ => "-- No down migration generated".to_string(),
    }
}

fn explain_query(query: &str) {
    println!("{}", "🪝 QAIL Query Explanation".cyan().bold());
    println!();
    println!("{} {}", "Query:".dimmed(), query.yellow());
    println!();

    match qail_core::parse(query) {
        Ok(cmd) => {
            println!("{}", "Parsed Structure:".green().bold());
            println!("  {} {}", "Action:".dimmed(), format!("{}", cmd.action).cyan());
            println!("  {} {}", "Table:".dimmed(), cmd.table.white());

            if !cmd.columns.is_empty() {
                println!("  {}", "Columns:".dimmed());
                for col in &cmd.columns {
                    println!("    • {}", col.to_string().white());
                }
            }

            if !cmd.cages.is_empty() {
                println!("  {}", "Cages:".dimmed());
                for cage in &cmd.cages {
                    let kind = match &cage.kind {
                        CageKind::Filter => "Filter".to_string(),
                        CageKind::Payload => "Payload".to_string(),
                        CageKind::Sort(order) => match order {
                            qail_core::ast::SortOrder::Asc => "Sort ↑".to_string(),
                            qail_core::ast::SortOrder::Desc => "Sort ↓".to_string(),
                            qail_core::ast::SortOrder::AscNullsFirst => "Sort ↑ (Nulls 1st)".to_string(),
                            qail_core::ast::SortOrder::AscNullsLast => "Sort ↑ (Nulls Last)".to_string(),
                            qail_core::ast::SortOrder::DescNullsFirst => "Sort ↓ (Nulls 1st)".to_string(),
                            qail_core::ast::SortOrder::DescNullsLast => "Sort ↓ (Nulls Last)".to_string(),
                        },
                        CageKind::Limit(n) => format!("Limit({})", n),
                        CageKind::Offset(n) => format!("Offset({})", n),
                        CageKind::Sample(n) => format!("Sample({}%)", n),
                        CageKind::Qualify => "Qualify".to_string(),
                        CageKind::Partition => "Partition".to_string(),
                    };
                    println!("    [{}]", kind.cyan());
                    for cond in &cage.conditions {
                        println!(
                            "      {} {:?} {}",
                            cond.left.to_string().white(),
                            cond.op,
                            cond.value.to_string().yellow()
                        );
                    }
                }
            }

            println!();
            println!("{}", "Generated SQL:".green().bold());
            println!("  {}", cmd.to_sql().white());
        }
        Err(e) => {
            eprintln!("{} {}", "Parse Error:".red().bold(), e);
        }
    }
}

fn run_repl() {
    use rustyline::error::ReadlineError;
    use rustyline::DefaultEditor;

    println!("{}", "🪝 QAIL REPL — Interactive Mode".cyan().bold());
    println!("{}", "Type queries to see generated SQL. Commands:".dimmed());
    println!("  {}  - Exit the REPL", ".exit".yellow());
    println!("  {} - Show symbol reference", ".help".yellow());
    println!("  {} - Clear screen", ".clear".yellow());
    println!();

    let mut rl = match DefaultEditor::new() {
        Ok(editor) => editor,
        Err(e) => {
            eprintln!("{} {}", "Failed to initialize REPL:".red(), e);
            return;
        }
    };

    // Load history if available
    let history_path = dirs::home_dir()
        .map(|p| p.join(".qail_history"))
        .unwrap_or_default();
    let _ = rl.load_history(&history_path);

    loop {
        let prompt = "qail> ".cyan().bold().to_string();
        match rl.readline(&prompt) {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                let _ = rl.add_history_entry(line);

                match line {
                    ".exit" | ".quit" | "exit" | "quit" => {
                        println!("{}", "Goodbye! 👋".green());
                        break;
                    }
                    ".help" | "help" => {
                        show_repl_help();
                        continue;
                    }
                    ".clear" | "clear" => {
                        print!("\x1B[2J\x1B[1;1H");
                        continue;
                    }
                    ".symbols" | "symbols" => {
                        show_symbols();
                        continue;
                    }
                    _ => {}
                }

                match qail_core::parse(line) {
                    Ok(cmd) => {
                        let sql = cmd.to_sql();
                        println!("{} {}", "→".green(), sql.white().bold());
                        println!();
                    }
                    Err(e) => {
                        eprintln!("{} {}", "✗".red(), e.to_string().red());
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("{}", "^C".dimmed());
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("{}", "Goodbye! 👋".green());
                break;
            }
            Err(err) => {
                eprintln!("{} {:?}", "Error:".red(), err);
                break;
            }
        }
    }

    let _ = rl.save_history(&history_path);
}

fn show_repl_help() {
    println!("{}", "QAIL REPL Commands:".cyan().bold());
    println!("  {}     - Exit the REPL", ".exit".yellow());
    println!("  {}     - Show this help", ".help".yellow());
    println!("  {}    - Clear screen", ".clear".yellow());
    println!("  {}  - Show symbol reference", ".symbols".yellow());
    println!();
    println!("{}", "Query Examples (v2.0 syntax):".cyan().bold());
    println!("  get::users:'_");
    println!("  get::orders:'id'total[status=$1][lim=10]");
    println!("  set::users:[verified=true][id=$1]");
    println!("  get!::products:'category  (DISTINCT)");
    println!("  get::users<-profiles:'name'avatar  (LEFT JOIN)");
    println!();
}

fn show_symbols() {
    println!("{}", "🪝 QAIL Symbol Reference (v2.0)".cyan().bold());
    println!();

    let symbols = [
        ("::", "The Gate", "Defines the action", "SELECT/UPDATE/DELETE"),
        (":", "The Link", "Connects table to columns", "FROM table"),
        ("'", "The Label", "Marks a column", "col1, col2"),
        ("'_", "The Wildcard", "All columns", "*"),
        ("[]", "The Cage", "Constraints block", "WHERE, LIMIT, ORDER BY"),
        ("==", "The Equal", "Equality check", "= value"),
        ("+col", "Sort Asc", "Ascending sort", "ORDER BY col ASC"),
        ("-col", "Sort Desc", "Descending sort", "ORDER BY col DESC"),
        ("N..M", "The Range", "Limit/Offset", "LIMIT M-N OFFSET N"),
        ("~", "The Fuse", "Fuzzy match", "ILIKE '%val%'"),
        ("|", "The Split", "Logical OR", "OR"),
        ("&", "The Bind", "Logical AND", "AND"),
        ("$", "The Var", "Parameter", "$1, $2"),
    ];

    println!(
        "{:10} {:15} {:30} {}",
        "Symbol".white().bold(),
        "Name".white().bold(),
        "Function".white().bold(),
        "SQL Equivalent".white().bold()
    );
    println!("{}", "─".repeat(80).dimmed());

    for (symbol, name, function, sql) in symbols {
        println!(
            "{:10} {:15} {:30} {}",
            symbol.cyan().bold(),
            name.yellow(),
            function.white(),
            sql.dimmed()
        );
    }
}

/// Compare two schema .qail files and output migration commands.
fn diff_schemas_cmd(old_path: &str, new_path: &str, format: OutputFormat, cli: &Cli) -> Result<()> {
    println!("{} {} → {}", "Diffing:".cyan(), old_path.yellow(), new_path.yellow());
    
    // Load old schema
    let old_content = std::fs::read_to_string(old_path)
        .map_err(|e| anyhow::anyhow!("Failed to read old schema '{}': {}", old_path, e))?;
    let old_schema = parse_qail(&old_content)
        .map_err(|e| anyhow::anyhow!("Failed to parse old schema: {}", e))?;
    
    // Load new schema
    let new_content = std::fs::read_to_string(new_path)
        .map_err(|e| anyhow::anyhow!("Failed to read new schema '{}': {}", new_path, e))?;
    let new_schema = parse_qail(&new_content)
        .map_err(|e| anyhow::anyhow!("Failed to parse new schema: {}", e))?;
    
    // Compute diff
    let cmds = diff_schemas(&old_schema, &new_schema);
    
    if cmds.is_empty() {
        println!("{}", "No changes detected.".green());
        return Ok(());
    }
    
    println!("{} {} migration command(s):", "Found:".green(), cmds.len());
    println!();
    
    let dialect: Dialect = cli.dialect.clone().into();
    
    match format {
        OutputFormat::Sql => {
            for cmd in &cmds {
                println!("{};", cmd.to_sql_with_dialect(dialect));
            }
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&cmds)?);
        }
        OutputFormat::Pretty => {
            for (i, cmd) in cmds.iter().enumerate() {
                println!("{} {} {}", 
                    format!("{}.", i + 1).cyan(),
                    format!("{}", cmd.action).yellow(),
                    cmd.table.white()
                );
                println!("   {}", cmd.to_sql_with_dialect(dialect).dimmed());
            }
        }
    }
    
    Ok(())
}

/// Apply migrations forward using qail-pg native driver.
async fn migrate_up(schema_diff_path: &str, url: &str) -> Result<()> {
    println!("{} {}", "Migrating UP:".cyan().bold(), url.yellow());
    
    // Load the two schemas and compute diff
    // schema_diff_path can be either:
    // 1. A single diff file (JSON of Vec<QailCmd>)
    // 2. Two schema files separated by ":"
    
    let cmds = if schema_diff_path.contains(':') && !schema_diff_path.starts_with("postgres") {
        // Two schema files: old:new
        let parts: Vec<&str> = schema_diff_path.splitn(2, ':').collect();
        let old_path = parts[0];
        let new_path = parts[1];
        
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
        // For now, only support two-file format
        return Err(anyhow::anyhow!("Please provide two .qail files: old.qail:new.qail"));
    };
    
    if cmds.is_empty() {
        println!("{}", "No migrations to apply.".green());
        return Ok(());
    }
    
    println!("{} {} migration(s) to apply", "Found:".cyan(), cmds.len());
    
    // Parse URL and connect using qail-pg
    let (host, port, user, password, database) = parse_pg_url(url)?;
    let mut driver = if let Some(pwd) = password {
        PgDriver::connect_with_password(&host, port, &user, &database, &pwd).await
            .map_err(|e| anyhow::anyhow!("Failed to connect: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, &database).await
            .map_err(|e| anyhow::anyhow!("Failed to connect: {}", e))?
    };
    
    for (i, cmd) in cmds.iter().enumerate() {
        println!("  {} {} {}", format!("[{}/{}]", i + 1, cmds.len()).cyan(), format!("{}", cmd.action).yellow(), &cmd.table);
        
        driver.execute(cmd).await
            .map_err(|e| anyhow::anyhow!("Migration failed: {}", e))?;
    }
    
    println!("{}", "✓ All migrations applied successfully!".green().bold());
    Ok(())
}

/// Rollback migrations using qail-pg native driver.
async fn migrate_down(schema_diff_path: &str, url: &str) -> Result<()> {
    println!("{} {}", "Migrating DOWN:".cyan().bold(), url.yellow());
    
    // For rollback, we reverse the diff: old becomes new, new becomes old
    let cmds = if schema_diff_path.contains(':') && !schema_diff_path.starts_with("postgres") {
        let parts: Vec<&str> = schema_diff_path.splitn(2, ':').collect();
        let old_path = parts[0];
        let new_path = parts[1];
        
        // Swap: rollback means going from new -> old
        let old_content = std::fs::read_to_string(new_path)
            .map_err(|e| anyhow::anyhow!("Failed to read new schema: {}", e))?;
        let new_content = std::fs::read_to_string(old_path)
            .map_err(|e| anyhow::anyhow!("Failed to read old schema: {}", e))?;
        
        let old_schema = parse_qail(&old_content)
            .map_err(|e| anyhow::anyhow!("Failed to parse schema: {}", e))?;
        let new_schema = parse_qail(&new_content)
            .map_err(|e| anyhow::anyhow!("Failed to parse schema: {}", e))?;
        
        diff_schemas(&old_schema, &new_schema)
    } else {
        println!("{}", "Warning: Rollback requires two .qail files".yellow());
        println!("  Use format: qail migrate down old.qail:new.qail <url>");
        return Ok(());
    };
    
    if cmds.is_empty() {
        println!("{}", "No rollbacks to apply.".green());
        return Ok(());
    }
    
    println!("{} {} rollback(s) to apply", "Found:".cyan(), cmds.len());
    
    let (host, port, user, password, database) = parse_pg_url(url)?;
    let mut driver = if let Some(pwd) = password {
        PgDriver::connect_with_password(&host, port, &user, &database, &pwd).await
            .map_err(|e| anyhow::anyhow!("Failed to connect: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, &database).await
            .map_err(|e| anyhow::anyhow!("Failed to connect: {}", e))?
    };
    
    for (i, cmd) in cmds.iter().enumerate() {
        println!("  {} {} {}", format!("[{}/{}]", i + 1, cmds.len()).cyan(), format!("{}", cmd.action).yellow(), &cmd.table);
        
        driver.execute(cmd).await
            .map_err(|e| anyhow::anyhow!("Rollback failed: {}", e))?;
    }
    
    println!("{}", "✓ All rollbacks applied successfully!".green().bold());
    Ok(())
}

/// Parse a PostgreSQL URL into (host, port, user, password, database).
fn parse_pg_url(url: &str) -> Result<(String, u16, String, Option<String>, String)> {
    let parsed = url::Url::parse(url)
        .map_err(|e| anyhow::anyhow!("Invalid URL: {}", e))?;
    
    let host = parsed.host_str()
        .ok_or_else(|| anyhow::anyhow!("Missing host in URL"))?
        .to_string();
    
    let port = parsed.port().unwrap_or(5432);
    
    let user = if parsed.username().is_empty() {
        "postgres".to_string()
    } else {
        parsed.username().to_string()
    };
    
    let password = parsed.password().map(|s| s.to_string());
    
    let database = parsed.path().trim_start_matches('/').to_string();
    if database.is_empty() {
        return Err(anyhow::anyhow!("Missing database in URL"));
    }
    
    Ok((host, port, user, password, database))
}
