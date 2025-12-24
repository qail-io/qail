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
    Json,
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
    /// Introspect database schema and update qail.schema.json
    Pull {
        /// Database connection URL (postgres:// or mysql://)
        url: String,
        /// Output format (json or qail)
        #[arg(short, long, value_enum, default_value = "json")]
        format: SchemaFormat,
    },
    /// Format a QAIL query to canonical v2 syntax
    Fmt {
        /// The QAIL query to format
        query: String,
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
        Some(Commands::Pull { url, format }) => {
            let output_format = match format {
                SchemaFormat::Json => qail::introspection::SchemaOutputFormat::Json,
                SchemaFormat::Qail => qail::introspection::SchemaOutputFormat::Qail,
            };
            qail::introspection::pull_schema(url, output_format).await?;
        },
        Some(Commands::Fmt { query }) => {
            format_query(query)?;
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
