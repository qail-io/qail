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

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use colored::*;
use qail_core::fmt::Formatter;
use qail_core::prelude::*;
use qail_core::transpiler::{Dialect, ToSql};

use qail::introspection;
use qail::lint::lint_schema;
use qail::migrations::{
    migrate_analyze, migrate_down, migrate_plan, migrate_status, migrate_up, watch_schema,
};
use qail::repl::run_repl;
use qail::schema::{OutputFormat as SchemaOutputFormat, check_schema, diff_schemas_cmd};

#[derive(Parser)]
#[command(name = "qail")]
#[command(author = "QAIL Contributors")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "🪝 QAIL — Schema-First Database Toolkit", long_about = None)]
#[command(after_help = "EXAMPLES:
    qail pull postgres://...           # Extract schema from DB
    qail diff old.qail new.qail        # Compare schemas
    qail migrate up old:new postgres:  # Apply migrations
    qail lint schema.qail              # Check best practices")]
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
    Sqlite,
}

impl From<CliDialect> for Dialect {
    fn from(val: CliDialect) -> Self {
        match val {
            CliDialect::Postgres => Dialect::Postgres,
            CliDialect::Sqlite => Dialect::SQLite,
        }
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Parse and explain a QAIL query
    Explain { query: String },
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
    Fmt { query: String },
    /// Validate a QAIL schema file
    Check {
        /// Schema file path (or old:new for migration validation)
        schema: String,
    },
    /// Diff two schema files and show migration AST
    Diff {
        /// Old schema .qail file
        old: String,
        /// New schema .qail file
        new: String,
        /// Output format (sql or json)
        #[arg(short, long, value_enum, default_value = "sql")]
        format: OutputFormat,
    },
    /// Lint schema for best practices and potential issues
    Lint {
        /// Schema file to lint
        schema: String,
        /// Show only errors (no warnings)
        #[arg(long)]
        strict: bool,
    },
    /// Watch schema file for changes and auto-generate migrations
    Watch {
        /// Schema file to watch
        schema: String,
        /// Database URL to apply changes to (optional)
        #[arg(short, long)]
        url: Option<String>,
        /// Auto-apply changes without confirmation
        #[arg(long)]
        auto_apply: bool,
    },
    /// Apply migrations from schema diff
    Migrate {
        #[command(subcommand)]
        action: MigrateAction,
    },
}

#[derive(Subcommand, Clone)]
enum MigrateAction {
    /// Show migration status and history
    Status { url: String },
    /// Analyze migration impact on codebase before executing
    Analyze {
        /// Schema diff (old.qail:new.qail)
        schema_diff: String,
        /// Codebase path to scan
        #[arg(short, long, default_value = "./src")]
        codebase: String,
        /// CI/CD mode: output GitHub Actions annotations, exit code 1 on errors
        #[arg(long)]
        ci: bool,
    },
    /// Preview migration SQL without executing (dry-run)
    Plan {
        /// Schema diff (old.qail:new.qail)
        schema_diff: String,
        /// Save SQL to file
        #[arg(short, long)]
        output: Option<String>,
    },
    /// Apply migrations (forward)
    Up {
        /// Schema diff file or inline diff
        schema_diff: String,
        /// Database URL
        url: String,
        /// Codebase path to scan for breaking changes (blocks if found)
        #[arg(short, long)]
        codebase: Option<String>,
        /// Force migration even if breaking changes detected
        #[arg(long)]
        force: bool,
    },
    /// Rollback migrations
    Down {
        /// Schema diff file or inline diff
        schema_diff: String,
        /// Database URL
        url: String,
    },
    /// Create a new named migration file
    Create {
        /// Name for the migration (e.g., add_user_avatars)
        name: String,
        /// Dependencies - migrations that must run first
        #[arg(short, long)]
        depends: Option<String>,
        /// Author of the migration
        #[arg(short, long)]
        author: Option<String>,
    },
    /// Apply migration to shadow database (blue-green)
    Shadow {
        /// Schema diff (old.qail:new.qail)
        schema_diff: String,
        /// Database URL
        url: String,
    },
    /// Promote shadow database to primary
    Promote {
        /// Database URL
        url: String,
    },
    /// Abort shadow migration (drop shadow)
    Abort {
        /// Database URL
        url: String,
    },
}

/// Parse schema diff string (old.qail:new.qail) into migration commands
fn parse_schema_diff(schema_diff: &str) -> Result<Vec<qail_core::ast::Qail>> {
    use qail_core::migrate::{diff_schemas, parse_qail};

    if schema_diff.contains(':') && !schema_diff.starts_with("postgres") {
        let parts: Vec<&str> = schema_diff.splitn(2, ':').collect();
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

        Ok(diff_schemas(&old_schema, &new_schema))
    } else {
        Err(anyhow::anyhow!(
            "Please provide two .qail files: old.qail:new.qail"
        ))
    }
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
        }
        Some(Commands::Pull { url }) => {
            introspection::pull_schema(url, introspection::SchemaOutputFormat::Qail).await?;
        }
        Some(Commands::Fmt { query }) => {
            format_query(query)?;
        }
        Some(Commands::Check { schema }) => {
            check_schema(schema)?;
        }
        Some(Commands::Diff { old, new, format }) => {
            let schema_fmt = match format {
                OutputFormat::Sql => SchemaOutputFormat::Sql,
                OutputFormat::Json => SchemaOutputFormat::Json,
                OutputFormat::Pretty => SchemaOutputFormat::Pretty,
            };
            let dialect: Dialect = cli.dialect.clone().into();
            diff_schemas_cmd(old, new, schema_fmt, dialect)?;
        }
        Some(Commands::Lint { schema, strict }) => {
            lint_schema(schema, *strict)?;
        }
        Some(Commands::Watch {
            schema,
            url,
            auto_apply,
        }) => {
            watch_schema(schema, url.as_deref(), *auto_apply).await?;
        }
        Some(Commands::Migrate { action }) => match action {
            MigrateAction::Status { url } => migrate_status(url).await?,
            MigrateAction::Analyze {
                schema_diff,
                codebase,
                ci,
            } => migrate_analyze(schema_diff, codebase, *ci)?,
            MigrateAction::Plan {
                schema_diff,
                output,
            } => migrate_plan(schema_diff, output.as_deref())?,
            MigrateAction::Up { schema_diff, url, codebase, force } => migrate_up(schema_diff, url, codebase.as_deref(), *force).await?,
            MigrateAction::Down { schema_diff, url } => migrate_down(schema_diff, url).await?,
            MigrateAction::Create {
                name,
                depends,
                author,
            } => {
                qail::migrations::migrate_create(name, depends.as_deref(), author.as_deref())?;
            }
            MigrateAction::Shadow { schema_diff, url } => {
                let cmds = parse_schema_diff(schema_diff)?;
                qail::shadow::run_shadow_migration(url, &cmds).await?;
            }
            MigrateAction::Promote { url } => {
                qail::shadow::promote_shadow(url).await?;
            }
            MigrateAction::Abort { url } => {
                qail::shadow::abort_shadow(url).await?;
            }
        },
        None => {
            if let Some(query) = &cli.query {
                transpile_query(query, &cli)?;
            } else {
                println!(
                    "{}",
                    "🪝 QAIL — The Horizontal Query Language".cyan().bold()
                );
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

    let cmd = qail_core::parse(query).map_err(|e| anyhow::anyhow!("Parse error: {}", e))?;
    let dialect: Dialect = cli.dialect.clone().into();

    match cli.format {
        OutputFormat::Sql => println!("{}", cmd.to_sql_with_dialect(dialect)),
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&cmd)?),
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
    let formatted = formatter
        .format(&cmd)
        .map_err(|e| anyhow::anyhow!("Format error: {}", e))?;
    println!("{}", formatted);
    Ok(())
}

fn generate_migration(query: &str, name_override: Option<String>) -> Result<()> {
    let cmd = qail_core::parse(query).map_err(|e| anyhow::anyhow!("Parse error: {}", e))?;

    if !matches!(cmd.action, Action::Make | Action::Mod) {
        anyhow::bail!(
            "Only 'make' and 'mod' actions are supported for migrations. Got: {}",
            cmd.action
        );
    }

    let up_sql = cmd.to_sql();
    let down_sql = qail::sql_gen::generate_down_sql(&cmd);

    let name = name_override.unwrap_or_else(|| format!("{}_{}", cmd.action, cmd.table));
    let timestamp = chrono::Local::now().format("%Y%m%d%H%M%S");

    println!("{}", "Generated Migration:".green().bold());
    println!();
    println!("-- Name: {}_{}", timestamp, name);
    println!("-- UP:");
    println!("{};", up_sql);
    println!();
    println!("-- DOWN:");
    println!("{};", down_sql);

    Ok(())
}

fn explain_query(query: &str) {
    println!("{}", "🔍 Query Analysis".cyan().bold());
    println!();
    println!("  {} {}", "Query:".dimmed(), query.yellow());
    println!();

    match qail_core::parse(query) {
        Ok(cmd) => {
            println!(
                "  {} {}",
                "Action:".dimmed(),
                format!("{}", cmd.action).green()
            );
            println!("  {} {}", "Table:".dimmed(), cmd.table.white());

            if !cmd.columns.is_empty() {
                println!("  {} {}", "Columns:".dimmed(), cmd.columns.len());
            }

            println!();
            println!("  {} {}", "SQL:".cyan(), cmd.to_sql().white().bold());
        }
        Err(e) => {
            eprintln!("{} {}", "Parse Error:".red().bold(), e);
        }
    }
}

pub fn show_symbols() {
    println!("{}", "🪝 QAIL Symbol Reference (v2.0)".cyan().bold());
    println!();

    let symbols = [
        ("::", "separator", "Table delimiter", "FROM"),
        ("'", "field", "Column selector", "SELECT col"),
        ("'_", "all", "All columns", "SELECT *"),
        ("[", "filter", "WHERE condition", "WHERE ..."),
        ("]", "close", "End filter/modifier", ""),
        ("[]", "values", "Insert values", "VALUES (...)"),
        ("$", "param", "Placeholder", "$1, $2"),
        ("<-", "left", "LEFT JOIN", "LEFT JOIN"),
        ("->", "inner", "INNER JOIN", "JOIN"),
        ("<>", "full", "FULL OUTER JOIN", "FULL JOIN"),
        ("!", "distinct", "DISTINCT modifier", "SELECT DISTINCT"),
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
    println!();
}
