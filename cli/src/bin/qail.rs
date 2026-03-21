//! qail — The QAIL CLI
//!
//! A blazing fast CLI for parsing and transpiling QAIL queries.
//!
//! # Usage
//!
//! ```bash
//! # Parse and transpile a query (v2 keyword syntax)
//! qail "get users fields id, email where active = true limit 10"
//!
//! # Interactive REPL mode
//! qail repl
//! ```

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use qail::colors::*;
use qail_core::fmt::Formatter;
use qail_core::prelude::*;
use qail_core::transpiler::{Dialect, ToSql};

use qail::introspection;
use qail::lint::lint_schema;
#[cfg(feature = "watch")]
use qail::migrations::watch_schema;
use qail::migrations::{
    ApplyPhase, MigrateDirection, migrate_analyze, migrate_apply, migrate_down, migrate_plan,
    migrate_reset, migrate_rollback, migrate_status, migrate_up,
};
#[cfg(feature = "repl")]
use qail::repl::run_repl;
use qail::resolve::resolve_db_url;
use qail::schema::{OutputFormat as SchemaOutputFormat, check_schema, diff_schemas_cmd};
use qail::schema_tools::{doctor_schema, format_schema_source, merge_schema, split_schema};

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

#[derive(Clone, ValueEnum)]
enum CliApplyPhase {
    All,
    Expand,
    Backfill,
    Contract,
}

impl From<CliApplyPhase> for ApplyPhase {
    fn from(value: CliApplyPhase) -> Self {
        match value {
            CliApplyPhase::All => ApplyPhase::All,
            CliApplyPhase::Expand => ApplyPhase::Expand,
            CliApplyPhase::Backfill => ApplyPhase::Backfill,
            CliApplyPhase::Contract => ApplyPhase::Contract,
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum CliMigrateDirection {
    Up,
    Down,
}

impl From<CliMigrateDirection> for MigrateDirection {
    fn from(value: CliMigrateDirection) -> Self {
        match value {
            CliMigrateDirection::Up => MigrateDirection::Up,
            CliMigrateDirection::Down => MigrateDirection::Down,
        }
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new QAIL project
    #[command(after_help = r#"EXAMPLES:
    # Interactive mode
    qail init
    
    # Non-interactive mode (CI/scripting)
    qail init --name myapp --mode postgres --url postgres://localhost/mydb
    qail init --name myapp --mode hybrid --url postgres://localhost/mydb --deployment docker"#)]
    Init {
        /// Project name
        #[arg(short, long)]
        name: Option<String>,
        /// Database mode (postgres, qdrant, hybrid)
        #[arg(short, long)]
        mode: Option<String>,
        /// Database URL (skips interactive prompt)
        #[arg(short, long)]
        url: Option<String>,
        /// Deployment type: host, docker, podman (default: host)
        #[arg(long)]
        deployment: Option<String>,
    },
    /// Parse and explain a QAIL query
    Explain { query: String },
    /// Interactive QAIL REPL — type queries, see SQL in real-time
    #[cfg(feature = "repl")]
    Repl,

    /// Generate a migration file
    Mig {
        /// The QAIL migration command (e.g., make users fields id UUID, email VARCHAR)
        query: String,
        /// Optional name for the migration
        #[arg(short, long)]
        name: Option<String>,
    },
    /// Introspect database schema and output schema.qail
    #[command(after_help = r#"WHAT IT DOES:
    Connects to a live database and extracts the complete schema as a .qail file.
    Tables, columns, types, constraints, indexes - everything.

URL RESOLUTION:
    1. --url flag (highest priority)
    2. DATABASE_URL env var
    3. [postgres].url in qail.toml

EXAMPLES:
    # Pull using qail.toml config (no URL needed!)
    qail pull
    
    # Pull with explicit URL
    qail pull --url postgres://localhost/mydb
    
    # Pull from remote via SSH tunnel
    qail pull --ssh myserver
    
    # Compare with expected
    qail pull > live.qail
    qail diff expected.qail live.qail"#)]
    Pull {
        /// Database connection URL (reads from qail.toml if not provided)
        #[arg(short, long)]
        url: Option<String>,
        /// SSH host for tunneling (e.g., "myserver" or "user@host")
        #[arg(long)]
        ssh: Option<String>,
    },
    /// Format a QAIL query or schema source path (file/dir)
    Fmt { input: String },
    /// Validate a QAIL schema file (and optionally audit source for RLS coverage)
    Check {
        /// Schema file path (or old:new for migration validation)
        schema: String,
        /// Source directory to scan for RLS audit (e.g., ./src)
        #[arg(long)]
        src: Option<String>,
        /// Migrations directory to merge before validation
        #[arg(long, default_value = "migrations")]
        migrations: String,
        /// Fail if N+1 query patterns are detected
        #[arg(long)]
        nplus1_deny: bool,
    },
    /// Diff two schema files and show migration AST
    Diff {
        /// Old schema .qail file (not required with --live)
        old: String,
        /// New schema .qail file
        new: String,
        /// Output format (sql or json)
        #[arg(short, long, value_enum, default_value = "sql")]
        format: OutputFormat,
        /// Use live database introspection as "old" schema (drift detection)
        #[arg(long)]
        live: bool,
        /// Database URL (required with --live)
        #[arg(long)]
        url: Option<String>,
    },
    /// Lint schema for best practices and potential issues
    Lint {
        /// Schema file to lint
        schema: String,
        /// Show only errors (no warnings)
        #[arg(long)]
        strict: bool,
    },
    /// Watch schema file for changes and auto-generate migrations [requires --features watch]
    #[cfg(feature = "watch")]
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
    #[command(after_help = r#"WORKFLOW:
    1. Edit schema.qail (your desired state)
    2. qail migrate plan old.qail:new.qail     # Preview SQL
    3. qail migrate analyze old:new -c ./src   # Check for breaking changes
    4. qail migrate up old:new postgres://...  # Apply

SUBCOMMANDS:
    status   - Show migration history for a database
    plan     - Generate SQL from schema diff (dry-run)
    analyze  - Scan codebase for breaking changes before migrating
    up       - Apply migrations forward
    down     - Rollback migrations
    rollback - Roll back to a specific applied version
    apply    - Run all pending migrations from migrations/ folder
    create   - Generate a new named migration file
    shadow   - Apply to shadow database (blue-green deployment)
    promote  - Swap shadow to primary
    abort    - Drop shadow database

SCHEMA DIFF FORMAT:
    old.qail:new.qail    Compare two files
    
EXAMPLES:
    # Preview migration SQL
    qail migrate plan v1.qail:v2.qail
    
    # Apply with safety check
    qail migrate up v1:v2 postgres://... -c ./src
    
    # Blue-green deployment
    qail migrate shadow schema.qail postgres://... --live
    qail migrate promote postgres://...
    
    # CI/CD integration
    qail migrate analyze old:new --ci  # Fails if breaking changes"#)]
    Migrate {
        #[command(subcommand)]
        action: MigrateAction,
    },
    /// Vector database operations (Qdrant) [requires --features vector]
    #[cfg(feature = "vector")]
    #[command(after_help = r#"QDRANT OPERATIONS:
    QAIL integrates with Qdrant vector database for hybrid PostgreSQL + vector search.

SUBCOMMANDS:
    create    - Create a new vector collection
    drop      - Delete a collection
    backup    - Snapshot a collection
    restore   - Restore from snapshot
    snapshots - List available snapshots

EXAMPLES:
    # Create collection for OpenAI embeddings (1536 dimensions)
    qail vector create products --size 1536 --distance cosine http://localhost:6334
    
    # Backup before changes
    qail vector backup products -o products_backup.snapshot http://localhost:6333
    
    # Restore from backup
    qail vector restore products -s products_backup.snapshot http://localhost:6333
    
    # Clean up
    qail vector drop products http://localhost:6334"#)]
    Vector {
        #[command(subcommand)]
        action: VectorAction,
    },
    /// Sync operations for hybrid mode (PostgreSQL + Qdrant)
    #[command(after_help = r#"HYBRID MODE:
    QAIL can sync data between PostgreSQL and Qdrant automatically.
    Define [[sync]] rules in qail.toml, then generate triggers.

SUBCOMMANDS:
    generate - Create PostgreSQL triggers from qail.toml [[sync]] rules
    list     - Show configured sync rules

QAIL.TOML CONFIG:
    [[sync]]
    table = "products"
    collection = "products_vectors"
    embedding_column = "embedding"
    id_column = "id"
    payload = ["name", "category", "price"]

EXAMPLES:
    # Generate trigger SQL
    qail sync generate
    
    # View configured rules
    qail sync list"#)]
    Sync {
        #[command(subcommand)]
        action: SyncAction,
    },
    /// Run the sync worker daemon (polls _qail_queue)
    #[command(after_help = r#"WHAT IT DOES:
    Polls PostgreSQL _qail_queue table for pending vector sync operations.
    Processes inserts/updates/deletes and syncs to Qdrant.

REQUIREMENTS:
    • PostgreSQL with _qail_queue table (created by qail sync generate)
    • Qdrant running and accessible
    • qail.toml with postgres.url and qdrant.url configured

EXAMPLES:
    # Run with defaults (1s interval, 100 batch)
    qail worker
    
    # Faster polling for real-time sync
    qail worker -i 100 -b 50
    
    # Production (run as systemd service)
    qail worker -i 500 -b 200"#)]
    /// Hybrid sync worker [requires --features vector]
    #[cfg(feature = "vector")]
    Worker {
        /// Poll interval in milliseconds
        #[arg(short, long, default_value = "1000")]
        interval: u64,
        /// Batch size per poll
        #[arg(short, long, default_value = "100")]
        batch: u32,
    },
    /// Execute type-safe QAIL statements
    #[command(after_help = r#"SYNTAX:
    add <table> fields <col1>, <col2> values <val1>, <val2>
    set <table>[id = $1] fields name = 'new', updated_at = now
    del <table>[id = $1]
    get <table>'id'name[active = true]
    cnt <table>[active = true]

VALUE TYPES:
    Strings      'hello', "world"
    Numbers      42, -3.14
    Booleans     true, false
    Null         null
    Parameters   $1, $2 (positional) or :name, :user_id (named)
    Intervals    24h, 7d, 30m, 1y, 6mo (auto-converts to INTERVAL)
    JSON         ["a", "b"], {"key": "value"} (auto-converts to ::jsonb)
    NOW          now (current timestamp)

MULTI-LINE STRINGS:
    Use triple quotes for HTML, markdown, or long text:
    
    add articles fields content values '''
    <article>
      <p>Multi-line content preserved.</p>
    </article>
    '''

JSON VALUES:
    Arrays and objects auto-convert to PostgreSQL jsonb:
    
    add users fields tags values ["admin", "vip"]
    add config fields data values {"theme": "dark", "count": 42}

FILE FORMAT (.qail):
    One statement per line (unless in triple quotes).
    Comments: # or -- at line start.
    
    # Insert user
    add users fields email, name values 'a@b.com', 'Alice'
    
    # Update with named param
    set users[id = :id] fields name = :name
    
    # Delete old records
    del logs[created_at < 30d]

SSH TUNNELING:
    Access remote databases through SSH jump host:
    
    qail exec -f seed.qail --ssh myserver --url postgres://user:pass@localhost:5432/db
    
    This creates: local:random_port -> myserver:5432 tunnel automatically.

TRANSACTIONS:
    Wrap multiple statements in a single transaction (rollback on error):
    
    qail exec -f batch.qail --url postgres://... --tx

DRY-RUN:
    Preview generated SQL without executing:
    
    qail exec -f data.qail --dry-run

EXAMPLES:
    # Inline insert
    qail exec "add users fields name, active values 'Alice', true" --url postgres://...
    
    # Query with table display
    qail exec "get users'id'name'email[active = true]" --url postgres://...
    
    # JSON output for scripting (pipe to jq)
    qail exec "get users" --url postgres://... --json
    qail exec "get users" --url postgres://... --json | jq '.[].email'
    
    # From file with SSH tunnel
    qail exec -f seed.qail --ssh myserver --url postgres://...
    
    # Transactional batch
    qail exec -f migrations.qail --url postgres://... --tx
    
    # Preview SQL only
    qail exec -f data.qail --dry-run"#)]
    Exec {
        /// QAIL query string (e.g., "add users fields name, email values 'test', 'a@b.com'")
        query: Option<String>,
        /// Path to .qail file (supports multi-line with triple quotes)
        #[arg(short, long)]
        file: Option<String>,
        /// Database URL
        #[arg(short, long)]
        url: Option<String>,
        /// SSH host for tunneling (e.g., "myserver" or "user@host")
        #[arg(long)]
        ssh: Option<String>,
        /// Wrap all statements in a transaction
        #[arg(long)]
        tx: bool,
        /// Dry-run: print generated SQL without executing
        #[arg(long)]
        dry_run: bool,
        /// Output SELECT results as JSON array
        #[arg(long)]
        json: bool,
    },
    /// Seed a database with fixture data (alias for `exec -f seed.qail`)
    #[command(after_help = r#"SEED DATA:
    Run a .qail seed file against a database. This is a convenience
    alias for `qail exec -f <file>` with a default of `seed.qail`.

    SEED FILE FORMAT:
    Same as exec — one QAIL statement per line, comments with # or --.

    # seed.qail
    add users fields name, email values 'Alice', 'alice@test.com'
    add users fields name, email values 'Bob', 'bob@test.com'
    add products fields name, price values 'Widget', 9.99

EXAMPLES:
    # Seed using default seed.qail in current directory
    qail seed --url postgres://localhost/mydb

    # Seed with custom file
    qail seed -f fixtures/dev.qail --url postgres://localhost/mydb

    # Seed with SSH tunnel
    qail seed --ssh staging --url postgres://localhost/mydb

    # Seed in a transaction (rollback on error)
    qail seed --tx --url postgres://localhost/mydb

    # Preview SQL without executing
    qail seed --dry-run"#)]
    Seed {
        /// Path to seed file (default: seed.qail)
        #[arg(short, long, default_value = "seed.qail")]
        file: String,
        /// Database URL
        #[arg(short, long)]
        url: Option<String>,
        /// SSH host for tunneling
        #[arg(long)]
        ssh: Option<String>,
        /// Wrap all statements in a transaction
        #[arg(long)]
        tx: bool,
        /// Dry-run: print generated SQL without executing
        #[arg(long)]
        dry_run: bool,
    },
    /// Generate typed Rust schema from schema.qail
    Types {
        /// Path to schema.qail file
        #[arg(default_value = "schema.qail")]
        schema: String,
        /// Output file path (prints to stdout if not specified)
        #[arg(short, long)]
        output: Option<String>,
    },
    /// Database branching for data virtualization
    #[command(after_help = r#"DATA VIRTUALIZATION:
    Create database branches for isolated experimentation.
    Changes on a branch are stored as overlay rows — no schema changes needed.

SUBCOMMANDS:
    create  - Create a new branch
    list    - List all branches
    delete  - Soft-delete a branch
    merge   - Mark a branch as merged

EXAMPLES:
    qail branch create feature-auth --url postgres://localhost/mydb
    qail branch list --url postgres://localhost/mydb
    qail branch merge feature-auth --url postgres://localhost/mydb
    qail branch delete feature-auth --url postgres://localhost/mydb"#)]
    Branch {
        #[command(subcommand)]
        action: BranchAction,
    },
    /// Modular schema tooling
    Schema {
        #[command(subcommand)]
        action: SchemaAction,
    },
}

#[derive(Subcommand, Clone)]
enum SyncAction {
    /// Generate trigger migrations from [[sync]] rules in qail.toml
    Generate,
    /// List configured sync rules
    List,
}

#[derive(Subcommand, Clone)]
enum SchemaAction {
    /// Diagnose module-order and schema integrity issues
    Doctor {
        /// Schema source path (schema.qail, schema/, or module file)
        #[arg(default_value = "schema.qail")]
        schema: String,
        /// Fail on warnings (not just errors)
        #[arg(long)]
        strict: bool,
    },
    /// Split monolithic schema into modular schema/ directory
    Split {
        /// Input schema source (usually schema.qail)
        #[arg(default_value = "schema.qail")]
        input: String,
        /// Output directory for modules
        #[arg(short, long, default_value = "schema")]
        out: String,
        /// Overwrite files in output directory
        #[arg(long)]
        force: bool,
    },
    /// Merge modular schema source into one .qail file
    Merge {
        /// Input schema source (schema/ or schema.qail)
        #[arg(default_value = "schema")]
        input: String,
        /// Output merged schema file
        #[arg(short, long, default_value = "schema.qail")]
        output: String,
    },
}

#[derive(Subcommand, Clone)]
enum BranchAction {
    /// Create a new database branch
    Create {
        /// Branch name
        name: String,
        /// Parent branch (default: main)
        #[arg(long)]
        parent: Option<String>,
        /// Database URL
        #[arg(short, long)]
        url: Option<String>,
    },
    /// List all branches
    List {
        /// Database URL
        #[arg(short, long)]
        url: Option<String>,
    },
    /// Soft-delete a branch
    Delete {
        /// Branch name
        name: String,
        /// Database URL
        #[arg(short, long)]
        url: Option<String>,
    },
    /// Merge a branch (mark overlay as merged)
    Merge {
        /// Branch name
        name: String,
        /// Database URL
        #[arg(short, long)]
        url: Option<String>,
    },
}

#[derive(Subcommand, Clone)]
enum MigrateAction {
    /// Show migration status and history
    #[command(after_help = r#"EXAMPLES:
    qail migrate status
    qail migrate status --url postgres://user:pass@localhost:5432/mydb

    Output includes version, name, applied_at, and checksum for each migration."#)]
    Status {
        /// Database URL (reads from qail.toml if not provided)
        #[arg(short, long)]
        url: Option<String>,
    },
    /// Analyze migration impact on codebase before executing
    #[command(after_help = r#"EXAMPLES:
    # Scan ./src for queries affected by schema changes
    qail migrate analyze v1.qail:v2.qail -c ./src
    
    # CI mode: exits 1 if breaking changes found, outputs GitHub annotations
    qail migrate analyze v1.qail:v2.qail --ci

    # Machine-readable output for CI gates
    qail migrate analyze v1.qail:v2.qail --json"#)]
    Analyze {
        /// Schema diff (old.qail:new.qail)
        schema_diff: String,
        /// Codebase path to scan
        #[arg(short, long, default_value = "./src")]
        codebase: String,
        /// CI/CD mode: output GitHub Actions annotations, exit code 1 on errors
        #[arg(long)]
        ci: bool,
        /// Output analysis as JSON (suitable for CI parsing)
        #[arg(long)]
        json: bool,
    },
    /// Preview migration SQL without executing (dry-run)
    #[command(after_help = r#"EXAMPLES:
    # Preview migration between two schema versions
    qail migrate plan v1.qail:v2.qail
    
    # Save generated SQL to a file
    qail migrate plan v1.qail:v2.qail -o migration.sql"#)]
    Plan {
        /// Schema diff (old.qail:new.qail)
        schema_diff: String,
        /// Save SQL to file
        #[arg(short, long)]
        output: Option<String>,
    },
    /// Apply migrations (forward)
    #[command(after_help = r#"SCHEMA DIFF FORMAT:
    old.qail:new.qail — two schema files separated by colon

EXAMPLES:
    # Apply migration
    qail migrate up v1.qail:v2.qail postgres://user@localhost/mydb
    
    # Apply with breaking-change check against source code
    qail migrate up v1.qail:v2.qail postgres://... -c ./src
    
    # Force apply even if breaking changes detected
    qail migrate up v1.qail:v2.qail postgres://... -c ./src --force

    # Explicitly allow destructive operations (DROP / narrowing type / SET NOT NULL on non-empty tables)
    qail migrate up v1.qail:v2.qail postgres://... --allow-destructive

    # Override lock-risk preflight guardrails (not recommended)
    qail migrate up v1.qail:v2.qail postgres://... --allow-lock-risk

    # Wait until global migration lock is available
    qail migrate up v1.qail:v2.qail postgres://... --wait-for-lock
    qail migrate up v1.qail:v2.qail postgres://... --lock-timeout-secs 30"#)]
    Up {
        /// Schema diff file or inline diff
        schema_diff: String,
        /// Database URL (reads from qail.toml if not provided)
        #[arg(short, long)]
        url: Option<String>,
        /// Codebase path to scan for breaking changes (blocks if found)
        #[arg(short, long)]
        codebase: Option<String>,
        /// Force migration even if breaking changes detected
        #[arg(long)]
        force: bool,
        /// Explicitly allow destructive migration operations
        #[arg(long)]
        allow_destructive: bool,
        /// Skip shadow receipt verification gate (not recommended)
        #[arg(long)]
        allow_no_shadow_receipt: bool,
        /// Skip lock-risk preflight guardrails (not recommended)
        #[arg(long)]
        allow_lock_risk: bool,
        /// Wait for the global migration lock instead of failing fast
        #[arg(long)]
        wait_for_lock: bool,
        /// Max seconds to wait for lock (implies wait-for-lock)
        #[arg(long)]
        lock_timeout_secs: Option<u64>,
    },
    /// Rollback migrations
    #[command(after_help = r#"EXAMPLES:
    # Rollback from current schema to target schema
    qail migrate down current.qail:target.qail postgres://user@localhost/mydb
    qail migrate down v2.qail:v1.qail postgres://user@localhost/mydb

    # Force rollback on unsafe type narrowing changes (non-interactive/CI)
    qail migrate down current.qail:target.qail postgres://... --force

    # Wait until global migration lock is available
    qail migrate down current.qail:target.qail postgres://... --wait-for-lock
    qail migrate down current.qail:target.qail postgres://... --lock-timeout-secs 30"#)]
    Down {
        /// Schema diff file or inline diff
        schema_diff: String,
        /// Database URL (reads from qail.toml if not provided)
        #[arg(short, long)]
        url: Option<String>,
        /// Force rollback even when unsafe type narrowing is detected
        #[arg(long)]
        force: bool,
        /// Wait for the global migration lock instead of failing fast
        #[arg(long)]
        wait_for_lock: bool,
        /// Max seconds to wait for lock (implies wait-for-lock)
        #[arg(long)]
        lock_timeout_secs: Option<u64>,
    },
    /// Roll back applied folder migrations to a target version
    #[command(after_help = r#"WHAT IT DOES:
    Reads applied migration history from _qail_migrations and executes matching
    *.down.qail files in reverse order until the target version is reached.

TARGET:
    --to <version>  Keep this version applied, roll back everything after it
    --to base       Roll back all applied folder migrations

EXAMPLES:
    qail migrate rollback --to 20260318094500123_add_users.up.qail
    qail migrate rollback --to base --url postgres://user@localhost/mydb
    qail migrate rollback --to base --wait-for-lock
    qail migrate rollback --to base --lock-timeout-secs 30"#)]
    Rollback {
        /// Target applied migration version to keep (or "base")
        #[arg(long)]
        to: String,
        /// Database URL (reads from qail.toml if not provided)
        #[arg(short, long)]
        url: Option<String>,
        /// Wait for the global migration lock instead of failing fast
        #[arg(long)]
        wait_for_lock: bool,
        /// Max seconds to wait for lock (implies wait-for-lock)
        #[arg(long)]
        lock_timeout_secs: Option<u64>,
    },
    /// Apply migrations from migrations/ folder (reads .qail files)
    #[command(after_help = r#"WHAT IT DOES:
    Scans migrations/ directory for .qail files, determines which have not
    been applied to the database, and runs them in order.

EXAMPLES:
    # Apply pending migrations (URL from qail.toml)
    qail migrate apply
    
    # Apply with explicit URL
    qail migrate apply --url postgres://user@localhost/mydb

    # Apply pending rollback files (*.down.qail)
    qail migrate apply --direction down

    # Apply only expand/backfill/contract phase
    qail migrate apply --phase expand
    qail migrate apply --phase backfill --backfill-chunk-size 10000

    # Contract safety guard with code reference scan
    qail migrate apply --phase contract --codebase ./src

    # Allow destructive operations and lock-risk overrides (if policy requires explicit flags)
    qail migrate apply --allow-destructive
    qail migrate apply --allow-lock-risk
    qail migrate apply --allow-no-shadow-receipt
    qail migrate apply --adopt-existing

    # Wait until global migration lock is available
    qail migrate apply --wait-for-lock
    qail migrate apply --lock-timeout-secs 30"#)]
    Apply {
        /// Database URL (reads from qail.toml if not provided)
        #[arg(short, long)]
        url: Option<String>,
        /// Direction to apply from migrations folder
        #[arg(long, value_enum, default_value = "up")]
        direction: CliMigrateDirection,
        /// Migration phase to apply (all, expand, backfill, contract)
        #[arg(long, value_enum, default_value = "all")]
        phase: CliApplyPhase,
        /// Codebase path for contract-reference safety checks
        #[arg(short, long)]
        codebase: Option<String>,
        /// Override contract guard even when references still exist in code
        #[arg(long)]
        allow_contract_with_references: bool,
        /// Explicitly allow destructive migration operations
        #[arg(long)]
        allow_destructive: bool,
        /// Skip shadow receipt verification gate (not recommended)
        #[arg(long)]
        allow_no_shadow_receipt: bool,
        /// Skip lock-risk preflight guardrails (not recommended)
        #[arg(long)]
        allow_lock_risk: bool,
        /// Treat duplicate-object create errors as already-adopted during baseline cutover
        #[arg(long)]
        adopt_existing: bool,
        /// Default chunk size for chunked backfill runner directives
        #[arg(long, default_value_t = 5000)]
        backfill_chunk_size: usize,
        /// Wait for the global migration lock instead of failing fast
        #[arg(long)]
        wait_for_lock: bool,
        /// Max seconds to wait for lock (implies wait-for-lock)
        #[arg(long)]
        lock_timeout_secs: Option<u64>,
    },
    /// Create a new named migration file
    #[command(after_help = r#"EXAMPLES:
    qail migrate create add_user_avatars
    qail migrate create add_user_avatars --author "orion" --depends add_users
    
    Creates: migrations/<timestamp>_add_user_avatars.qail"#)]
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
    #[command(after_help = r#"BLUE-GREEN MIGRATION WORKFLOW:
    1. qail migrate shadow v1:v2 postgres://...     # Create shadow + apply + sync
    2. Test against shadow database
    3. qail migrate promote postgres://...           # Apply to primary, drop shadow
       OR qail migrate abort postgres://...          # Drop shadow, keep primary

    With --live (drift-safe):
    qail migrate shadow schema.qail postgres://... --live"#)]
    Shadow {
        /// Schema diff (old.qail:new.qail) or just new.qail with --live
        schema_diff: String,
        /// Database URL (reads from qail.toml if not provided)
        #[arg(short, long)]
        url: Option<String>,
        /// Use live database introspection instead of old.qail file (catches drift)
        #[arg(long)]
        live: bool,
    },
    /// Promote shadow database to primary
    Promote {
        /// Database URL (reads from qail.toml if not provided)
        #[arg(short, long)]
        url: Option<String>,
    },
    /// Abort shadow migration (drop shadow)
    Abort {
        /// Database URL (reads from qail.toml if not provided)
        #[arg(short, long)]
        url: Option<String>,
    },
    /// Reset database: drop all, clear history, re-apply target schema
    #[command(after_help = r#"WHAT IT DOES:
    Three-phase atomic reset:
      1. DROP all objects found in the target schema
      2. CLEAR _qail_migrations history table
      3. CREATE all objects from the target schema
    
    Each phase runs in its own transaction for safety.

EXAMPLES:
    qail migrate reset schema.qail postgres://user@localhost/mydb
    qail migrate reset schema.qail postgres://... --wait-for-lock
    qail migrate reset schema.qail postgres://... --lock-timeout-secs 30

WARNING:
    This is destructive — all data in matching tables will be lost."#)]
    Reset {
        /// Target schema file (.qail)
        schema: String,
        /// Database URL (reads from qail.toml if not provided)
        #[arg(short, long)]
        url: Option<String>,
        /// Wait for the global migration lock instead of failing fast
        #[arg(long)]
        wait_for_lock: bool,
        /// Max seconds to wait for lock (implies wait-for-lock)
        #[arg(long)]
        lock_timeout_secs: Option<u64>,
    },
}

#[cfg(feature = "vector")]
#[derive(Subcommand, Clone)]
enum VectorAction {
    /// Create a vector collection
    Create {
        /// Collection name
        collection: String,
        /// Vector size (dimensions, e.g., 1536 for OpenAI)
        #[arg(short, long)]
        size: u64,
        /// Distance metric (cosine, euclid, dot)
        #[arg(short, long, default_value = "cosine")]
        distance: String,
        /// Qdrant URL (e.g., http://localhost:6334)
        url: String,
    },
    /// Drop a vector collection
    Drop {
        /// Collection name
        collection: String,
        /// Qdrant URL
        url: String,
    },
    /// Create backup snapshot of a collection
    Backup {
        /// Collection name
        collection: String,
        /// Output file path (optional, downloads to local file)
        #[arg(short, long)]
        output: Option<String>,
        /// Qdrant REST URL (e.g., http://localhost:6333)
        url: String,
    },
    /// Restore collection from snapshot
    Restore {
        /// Collection name
        collection: String,
        /// Snapshot file path or URL
        #[arg(short, long)]
        snapshot: String,
        /// Qdrant REST URL
        url: String,
    },
    /// List available snapshots
    Snapshots {
        /// Collection name
        collection: String,
        /// Qdrant REST URL
        url: String,
    },
}

/// Parse schema diff and also return old schema commands, diff commands, and paths (for shadow migration)
fn parse_schema_diff_with_old(
    schema_diff: &str,
) -> Result<(
    Vec<qail_core::ast::Qail>,
    Vec<qail_core::ast::Qail>,
    String,
    String,
)> {
    use qail_core::migrate::{diff_schemas_checked, parse_qail_file, schema_to_commands};

    if schema_diff.contains(':') && !schema_diff.starts_with("postgres") {
        let parts: Vec<&str> = schema_diff.splitn(2, ':').collect();
        let old_path = parts[0];
        let new_path = parts[1];

        let old_schema = parse_qail_file(old_path)
            .map_err(|e| anyhow::anyhow!("Failed to parse old schema: {}", e))?;
        let new_schema = parse_qail_file(new_path)
            .map_err(|e| anyhow::anyhow!("Failed to parse new schema: {}", e))?;

        let old_cmds = schema_to_commands(&old_schema);
        let diff_cmds = diff_schemas_checked(&old_schema, &new_schema)
            .map_err(|e| anyhow::anyhow!("State-based diff unsupported for shadow input: {}", e))?;

        Ok((
            old_cmds,
            diff_cmds,
            old_path.to_string(),
            new_path.to_string(),
        ))
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
        Some(Commands::Init {
            name,
            mode,
            url,
            deployment,
        }) => {
            qail::init::run_init(name.clone(), mode.clone(), url.clone(), deployment.clone())?;
        }
        Some(Commands::Explain { query }) => explain_query(query),
        #[cfg(feature = "repl")]
        Some(Commands::Repl) => run_repl(),

        Some(Commands::Mig { query, name }) => {
            generate_migration(query, name.clone())?;
        }
        Some(Commands::Pull { url, ssh: _ssh }) => {
            let db_url = resolve_db_url(url.as_deref())?;
            introspection::pull_schema(&db_url, introspection::SchemaOutputFormat::Qail).await?;
        }
        Some(Commands::Fmt { input }) => {
            format_input(input)?;
        }
        Some(Commands::Check {
            schema,
            src,
            migrations,
            nplus1_deny,
        }) => {
            check_schema(schema, src.as_deref(), migrations, *nplus1_deny)?;
        }
        Some(Commands::Diff {
            old,
            new,
            format,
            live,
            url,
        }) => {
            let schema_fmt = match format {
                OutputFormat::Sql => SchemaOutputFormat::Sql,
                OutputFormat::Json => SchemaOutputFormat::Json,
                OutputFormat::Pretty => SchemaOutputFormat::Pretty,
            };
            let dialect: Dialect = cli.dialect.clone().into();
            if *live {
                // Live drift detection: introspect DB as "old", compare with file as "new"
                let db_url = resolve_db_url(url.as_deref())?;
                qail::schema::diff_live(&db_url, new, schema_fmt, dialect).await?;
            } else {
                diff_schemas_cmd(old, new, schema_fmt, dialect)?;
            }
        }
        Some(Commands::Lint { schema, strict }) => {
            lint_schema(schema, *strict)?;
        }
        #[cfg(feature = "watch")]
        Some(Commands::Watch {
            schema,
            url,
            auto_apply,
        }) => {
            watch_schema(schema, url.as_deref(), *auto_apply).await?;
        }
        Some(Commands::Migrate { action }) => match action {
            MigrateAction::Status { url } => {
                let db_url = resolve_db_url(url.as_deref())?;
                migrate_status(&db_url).await?;
            }
            MigrateAction::Analyze {
                schema_diff,
                codebase,
                ci,
                json,
            } => migrate_analyze(schema_diff, codebase, *ci, *json)?,
            MigrateAction::Plan {
                schema_diff,
                output,
            } => migrate_plan(schema_diff, output.as_deref())?,
            MigrateAction::Up {
                schema_diff,
                url,
                codebase,
                force,
                allow_destructive,
                allow_no_shadow_receipt,
                allow_lock_risk,
                wait_for_lock,
                lock_timeout_secs,
            } => {
                let db_url = resolve_db_url(url.as_deref())?;
                migrate_up(
                    schema_diff,
                    &db_url,
                    codebase.as_deref(),
                    *force,
                    *allow_destructive,
                    *allow_no_shadow_receipt,
                    *allow_lock_risk,
                    *wait_for_lock,
                    *lock_timeout_secs,
                )
                .await?;
            }
            MigrateAction::Down {
                schema_diff,
                url,
                force,
                wait_for_lock,
                lock_timeout_secs,
            } => {
                let db_url = resolve_db_url(url.as_deref())?;
                migrate_down(
                    schema_diff,
                    &db_url,
                    *force,
                    *wait_for_lock,
                    *lock_timeout_secs,
                )
                .await?;
            }
            MigrateAction::Rollback {
                to,
                url,
                wait_for_lock,
                lock_timeout_secs,
            } => {
                let db_url = resolve_db_url(url.as_deref())?;
                migrate_rollback(to, &db_url, *wait_for_lock, *lock_timeout_secs).await?;
            }
            MigrateAction::Reset {
                schema,
                url,
                wait_for_lock,
                lock_timeout_secs,
            } => {
                let db_url = resolve_db_url(url.as_deref())?;
                migrate_reset(schema, &db_url, *wait_for_lock, *lock_timeout_secs).await?;
            }
            MigrateAction::Apply {
                url,
                direction,
                phase,
                codebase,
                allow_contract_with_references,
                allow_destructive,
                allow_no_shadow_receipt,
                allow_lock_risk,
                adopt_existing,
                backfill_chunk_size,
                wait_for_lock,
                lock_timeout_secs,
            } => {
                let db_url = resolve_db_url(url.as_deref())?;
                migrate_apply(
                    &db_url,
                    (*direction).into(),
                    phase.clone().into(),
                    codebase.as_deref(),
                    *allow_contract_with_references,
                    *allow_destructive,
                    *allow_no_shadow_receipt,
                    *allow_lock_risk,
                    *adopt_existing,
                    *backfill_chunk_size,
                    *wait_for_lock,
                    *lock_timeout_secs,
                )
                .await?;
            }
            MigrateAction::Create {
                name,
                depends,
                author,
            } => {
                qail::migrations::migrate_create(name, depends.as_deref(), author.as_deref())?;
            }
            MigrateAction::Shadow {
                schema_diff,
                url,
                live,
            } => {
                let db_url = resolve_db_url(url.as_deref())?;
                if *live {
                    qail::shadow::run_shadow_migration_live(&db_url, schema_diff).await?;
                } else {
                    let (old_cmds, diff_cmds, old_path, new_path) =
                        parse_schema_diff_with_old(schema_diff)?;
                    qail::shadow::run_shadow_migration(
                        &db_url, &old_cmds, &diff_cmds, &old_path, &new_path,
                    )
                    .await?;
                }
            }
            MigrateAction::Promote { url } => {
                let db_url = resolve_db_url(url.as_deref())?;
                qail::shadow::promote_shadow(&db_url).await?;
            }
            MigrateAction::Abort { url } => {
                let db_url = resolve_db_url(url.as_deref())?;
                qail::shadow::abort_shadow(&db_url).await?;
            }
        },
        #[cfg(feature = "vector")]
        Some(Commands::Vector { action }) => match action {
            VectorAction::Create {
                collection,
                size,
                distance,
                url,
            } => {
                qail::vector::vector_create(collection, *size, distance, url).await?;
            }
            VectorAction::Drop { collection, url } => {
                qail::vector::vector_drop(collection, url).await?;
            }
            VectorAction::Backup {
                collection,
                output,
                url,
            } => {
                let snapshot = qail::snapshot::snapshot_create(collection, url).await?;
                if let Some(out_path) = output {
                    qail::snapshot::snapshot_download(collection, &snapshot.name, out_path, url)
                        .await?;
                }
            }
            VectorAction::Restore {
                collection,
                snapshot,
                url,
            } => {
                qail::snapshot::snapshot_restore(collection, snapshot, url).await?;
            }
            VectorAction::Snapshots { collection, url } => {
                let snapshots = qail::snapshot::snapshot_list(collection, url).await?;
                if snapshots.is_empty() {
                    println!("No snapshots found for '{}'", collection);
                } else {
                    println!("Snapshots for '{}':", collection);
                    for s in snapshots {
                        println!(
                            "  {} ({} bytes, created: {})",
                            s.name,
                            s.size,
                            s.creation_time.as_deref().unwrap_or("unknown")
                        );
                    }
                }
            }
        },
        Some(Commands::Sync { action }) => match action {
            SyncAction::Generate => {
                qail::sync::generate_sync_triggers()?;
            }
            SyncAction::List => {
                qail::sync::list_sync_rules()?;
            }
        },
        #[cfg(feature = "vector")]
        Some(Commands::Worker { interval, batch }) => {
            qail::worker::run_worker(*interval, *batch).await?;
        }
        Some(Commands::Exec {
            query,
            file,
            url,
            ssh,
            tx,
            dry_run,
            json,
        }) => {
            qail::exec::run_exec(qail::exec::ExecConfig {
                query: query.clone(),
                file: file.clone(),
                url: url.clone(),
                ssh: ssh.clone(),
                tx: *tx,
                dry_run: *dry_run,
                json: *json,
            })
            .await?;
        }
        Some(Commands::Seed {
            file,
            url,
            ssh,
            tx,
            dry_run,
        }) => {
            println!("{}", format!("Seeding from: {}", file).cyan());
            qail::exec::run_exec(qail::exec::ExecConfig {
                query: None,
                file: Some(file.clone()),
                url: url.clone(),
                ssh: ssh.clone(),
                tx: *tx,
                dry_run: *dry_run,
                json: false,
            })
            .await?;
        }
        Some(Commands::Types { schema, output }) => {
            qail::types::generate_types(schema, output.as_deref())?;
        }
        Some(Commands::Branch { action }) => {
            // Resolve DB URL from --url flag or qail.toml
            let get_url = |url: &Option<String>| -> Result<String> {
                if let Some(u) = url {
                    Ok(u.clone())
                } else {
                    // Try reading from qail.toml
                    let config = std::fs::read_to_string("qail.toml").unwrap_or_default();
                    for line in config.lines() {
                        let line = line.trim();
                        if line.starts_with("url") && line.contains('=') {
                            let val = line
                                .split_once('=')
                                .map(|x| x.1)
                                .unwrap_or("")
                                .trim()
                                .trim_matches('"');
                            if val.starts_with("postgres") {
                                return Ok(val.to_string());
                            }
                        }
                    }
                    anyhow::bail!("No database URL. Use --url or set postgres.url in qail.toml")
                }
            };
            match action {
                BranchAction::Create { name, parent, url } => {
                    let db_url = get_url(url)?;
                    qail::branch::branch_create(name, parent.as_deref(), &db_url).await?;
                }
                BranchAction::List { url } => {
                    let db_url = get_url(url)?;
                    qail::branch::branch_list(&db_url).await?;
                }
                BranchAction::Delete { name, url } => {
                    let db_url = get_url(url)?;
                    qail::branch::branch_delete(name, &db_url).await?;
                }
                BranchAction::Merge { name, url } => {
                    let db_url = get_url(url)?;
                    qail::branch::branch_merge(name, &db_url).await?;
                }
            }
        }
        Some(Commands::Schema { action }) => match action {
            SchemaAction::Doctor { schema, strict } => {
                doctor_schema(schema, *strict)?;
            }
            SchemaAction::Split { input, out, force } => {
                split_schema(input, out, *force)?;
            }
            SchemaAction::Merge { input, output } => {
                merge_schema(input, output)?;
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
        OutputFormat::Json => {
            let payload = serde_json::json!({
                "wire": qail_core::wire::encode_cmd_text(&cmd),
                "sql": cmd.to_sql_with_dialect(dialect),
                "action": format!("{}", cmd.action),
                "table": cmd.table.clone(),
            });
            println!("{}", serde_json::to_string_pretty(&payload)?);
        }
        OutputFormat::Pretty => {
            println!("{}", "Generated SQL:".green().bold());
            println!("{}", cmd.to_sql_with_dialect(dialect).white());
        }
    }

    Ok(())
}

fn format_input(input: &str) -> Result<()> {
    let path = std::path::Path::new(input);
    if path.exists() {
        return format_schema_source(input);
    }

    let cmd = qail_core::parse(input).map_err(|e| anyhow::anyhow!("Parse error: {}", e))?;
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
    let timestamp = qail::time::timestamp_version();

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
