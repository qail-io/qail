//! qail init - Project initialization
//!
//! Interactive setup for QAIL projects with support for:
//! - PostgreSQL only
//! - Qdrant only
//! - Hybrid (PostgreSQL + Qdrant with sync)

use crate::colors::*;
use anyhow::Result;
use std::fs;
use std::io::{self, Write};

use std::process::Command;

/// A detected database instance.
#[derive(Debug, Clone)]
struct DetectedDb {
    source: String, // "host", "docker", "podman"
    name: String,   // container name or "local"
    host: String,   // hostname
    port: u16,      // mapped port
}

impl std::fmt::Display for DetectedDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{} ({})", self.host, self.port, self.source)?;
        if self.name != "local" {
            write!(f, " — {}", self.name)?;
        }
        Ok(())
    }
}

/// Detect running PostgreSQL instances across host, Docker, and Podman.
fn detect_databases() -> Vec<DetectedDb> {
    let mut found: Vec<DetectedDb> = Vec::new();

    // 1. Host: scan for postgres processes listening on any port via lsof
    if let Ok(output) = Command::new("lsof")
        .args(["-iTCP", "-sTCP:LISTEN", "-nP"])
        .output()
        && output.status.success()
    {
        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines() {
            let lower = line.to_lowercase();
            if lower.contains("postgres") || lower.contains("postmaster") {
                // Parse port from lsof output: "... *:5432 (LISTEN)" or "... 127.0.0.1:5433 (LISTEN)"
                if let Some(port) = parse_lsof_port(line)
                    && !found.iter().any(|d| d.port == port && d.source == "host")
                {
                    found.push(DetectedDb {
                        source: "host".into(),
                        name: "local".into(),
                        host: "localhost".into(),
                        port,
                    });
                }
            }
        }
    }

    // 2. Docker: scan for containers with postgres image or exposed 5432
    detect_container_dbs("docker", &mut found);

    // 3. Podman: same approach
    detect_container_dbs("podman", &mut found);

    found
}

/// Parse port number from an lsof output line like:
/// `postgres  1234 user   5u  IPv4 ...  TCP *:5432 (LISTEN)`
/// `postgres  1234 user   5u  IPv6 ...  TCP [::1]:5433 (LISTEN)`
fn parse_lsof_port(line: &str) -> Option<u16> {
    // Find the TCP address:port part — typically the 9th field or contains ":"
    for token in line.split_whitespace() {
        if token.contains("(LISTEN)") {
            continue;
        }
        // Look for patterns like *:5432, localhost:5432, [::1]:5432, 127.0.0.1:5432
        if let Some(colon_pos) = token.rfind(':') {
            let port_str = &token[colon_pos + 1..];
            if let Ok(port) = port_str.parse::<u16>() {
                // Sanity check: valid port range
                if port > 0 {
                    return Some(port);
                }
            }
        }
    }
    None
}

/// Detect PostgreSQL in Docker or Podman containers.
fn detect_container_dbs(runtime: &str, found: &mut Vec<DetectedDb>) {
    // Use --format to get structured output: name, image, ports
    let output = Command::new(runtime)
        .args(["ps", "--format", "{{.Names}}\t{{.Image}}\t{{.Ports}}"])
        .output();

    let output = match output {
        Ok(o) if o.status.success() => o,
        _ => return, // runtime not installed or not running
    };

    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() < 3 {
            continue;
        }
        let name = parts[0];
        let image = parts[1].to_lowercase();
        let ports = parts[2];

        // Check if the image is postgres-related
        let is_pg = image.contains("postgres")
            || image.contains("postgis")
            || image.contains("timescale")
            || image.contains("supabase");
        if !is_pg {
            continue;
        }

        // Parse port mappings like "0.0.0.0:5433->5432/tcp, :::5433->5432/tcp"
        for mapping in ports.split(',') {
            let mapping = mapping.trim();
            if let Some(host_port) = parse_container_port(mapping)
                && !found
                    .iter()
                    .any(|d| d.port == host_port && d.source == runtime)
            {
                found.push(DetectedDb {
                    source: runtime.into(),
                    name: name.into(),
                    host: "localhost".into(),
                    port: host_port,
                });
            }
        }
    }
}

/// Parse host port from container port mapping like "0.0.0.0:5433->5432/tcp"
fn parse_container_port(mapping: &str) -> Option<u16> {
    // Format: "host_ip:host_port->container_port/proto"
    let arrow = mapping.find("->")?;
    let before_arrow = &mapping[..arrow];
    // host_port is after the last ":"
    let colon = before_arrow.rfind(':')?;
    let port_str = &before_arrow[colon + 1..];
    port_str.parse::<u16>().ok()
}

/// Prompt user to select a detected database or enter URL manually.
fn prompt_db_url(mode_label: &str, default: &str) -> Result<String> {
    println!(
        "\n{}",
        format!("Scanning for {} instances...", mode_label).dimmed()
    );

    let detected = detect_databases();

    if detected.is_empty() {
        println!("  {} No running instances detected", "⚠".yellow());
        return prompt(&format!("{} URL", mode_label), default);
    }

    println!("  {} Found {} instance(s):\n", "✓".green(), detected.len());

    for (i, db) in detected.iter().enumerate() {
        let label = match db.source.as_str() {
            "host" => "🖥  host".to_string(),
            "docker" => "🐳 docker".to_string(),
            "podman" => "🦭 podman".to_string(),
            _ => db.source.clone(),
        };
        println!("    {}  {} {}", format!("{}.", i + 1).cyan(), label, db);
    }
    println!(
        "    {}  Enter URL manually",
        format!("{}.", detected.len() + 1).cyan()
    );
    println!();

    let choice = prompt(&format!("Select [1-{}]", detected.len() + 1), "1")?;

    let idx: usize = choice.parse().unwrap_or(1);
    if idx >= 1 && idx <= detected.len() {
        let db = &detected[idx - 1];
        // Build a URL template for them to fill in user/db
        let url = format!("postgres://postgres@{}:{}/postgres", db.host, db.port);
        println!("  {} {}", "→".dimmed(), url.yellow());

        // Let them optionally customize the detected URL
        let final_url = prompt("Customize URL (or press Enter)", &url)?;
        Ok(final_url)
    } else {
        prompt(&format!("{} URL", mode_label), default)
    }
}

/// Database mode for the project.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Mode {
    Postgres,
    Qdrant,
    Hybrid,
}

impl Mode {
    fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "postgres" | "pg" | "1" => Some(Mode::Postgres),
            "qdrant" | "q" | "2" => Some(Mode::Qdrant),
            "hybrid" | "h" | "3" => Some(Mode::Hybrid),
            _ => None,
        }
    }
}

/// Deployment type.
#[derive(Debug, Clone, Copy)]
pub enum Deployment {
    Host,
    Docker,
    Podman,
}

impl Deployment {
    fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "host" | "local" | "1" => Some(Deployment::Host),
            "docker" | "d" | "2" => Some(Deployment::Docker),
            "podman" | "p" | "3" => Some(Deployment::Podman),
            _ => None,
        }
    }
}

/// Project configuration collected from prompts.
pub struct InitConfig {
    pub name: String,
    pub mode: Mode,
    pub deployment: Deployment,
    pub postgres_url: Option<String>,
    pub qdrant_url: Option<String>,
}

/// Run the interactive init process.
/// When all args are provided, runs non-interactively (CI/scripting friendly).
pub fn run_init(
    name: Option<String>,
    mode_arg: Option<String>,
    url_arg: Option<String>,
    deployment_arg: Option<String>,
) -> Result<()> {
    println!("{}", "🪝 QAIL Project Initialization".cyan().bold());
    println!();

    // 1. Project name
    let name = match name {
        Some(n) => n,
        None => prompt("Project name", "my_app")?,
    };

    // 2. Mode selection
    let mode = match mode_arg.and_then(|m| Mode::from_str(&m)) {
        Some(m) => m,
        None => {
            println!("\n{}", "Select database mode:".white().bold());
            println!("  {} PostgreSQL only", "1.".dimmed());
            println!("  {} Qdrant only", "2.".dimmed());
            println!("  {} Hybrid (PostgreSQL + Qdrant)", "3.".dimmed());
            let choice = prompt("Mode [1/2/3]", "1")?;
            Mode::from_str(&choice).unwrap_or(Mode::Postgres)
        }
    };

    // 3. Deployment type
    let deployment = match deployment_arg.and_then(|d| Deployment::from_str(&d)) {
        Some(d) => d,
        None => {
            println!("\n{}", "Select deployment type:".white().bold());
            println!("  {} Host (local install)", "1.".dimmed());
            println!("  {} Docker", "2.".dimmed());
            println!("  {} Podman", "3.".dimmed());
            let deployment_choice = prompt("Deployment [1/2/3]", "1")?;
            Deployment::from_str(&deployment_choice).unwrap_or(Deployment::Host)
        }
    };

    // 4. Database URLs
    let postgres_url = if mode == Mode::Postgres || mode == Mode::Hybrid {
        Some(match &url_arg {
            Some(u) => u.clone(),
            None => prompt_db_url("PostgreSQL", "postgres://localhost/mydb")?,
        })
    } else {
        None
    };

    let qdrant_url = if mode == Mode::Qdrant || mode == Mode::Hybrid {
        Some(prompt("Qdrant URL", "http://localhost:6333")?)
    } else {
        None
    };

    let config = InitConfig {
        name,
        mode,
        deployment,
        postgres_url,
        qdrant_url,
    };

    println!();

    // Generate files
    generate_qail_toml(&config)?;

    if config.mode == Mode::Hybrid {
        generate_queue_migration()?;
    }

    println!();
    println!(
        "{} Project '{}' initialized!",
        "✓".green(),
        config.name.yellow()
    );
    println!();

    match config.mode {
        Mode::Postgres => {
            println!("Next steps:");
            println!(
                "  {} Run 'qail pull' to introspect existing schema",
                "1.".dimmed()
            );
            println!("  {} Or create schema.qail manually", "2.".dimmed());
        }
        Mode::Qdrant => {
            println!("Next steps:");
            println!(
                "  {} Run 'qail vector create <collection>' to create collections",
                "1.".dimmed()
            );
        }
        Mode::Hybrid => {
            println!("Next steps:");
            println!(
                "  {} Run 'qail migrate up' to create _qail_queue table",
                "1.".dimmed()
            );
            println!("  {} Configure [[sync]] rules in qail.toml", "2.".dimmed());
            println!("  {} Run 'qail worker' to start sync daemon", "3.".dimmed());
        }
    }

    Ok(())
}

/// Prompt user for input with a default value.
fn prompt(question: &str, default: &str) -> Result<String> {
    print!("{} [{}]: ", question.white(), default.dimmed());
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let input = input.trim();

    if input.is_empty() {
        Ok(default.to_string())
    } else {
        Ok(input.to_string())
    }
}

/// Generate qail.toml configuration file.
fn generate_qail_toml(config: &InitConfig) -> Result<()> {
    let mode_str = match config.mode {
        Mode::Postgres => "postgres",
        Mode::Qdrant => "qdrant",
        Mode::Hybrid => "hybrid",
    };

    let mut content = format!(
        r#"# QAIL Project Configuration
# Generated by: qail init
# Docs: https://dev.qail.io/docs/config

[project]
name = "{}"
mode = "{}"
# schema = "schema.qail"
# schema_strict_manifest = false
# migrations_dir = "deltas"
"#,
        config.name, mode_str
    );

    if let Some(url) = &config.postgres_url {
        content.push_str(&format!(
            r#"
[postgres]
url = "{}"

# Pool tuning (uncomment to override defaults)
# max_connections = 10
# min_connections = 1
# idle_timeout_secs = 600
# acquire_timeout_secs = 30
# connect_timeout_secs = 10
# test_on_acquire = false

# [postgres.rls]
# default_role = "app_user"
# super_admin_role = "super_admin"
"#,
            url
        ));
    }

    if let Some(url) = &config.qdrant_url {
        content.push_str(&format!(
            r#"
[qdrant]
url = "{}"
grpc = "{}:6334"
# max_connections = 10
"#,
            url,
            url.trim_end_matches(":6333")
        ));
    }

    // Always include commented-out gateway section
    content.push_str(
        r#"
# [migrations.policy]
# destructive = "require-flag"     # deny | require-flag | allow
# lock_risk = "require-flag"       # deny | require-flag | allow
# lock_risk_max_score = 90          # 0..100
# require_shadow_receipt = true
# allow_no_shadow_receipt = true
# receipt_validation = "error"      # warn | error
#
# [gateway]
# bind = "0.0.0.0:8080"
# cors = true
# policy = "policies.yaml"
#
# [gateway.cache]
# enabled = true
# max_entries = 1000
# ttl_secs = 60
"#,
    );

    if config.mode == Mode::Hybrid {
        content.push_str(
            r#"
# Sync rules - define which tables sync to Qdrant
# [[sync]]
# source_table = "products"
# trigger_column = "description"
# target_collection = "products_search"
# embedding_model = "candle:bert-base"
"#,
        );
    }

    fs::write("qail.toml", content)?;
    println!("{} Created qail.toml", "✓".green());

    // Generate .env.example for secret management
    let env_example = r#"# Required environment variables for qail.toml
# Copy to .env and fill in values
#
# DATABASE_URL overrides [postgres].url
# DATABASE_URL=postgres://user:password@localhost:5432/mydb
#
# QDRANT_URL overrides [qdrant].url
# QDRANT_URL=http://localhost:6333
#
# QAIL_BIND overrides [gateway].bind
# QAIL_BIND=0.0.0.0:8080
"#;
    if !std::path::Path::new(".env.example").exists() {
        fs::write(".env.example", env_example)?;
        println!("{} Created .env.example", "✓".green());
    }

    Ok(())
}

/// Generate the _qail_queue migration for hybrid mode.
fn generate_queue_migration() -> Result<()> {
    let migrations_dir = crate::migrations::resolve_deltas_dir(true)?;

    let up_content = r#"-- QAIL Sync Queue - Outbox Pattern
-- Auto-generated by qail init
-- Enables async sync between PostgreSQL and Qdrant

table _qail_queue (
  id serial primary_key,
  ref_table text not_null,
  ref_id text not_null,
  operation text not_null,
  payload jsonb,
  status text default 'pending',
  retry_count int default 0,
  error_message text,
  created_at timestamptz default NOW(),
  processed_at timestamptz
)

-- Indexes (applied separately)
-- CREATE INDEX idx_qail_queue_poll ON _qail_queue (status, id);
-- CREATE INDEX idx_qail_queue_ref ON _qail_queue (ref_table, ref_id);
"#;

    let down_content = r#"# QAIL Sync Queue - Rollback
# Auto-generated by qail init

drop table _qail_queue
"#;

    fs::write(migrations_dir.join("001_qail_queue.up.qail"), up_content)?;
    fs::write(
        migrations_dir.join("001_qail_queue.down.qail"),
        down_content,
    )?;

    println!("{} Created migrations/001_qail_queue.up.qail", "✓".green());
    println!(
        "{} Created migrations/001_qail_queue.down.qail",
        "✓".green()
    );
    Ok(())
}
