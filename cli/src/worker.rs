//! qail worker - Sync worker daemon
//!
//! Polls _qail_queue from PostgreSQL, generates embeddings,
//! and syncs to Qdrant. Implements the "Transactional Outbox" pattern.

use crate::colors::*;
use anyhow::Result;
use qail_core::ast::builders::{binary, col, count, int, now, now_minus};
use qail_core::ast::{BinaryOp, Operator, Qail, Value};
use serde::Deserialize;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Queue item from _qail_queue table
#[derive(Debug)]
pub struct QueueItem {
    pub id: i64,
    pub ref_table: String,
    pub ref_id: String,
    pub operation: String,
    pub payload: Option<serde_json::Value>,
}

/// Worker configuration from qail.toml
#[derive(Debug, Deserialize)]
struct WorkerConfig {
    project: ProjectConfig,
    postgres: Option<PostgresConfig>,
    qdrant: Option<QdrantConfig>,
    #[serde(default)]
    sync: Vec<SyncRule>,
}

#[derive(Debug, Deserialize)]
struct ProjectConfig {
    mode: String,
}

#[derive(Debug, Deserialize)]
struct PostgresConfig {
    url: String,
}

#[derive(Debug, Deserialize)]
struct QdrantConfig {
    url: String,
    grpc: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SyncRule {
    source_table: String,
    target_collection: String,
    #[serde(default)]
    trigger_column: Option<String>,
    #[serde(default, rename = "embedding_model")]
    _embedding_model: Option<String>,
}

/// Embedding model trait - user implements this
pub trait EmbeddingModel: Send + Sync {
    fn embed(&self, text: &str) -> Vec<f32>;
    fn dimensions(&self) -> usize;
}

/// Dummy embedding model for testing (random vectors)
pub struct DummyEmbedding {
    dim: usize,
}

impl DummyEmbedding {
    pub fn new(dim: usize) -> Self {
        Self { dim }
    }
}

impl EmbeddingModel for DummyEmbedding {
    fn embed(&self, text: &str) -> Vec<f32> {
        // Simple hash-based pseudo-random for deterministic testing
        let hash = text
            .bytes()
            .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
        (0..self.dim)
            .map(|i| {
                let x = hash.wrapping_mul((i + 1) as u64);
                ((x % 1000) as f32 / 1000.0) - 0.5
            })
            .collect()
    }

    fn dimensions(&self) -> usize {
        self.dim
    }
}

/// Run the worker daemon
pub async fn run_worker(poll_interval_ms: u64, batch_size: u32) -> Result<()> {
    println!("{}", "🔄 QAIL Worker Daemon".cyan().bold());
    println!();

    // Load config
    let config = load_config()?;

    if config.project.mode != "hybrid" {
        anyhow::bail!(
            "Worker only runs in 'hybrid' mode. Current mode: {}",
            config.project.mode
        );
    }

    let pg_url = config
        .postgres
        .ok_or_else(|| anyhow::anyhow!("Missing [postgres] config in qail.toml"))?
        .url;

    let qdrant_config = config
        .qdrant
        .ok_or_else(|| anyhow::anyhow!("Missing [qdrant] config in qail.toml"))?;

    let qdrant_grpc = qdrant_config.grpc.unwrap_or_else(|| {
        // Convert REST URL to gRPC (6333 -> 6334)
        qdrant_config.url.replace(":6333", ":6334")
    });

    println!("PostgreSQL: {}", crate::util::redact_url(&pg_url).dimmed());
    println!("Qdrant gRPC: {}", qdrant_grpc.dimmed());
    println!("Poll interval: {}ms", poll_interval_ms);
    println!("Batch size: {}", batch_size);
    println!();

    // Build sync rule lookup
    let sync_rules: std::collections::HashMap<String, &SyncRule> = config
        .sync
        .iter()
        .map(|r| (r.source_table.clone(), r))
        .collect();

    if sync_rules.is_empty() {
        println!(
            "{} No [[sync]] rules configured. Worker has nothing to do.",
            "⚠".yellow()
        );
        return Ok(());
    }

    println!("Sync rules:");
    for rule in &config.sync {
        println!(
            "  {} → {}",
            rule.source_table.yellow(),
            rule.target_collection.cyan()
        );
    }
    println!();

    // Connect to databases with retry
    let (pg_host, pg_port, pg_user, pg_database, pg_password) = parse_postgres_url(&pg_url)?;
    let (qdrant_host, qdrant_port) = parse_grpc_url(&qdrant_grpc)?;

    // Retry configuration
    const MAX_RETRIES: u32 = 10;
    const INITIAL_BACKOFF_MS: u64 = 500;
    const MAX_BACKOFF_MS: u64 = 30_000;

    // Connect to PostgreSQL with retry
    println!("{} Connecting to PostgreSQL...", "→".cyan());
    let mut pg = None;
    for attempt in 1..=MAX_RETRIES {
        let result = if let Some(ref password) = pg_password {
            qail_pg::PgDriver::connect_with_password(
                &pg_host,
                pg_port,
                &pg_user,
                &pg_database,
                password,
            )
            .await
        } else {
            qail_pg::PgDriver::connect(&pg_host, pg_port, &pg_user, &pg_database).await
        };

        match result {
            Ok(driver) => {
                pg = Some(driver);
                break;
            }
            Err(e) => {
                let backoff =
                    std::cmp::min(INITIAL_BACKOFF_MS * 2u64.pow(attempt - 1), MAX_BACKOFF_MS);
                if attempt == MAX_RETRIES {
                    println!(
                        "{} PostgreSQL connection failed after {} attempts: {}",
                        "✗".red(),
                        MAX_RETRIES,
                        e
                    );
                    anyhow::bail!("Failed to connect to PostgreSQL: {}", e);
                }
                println!(
                    "{} PostgreSQL connection failed (attempt {}/{}), retrying in {}ms...",
                    "!".yellow(),
                    attempt,
                    MAX_RETRIES,
                    backoff
                );
                tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
            }
        }
    }
    let Some(mut pg) = pg else {
        anyhow::bail!("Failed to connect to PostgreSQL");
    };
    println!("{} Connected to PostgreSQL", "✓".green());

    // Connect to Qdrant with retry
    println!("{} Connecting to Qdrant...", "→".cyan());
    let mut qdrant = None;
    for attempt in 1..=MAX_RETRIES {
        match qail_qdrant::QdrantDriver::connect(&qdrant_host, qdrant_port).await {
            Ok(driver) => {
                qdrant = Some(driver);
                break;
            }
            Err(e) => {
                let backoff =
                    std::cmp::min(INITIAL_BACKOFF_MS * 2u64.pow(attempt - 1), MAX_BACKOFF_MS);
                if attempt == MAX_RETRIES {
                    println!(
                        "{} Qdrant connection failed after {} attempts: {}",
                        "✗".red(),
                        MAX_RETRIES,
                        e
                    );
                    anyhow::bail!("Failed to connect to Qdrant: {}", e);
                }
                println!(
                    "{} Qdrant connection failed (attempt {}/{}), retrying in {}ms...",
                    "!".yellow(),
                    attempt,
                    MAX_RETRIES,
                    backoff
                );
                tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
            }
        }
    }
    let Some(mut qdrant) = qdrant else {
        anyhow::bail!("Failed to connect to Qdrant");
    };
    println!("{} Connected to Qdrant", "✓".green());

    // Use dummy embedding for now (user would inject their own)
    let embedding_model = DummyEmbedding::new(1536);

    println!();
    println!(
        "{}",
        "Starting poll loop... (Ctrl+C to stop)".white().bold()
    );
    println!();

    let poll_interval = Duration::from_millis(poll_interval_ms);
    let mut total_processed = 0u64;
    let start_time = Instant::now();
    let mut consecutive_errors = 0u32;
    let mut last_janitor_run = Instant::now();
    const JANITOR_INTERVAL_SECS: u64 = 60;

    // Run initial janitor sweep on startup (recover any zombie jobs from previous crash)
    println!("{} Running startup zombie job recovery...", "→".cyan());
    match recover_stale_jobs(&mut pg).await {
        Ok(recovered) if recovered > 0 => {
            println!(
                "{} Recovered {} zombie jobs from previous worker crash",
                "✓".green(),
                recovered
            );
        }
        Ok(_) => {}
        Err(e) => {
            println!("{} Janitor sweep failed: {}", "!".yellow(), e);
        }
    }

    // Graceful shutdown: spawn signal handler
    let running = Arc::new(AtomicBool::new(true));
    let running_clone = Arc::clone(&running);
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            println!("\n🛑 Shutdown signal received. Finishing current batch...");
            running_clone.store(false, Ordering::SeqCst);
        }
    });

    while running.load(Ordering::SeqCst) {
        // Check for too many consecutive errors (circuit breaker)
        if consecutive_errors >= 5 {
            println!(
                "{} Too many consecutive errors, reconnecting...",
                "!".yellow()
            );

            // Reconnect to Qdrant
            for attempt in 1..=MAX_RETRIES {
                match qail_qdrant::QdrantDriver::connect(&qdrant_host, qdrant_port).await {
                    Ok(driver) => {
                        qdrant = driver;
                        println!("{} Reconnected to Qdrant", "✓".green());
                        consecutive_errors = 0;
                        break;
                    }
                    Err(e) => {
                        let backoff = std::cmp::min(
                            INITIAL_BACKOFF_MS * 2u64.pow(attempt - 1),
                            MAX_BACKOFF_MS,
                        );
                        if attempt == MAX_RETRIES {
                            println!(
                                "{} Qdrant reconnection failed after {} attempts",
                                "✗".red(),
                                MAX_RETRIES
                            );
                            // Wait before trying the whole loop again
                            tokio::time::sleep(Duration::from_secs(60)).await;
                            consecutive_errors = 0; // Reset to try again
                            break;
                        }
                        println!(
                            "{} Reconnect failed (attempt {}/{}): {}, retrying in {}ms...",
                            "!".yellow(),
                            attempt,
                            MAX_RETRIES,
                            e,
                            backoff
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
                    }
                }
            }
            continue;
        }

        // Periodic janitor: recover zombie jobs every 60 seconds
        if last_janitor_run.elapsed().as_secs() >= JANITOR_INTERVAL_SECS {
            if let Ok(recovered) = recover_stale_jobs(&mut pg).await
                && recovered > 0
            {
                println!("🧹 Janitor: Recovered {} zombie jobs", recovered);
            }
            last_janitor_run = Instant::now();
        }

        // Poll for pending items
        let items = match fetch_pending_items(&mut pg, batch_size).await {
            Ok(items) => {
                consecutive_errors = 0;
                items
            }
            Err(e) => {
                consecutive_errors += 1;
                println!(
                    "{} PostgreSQL poll failed: {} (consecutive: {})",
                    "!".yellow(),
                    e,
                    consecutive_errors
                );
                tokio::time::sleep(poll_interval).await;
                continue;
            }
        };

        if items.is_empty() {
            tokio::time::sleep(poll_interval).await;
            continue;
        }

        println!("{} Processing {} items...", "→".cyan(), items.len());

        for item in items {
            let result =
                process_item(&item, &sync_rules, &mut pg, &mut qdrant, &embedding_model).await;

            match result {
                Ok(_) => {
                    if let Err(e) = mark_processed(&mut pg, item.id).await {
                        println!(
                            "{} Failed to mark item {} as processed: {}",
                            "!".yellow(),
                            item.id,
                            e
                        );
                    } else {
                        total_processed += 1;
                    }
                    consecutive_errors = 0;
                }
                Err(e) => {
                    let error_str = e.to_string();
                    // Check if this is a connection error
                    if error_str.contains("Connection")
                        || error_str.contains("refused")
                        || error_str.contains("broken pipe")
                    {
                        consecutive_errors += 1;
                        println!(
                            "{} Connection error on item {}: {} (consecutive: {})",
                            "!".yellow(),
                            item.id,
                            e,
                            consecutive_errors
                        );
                    }
                    if let Err(mark_err) = mark_failed(&mut pg, item.id, &error_str).await {
                        println!(
                            "{} Failed to mark item {} as failed: {}",
                            "!".yellow(),
                            item.id,
                            mark_err
                        );
                    } else {
                        println!("{} Failed item {}: {}", "✗".red(), item.id, e);
                    }
                }
            }
        }

        let elapsed = start_time.elapsed().as_secs();
        let rate = if elapsed > 0 {
            total_processed / elapsed
        } else {
            total_processed
        };
        println!(
            "{} Processed {} total ({}/sec)",
            "✓".green(),
            total_processed,
            rate
        );
    }

    // Graceful shutdown complete
    println!(
        "✅ Graceful shutdown complete. Processed {} total items.",
        total_processed
    );
    Ok(())
}

fn load_config() -> Result<WorkerConfig> {
    let config_path = Path::new("qail.toml");
    if !config_path.exists() {
        anyhow::bail!("qail.toml not found. Run 'qail init' first.");
    }
    let content = fs::read_to_string(config_path)?;
    let config: WorkerConfig = toml::from_str(&content)?;
    Ok(config)
}

fn parse_grpc_url(url: &str) -> Result<(String, u16)> {
    let raw = url.trim();
    if raw.is_empty() {
        anyhow::bail!("Qdrant gRPC endpoint is empty");
    }

    let without_scheme = raw
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let authority = without_scheme.split('/').next().unwrap_or(without_scheme);
    if authority.is_empty() {
        anyhow::bail!("Invalid Qdrant gRPC endpoint: '{url}'");
    }

    if authority.starts_with('[')
        && let Some(end) = authority.find(']')
    {
        let host = &authority[1..end];
        let port = authority[end + 1..]
            .strip_prefix(':')
            .and_then(|p| p.parse().ok())
            .unwrap_or(6334);
        return Ok((host.to_string(), port));
    }

    if let Some((host, port_str)) = authority.rsplit_once(':')
        && !host.is_empty()
        && !host.contains(':')
        && let Ok(port) = port_str.parse::<u16>()
    {
        return Ok((host.to_string(), port));
    }

    Ok((authority.to_string(), 6334))
}

/// Parse PostgreSQL URL: postgres://user:password@host:port/database
fn parse_postgres_url(url: &str) -> Result<(String, u16, String, String, Option<String>)> {
    let url = url
        .trim_start_matches("postgres://")
        .trim_start_matches("postgresql://");

    // Split by @ to separate credentials from host
    let (credentials, host_part): (Option<&str>, &str) = if url.contains('@') {
        let parts: Vec<&str> = url.splitn(2, '@').collect();
        (
            Some(parts[0]),
            parts.get(1).copied().unwrap_or("localhost/postgres"),
        )
    } else {
        (None, url)
    };

    // Parse host:port/database
    let (host_port, database) = if host_part.contains('/') {
        let parts: Vec<&str> = host_part.splitn(2, '/').collect();
        (parts[0], parts.get(1).unwrap_or(&"postgres").to_string())
    } else {
        (host_part, "postgres".to_string())
    };

    // Parse host:port
    let (host, port) = if host_port.contains(':') {
        let parts: Vec<&str> = host_port.split(':').collect();
        (
            parts[0].to_string(),
            parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(5432),
        )
    } else {
        (host_port.to_string(), 5432u16)
    };

    // Parse user:password
    let (user, password) = if let Some(creds) = credentials {
        if creds.contains(':') {
            let parts: Vec<&str> = creds.splitn(2, ':').collect();
            (
                parts[0].to_string(),
                Some(parts.get(1).unwrap_or(&"").to_string()),
            )
        } else {
            (creds.to_string(), None)
        }
    } else {
        ("postgres".to_string(), None)
    };

    Ok((host, port, user, database, password))
}

async fn fetch_pending_items(pg: &mut qail_pg::PgDriver, limit: u32) -> Result<Vec<QueueItem>> {
    // Atomic claim using UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING ...
    // This prevents worker races under concurrent consumers.
    let claim_subquery = Qail::get("_qail_queue")
        .column("id")
        .where_eq("status", "pending")
        .lt("retry_count", 5) // Poison-pill protection
        .order_asc("id")
        .limit(limit as i64)
        .for_update_skip_locked();

    let claim_cmd = Qail::set("_qail_queue")
        .set_value("status", "processing")
        .set_value("processed_at", now())
        .filter(
            "id",
            Operator::In,
            Value::Subquery(Box::new(claim_subquery)),
        )
        .returning(["id", "ref_table", "ref_id", "operation", "payload"]);

    let rows = pg.fetch_all(&claim_cmd).await?;

    let items: Vec<QueueItem> = rows
        .iter()
        .map(|row| QueueItem {
            id: row.get_i64_by_name("id").unwrap_or(0),
            ref_table: row.get_string_by_name("ref_table").unwrap_or_default(),
            ref_id: row.get_string_by_name("ref_id").unwrap_or_default(),
            operation: row.get_string_by_name("operation").unwrap_or_default(),
            payload: row
                .get_json_by_name("payload")
                .and_then(|s| serde_json::from_str(&s).ok()),
        })
        .collect();

    Ok(items)
}

async fn process_item(
    item: &QueueItem,
    sync_rules: &std::collections::HashMap<String, &SyncRule>,
    pg: &mut qail_pg::PgDriver,
    qdrant: &mut qail_qdrant::QdrantDriver,
    embedding_model: &dyn EmbeddingModel,
) -> Result<()> {
    let rule = sync_rules
        .get(&item.ref_table)
        .ok_or_else(|| anyhow::anyhow!("No sync rule for table: {}", item.ref_table))?;

    match item.operation.as_str() {
        "UPSERT" => {
            // READ-REPAIR PATTERN: Do NOT use stale payload from queue!
            // The queue is a "dirty flag", not a source of truth.
            // Always fetch FRESH data from the source table to prevent time-travel bugs.

            let trigger_col = rule.trigger_column.as_deref().unwrap_or("description");
            let fetch_cmd = Qail::get(item.ref_table.as_str())
                .column(trigger_col)
                .where_eq("id", item.ref_id.as_str())
                .limit(1);

            match pg.fetch_all(&fetch_cmd).await {
                Ok(rows) if !rows.is_empty() => {
                    // Row exists - extract fresh text and upsert
                    let text = rows[0]
                        .get_string(0)
                        .ok_or_else(|| anyhow::anyhow!("No text in column '{}'", trigger_col))?;

                    // Generate embedding from FRESH data
                    let vector = embedding_model.embed(&text);

                    // Upsert to Qdrant
                    let point = qail_qdrant::Point {
                        id: qail_qdrant::PointId::Num(item.ref_id.parse().unwrap_or(0)),
                        vector,
                        payload: std::collections::HashMap::new(),
                    };

                    qdrant
                        .upsert(&rule.target_collection, &[point], true)
                        .await?;
                }
                Ok(_) | Err(_) => {
                    // Row doesn't exist - treat as DELETE
                    // This handles the case where the row was deleted after the queue event
                    let point_id = item.ref_id.parse().unwrap_or(0);
                    qdrant
                        .delete_points(&rule.target_collection, &[point_id])
                        .await?;
                }
            }
        }
        "DELETE" => {
            // Deletes are idempotent and safe to execute out-of-order
            let point_id = item.ref_id.parse().unwrap_or(0);
            qdrant
                .delete_points(&rule.target_collection, &[point_id])
                .await?;
        }
        _ => {
            anyhow::bail!("Unknown operation: {}", item.operation);
        }
    }

    Ok(())
}

async fn mark_processed(pg: &mut qail_pg::PgDriver, id: i64) -> Result<()> {
    let cmd = Qail::set("_qail_queue")
        .set_value("status", "processed")
        .set_value("processed_at", now())
        .where_eq("id", id);
    pg.execute(&cmd).await?;
    Ok(())
}

async fn mark_failed(pg: &mut qail_pg::PgDriver, id: i64, error: &str) -> Result<()> {
    let retry_plus_one = binary(col("retry_count"), BinaryOp::Add, int(1)).build();
    let cmd = Qail::set("_qail_queue")
        .set_value("status", "failed")
        .set_value("retry_count", retry_plus_one)
        .set_value("error_message", error)
        .where_eq("id", id);
    pg.execute(&cmd).await?;
    Ok(())
}

/// Janitor: Recover zombie jobs (crashed workers left jobs in 'processing' state)
/// This runs periodically to reset stale jobs back to 'pending' for retry.
async fn recover_stale_jobs(pg: &mut qail_pg::PgDriver) -> Result<u64> {
    // Reset jobs that have been 'processing' for more than 10 minutes
    // These are likely from crashed workers
    let retry_plus_one = binary(col("retry_count"), BinaryOp::Add, int(1)).build();
    let recover_cmd = Qail::set("_qail_queue")
        .set_value("status", "pending")
        .set_value("retry_count", retry_plus_one)
        .where_eq("status", "processing")
        .lt("processed_at", now_minus("10 minutes"));
    pg.execute(&recover_cmd).await?;

    // Count how many were recovered (for logging)
    let count_cmd = Qail::get("_qail_queue")
        .select_expr(count().alias("count"))
        .where_eq("status", "pending")
        .gt("retry_count", 0)
        .gte("processed_at", now_minus("1 minute"));
    let rows = pg.fetch_all(&count_cmd).await.unwrap_or_default();
    let recovered = rows
        .first()
        .and_then(|r| r.get_i64_by_name("count").or_else(|| r.get_i64(0)))
        .unwrap_or(0) as u64;

    Ok(recovered)
}

#[cfg(test)]
mod tests {
    use super::parse_grpc_url;

    #[test]
    fn parse_grpc_url_supports_host_port_and_scheme() {
        assert_eq!(
            parse_grpc_url("localhost:6334").expect("parse localhost"),
            ("localhost".to_string(), 6334)
        );
        assert_eq!(
            parse_grpc_url("https://cloud.qdrant.io:443").expect("parse https endpoint"),
            ("cloud.qdrant.io".to_string(), 443)
        );
        assert_eq!(
            parse_grpc_url("http://qdrant.internal:6334/grpc").expect("parse path endpoint"),
            ("qdrant.internal".to_string(), 6334)
        );
    }

    #[test]
    fn parse_grpc_url_supports_bracketed_ipv6_and_default_port() {
        assert_eq!(
            parse_grpc_url("[::1]:6334").expect("parse ipv6"),
            ("::1".to_string(), 6334)
        );
        assert_eq!(
            parse_grpc_url("qdrant.internal").expect("parse host"),
            ("qdrant.internal".to_string(), 6334)
        );
    }

    #[test]
    fn parse_grpc_url_rejects_empty() {
        let err = parse_grpc_url("").expect_err("empty endpoint must fail");
        assert!(err.to_string().contains("empty"));
    }
}
