//! qail worker - Sync worker daemon
//!
//! Polls _qail_queue from PostgreSQL, generates embeddings,
//! and syncs to Qdrant. Implements the "Transactional Outbox" pattern.

use anyhow::Result;
use colored::*;
use serde::Deserialize;
use std::fs;
use std::path::Path;
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
#[allow(dead_code)]
struct SyncRule {
    source_table: String,
    target_collection: String,
    #[serde(default)]
    trigger_column: Option<String>,
    #[serde(default)]
    embedding_model: Option<String>,
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
        let hash = text.bytes().fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
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
        anyhow::bail!("Worker only runs in 'hybrid' mode. Current mode: {}", config.project.mode);
    }

    let pg_url = config.postgres
        .ok_or_else(|| anyhow::anyhow!("Missing [postgres] config in qail.toml"))?
        .url;
    
    let qdrant_config = config.qdrant
        .ok_or_else(|| anyhow::anyhow!("Missing [qdrant] config in qail.toml"))?;
    
    let qdrant_grpc = qdrant_config.grpc.unwrap_or_else(|| {
        // Convert REST URL to gRPC (6333 -> 6334)
        qdrant_config.url.replace(":6333", ":6334")
    });

    println!("PostgreSQL: {}", pg_url.dimmed());
    println!("Qdrant gRPC: {}", qdrant_grpc.dimmed());
    println!("Poll interval: {}ms", poll_interval_ms);
    println!("Batch size: {}", batch_size);
    println!();

    // Build sync rule lookup
    let sync_rules: std::collections::HashMap<String, &SyncRule> = config.sync
        .iter()
        .map(|r| (r.source_table.clone(), r))
        .collect();

    if sync_rules.is_empty() {
        println!("{} No [[sync]] rules configured. Worker has nothing to do.", "⚠".yellow());
        return Ok(());
    }

    println!("Sync rules:");
    for rule in &config.sync {
        println!("  {} → {}", rule.source_table.yellow(), rule.target_collection.cyan());
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
            qail_pg::PgDriver::connect_with_password(&pg_host, pg_port, &pg_user, &pg_database, password).await
        } else {
            qail_pg::PgDriver::connect(&pg_host, pg_port, &pg_user, &pg_database).await
        };
        
        match result {
            Ok(driver) => {
                pg = Some(driver);
                break;
            }
            Err(e) => {
                let backoff = std::cmp::min(INITIAL_BACKOFF_MS * 2u64.pow(attempt - 1), MAX_BACKOFF_MS);
                if attempt == MAX_RETRIES {
                    println!("{} PostgreSQL connection failed after {} attempts: {}", "✗".red(), MAX_RETRIES, e);
                    anyhow::bail!("Failed to connect to PostgreSQL: {}", e);
                }
                println!("{} PostgreSQL connection failed (attempt {}/{}), retrying in {}ms...", 
                    "!".yellow(), attempt, MAX_RETRIES, backoff);
                tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
            }
        }
    }
    let mut pg = pg.unwrap();
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
                let backoff = std::cmp::min(INITIAL_BACKOFF_MS * 2u64.pow(attempt - 1), MAX_BACKOFF_MS);
                if attempt == MAX_RETRIES {
                    println!("{} Qdrant connection failed after {} attempts: {}", "✗".red(), MAX_RETRIES, e);
                    anyhow::bail!("Failed to connect to Qdrant: {}", e);
                }
                println!("{} Qdrant connection failed (attempt {}/{}), retrying in {}ms...", 
                    "!".yellow(), attempt, MAX_RETRIES, backoff);
                tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
            }
        }
    }
    let mut qdrant = qdrant.unwrap();
    println!("{} Connected to Qdrant", "✓".green());

    // Use dummy embedding for now (user would inject their own)
    let embedding_model = DummyEmbedding::new(1536);

    println!();
    println!("{}", "Starting poll loop... (Ctrl+C to stop)".white().bold());
    println!();

    let poll_interval = Duration::from_millis(poll_interval_ms);
    let mut total_processed = 0u64;
    let start_time = Instant::now();
    let mut consecutive_errors = 0u32;

    loop {
        // Check for too many consecutive errors (circuit breaker)
        if consecutive_errors >= 5 {
            println!("{} Too many consecutive errors, reconnecting...", "!".yellow());
            
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
                        let backoff = std::cmp::min(INITIAL_BACKOFF_MS * 2u64.pow(attempt - 1), MAX_BACKOFF_MS);
                        if attempt == MAX_RETRIES {
                            println!("{} Qdrant reconnection failed after {} attempts", "✗".red(), MAX_RETRIES);
                            // Wait before trying the whole loop again
                            tokio::time::sleep(Duration::from_secs(60)).await;
                            consecutive_errors = 0; // Reset to try again
                            break;
                        }
                        println!("{} Reconnect failed (attempt {}/{}): {}, retrying in {}ms...", 
                            "!".yellow(), attempt, MAX_RETRIES, e, backoff);
                        tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
                    }
                }
            }
            continue;
        }

        // Poll for pending items
        let items = match fetch_pending_items(&mut pg, batch_size).await {
            Ok(items) => {
                consecutive_errors = 0;
                items
            }
            Err(e) => {
                consecutive_errors += 1;
                println!("{} PostgreSQL poll failed: {} (consecutive: {})", "!".yellow(), e, consecutive_errors);
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
            let result = process_item(&item, &sync_rules, &mut qdrant, &embedding_model).await;
            
            match result {
                Ok(_) => {
                    if let Err(e) = mark_processed(&mut pg, item.id).await {
                        println!("{} Failed to mark item {} as processed: {}", "!".yellow(), item.id, e);
                    } else {
                        total_processed += 1;
                    }
                    consecutive_errors = 0;
                }
                Err(e) => {
                    let error_str = e.to_string();
                    // Check if this is a connection error
                    if error_str.contains("Connection") || error_str.contains("refused") || error_str.contains("broken pipe") {
                        consecutive_errors += 1;
                        println!("{} Connection error on item {}: {} (consecutive: {})", "!".yellow(), item.id, e, consecutive_errors);
                    }
                    if let Err(mark_err) = mark_failed(&mut pg, item.id, &error_str).await {
                        println!("{} Failed to mark item {} as failed: {}", "!".yellow(), item.id, mark_err);
                    } else {
                        println!("{} Failed item {}: {}", "✗".red(), item.id, e);
                    }
                }
            }
        }

        let elapsed = start_time.elapsed().as_secs();
        let rate = if elapsed > 0 { total_processed / elapsed } else { total_processed };
        println!("{} Processed {} total ({}/sec)", "✓".green(), total_processed, rate);
    }
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
    let url = url.trim_start_matches("http://").trim_start_matches("https://");
    let parts: Vec<&str> = url.split(':').collect();
    let host = parts.first().unwrap_or(&"localhost").to_string();
    let port = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(6334);
    Ok((host, port))
}

/// Parse PostgreSQL URL: postgres://user:password@host:port/database
fn parse_postgres_url(url: &str) -> Result<(String, u16, String, String, Option<String>)> {
    let url = url.trim_start_matches("postgres://").trim_start_matches("postgresql://");
    
    // Split by @ to separate credentials from host
    let (credentials, host_part): (Option<&str>, &str) = if url.contains('@') {
        let parts: Vec<&str> = url.splitn(2, '@').collect();
        (Some(parts[0]), parts.get(1).copied().unwrap_or("localhost/postgres"))
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
        (parts[0].to_string(), parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(5432))
    } else {
        (host_port.to_string(), 5432u16)
    };
    
    // Parse user:password
    let (user, password) = if let Some(creds) = credentials {
        if creds.contains(':') {
            let parts: Vec<&str> = creds.splitn(2, ':').collect();
            (parts[0].to_string(), Some(parts.get(1).unwrap_or(&"").to_string()))
        } else {
            (creds.to_string(), None)
        }
    } else {
        ("postgres".to_string(), None)
    };
    
    Ok((host, port, user, database, password))
}

async fn fetch_pending_items(pg: &mut qail_pg::PgDriver, limit: u32) -> Result<Vec<QueueItem>> {
    let sql = format!(
        "SELECT id, ref_table, ref_id, operation, payload \
         FROM _qail_queue \
         WHERE status = 'pending' \
         ORDER BY id ASC \
         LIMIT {}",
        limit
    );
    
    let rows = pg.fetch_raw(&sql).await?;
    
    let items: Vec<QueueItem> = rows.iter().map(|row| {
        QueueItem {
            id: row.get_i64_by_name("id").unwrap_or(0),
            ref_table: row.get_string_by_name("ref_table").unwrap_or_default(),
            ref_id: row.get_string_by_name("ref_id").unwrap_or_default(),
            operation: row.get_string_by_name("operation").unwrap_or_default(),
            payload: row.get_json_by_name("payload")
                .and_then(|s| serde_json::from_str(&s).ok()),
        }
    }).collect();
    
    Ok(items)
}

async fn process_item(
    item: &QueueItem,
    sync_rules: &std::collections::HashMap<String, &SyncRule>,
    qdrant: &mut qail_qdrant::QdrantDriver,
    embedding_model: &dyn EmbeddingModel,
) -> Result<()> {
    let rule = sync_rules.get(&item.ref_table)
        .ok_or_else(|| anyhow::anyhow!("No sync rule for table: {}", item.ref_table))?;

    match item.operation.as_str() {
        "UPSERT" => {
            // Extract text from payload
            let text = extract_text_from_payload(&item.payload, rule.trigger_column.as_deref())?;
            
            // Generate embedding
            let vector = embedding_model.embed(&text);
            
            // Upsert to Qdrant
            let point = qail_qdrant::Point {
                id: qail_qdrant::PointId::Num(item.ref_id.parse().unwrap_or(0)),
                vector,
                payload: std::collections::HashMap::new(),
            };
            
            qdrant.upsert(&rule.target_collection, &[point], true).await?;
        }
        "DELETE" => {
            // Delete from Qdrant
            let point_id = item.ref_id.parse().unwrap_or(0);
            qdrant.delete_points(&rule.target_collection, &[point_id]).await?;
        }
        _ => {
            anyhow::bail!("Unknown operation: {}", item.operation);
        }
    }
    
    Ok(())
}

fn extract_text_from_payload(payload: &Option<serde_json::Value>, trigger_col: Option<&str>) -> Result<String> {
    let payload = payload.as_ref()
        .ok_or_else(|| anyhow::anyhow!("Missing payload for embedding"))?;
    
    if let Some(col) = trigger_col {
        // Extract specific column
        payload.get(col)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("Missing column '{}' in payload", col))
    } else {
        // Use entire payload as JSON string
        Ok(serde_json::to_string(payload)?)
    }
}

async fn mark_processed(pg: &mut qail_pg::PgDriver, id: i64) -> Result<()> {
    let sql = format!(
        "UPDATE _qail_queue SET status = 'processed', processed_at = NOW() WHERE id = {}",
        id
    );
    pg.execute_raw(&sql).await?;
    Ok(())
}

async fn mark_failed(pg: &mut qail_pg::PgDriver, id: i64, error: &str) -> Result<()> {
    let escaped_error = error.replace('\'', "''");
    let sql = format!(
        "UPDATE _qail_queue SET status = 'failed', retry_count = retry_count + 1, error_message = '{}' WHERE id = {}",
        escaped_error, id
    );
    pg.execute_raw(&sql).await?;
    Ok(())
}
