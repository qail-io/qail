//! Gateway Stress Test
//!
//! Exercises all REST endpoints under high concurrency and reports
//! latency percentiles, throughput, error rates, and cache statistics.
//!
//! # Usage
//!
//! ```bash
//! # Terminal 1: Start the gateway
//! DATABASE_URL="postgres://localhost/qail" cargo run -p qail-gateway --example serve
//!
//! # Terminal 2: Run stress test
//! GATEWAY_URL=http://localhost:8080 cargo run -p qail-gateway --example stress --release
//! ```
//!
//! # Environment Variables
//!
//! - `GATEWAY_URL` — target gateway (default: `http://localhost:8080`)
//! - `STRESS_CONCURRENCY` — base concurrency (default: 50)
//! - `STRESS_DURATION` — per-phase duration in seconds (default: 10)

use hdrhistogram::Histogram;
use reqwest::Client;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

// ============================================================================
// Configuration
// ============================================================================

struct Config {
    base_url: String,
    concurrency: usize,
    duration: Duration,
}

impl Config {
    fn from_env() -> Self {
        Self {
            base_url: std::env::var("GATEWAY_URL")
                .unwrap_or_else(|_| "http://localhost:8080".to_string()),
            concurrency: std::env::var("STRESS_CONCURRENCY")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(50),
            duration: Duration::from_secs(
                std::env::var("STRESS_DURATION")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(10),
            ),
        }
    }
}

// ============================================================================
// Metrics
// ============================================================================

struct PhaseMetrics {
    histogram: Mutex<Histogram<u64>>,
    success: AtomicU64,
    client_errors: AtomicU64,   // 4xx
    server_errors: AtomicU64,   // 5xx
    network_errors: AtomicU64,  // connection failures
}

impl PhaseMetrics {
    fn new() -> Self {
        Self {
            histogram: Mutex::new(
                Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)
                    .expect("histogram init"),
            ),
            success: AtomicU64::new(0),
            client_errors: AtomicU64::new(0),
            server_errors: AtomicU64::new(0),
            network_errors: AtomicU64::new(0),
        }
    }

    async fn record(&self, duration: Duration, status: Option<u16>) {
        let micros = duration.as_micros() as u64;
        let _ = self.histogram.lock().await.record(micros);

        match status {
            Some(s) if (200..400).contains(&s) => {
                self.success.fetch_add(1, Ordering::Relaxed);
            }
            Some(s) if (400..500).contains(&s) => {
                self.client_errors.fetch_add(1, Ordering::Relaxed);
            }
            Some(_) => {
                self.server_errors.fetch_add(1, Ordering::Relaxed);
            }
            None => {
                self.network_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    async fn report(&self, phase_name: &str) {
        let h = self.histogram.lock().await;
        let total = self.success.load(Ordering::Relaxed)
            + self.client_errors.load(Ordering::Relaxed)
            + self.server_errors.load(Ordering::Relaxed)
            + self.network_errors.load(Ordering::Relaxed);

        let ok = self.success.load(Ordering::Relaxed);
        let err_4xx = self.client_errors.load(Ordering::Relaxed);
        let err_5xx = self.server_errors.load(Ordering::Relaxed);
        let err_net = self.network_errors.load(Ordering::Relaxed);

        println!();
        println!("  {}", phase_name);
        println!("  ────────────────────────────────────────────");
        println!(
            "  Requests: {} total | {} ok | {} 4xx | {} 5xx | {} net-err",
            total, ok, err_4xx, err_5xx, err_net
        );

        if h.len() > 0 {
            println!(
                "  Latency:  p50={:.1}ms | p95={:.1}ms | p99={:.1}ms | max={:.1}ms",
                h.value_at_quantile(0.50) as f64 / 1000.0,
                h.value_at_quantile(0.95) as f64 / 1000.0,
                h.value_at_quantile(0.99) as f64 / 1000.0,
                h.max() as f64 / 1000.0,
            );
        }

        if total > 0 {
            let duration_s = h.max() as f64 / 1_000_000.0; // rough estimate
            if duration_s > 0.0 {
                // QPS is better computed from wall clock; we'll use the phase runner's value
            }
        }
    }
}

// ============================================================================
// Phase Runner
// ============================================================================

async fn run_phase<F, Fut>(
    name: &str,
    concurrency: usize,
    duration: Duration,
    client: &Client,
    base_url: &str,
    metrics: &Arc<PhaseMetrics>,
    task_fn: F,
) where
    F: Fn(Client, String, Arc<PhaseMetrics>, u64) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    let start = Instant::now();
    let task_fn = Arc::new(task_fn);

    let mut handles = Vec::with_capacity(concurrency);

    for worker_id in 0..concurrency {
        let client = client.clone();
        let base_url = base_url.to_string();
        let metrics = Arc::clone(metrics);
        let task_fn = Arc::clone(&task_fn);
        let deadline = duration;

        handles.push(tokio::spawn(async move {
            let worker_start = Instant::now();
            let mut iteration = 0u64;
            while worker_start.elapsed() < deadline {
                task_fn(
                    client.clone(),
                    base_url.clone(),
                    Arc::clone(&metrics),
                    worker_id as u64 * 1_000_000 + iteration,
                )
                .await;
                iteration += 1;
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    let wall_time = start.elapsed();
    let total = metrics.success.load(Ordering::Relaxed)
        + metrics.client_errors.load(Ordering::Relaxed)
        + metrics.server_errors.load(Ordering::Relaxed)
        + metrics.network_errors.load(Ordering::Relaxed);

    let qps = total as f64 / wall_time.as_secs_f64();

    metrics.report(name).await;
    println!("  Wall:     {:.2}s | {:.0} QPS", wall_time.as_secs_f64(), qps);
}

// ============================================================================
// Request helpers
// ============================================================================

async fn do_request(
    metrics: &Arc<PhaseMetrics>,
    request: reqwest::RequestBuilder,
) {
    let start = Instant::now();
    match request.send().await {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let _ = resp.bytes().await; // consume body
            metrics.record(start.elapsed(), Some(status)).await;
        }
        Err(_) => {
            metrics.record(start.elapsed(), None).await;
        }
    }
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_env();
    let client = Client::builder()
        .pool_max_idle_per_host(config.concurrency * 2)
        .timeout(Duration::from_secs(30))
        .build()?;

    println!("═══════════════════════════════════════════════════");
    println!("  QAIL Gateway Stress Test");
    println!("═══════════════════════════════════════════════════");
    println!("  Target:      {}", config.base_url);
    println!("  Concurrency: {}", config.concurrency);
    println!("  Duration:    {}s per phase", config.duration.as_secs());
    println!("═══════════════════════════════════════════════════");

    // ── Health check ───────────────────────────────────────────
    print!("\n⏳ Health check... ");
    match client
        .get(format!("{}/health", config.base_url))
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => println!("✅ OK"),
        Ok(resp) => {
            println!("❌ Status {}", resp.status());
            return Err("Gateway health check failed".into());
        }
        Err(e) => {
            println!("❌ {}", e);
            return Err("Cannot reach gateway".into());
        }
    }

    // ── Phase 1: Warmup ────────────────────────────────────────
    {
        println!("\n🔥 Phase 1: Warmup (10 concurrent, 5s)");
        let m = Arc::new(PhaseMetrics::new());
        run_phase(
            "Warmup",
            10,
            Duration::from_secs(5),
            &client,
            &config.base_url,
            &m,
            |client, base, metrics, _| async move {
                let req = client.get(format!("{}/api/harbors?limit=5", base));
                do_request(&metrics, req).await;
            },
        )
        .await;
    }

    // ── Phase 2: Read Flood ────────────────────────────────────
    for concurrency in [
        config.concurrency,
        config.concurrency * 2,
        config.concurrency * 4,
    ] {
        println!(
            "\n📖 Phase 2: Read Flood ({} concurrent, {}s)",
            concurrency,
            config.duration.as_secs()
        );
        let m = Arc::new(PhaseMetrics::new());
        run_phase(
            &format!("Read Flood ({}c)", concurrency),
            concurrency,
            config.duration,
            &client,
            &config.base_url,
            &m,
            |client, base, metrics, i| async move {
                // Round-robin across tables to test cache + pool behavior
                let tables = ["harbors", "vessels", "odysseys", "operators", "destinations"];
                let table = tables[(i as usize) % tables.len()];
                let limit = (i % 10) + 1;
                let req = client.get(format!("{}/api/{}?limit={}", base, table, limit));
                do_request(&metrics, req).await;
            },
        )
        .await;
    }

    // ── Phase 3: Write Storm ───────────────────────────────────
    {
        println!(
            "\n✏️  Phase 3: Write Storm ({} concurrent, {}s)",
            config.concurrency,
            config.duration.as_secs()
        );
        let m = Arc::new(PhaseMetrics::new());
        run_phase(
            "Write Storm (POST)",
            config.concurrency,
            config.duration,
            &client,
            &config.base_url,
            &m,
            |client, base, metrics, i| async move {
                let body = serde_json::json!({
                    "name": format!("StressHarbor-{}", i),
                    "slug": format!("stress-harbor-{}", i),
                    "is_active": false,
                });
                let req = client
                    .post(format!("{}/api/harbors", base))
                    .json(&body);
                do_request(&metrics, req).await;
            },
        )
        .await;
    }

    // ── Phase 4: Mixed CRUD ────────────────────────────────────
    {
        let c = config.concurrency * 2;
        println!(
            "\n🔀 Phase 4: Mixed CRUD ({} concurrent, {}s)",
            c,
            (config.duration.as_secs() as f64 * 1.5) as u64
        );
        let m = Arc::new(PhaseMetrics::new());
        let mixed_duration = Duration::from_secs((config.duration.as_secs() as f64 * 1.5) as u64);
        run_phase(
            "Mixed CRUD",
            c,
            mixed_duration,
            &client,
            &config.base_url,
            &m,
            |client, base, metrics, i| async move {
                let op = i % 10;
                match op {
                    // 60% reads across tables
                    0..=2 => {
                        let req = client.get(format!("{}/api/harbors?limit=10", base));
                        do_request(&metrics, req).await;
                    }
                    3..=4 => {
                        let req = client.get(format!("{}/api/vessels?limit=10", base));
                        do_request(&metrics, req).await;
                    }
                    5 => {
                        let req = client.get(format!("{}/api/odysseys?limit=10", base));
                        do_request(&metrics, req).await;
                    }
                    // 20% creates
                    6..=7 => {
                        let body = serde_json::json!({
                            "name": format!("Mixed-Harbor-{}", i),
                            "slug": format!("mixed-harbor-{}", i),
                            "is_active": false,
                        });
                        let req = client
                            .post(format!("{}/api/harbors", base))
                            .json(&body);
                        do_request(&metrics, req).await;
                    }
                    // 10% operators
                    8 => {
                        let req = client.get(format!("{}/api/operators?limit=5", base));
                        do_request(&metrics, req).await;
                    }
                    // 10% destinations
                    _ => {
                        let req = client.get(format!("{}/api/destinations?limit=5", base));
                        do_request(&metrics, req).await;
                    }
                }
            },
        )
        .await;
    }

    // ── Phase 5: Burst (rate limiter test) ─────────────────────
    {
        let burst_concurrency = config.concurrency * 10; // 500 at default
        println!(
            "\n💥 Phase 5: Burst ({} concurrent, 3s) — rate limiter test",
            burst_concurrency
        );
        let m = Arc::new(PhaseMetrics::new());
        run_phase(
            "Burst (Rate Limiter)",
            burst_concurrency,
            Duration::from_secs(3),
            &client,
            &config.base_url,
            &m,
            |client, base, metrics, i| async move {
                let tables = ["harbors", "vessels", "odysseys", "operators"];
                let table = tables[(i as usize) % tables.len()];
                let req = client.get(format!("{}/api/{}?limit=1", base, table));
                do_request(&metrics, req).await;
            },
        )
        .await;

        let rejected = m.client_errors.load(Ordering::Relaxed);
        let total = m.success.load(Ordering::Relaxed) + rejected
            + m.server_errors.load(Ordering::Relaxed)
            + m.network_errors.load(Ordering::Relaxed);
        if total > 0 {
            println!(
                "  Rate-limited: {} / {} ({:.1}%)",
                rejected,
                total,
                rejected as f64 / total as f64 * 100.0
            );
        }
    }

    // ── Phase 6: Nested Reads ──────────────────────────────────
    {
        println!(
            "\n🔗 Phase 6: Nested Reads ({} concurrent, {}s)",
            config.concurrency,
            config.duration.as_secs()
        );
        let m = Arc::new(PhaseMetrics::new());
        run_phase(
            "Nested Reads (parent/child)",
            config.concurrency,
            config.duration,
            &client,
            &config.base_url,
            &m,
            |client, base, metrics, i| async move {
                // Use real operator UUIDs to test nested routes with actual joins
                let operator_ids = [
                    "680a70b3-7fb4-431b-9168-e3f6143e80da",
                    "12cbe5f9-5923-4c27-b001-e67fefffa68d",
                    "aed2742d-b456-4ba8-8a36-d9bdc868874c",
                    "f070bf51-7211-4497-bee5-a59920584fca",
                    "a6fda6c9-2ac6-4263-a3f8-e8b94b5d0153",
                ];
                let op_id = operator_ids[(i as usize) % operator_ids.len()];
                // Alternate between vessels and odysseys nested under operators
                let child = if i % 2 == 0 { "vessels" } else { "odysseys" };
                let req = client.get(format!(
                    "{}/api/operators/{}/{}?limit=10",
                    base, op_id, child
                ));
                do_request(&metrics, req).await;
            },
        )
        .await;
    }

    // ── Final Summary ──────────────────────────────────────────
    println!();
    println!("═══════════════════════════════════════════════════");
    println!("  ✅ All phases complete");
    println!("═══════════════════════════════════════════════════");

    // Try to scrape cache stats from /metrics
    if let Ok(resp) = client
        .get(format!("{}/metrics", config.base_url))
        .send()
        .await
    {
        if resp.status().is_success() {
            if let Ok(body) = resp.text().await {
                println!();
                println!("  📊 Prometheus Metrics (selection):");
                for line in body.lines() {
                    if line.starts_with("qail_queries_total")
                        || line.starts_with("qail_pool_")
                        || line.starts_with("qail_query_duration")
                    {
                        println!("     {}", line);
                    }
                }
            }
        }
    }

    Ok(())
}
