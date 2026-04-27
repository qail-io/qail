//! A/B Chaos Test — Engine REST API vs Qail Gateway
//!
//! Hits both servers with identical HTTP requests to the same endpoint
//! and compares throughput + latency under concurrent load.
//!
//! Requires:
//!   - Engine running on ENGINE_URL (default: http://localhost:8080)
//!   - Qail Gateway running on GATEWAY_URL (default: http://localhost:9090)
//!
//! Run:
//!   ENGINE_URL=http://localhost:8080 \
//!   GATEWAY_URL=http://localhost:9090 \
//!   CONCURRENCY=20 DURATION=10 \
//!   cargo run -p qail-gateway --example chaos_ab --release

use reqwest::Client;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

// ============================================================================
// Configuration
// ============================================================================

struct Config {
    engine_url: String,
    gateway_url: String,
    concurrency: usize,
    duration: Duration,
}

impl Config {
    fn from_env() -> Self {
        Self {
            engine_url: std::env::var("ENGINE_URL")
                .unwrap_or_else(|_| "http://localhost:8080".to_string()),
            gateway_url: std::env::var("GATEWAY_URL")
                .unwrap_or_else(|_| "http://localhost:9090".to_string()),
            concurrency: std::env::var("CONCURRENCY")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(20),
            duration: Duration::from_secs(
                std::env::var("DURATION")
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

struct Metrics {
    latencies: std::sync::Mutex<Vec<Duration>>,
    success: AtomicU64,
    errors_4xx: AtomicU64,
    errors_5xx: AtomicU64,
    errors_net: AtomicU64,
}

impl Metrics {
    fn new() -> Self {
        Self {
            latencies: std::sync::Mutex::new(Vec::with_capacity(100_000)),
            success: AtomicU64::new(0),
            errors_4xx: AtomicU64::new(0),
            errors_5xx: AtomicU64::new(0),
            errors_net: AtomicU64::new(0),
        }
    }

    fn record(&self, d: Duration, status: Option<u16>) {
        self.latencies.lock().unwrap().push(d);
        match status {
            Some(s) if (200..400).contains(&s) => {
                self.success.fetch_add(1, Ordering::Relaxed);
            }
            Some(s) if (400..500).contains(&s) => {
                self.errors_4xx.fetch_add(1, Ordering::Relaxed);
            }
            Some(_) => {
                self.errors_5xx.fetch_add(1, Ordering::Relaxed);
            }
            None => {
                self.errors_net.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn report(&self, label: &str, wall_time: Duration) -> Stats {
        let mut times = self.latencies.lock().unwrap().clone();
        times.sort();
        let total = times.len();
        let ok = self.success.load(Ordering::Relaxed);
        let e4 = self.errors_4xx.load(Ordering::Relaxed);
        let e5 = self.errors_5xx.load(Ordering::Relaxed);
        let en = self.errors_net.load(Ordering::Relaxed);
        let qps = total as f64 / wall_time.as_secs_f64();

        let (p50, p95, p99, min, max) = if !times.is_empty() {
            (
                times[total / 2],
                times[std::cmp::min((total as f64 * 0.95) as usize, total - 1)],
                times[std::cmp::min((total as f64 * 0.99) as usize, total - 1)],
                times[0],
                times[total - 1],
            )
        } else {
            (
                Duration::ZERO,
                Duration::ZERO,
                Duration::ZERO,
                Duration::ZERO,
                Duration::ZERO,
            )
        };

        println!("\n  ━━━ {} ━━━", label);
        println!(
            "  Requests: {} | ok={} | 4xx={} | 5xx={} | net-err={}",
            total, ok, e4, e5, en
        );
        println!(
            "  p50={:.2}ms | p95={:.2}ms | p99={:.2}ms",
            p50.as_secs_f64() * 1000.0,
            p95.as_secs_f64() * 1000.0,
            p99.as_secs_f64() * 1000.0
        );
        println!(
            "  min={:.2}ms | max={:.2}ms",
            min.as_secs_f64() * 1000.0,
            max.as_secs_f64() * 1000.0
        );
        println!(
            "  Throughput: {:.0} QPS | Wall: {:.2}s",
            qps,
            wall_time.as_secs_f64()
        );

        Stats {
            label: label.to_string(),
            qps,
            p50,
            p95,
            p99,
            ok,
            errors: e4 + e5 + en,
        }
    }
}

#[allow(dead_code)]
struct Stats {
    label: String,
    qps: f64,
    p50: Duration,
    p95: Duration,
    p99: Duration,
    ok: u64,
    errors: u64,
}

// ============================================================================
// Attack Runner
// ============================================================================

async fn attack(
    label: &str,
    base_url: &str,
    endpoint: &str,
    concurrency: usize,
    duration: Duration,
    client: &Client,
) -> Stats {
    let metrics = Arc::new(Metrics::new());
    let url = format!("{}{}", base_url, endpoint);
    let mut handles = Vec::with_capacity(concurrency);
    let wall_start = Instant::now();

    for _ in 0..concurrency {
        let client = client.clone();
        let metrics = Arc::clone(&metrics);
        let url = url.clone();
        let deadline = duration;

        handles.push(tokio::spawn(async move {
            let start = Instant::now();
            while start.elapsed() < deadline {
                let req_start = Instant::now();
                match client.get(&url).send().await {
                    Ok(resp) => {
                        let status = resp.status().as_u16();
                        let _ = resp.bytes().await; // consume body
                        metrics.record(req_start.elapsed(), Some(status));
                    }
                    Err(_) => {
                        metrics.record(req_start.elapsed(), None);
                    }
                }
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    let wall_time = wall_start.elapsed();
    metrics.report(label, wall_time)
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

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║         A/B CHAOS TEST — Engine REST vs Qail Gateway           ║");
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!("║  Engine:      {:<50}║", config.engine_url);
    println!("║  Gateway:     {:<50}║", config.gateway_url);
    println!("║  Concurrency: {:<50}║", config.concurrency);
    println!(
        "║  Duration:    {:<50}║",
        format!("{}s per phase", config.duration.as_secs())
    );
    println!("╚══════════════════════════════════════════════════════════════════╝");

    // Health checks
    print!("\n⏳ Engine health... ");
    match client
        .get(format!("{}/health", config.engine_url))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => println!("✅ OK"),
        Ok(r) => {
            println!("❌ {}", r.status());
            return Err("Engine health failed".into());
        }
        Err(e) => {
            println!("❌ {}", e);
            return Err("Cannot reach engine".into());
        }
    }
    print!("⏳ Gateway health... ");
    match client
        .get(format!("{}/health", config.gateway_url))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => println!("✅ OK"),
        Ok(r) => {
            println!("❌ {}", r.status());
            return Err("Gateway health failed".into());
        }
        Err(e) => {
            println!("❌ {}", e);
            return Err("Cannot reach gateway".into());
        }
    }

    // ==== Test 1: GET /api/harbors (list all) ====
    println!("\n\n🔥 TEST 1: GET /api/harbors — List all active harbors");
    println!("   (Engine: list_harbors_public → SQLx query)");
    println!("   (Gateway: auto-REST → Qail AST + prepared stmt)\n");

    let endpoint = "/api/harbors";

    // Warmup both
    println!("  Warming up...");
    let _ = attack(
        "warmup-engine",
        &config.engine_url,
        endpoint,
        5,
        Duration::from_secs(2),
        &client,
    )
    .await;
    let _ = attack(
        "warmup-gateway",
        &config.gateway_url,
        endpoint,
        5,
        Duration::from_secs(2),
        &client,
    )
    .await;

    let mut all_results: Vec<(String, Vec<Stats>)> = Vec::new();

    // Run at different concurrency levels
    for concurrency in [
        config.concurrency,
        config.concurrency * 2,
        config.concurrency * 4,
    ] {
        println!(
            "\n  ── {} concurrent workers, {}s ──",
            concurrency,
            config.duration.as_secs()
        );

        let engine_stats = attack(
            &format!("Engine REST ({concurrency}c)"),
            &config.engine_url,
            endpoint,
            concurrency,
            config.duration,
            &client,
        )
        .await;

        let gateway_stats = attack(
            &format!("Qail Gateway ({concurrency}c)"),
            &config.gateway_url,
            endpoint,
            concurrency,
            config.duration,
            &client,
        )
        .await;

        // Quick comparison
        let speedup = gateway_stats.qps / engine_stats.qps.max(1.0);
        println!(
            "\n  → Gateway is {:.1}× the throughput of Engine at {}c",
            speedup, concurrency
        );

        all_results.push((
            format!("{}c", concurrency),
            vec![engine_stats, gateway_stats],
        ));
    }

    // ==== Test 2: GET /api/harbors?limit=5 (paginated) ====
    println!("\n\n🔥 TEST 2: GET /api/harbors?limit=5 — Paginated (small)");
    let endpoint2 = "/api/harbors?limit=5";

    let engine_s = attack(
        "Engine REST (paginated)",
        &config.engine_url,
        endpoint2,
        config.concurrency,
        config.duration,
        &client,
    )
    .await;

    let gateway_s = attack(
        "Qail Gateway (paginated)",
        &config.gateway_url,
        endpoint2,
        config.concurrency,
        config.duration,
        &client,
    )
    .await;

    let speedup2 = gateway_s.qps / engine_s.qps.max(1.0);
    println!(
        "\n  → Gateway is {:.1}× the throughput of Engine (paginated)",
        speedup2
    );

    // ==== Final Summary ====
    println!(
        "\n\n╔══════════════════════════════════════════════════════════════════════════════════════════╗"
    );
    println!(
        "║  FINAL RESULTS                                                                         ║"
    );
    println!(
        "╠══════════════════════════════════════════════════════════════════════════════════════════╣"
    );

    for (label, stats) in &all_results {
        for s in stats {
            println!(
                "║  {:40} {:>7.0} QPS  p50 {:>8.2}ms  p99 {:>8.2}ms  ok={} err={} ║",
                s.label,
                s.qps,
                s.p50.as_secs_f64() * 1000.0,
                s.p99.as_secs_f64() * 1000.0,
                s.ok,
                s.errors,
            );
        }
        if stats.len() == 2 {
            let ratio = stats[1].qps / stats[0].qps.max(1.0);
            println!(
                "║  {:40} → Gateway = {:.1}× Engine{}║",
                label,
                ratio,
                " ".repeat(30 - format!("{:.1}", ratio).len()),
            );
        }
    }

    // Paginated
    println!(
        "║  {:40} {:>7.0} QPS  p50 {:>8.2}ms  p99 {:>8.2}ms  ok={} err={} ║",
        engine_s.label,
        engine_s.qps,
        engine_s.p50.as_secs_f64() * 1000.0,
        engine_s.p99.as_secs_f64() * 1000.0,
        engine_s.ok,
        engine_s.errors
    );
    println!(
        "║  {:40} {:>7.0} QPS  p50 {:>8.2}ms  p99 {:>8.2}ms  ok={} err={} ║",
        gateway_s.label,
        gateway_s.qps,
        gateway_s.p50.as_secs_f64() * 1000.0,
        gateway_s.p99.as_secs_f64() * 1000.0,
        gateway_s.ok,
        gateway_s.errors
    );

    println!(
        "╚══════════════════════════════════════════════════════════════════════════════════════════╝"
    );

    Ok(())
}
