//! Battle Test: Mid-query PostgreSQL restart and recovery.
//!
//! Purpose:
//! - Force a server restart while a long query is running.
//! - Verify in-flight query fails fast with a transient/restart-class error.
//! - Verify fresh reconnect succeeds after server comes back.
//!
//! Run:
//!   QAIL_RESTART_CMD='brew services restart postgresql@18' \
//!   cargo run --release -p qail-pg --example battle_restart
//!
//! Optional env:
//! - DATABASE_URL
//! - QAIL_PG_HOST / PGHOST (default: localhost)
//! - QAIL_PG_PORT / PGPORT (default: 5432)
//! - QAIL_PG_USER / PGUSER (default: postgres)
//! - QAIL_PG_DB / PGDATABASE (default: postgres)
//! - QAIL_PG_PASSWORD / PGPASSWORD (optional)
//! - QAIL_RESTART_DELAY_MS (default: 1200)
//! - QAIL_LONG_QUERY_SECONDS (default: 20)
//! - QAIL_RECOVERY_TIMEOUT_MS (default: 30000)
//! - QAIL_RETRY_INTERVAL_MS (default: 250)

use qail_core::ast::Qail;
use qail_pg::{PgDriver, PgError};
use std::process::Command;
use std::time::{Duration, Instant};

const DEFAULT_RESTART_DELAY_MS: u64 = 1200;
const DEFAULT_LONG_QUERY_SECONDS: u64 = 20;
const DEFAULT_RECOVERY_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_RETRY_INTERVAL_MS: u64 = 250;

#[derive(Clone, Debug)]
enum ConnectionTarget {
    DatabaseUrl(String),
    Params {
        host: String,
        port: u16,
        user: String,
        database: String,
        password: Option<String>,
    },
}

impl ConnectionTarget {
    async fn connect(&self) -> Result<PgDriver, PgError> {
        match self {
            Self::DatabaseUrl(url) => PgDriver::connect_url(url).await,
            Self::Params {
                host,
                port,
                user,
                database,
                password,
            } => {
                if let Some(password) = password {
                    PgDriver::connect_with_password(host, *port, user, database, password).await
                } else {
                    PgDriver::connect(host, *port, user, database).await
                }
            }
        }
    }
}

fn env_first(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        std::env::var(key)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    })
}

fn env_u64(key: &str, default: u64) -> Result<u64, String> {
    match std::env::var(key) {
        Ok(raw) => raw
            .trim()
            .parse::<u64>()
            .map_err(|_| format!("{key} must be an integer, got '{raw}'")),
        Err(_) => Ok(default),
    }
}

fn load_target() -> Result<ConnectionTarget, String> {
    if let Some(url) = env_first(&["DATABASE_URL"]) {
        return Ok(ConnectionTarget::DatabaseUrl(url));
    }

    let host = env_first(&["QAIL_PG_HOST", "PGHOST"]).unwrap_or_else(|| "localhost".to_string());
    let port_raw = env_first(&["QAIL_PG_PORT", "PGPORT"]).unwrap_or_else(|| "5432".to_string());
    let port = port_raw
        .parse::<u16>()
        .map_err(|_| format!("Invalid port: {port_raw}"))?;
    let user = env_first(&["QAIL_PG_USER", "PGUSER"]).unwrap_or_else(|| "postgres".to_string());
    let database =
        env_first(&["QAIL_PG_DB", "PGDATABASE"]).unwrap_or_else(|| "postgres".to_string());
    let password = env_first(&["QAIL_PG_PASSWORD", "PGPASSWORD"]);

    Ok(ConnectionTarget::Params {
        host,
        port,
        user,
        database,
        password,
    })
}

fn run_shell(command: &str) -> Result<(), String> {
    let output = Command::new("sh")
        .arg("-c")
        .arg(command)
        .output()
        .map_err(|e| format!("Failed to spawn restart command: {e}"))?;

    if output.status.success() {
        return Ok(());
    }

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    Err(format!(
        "Restart command failed (status: {:?})\nstdout: {}\nstderr: {}",
        output.status.code(),
        stdout,
        stderr
    ))
}

fn is_expected_restart_error(err: &PgError) -> bool {
    if err.is_transient_server_error() {
        return true;
    }

    if let Some(code) = err.sqlstate()
        && (matches!(code, "57P01" | "57P02" | "57P03") || code.starts_with("08"))
    {
        return true;
    }

    let lower = err.to_string().to_ascii_lowercase();
    lower.contains("connection closed")
        || lower.contains("connection reset")
        || lower.contains("broken pipe")
        || lower.contains("terminating connection")
        || lower.contains("admin shutdown")
        || lower.contains("crash shutdown")
        || lower.contains("eof")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("============================================================");
    println!("Battle Test: Mid-query restart + reconnect recovery");
    println!("============================================================");

    let restart_cmd = std::env::var("QAIL_RESTART_CMD")
        .map_err(|_| "QAIL_RESTART_CMD is required for this test")?;
    let restart_delay_ms = env_u64("QAIL_RESTART_DELAY_MS", DEFAULT_RESTART_DELAY_MS)?;
    let long_query_seconds = env_u64("QAIL_LONG_QUERY_SECONDS", DEFAULT_LONG_QUERY_SECONDS)?;
    let recovery_timeout_ms = env_u64("QAIL_RECOVERY_TIMEOUT_MS", DEFAULT_RECOVERY_TIMEOUT_MS)?;
    let retry_interval_ms = env_u64("QAIL_RETRY_INTERVAL_MS", DEFAULT_RETRY_INTERVAL_MS)?;
    let target = load_target()?;

    println!("1) Connecting...");
    let mut driver = target.connect().await?;
    println!("   connected");

    let restart_worker = std::thread::spawn(move || -> Result<(), String> {
        std::thread::sleep(Duration::from_millis(restart_delay_ms));
        run_shell(&restart_cmd)
    });

    println!("2) Running long query (pg_sleep({long_query_seconds})) while restart is injected...");
    let sleep_pl = format!("BEGIN PERFORM pg_sleep({long_query_seconds}); END;");
    let sleep_q = Qail::do_block(&sleep_pl, "plpgsql");

    let start = Instant::now();
    let in_flight = driver.execute(&sleep_q).await;
    let elapsed = start.elapsed();

    let restart_result = restart_worker
        .join()
        .map_err(|_| "Restart worker thread panicked")?;
    if let Err(msg) = restart_result {
        return Err(msg.into());
    }

    println!("   in-flight duration: {:.2?}", elapsed);

    match in_flight {
        Ok(_) => {
            return Err("Long query succeeded unexpectedly during restart".into());
        }
        Err(err) => {
            println!("   in-flight error: {err}");
            if !is_expected_restart_error(&err) {
                return Err(format!("Unexpected in-flight error type: {err}").into());
            }
        }
    }

    let expected_upper_bound = Duration::from_secs(long_query_seconds.saturating_sub(1));
    if elapsed >= expected_upper_bound {
        return Err(format!(
            "In-flight query ran too long ({elapsed:.2?}); restart likely not injected in time"
        )
        .into());
    }

    println!("3) Reconnecting with retry loop...");
    let reconnect_start = Instant::now();
    let mut attempts: u32 = 0;
    let mut last_err: Option<String> = None;

    while reconnect_start.elapsed() < Duration::from_millis(recovery_timeout_ms) {
        attempts += 1;
        match target.connect().await {
            Ok(mut fresh) => {
                let probe = Qail::get("generate_series(1,1)");
                match fresh.fetch_all(&probe).await {
                    Ok(_) => {
                        println!(
                            "   reconnect succeeded after {attempts} attempt(s) in {:.2?}",
                            reconnect_start.elapsed()
                        );
                        println!("PASS");
                        return Ok(());
                    }
                    Err(err) => {
                        last_err = Some(format!("Probe failed: {err}"));
                    }
                }
            }
            Err(err) => {
                last_err = Some(err.to_string());
            }
        }

        tokio::time::sleep(Duration::from_millis(retry_interval_ms)).await;
    }

    Err(format!(
        "Reconnect timed out after {attempts} attempts and {:.2?}. Last error: {}",
        reconnect_start.elapsed(),
        last_err.unwrap_or_else(|| "unknown".to_string())
    )
    .into())
}
