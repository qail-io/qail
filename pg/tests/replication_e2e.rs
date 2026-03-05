//! Logical replication end-to-end test (containerized).
//!
//! Flow:
//! 1. Start PostgreSQL with `wal_level=logical` in Docker/Podman.
//! 2. Create table + publication.
//! 3. Open replication connection (`replication=database`).
//! 4. Create temporary logical slot (`pgoutput`).
//! 5. `START_REPLICATION ... LOGICAL ...`.
//! 6. Insert a row on a writer connection.
//! 7. Assert at least one `XLogData` message is received.
//!
//! Run manually:
//! `cargo test -p qail-pg --test replication_e2e -- --ignored --nocapture`

use qail_pg::{PgConnection, PgDriver, ReplicationOption, ReplicationStreamMessage};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const USER: &str = "qail";
const PASSWORD: &str = "qail";
const DATABASE: &str = "qail_replication";
const PUBLICATION: &str = "qail_replication_pub";
const TABLE: &str = "replication_events";

struct ContainerGuard {
    runtime: String,
    name: String,
}

impl Drop for ContainerGuard {
    fn drop(&mut self) {
        let _ = Command::new(&self.runtime)
            .args(["rm", "-f", &self.name])
            .output();
    }
}

fn runtime_bin() -> Result<String, String> {
    if let Ok(explicit) = std::env::var("QAIL_CONTAINER_RUNTIME") {
        run_cmd(&explicit, &["version"]).map_err(|e| {
            format!(
                "container runtime '{}' unavailable from QAIL_CONTAINER_RUNTIME. {}",
                explicit, e
            )
        })?;
        return Ok(explicit);
    }

    for candidate in ["docker", "podman"] {
        if run_cmd(candidate, &["version"]).is_ok() {
            return Ok(candidate.to_string());
        }
    }

    Err("no supported container runtime available (tried docker, podman)".to_string())
}

fn run_cmd(runtime: &str, args: &[&str]) -> Result<String, String> {
    let output = Command::new(runtime)
        .args(args)
        .output()
        .map_err(|e| format!("failed to execute {} {:?}: {}", runtime, args, e))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "{} {:?} failed (status={}): {}",
            runtime,
            args,
            output.status,
            stderr.trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn pick_free_port() -> u16 {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .expect("bind ephemeral port")
        .local_addr()
        .expect("read local addr")
        .port()
}

async fn wait_for_pg(host: &str, port: u16, timeout: Duration) -> Result<(), String> {
    let start = std::time::Instant::now();
    let mut last_err = String::new();
    while start.elapsed() < timeout {
        match PgConnection::connect_with_password(host, port, USER, DATABASE, Some(PASSWORD)).await
        {
            Ok(_) => return Ok(()),
            Err(e) => {
                last_err = e.to_string();
                tokio::time::sleep(Duration::from_millis(400)).await;
            }
        }
    }
    Err(format!(
        "postgres did not become ready within {:?} (last error: {})",
        timeout, last_err
    ))
}

async fn setup_schema(host: &str, port: u16) -> Result<(), String> {
    let mut conn = PgConnection::connect_with_password(host, port, USER, DATABASE, Some(PASSWORD))
        .await
        .map_err(|e| format!("setup connect failed: {}", e))?;

    conn.execute_simple(&format!(
        "CREATE TABLE IF NOT EXISTS {} (id BIGSERIAL PRIMARY KEY, payload TEXT NOT NULL)",
        TABLE
    ))
    .await
    .map_err(|e| format!("create table failed: {}", e))?;

    conn.execute_simple(&format!("DROP PUBLICATION IF EXISTS {}", PUBLICATION))
        .await
        .map_err(|e| format!("drop publication failed: {}", e))?;

    conn.execute_simple(&format!(
        "CREATE PUBLICATION {} FOR TABLE {}",
        PUBLICATION, TABLE
    ))
    .await
    .map_err(|e| format!("create publication failed: {}", e))?;

    Ok(())
}

#[tokio::test]
#[ignore = "Requires Docker/Podman and ~20s runtime"]
async fn logical_replication_receives_xlog_data() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = runtime_bin().map_err(|e| {
        format!(
            "{}. set QAIL_CONTAINER_RUNTIME=docker|podman to force runtime selection.",
            e
        )
    })?;

    let host_port = pick_free_port();
    let ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let container = format!("qail-repl-e2e-{}-{}", std::process::id(), ts);

    run_cmd(
        &runtime,
        &[
            "run",
            "-d",
            "--rm",
            "--name",
            &container,
            "-e",
            &format!("POSTGRES_USER={}", USER),
            "-e",
            &format!("POSTGRES_PASSWORD={}", PASSWORD),
            "-e",
            &format!("POSTGRES_DB={}", DATABASE),
            "-p",
            &format!("127.0.0.1:{}:5432", host_port),
            "postgres:17",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=10",
            "-c",
            "max_wal_senders=10",
        ],
    )
    .map_err(|e| format!("failed to start postgres container: {}", e))?;
    let _guard = ContainerGuard {
        runtime: runtime.clone(),
        name: container.clone(),
    };

    wait_for_pg("127.0.0.1", host_port, Duration::from_secs(45))
        .await
        .map_err(|e| format!("postgres readiness check failed: {}", e))?;
    setup_schema("127.0.0.1", host_port)
        .await
        .map_err(|e| format!("schema setup failed: {}", e))?;

    let mut repl = PgDriver::connect_logical_replication(
        "127.0.0.1",
        host_port,
        USER,
        DATABASE,
        Some(PASSWORD),
    )
    .await
    .map_err(|e| format!("replication connect failed: {}", e))?;

    let slot_name = format!(
        "qailslot{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs()
    );
    let slot = repl
        .create_logical_replication_slot(&slot_name, "pgoutput", true, false)
        .await
        .map_err(|e| format!("create replication slot failed: {}", e))?;

    repl.start_logical_replication(
        &slot_name,
        &slot.consistent_point,
        &[
            ReplicationOption {
                key: "proto_version".to_string(),
                value: "1".to_string(),
            },
            ReplicationOption {
                key: "publication_names".to_string(),
                value: PUBLICATION.to_string(),
            },
        ],
    )
    .await
    .map_err(|e| format!("start replication failed: {}", e))?;

    let marker = format!(
        "marker-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let marker_sql = marker.replace('\'', "''");
    let mut writer =
        PgConnection::connect_with_password("127.0.0.1", host_port, USER, DATABASE, Some(PASSWORD))
            .await
            .map_err(|e| format!("writer connect failed: {}", e))?;
    writer
        .execute_simple(&format!(
            "INSERT INTO {} (payload) VALUES ('{}')",
            TABLE, marker_sql
        ))
        .await
        .map_err(|e| format!("writer insert failed: {}", e))?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut saw_xlog = false;
    while tokio::time::Instant::now() < deadline {
        let msg = tokio::time::timeout(Duration::from_secs(5), repl.recv_replication_message())
            .await
            .map_err(|_| "timed out waiting for replication message")?
            .map_err(|e| format!("recv replication message failed: {}", e))?;

        match msg {
            ReplicationStreamMessage::XLogData(x) => {
                saw_xlog = true;
                repl.send_standby_status_update(x.wal_end, x.wal_end, x.wal_end, false)
                    .await
                    .map_err(|e| format!("send standby status update failed: {}", e))?;
                break;
            }
            ReplicationStreamMessage::Keepalive(k) => {
                if k.reply_requested {
                    repl.send_standby_status_update(k.wal_end, k.wal_end, k.wal_end, false)
                        .await
                        .map_err(|e| format!("send keepalive reply failed: {}", e))?;
                }
            }
            ReplicationStreamMessage::Raw { .. } => {}
        }
    }

    assert!(saw_xlog, "did not receive XLogData after insert");
    Ok(())
}
