//! Single-run QAIL strict benchmark for ABBA orchestration.
//!
//! Usage:
//!   cargo run --release -p qail-pg --example qail_strict_once -- literal --plain
//!   cargo run --release -p qail-pg --example qail_strict_once -- param --plain

use qail_core::ast::Qail;
use qail_pg::PgConnection;
use std::time::Duration;
use std::time::Instant;

const BATCH_SIZE: usize = 10_000;
const ITERATIONS: usize = 5;

#[derive(Clone, Copy)]
enum Workload {
    Literal,
    Param,
}

impl Workload {
    fn parse(s: &str) -> Result<Self, String> {
        match s {
            "literal" => Ok(Self::Literal),
            "param" | "parameterized" => Ok(Self::Param),
            other => Err(format!(
                "unknown workload '{}' (expected literal or param)",
                other
            )),
        }
    }
}

fn build_workload(workload: Workload) -> Vec<Qail> {
    match workload {
        Workload::Literal => (1..=BATCH_SIZE)
            .map(|i| {
                let limit = ((i % 10) + 1) as i64;
                Qail::get("harbors").columns(["id", "name"]).limit(limit)
            })
            .collect(),
        Workload::Param => (1..=BATCH_SIZE)
            .map(|i| {
                let id = ((i % 10_000) + 1) as i64;
                Qail::get("harbors").columns(["id", "name"]).eq("id", id)
            })
            .collect(),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut workload: Option<Workload> = None;
    let mut plain = false;

    for arg in std::env::args().skip(1) {
        if arg == "--plain" {
            plain = true;
            continue;
        }
        if workload.is_none() {
            workload = Some(Workload::parse(&arg)?);
            continue;
        }
        return Err(format!("unexpected argument '{}'", arg).into());
    }

    let workload =
        workload.ok_or_else(|| "missing workload argument: literal | param".to_string())?;
    let cmds = build_workload(workload);

    let mut conn = PgConnection::connect("127.0.0.1", 5432, "orion", "example_staging").await?;

    let warm = conn.pipeline_execute_count_ast_cached(&cmds).await?;
    if warm != cmds.len() {
        return Err(format!("warmup completed {} queries, expected {}", warm, cmds.len()).into());
    }

    let mut total = Duration::ZERO;
    for _ in 0..ITERATIONS {
        let start = Instant::now();
        let completed = conn.pipeline_execute_count_ast_cached(&cmds).await?;
        let elapsed = start.elapsed();
        total += elapsed;
        if completed != cmds.len() {
            return Err(format!(
                "run completed {} queries, expected {}",
                completed,
                cmds.len()
            )
            .into());
        }
    }

    let qps = (cmds.len() * ITERATIONS) as f64 / total.as_secs_f64();
    if plain {
        println!("{:.3}", qps);
    } else {
        let label = match workload {
            Workload::Literal => "literal",
            Workload::Param => "param",
        };
        println!("qail strict {}: {:.0} q/s", label, qps);
    }

    Ok(())
}
