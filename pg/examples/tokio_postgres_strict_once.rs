//! Single-run tokio-postgres strict benchmark for ABBA orchestration.
//!
//! Usage:
//!   cargo run --release -p qail-pg --example tokio_postgres_strict_once -- literal --plain
//!   cargo run --release -p qail-pg --example tokio_postgres_strict_once -- param --plain

use futures_util::future::try_join_all;
use futures_util::TryStreamExt;
use std::time::Duration;
use std::time::Instant;
use tokio_postgres::{Client, NoTls, Statement};

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

async fn run_literal_once(client: &Client, stmts: &[Statement]) -> Result<usize, tokio_postgres::Error> {
    let mut futs = Vec::with_capacity(BATCH_SIZE);
    for i in 1..=BATCH_SIZE {
        let idx = (i % 10) as usize;
        let client = client;
        let stmt = stmts[idx].clone();
        futs.push(async move {
            let stream = client.query_raw(&stmt, std::iter::empty::<i32>()).await?;
            futures_util::pin_mut!(stream);
            let mut count = 0usize;
            while stream.try_next().await?.is_some() {
                count += 1;
            }
            Ok::<usize, tokio_postgres::Error>(count)
        });
    }
    let counts = try_join_all(futs).await?;
    Ok(counts.into_iter().sum())
}

async fn run_param_once(client: &Client, stmt: &Statement) -> Result<usize, tokio_postgres::Error> {
    let ids: Vec<i64> = (1..=BATCH_SIZE).map(|i| ((i % 10_000) + 1) as i64).collect();
    let mut futs = Vec::with_capacity(BATCH_SIZE);
    for id in ids {
        let client = client;
        let stmt = stmt.clone();
        futs.push(async move {
            let stream = client.query_raw(&stmt, vec![id]).await?;
            futures_util::pin_mut!(stream);
            let mut count = 0usize;
            while stream.try_next().await?.is_some() {
                count += 1;
            }
            Ok::<usize, tokio_postgres::Error>(count)
        });
    }
    let counts = try_join_all(futs).await?;
    Ok(counts.into_iter().sum())
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

    let (client, connection) = tokio_postgres::connect(
        "host=127.0.0.1 port=5432 user=orion dbname=example_staging",
        NoTls,
    )
    .await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {}", e);
        }
    });

    let mut total = Duration::ZERO;

    match workload {
        Workload::Literal => {
            let mut stmts = Vec::with_capacity(10);
            for limit in 1..=10 {
                let sql = format!("SELECT id, name FROM harbors LIMIT {}", limit);
                stmts.push(client.prepare(&sql).await?);
            }

            let _ = run_literal_once(&client, &stmts).await?;
            for _ in 0..ITERATIONS {
                let start = Instant::now();
                let _ = run_literal_once(&client, &stmts).await?;
                total += start.elapsed();
            }
        }
        Workload::Param => {
            let stmt = client
                .prepare("SELECT id, name FROM harbors WHERE id = $1")
                .await?;
            let _ = run_param_once(&client, &stmt).await?;
            for _ in 0..ITERATIONS {
                let start = Instant::now();
                let _ = run_param_once(&client, &stmt).await?;
                total += start.elapsed();
            }
        }
    }

    let qps = (BATCH_SIZE * ITERATIONS) as f64 / total.as_secs_f64();
    if plain {
        println!("{:.3}", qps);
    } else {
        let label = match workload {
            Workload::Literal => "literal",
            Workload::Param => "param",
        };
        println!("tokio-postgres strict {}: {:.0} q/s", label, qps);
    }

    Ok(())
}
