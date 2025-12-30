//! BLOCKING I/O MILLION QUERY BENCHMARK
//!
//! Uses std::net::TcpStream instead of tokio to eliminate ALL async overhead.
//! This tests the absolute maximum performance of our wire protocol.
//!
//! Run: cargo run --release --example million_blocking

use qail_core::ast::Qail;
use qail_pg::protocol::AstEncoder;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Instant;

const TOTAL_QUERIES: usize = 1_000_000;
const QUERIES_PER_BATCH: usize = 1_000;
const BATCHES: usize = TOTAL_QUERIES / QUERIES_PER_BATCH;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect using blocking I/O
    let mut stream = TcpStream::connect("127.0.0.1:5432")?;
    stream.set_nodelay(true)?;

    // Manual startup (simplified - assumes trust auth)
    let startup = build_startup_message("orion", "swb_staging_local");
    stream.write_all(&startup)?;
    stream.flush()?;

    // Read until ReadyForQuery
    let mut buf = [0u8; 8192];
    loop {
        let n = stream.read(&mut buf)?;
        if n == 0 {
            return Err("Connection closed".into());
        }
        // Look for 'Z' (ReadyForQuery)
        if buf[..n].contains(&b'Z') {
            break;
        }
    }

    println!("🚀 BLOCKING I/O MILLION QUERY BENCHMARK");
    println!("========================================");
    println!("Total queries:    {:>12}", format_number(TOTAL_QUERIES));
    println!("Batch size:       {:>12}", QUERIES_PER_BATCH);
    println!("Batches:          {:>12}", BATCHES);
    println!("\n⚠️  PURE BLOCKING I/O - NO ASYNC OVERHEAD!\n");

    // Build commands once
    let cmds: Vec<Qail> = (1..=QUERIES_PER_BATCH)
        .map(|i| {
            let limit = (i % 10) + 1;
            Qail::get("harbors")
                .columns(["id", "name"])
                .limit(limit as i64)
        })
        .collect();

    // Pre-encode once
    let wire_bytes = AstEncoder::encode_batch(&cmds);

    println!("📊 Pipelining 1,000,000 queries via blocking I/O...");

    let start = Instant::now();
    let mut successful_queries = 0;
    let mut read_buf = vec![0u8; 1024 * 1024]; // 1MB buffer

    for batch in 0..BATCHES {
        if batch % 100 == 0 {
            println!("   Batch {}/{}", batch, BATCHES);
        }

        // Send all queries
        stream.write_all(&wire_bytes)?;
        stream.flush()?;

        // Read responses
        let mut queries_completed = 0;
        let mut offset = 0;

        while queries_completed < QUERIES_PER_BATCH {
            let n = stream.read(&mut read_buf[offset..])?;
            if n == 0 {
                return Err("Connection closed during read".into());
            }
            offset += n;

            // Count 'C' (CommandComplete) and 'n' (NoData) messages
            let mut i = 0;
            while i < offset {
                if i + 5 > offset {
                    break; // Need more data
                }
                let msg_type = read_buf[i];
                let msg_len = i32::from_be_bytes([
                    read_buf[i + 1],
                    read_buf[i + 2],
                    read_buf[i + 3],
                    read_buf[i + 4],
                ]) as usize;

                if i + 1 + msg_len > offset {
                    break; // Incomplete message
                }

                if msg_type == b'C' || msg_type == b'n' {
                    queries_completed += 1;
                }

                i += 1 + msg_len;
            }

            // Shift remaining data
            if i > 0 && i < offset {
                read_buf.copy_within(i..offset, 0);
                offset -= i;
            } else if i == offset {
                offset = 0;
            }
        }

        successful_queries += queries_completed;
    }

    let elapsed = start.elapsed();
    let qps = TOTAL_QUERIES as f64 / elapsed.as_secs_f64();
    let per_query_ns = elapsed.as_nanos() / TOTAL_QUERIES as u128;

    println!("\n📈 Results:");
    println!("┌──────────────────────────────────────────┐");
    println!("│ BLOCKING I/O - ONE MILLION QUERIES       │");
    println!("├──────────────────────────────────────────┤");
    println!("│ Total Time:     {:>23.2}s │", elapsed.as_secs_f64());
    println!("│ Queries/Second: {:>23} │", format_number(qps as usize));
    println!(
        "│ Per Query:      {:>20}ns │",
        format_number(per_query_ns as usize)
    );
    println!(
        "│ Successful:     {:>23} │",
        format_number(successful_queries)
    );
    println!("└──────────────────────────────────────────┘");

    println!("\n📊 vs tokio async (77,843 q/s):");
    let async_speedup = qps / 77843.0;
    if async_speedup > 1.0 {
        println!("   Blocking is {:.2}x FASTER than async!", async_speedup);
    } else {
        println!(
            "   Async is {:.2}x faster than blocking",
            1.0 / async_speedup
        );
    }

    println!("\n📊 vs Go pgx (321,787 q/s):");
    if qps > 321787.0 {
        println!("   QAIL blocking is {:.2}x FASTER than Go!", qps / 321787.0);
    } else {
        println!("   Go is {:.2}x faster", 321787.0 / qps);
    }

    Ok(())
}

fn build_startup_message(user: &str, database: &str) -> Vec<u8> {
    let mut buf = Vec::new();

    // Protocol version 3.0
    buf.extend_from_slice(&[0, 0, 0, 0]); // Length placeholder
    buf.extend_from_slice(&196608_i32.to_be_bytes()); // Version 3.0

    buf.extend_from_slice(b"user\0");
    buf.extend_from_slice(user.as_bytes());
    buf.push(0);

    buf.extend_from_slice(b"database\0");
    buf.extend_from_slice(database.as_bytes());
    buf.push(0);

    buf.push(0); // End of parameters

    // Fix length
    let len = buf.len() as i32;
    buf[0..4].copy_from_slice(&len.to_be_bytes());

    buf
}

fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.insert(0, ',');
        }
        result.insert(0, c);
    }
    result
}
