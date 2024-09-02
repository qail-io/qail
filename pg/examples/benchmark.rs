//! Performance benchmark: QAIL Binary vs String Approach
//!
//! Compares the performance of:
//! 1. QAIL Extended Query (binary params)
//! 2. Simulated ORM approach (string interpolation)
//!
//! Run: cargo bench -p qail-pg

use std::time::{Duration, Instant};

/// Simulate string-based SQL generation (typical ORM approach)
fn orm_style_sql(name: &str, active: bool, limit: i64) -> String {
    // This is what ORMs typically do - string concatenation with escaping
    let escaped_name = name.replace('\'', "''"); // SQL escape
    format!(
        "SELECT id, name, email FROM users WHERE name = '{}' AND active = {} LIMIT {}",
        escaped_name, active, limit
    )
}

/// QAIL-style binary parameter preparation
fn qail_style_params(name: &str, active: bool, limit: i64) -> (String, Vec<Option<Vec<u8>>>) {
    let sql = "SELECT id, name, email FROM users WHERE name = $1 AND active = $2 LIMIT $3";
    let params = vec![
        Some(name.as_bytes().to_vec()), // Direct bytes
        Some(if active { b"t".to_vec() } else { b"f".to_vec() }),
        Some(limit.to_string().into_bytes()),
    ];
    (sql.to_string(), params)
}

fn benchmark<F>(name: &str, iterations: usize, f: F) -> Duration
where
    F: Fn(),
{
    // Warmup
    for _ in 0..1000 {
        f();
    }

    let start = Instant::now();
    for _ in 0..iterations {
        f();
    }
    let elapsed = start.elapsed();

    println!(
        "{}: {:?} total, {:?} per iteration",
        name,
        elapsed,
        elapsed / iterations as u32
    );

    elapsed
}

fn main() {
    const ITERATIONS: usize = 100_000;

    println!("🏎️  QAIL Performance Benchmark");
    println!("================================");
    println!("Iterations: {}\n", ITERATIONS);

    // Test with various string lengths
    let test_cases = [
        ("Short name", "Alice"),
        ("Medium name", "Alexander Johnson"),
        (
            "Long name",
            "Alexander Benjamin Christopher Davidson Edwards",
        ),
        ("Escape needed", "O'Brien & Partners's \"Test\""),
    ];

    for (label, name) in test_cases {
        println!("📊 Test: {}", label);
        println!("   Name: \"{}\"", name);

        let orm_time = benchmark("   ORM (string concat)", ITERATIONS, || {
            let _sql = orm_style_sql(name, true, 100);
        });

        let qail_time = benchmark("   QAIL (binary params)", ITERATIONS, || {
            let (_sql, _params) = qail_style_params(name, true, 100);
        });

        let speedup = orm_time.as_nanos() as f64 / qail_time.as_nanos() as f64;
        println!("   ⚡ QAIL is {:.2}x faster\n", speedup);
    }

    // Memory allocation comparison
    println!("📊 Memory: String allocations");

    let name = "Alexander Benjamin Christopher Davidson Edwards";
    let orm_sql = orm_style_sql(name, true, 100);
    let (qail_sql, qail_params) = qail_style_params(name, true, 100);

    println!("   ORM SQL length:  {} bytes", orm_sql.len());
    println!("   QAIL SQL length: {} bytes (template)", qail_sql.len());
    println!(
        "   QAIL params:     {} bytes (binary)",
        qail_params
            .iter()
            .map(|p| p.as_ref().map_or(0, |v| v.len()))
            .sum::<usize>()
    );

    println!("\n✅ Benchmark complete");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_orm_escaping() {
        let sql = orm_style_sql("O'Brien", true, 10);
        assert!(sql.contains("O''Brien")); // Should escape quote
    }

    #[test]
    fn test_qail_binary() {
        let (sql, params) = qail_style_params("Alice", true, 10);
        assert!(sql.contains("$1"));
        assert_eq!(params[0], Some(b"Alice".to_vec()));
    }
}
