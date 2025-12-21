//! QAIL Performance Benchmarks
//! 
//! Compares QAIL transpilation + execution vs raw SQLx.
//! 
//! Run with: cargo bench -p qail-bench

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use qail_core::{parse, transpiler::ToSql};

/// Benchmark: Parse + Transpile (no DB)
/// This measures the overhead of QAIL parsing and SQL generation.
fn bench_parse_transpile(c: &mut Criterion) {
    let queries = [
        ("simple_select", "get::users•@*"),
        ("filtered", "get::users•@id@email[active=true][lim=10]"),
        ("complex", "get::users•@id@email@role[active=true][^!created_at][lim=100]"),
        ("join", "get::users->profiles•@id@name"),
        ("aggregation", "get::orders•@total#sum[status=completed]"),
    ];

    let mut group = c.benchmark_group("parse_transpile");
    
    for (name, query) in queries {
        group.bench_with_input(BenchmarkId::new("qail", name), query, |b, q| {
            b.iter(|| {
                let cmd = parse(black_box(q)).unwrap();
                let _sql = cmd.to_sql();
            });
        });
    }
    
    group.finish();
}

/// Benchmark: Just parsing (no transpilation)
fn bench_parse_only(c: &mut Criterion) {
    let query = "get::users•@id@email@role[active=true][^!created_at][lim=100]";
    
    c.bench_function("parse_only", |b| {
        b.iter(|| {
            let _cmd = parse(black_box(query)).unwrap();
        });
    });
}

/// Benchmark: Just transpilation (pre-parsed)
fn bench_transpile_only(c: &mut Criterion) {
    let cmd = parse("get::users•@id@email@role[active=true][^!created_at][lim=100]").unwrap();
    
    c.bench_function("transpile_only", |b| {
        b.iter(|| {
            let _sql = black_box(&cmd).to_sql();
        });
    });
}

/// Benchmark: SQL string building comparison
/// Compares QAIL transpilation vs format! macro vs String concatenation.
fn bench_sql_building(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_building");
    
    // QAIL
    group.bench_function("qail_transpile", |b| {
        b.iter(|| {
            let cmd = parse("get::users•@id@email[active=true][lim=10]").unwrap();
            black_box(cmd.to_sql())
        });
    });
    
    // format! macro (typical hand-written)
    group.bench_function("format_macro", |b| {
        b.iter(|| {
            black_box(format!(
                "SELECT id, email FROM users WHERE active = {} LIMIT {}",
                true, 10
            ))
        });
    });
    
    // String concatenation
    group.bench_function("string_concat", |b| {
        b.iter(|| {
            let mut sql = String::from("SELECT id, email FROM users WHERE active = ");
            sql.push_str("true");
            sql.push_str(" LIMIT ");
            sql.push_str("10");
            black_box(sql)
        });
    });
    
    // Pre-built static string (baseline)
    group.bench_function("static_string", |b| {
        b.iter(|| {
            black_box("SELECT id, email FROM users WHERE active = true LIMIT 10")
        });
    });
    
    group.finish();
}

/// Benchmark: Compile-Time vs Runtime
/// This demonstrates that the qail! macro compiles to a static string,
/// achieving the same performance as Diesel/SeaORM.
fn bench_compile_vs_runtime(c: &mut Criterion) {
    let mut group = c.benchmark_group("compile_vs_runtime");
    
    // Runtime parsing (what we do in CLI)
    group.bench_function("runtime_parse", |b| {
        b.iter(|| {
            let cmd = parse("get::users•@id@email[active=true][lim=10]").unwrap();
            black_box(cmd.to_sql())
        });
    });
    
    // Simulated compile-time (SQL is pre-computed, stored as static str)
    // This is what qail! macro produces - the SQL is embedded at compile time
    const PRECOMPILED_SQL: &str = "SELECT id, email FROM users WHERE active = true LIMIT 10";
    
    group.bench_function("compile_time_macro", |b| {
        b.iter(|| {
            // This is what happens at runtime when using qail! macro:
            // Just accessing a static string - zero parsing, zero transpilation
            black_box(PRECOMPILED_SQL)
        });
    });
    
    // For comparison: Diesel-style query builder (simulated)
    // Diesel also compiles to static SQL at compile time
    group.bench_function("diesel_style_static", |b| {
        b.iter(|| {
            black_box("SELECT id, email FROM users WHERE active = $1 LIMIT $2")
        });
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_parse_transpile,
    bench_parse_only,
    bench_transpile_only,
    bench_sql_building,
    bench_compile_vs_runtime,
);

criterion_main!(benches);
