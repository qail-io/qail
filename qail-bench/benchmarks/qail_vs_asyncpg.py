#!/usr/bin/env python3
"""
COMPARISON BENCHMARK: qail-py vs asyncpg

Tests qail-py (AST-native, no SQL strings) against asyncpg (SQL strings).

Usage:
    pip install asyncpg
    python qail_vs_asyncpg.py

Environment:
    PG_HOST, PG_PORT, PG_USER, PG_DATABASE, PG_PASSWORD
"""

import asyncio
import os
import time

# Configuration
ITERATIONS = 10_000

def get_env(key: str, default: str) -> str:
    return os.environ.get(key, default)

async def bench_qail():
    """Benchmark qail-py: AST → Wire Protocol (no SQL)"""
    from qail import QailCmd, PgDriver, Operator
    
    host = get_env("PG_HOST", "pg.qail.rs")
    port = int(get_env("PG_PORT", "5432"))
    user = get_env("PG_USER", "qail")
    database = get_env("PG_DATABASE", "qailtest")
    password = get_env("PG_PASSWORD", "qail_test_2024")
    
    driver = await PgDriver.connect(host, port, user, database, password)
    
    # Warmup
    for _ in range(100):
        cmd = QailCmd.get("harbors").columns(["id", "name"]).limit(5)
        result = await driver.fetch_all(cmd)
        driver = result[0]
    
    # Benchmark
    start = time.perf_counter()
    for i in range(ITERATIONS):
        limit = (i % 10) + 1
        cmd = QailCmd.get("harbors").columns(["id", "name"]).limit(limit)
        result = await driver.fetch_all(cmd)
        driver = result[0]
    
    elapsed = time.perf_counter() - start
    return elapsed, ITERATIONS / elapsed

async def bench_asyncpg():
    """Benchmark asyncpg: SQL strings → Wire Protocol"""
    try:
        import asyncpg
    except ImportError:
        return None, None
    
    host = get_env("PG_HOST", "pg.qail.rs")
    port = int(get_env("PG_PORT", "5432"))
    user = get_env("PG_USER", "qail")
    database = get_env("PG_DATABASE", "qailtest")
    password = get_env("PG_PASSWORD", "qail_test_2024")
    
    conn = await asyncpg.connect(
        host=host, port=port, user=user, database=database, password=password
    )
    
    # Warmup
    for _ in range(100):
        await conn.fetch("SELECT id, name FROM harbors LIMIT $1", 5)
    
    # Benchmark
    start = time.perf_counter()
    for i in range(ITERATIONS):
        limit = (i % 10) + 1
        await conn.fetch("SELECT id, name FROM harbors LIMIT $1", limit)
    
    elapsed = time.perf_counter() - start
    await conn.close()
    return elapsed, ITERATIONS / elapsed

async def main():
    print("🚀 QAIL-PY vs ASYNCPG BENCHMARK")
    print("=" * 50)
    print(f"Iterations: {ITERATIONS:,}")
    print()
    
    # Run qail benchmark
    print("🔥 Running qail-py (AST-native, no SQL)...")
    qail_time, qail_qps = await bench_qail()
    print(f"   qail-py: {qail_qps:,.0f} q/s ({qail_time:.2f}s)")
    
    # Run asyncpg benchmark
    print("\n🔥 Running asyncpg (SQL strings)...")
    asyncpg_time, asyncpg_qps = await bench_asyncpg()
    
    if asyncpg_qps:
        print(f"   asyncpg: {asyncpg_qps:,.0f} q/s ({asyncpg_time:.2f}s)")
        
        # Comparison
        ratio = qail_qps / asyncpg_qps
        print("\n📈 COMPARISON:")
        print("┌" + "─" * 50 + "┐")
        if ratio > 1:
            print(f"│ qail-py is {ratio:.2f}x FASTER than asyncpg 🚀        │")
        else:
            print(f"│ asyncpg is {1/ratio:.2f}x faster than qail-py        │")
        print("├" + "─" * 50 + "┤")
        print(f"│ qail-py:  {qail_qps:>12,.0f} q/s (AST-native)       │")
        print(f"│ asyncpg:  {asyncpg_qps:>12,.0f} q/s (SQL strings)      │")
        print("└" + "─" * 50 + "┘")
    else:
        print("   asyncpg not installed (pip install asyncpg)")
        print(f"\n📈 qail-py: {qail_qps:,.0f} queries/second")

if __name__ == "__main__":
    asyncio.run(main())
