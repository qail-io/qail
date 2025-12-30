"""
Fair Sequential Query Benchmark
Tests all Python drivers with the SAME query in SEQUENTIAL mode.

Drivers tested:
1. qail PyO3 (PgDriver) - Rust Tokio embedded
2. qail AsyncPgDriver (pure Python + PyO3 encoder)
3. asyncpg - baseline external driver
"""

import asyncio
import time

# Database config - use existing local database
DB_HOST = 'localhost'
DB_PORT = 5432
DB_USER = 'orion'
DB_NAME = 'swb_staging_local'
DB_PASS = None

# Same query for all drivers
# Simple SELECT from destinations table (4 rows)
QUERY_SQL = "SELECT id, name, slug, is_active FROM destinations ORDER BY name LIMIT 10"

NUM_QUERIES = 5000  # Sequential queries to run (repeat same query)

async def bench_asyncpg():
    """Benchmark asyncpg (external driver)"""
    try:
        import asyncpg
    except ImportError:
        print("  asyncpg not installed, skipping")
        return None
    
    conn = await asyncpg.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        database=DB_NAME
    )
    
    # Warmup
    for _ in range(100):
        await conn.fetch(QUERY_SQL)
    
    # Benchmark
    start = time.perf_counter()
    for _ in range(NUM_QUERIES):
        rows = await conn.fetch(QUERY_SQL)
    elapsed = time.perf_counter() - start
    
    await conn.close()
    
    qps = NUM_QUERIES / elapsed
    return ("asyncpg", qps, elapsed)

async def bench_qail_pyo3():
    """Benchmark qail PyO3 driver (embedded Rust)"""
    try:
        from qail import PgDriver, Qail, Operator
    except ImportError as e:
        print(f"  qail not installed, skipping PyO3 test: {e}")
        return None
    
    driver = await asyncio.to_thread(
        PgDriver.connect, 
        DB_HOST, DB_PORT, DB_USER, DB_NAME, ""  # Empty password for trust auth
    )
    
    # Build the same query using Qail
    def make_cmd():
        return (Qail.get("destinations")
                .columns(["id", "name", "slug", "is_active"])
                .order_by("name")
                .limit(10))
    
    # Warmup
    for _ in range(100):
        await asyncio.to_thread(driver.fetch_all, make_cmd())
    
    # Benchmark - sequential queries
    start = time.perf_counter()
    for _ in range(NUM_QUERIES):
        rows = await asyncio.to_thread(driver.fetch_all, make_cmd())
    elapsed = time.perf_counter() - start
    
    qps = NUM_QUERIES / elapsed
    return ("qail PyO3", qps, elapsed)

async def bench_qail_async():
    """Benchmark qail AsyncPgDriver (pure Python + PyO3 encoder)"""
    try:
        from qail import AsyncPgDriver, Qail, Operator
    except ImportError as e:
        print(f"  qail AsyncPgDriver not available, skipping: {e}")
        return None
    
    try:
        driver = await AsyncPgDriver.connect(
            DB_HOST, DB_PORT, DB_USER, DB_NAME, DB_PASS
        )
    except Exception as e:
        print(f"  AsyncPgDriver connection failed: {e}")
        return None
    
    # Build the same query using Qail
    def make_cmd():
        return (Qail.get("destinations")
                .columns(["id", "name", "slug", "is_active"])
                .order_by("name")
                .limit(10))
    
    # Warmup
    for _ in range(100):
        await driver.fetch_all(make_cmd())
    
    # Benchmark - sequential queries
    start = time.perf_counter()
    for _ in range(NUM_QUERIES):
        rows = await driver.fetch_all(make_cmd())
    elapsed = time.perf_counter() - start
    
    await driver.close()
    
    qps = NUM_QUERIES / elapsed
    return ("qail AsyncPgDriver", qps, elapsed)

async def main():
    print("=" * 60)
    print("Fair Sequential Query Benchmark")
    print(f"Query: SELECT id, name, slug, is_active FROM destinations")
    print(f"Queries: {NUM_QUERIES} sequential (same query repeated)")
    print(f"Database: {DB_NAME}")
    print("=" * 60)
    print()
    
    results = []
    
    # Run all benchmarks
    print("Testing asyncpg...")
    r = await bench_asyncpg()
    if r:
        results.append(r)
        print(f"  {r[0]}: {r[1]:,.0f} q/s ({r[2]*1000:.1f}ms total)")
    
    print("Testing qail PyO3...")
    r = await bench_qail_pyo3()
    if r:
        results.append(r)
        print(f"  {r[0]}: {r[1]:,.0f} q/s ({r[2]*1000:.1f}ms total)")
    
    print("Testing qail AsyncPgDriver...")
    r = await bench_qail_async()
    if r:
        results.append(r)
        print(f"  {r[0]}: {r[1]:,.0f} q/s ({r[2]*1000:.1f}ms total)")
    
    # Summary
    print()
    print("=" * 60)
    print("RESULTS (Sequential Mode - Single Connection)")
    print("=" * 60)
    
    if results:
        # Sort by speed (fastest first)
        results.sort(key=lambda x: -x[1])
        baseline = next((r[1] for r in results if r[0] == "asyncpg"), results[-1][1])
        
        for name, qps, elapsed in results:
            ratio = qps / baseline if baseline else 0
            avg_latency = (elapsed / NUM_QUERIES) * 1000  # ms per query
            print(f"{name:25s} {qps:>10,.0f} q/s  {avg_latency:.3f}ms/query  ({ratio:.1f}x vs asyncpg)")

if __name__ == "__main__":
    asyncio.run(main())
