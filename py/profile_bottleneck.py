"""
Profiling benchmark to isolate bottleneck sources in QAIL Python drivers.

Tests:
1. Pure Python overhead (no Rust)
2. QailCmd building time
3. Encoding time (PyO3 encode_cmd)
4. Network I/O time
"""

import asyncio
import time

DB_HOST = 'localhost'
DB_PORT = 5432
DB_USER = 'orion'
DB_NAME = 'swb_staging_local'

NUM_ITERATIONS = 10000

def profile_cmd_building():
    """Measure time to build Qail (Python → Rust)"""
    from qail import Qail, Operator
    
    start = time.perf_counter()
    for _ in range(NUM_ITERATIONS):
        cmd = (Qail.get("destinations")
               .columns(["id", "name", "slug", "is_active"])
               .filter("is_active", Operator.eq(), True)
               .order_by("name")
               .limit(10))
    elapsed = time.perf_counter() - start
    
    ops_per_sec = NUM_ITERATIONS / elapsed
    us_per_op = (elapsed / NUM_ITERATIONS) * 1_000_000
    print(f"Qail building:        {ops_per_sec:>10,.0f} ops/s  ({us_per_op:.2f} µs/op)")
    return elapsed

def bench_encoding():
    """Measure time to encode Qail to wire bytes"""
    from qail import Qail, encode_cmd
    
    # Pre-build cmd
    cmd = (Qail.get("destinations")
           .columns(["id", "name", "slug", "is_active"])
           .filter("is_active", Operator.eq(), True)
           .order_by("name")
           .limit(10))
    
    start = time.perf_counter()
    for _ in range(NUM_ITERATIONS):
        wire_bytes = encode_cmd(cmd)
    elapsed = time.perf_counter() - start
    
    ops_per_sec = NUM_ITERATIONS / elapsed
    us_per_op = (elapsed / NUM_ITERATIONS) * 1_000_000
    print(f"Encoding (PyO3):      {ops_per_sec:>10,.0f} ops/s  ({us_per_op:.2f} µs/op)")
    return elapsed

def profile_build_plus_encode():
    """Measure combined build + encode time"""
    from qail import Qail, encode_cmd
    
    start = time.perf_counter()
    for _ in range(NUM_ITERATIONS):
        cmd = (Qail.get("destinations")
               .columns(["id", "name", "slug", "is_active"])
               .order_by("name")
               .limit(10))
        wire_bytes = encode_cmd(cmd)
    elapsed = time.perf_counter() - start
    
    ops_per_sec = NUM_ITERATIONS / elapsed
    us_per_op = (elapsed / NUM_ITERATIONS) * 1_000_000
    print(f"Build + Encode:       {ops_per_sec:>10,.0f} ops/s  ({us_per_op:.2f} µs/op)")
    return elapsed

async def profile_asyncpg_query():
    """Measure pure asyncpg query time (baseline)"""
    import asyncpg
    
    conn = await asyncpg.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, database=DB_NAME)
    
    sql = "SELECT id, name, slug, is_active FROM destinations ORDER BY name LIMIT 10"
    
    # Warmup
    for _ in range(100):
        await conn.fetch(sql)
    
    start = time.perf_counter()
    for _ in range(NUM_ITERATIONS):
        rows = await conn.fetch(sql)
    elapsed = time.perf_counter() - start
    
    await conn.close()
    
    ops_per_sec = NUM_ITERATIONS / elapsed
    us_per_op = (elapsed / NUM_ITERATIONS) * 1_000_000
    print(f"asyncpg query:        {ops_per_sec:>10,.0f} ops/s  ({us_per_op:.2f} µs/op)")
    return elapsed

async def profile_qail_async_driver():
    """Measure qail AsyncPgDriver (Python asyncio + PyO3 encode)"""
    from qail import AsyncPgDriver, Qail
    
    driver = await AsyncPgDriver.connect(DB_HOST, DB_PORT, DB_USER, DB_NAME, None)
    
    # Pre-build cmd
    cmd = (Qail.get("destinations")
           .columns(["id", "name", "slug", "is_active"])
           .order_by("name")
           .limit(10))
    
    # Warmup
    for _ in range(100):
        await driver.fetch_all(cmd)
    
    # Test with pre-built cmd (isolates network time)
    start = time.perf_counter()
    for _ in range(NUM_ITERATIONS):
        rows = await driver.fetch_all(cmd)
    elapsed = time.perf_counter() - start
    
    await driver.close()
    
    ops_per_sec = NUM_ITERATIONS / elapsed
    us_per_op = (elapsed / NUM_ITERATIONS) * 1_000_000
    print(f"qail AsyncDriver:     {ops_per_sec:>10,.0f} ops/s  ({us_per_op:.2f} µs/op)")
    return elapsed

async def profile_qail_pyo3_driver():
    """Measure qail PyO3 driver (Rust tokio with GIL release)"""
    from qail import PgDriver, Qail
    
    driver = await asyncio.to_thread(
        PgDriver.connect, DB_HOST, DB_PORT, DB_USER, DB_NAME, ""
    )
    
    # Pre-build cmd
    cmd = (Qail.get("destinations")
           .columns(["id", "name", "slug", "is_active"])
           .order_by("name")
           .limit(10))
    
    # Warmup
    for _ in range(100):
        await asyncio.to_thread(driver.fetch_all, cmd)
    
    # Test with pre-built cmd
    start = time.perf_counter()
    for _ in range(NUM_ITERATIONS):
        rows = await asyncio.to_thread(driver.fetch_all, cmd)
    elapsed = time.perf_counter() - start
    
    ops_per_sec = NUM_ITERATIONS / elapsed
    us_per_op = (elapsed / NUM_ITERATIONS) * 1_000_000
    print(f"qail PyO3 driver:     {ops_per_sec:>10,.0f} ops/s  ({us_per_op:.2f} µs/op)")
    return elapsed

async def main():
    print("=" * 60)
    print("QAIL Python Driver Profiling")
    print(f"Iterations: {NUM_ITERATIONS:,}")
    print("=" * 60)
    print()
    
    print("--- Rust-side operations ---")
    t_build = profile_cmd_building()
    t_encode = profile_encoding()
    t_both = profile_build_plus_encode()
    print()
    
    print("--- Full query (including network) ---")
    t_asyncpg = await profile_asyncpg_query()
    t_async = await profile_qail_async_driver()
    t_pyo3 = await profile_qail_pyo3_driver()
    print()
    
    print("=" * 60)
    print("ANALYSIS")
    print("=" * 60)
    
    # Calculate where time is spent
    build_pct = (t_build / t_both) * 100
    encode_pct = ((t_both - t_build) / t_both) * 100
    
    asyncpg_us = (t_asyncpg / NUM_ITERATIONS) * 1_000_000
    async_us = (t_async / NUM_ITERATIONS) * 1_000_000
    pyo3_us = (t_pyo3 / NUM_ITERATIONS) * 1_000_000
    build_encode_us = (t_both / NUM_ITERATIONS) * 1_000_000
    
    print(f"Time breakdown per query:")
    print(f"  asyncpg query:      {asyncpg_us:.2f} µs (baseline)")
    print(f"  qail build+encode:  {build_encode_us:.2f} µs")
    print(f"  qail AsyncDriver:   {async_us:.2f} µs ({async_us - asyncpg_us:+.2f} µs vs asyncpg)")
    print(f"  qail PyO3 driver:   {pyo3_us:.2f} µs ({pyo3_us - asyncpg_us:+.2f} µs vs asyncpg)")
    print()
    print(f"Overhead sources:")
    print(f"  Build+Encode takes: {build_encode_us:.2f} µs ({build_encode_us/async_us*100:.1f}% of AsyncDriver query)")

if __name__ == "__main__":
    asyncio.run(main())
