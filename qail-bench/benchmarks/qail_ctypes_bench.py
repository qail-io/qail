#!/usr/bin/env python3
"""
QAIL-PY CTYPES BENCHMARK: 50 Million Query Stress Test

Uses ctypes FFI to Rust qail-ffi + native Python asyncio.
No PyO3 overhead.

Usage:
    cd qail-bench/benchmarks
    python qail_ctypes_bench.py
"""

import asyncio
import os
import sys
import time

# Add qail-py to path
sys.path.insert(0, "../qail-py/python")

from qail.native_driver import NativePgDriver
from qail.ffi import encode_batch_get

# Configuration
TOTAL_QUERIES = 50_000_000
BATCH_SIZE = 10_000
BATCHES = TOTAL_QUERIES // BATCH_SIZE

def get_env(key: str, default: str) -> str:
    return os.environ.get(key, default)

async def main():
    host = get_env("PG_HOST", "127.0.0.1")
    port = int(get_env("PG_PORT", "5432"))
    user = get_env("PG_USER", "postgres")
    database = get_env("PG_DATABASE", "postgres")
    password = get_env("PG_PASSWORD", "")
    
    print(f"🔌 Connecting to {host}:{port} as {user}")
    
    driver = await NativePgDriver.connect(host, port, user, database, password if password else None)
    
    print("🚀 QAIL-PY 50M STRESS TEST (CTYPES FFI + ASYNCIO)")
    print("=" * 55)
    print(f"Total queries:    {TOTAL_QUERIES:>15,}")
    print(f"Batch size:       {BATCH_SIZE:>15,}")
    print(f"Batches:          {BATCHES:>15,}")
    print("\n⚠️  Using ctypes FFI - NO PyO3!\n")
    
    # Pre-build batch of queries as tuples
    print("✅ Pre-building query tuples...")
    batch_queries = []
    for i in range(BATCH_SIZE):
        limit = (i % 10) + 1
        batch_queries.append(("harbors", ["id", "name"], limit))
    
    print(f"✅ {len(batch_queries):,} queries pre-built")
    print("\n📊 Executing 50 million queries...\n")
    
    start = time.perf_counter()
    successful = 0
    last_report = time.perf_counter()
    
    for batch_num in range(BATCHES):
        # Execute batch using ctypes FFI
        count = await driver.pipeline_batch(batch_queries)
        successful += count
        
        # Progress report every 1M queries
        now = time.perf_counter()
        if successful % 1_000_000 == 0 or (now - last_report) >= 5:
            elapsed = now - start
            qps = successful / elapsed
            remaining = TOTAL_QUERIES - successful
            eta = remaining / qps if qps > 0 else 0
            
            print(f"   {successful // 1_000_000:>3}M queries | {qps:>8,.0f} q/s | ETA: {eta:.0f}s | Batch {batch_num+1}/{BATCHES}")
            last_report = now
    
    elapsed = time.perf_counter() - start
    qps = TOTAL_QUERIES / elapsed
    per_query_ns = (elapsed / TOTAL_QUERIES) * 1_000_000_000
    
    print("\n📈 FINAL RESULTS:")
    print("┌" + "─" * 50 + "┐")
    print("│ 50M STRESS TEST (ctypes FFI + asyncio)           │")
    print("├" + "─" * 50 + "┤")
    print(f"│ Total Time:        {elapsed:>20.1f}s │")
    print(f"│ Queries/Second:    {qps:>20,.0f} │")
    print(f"│ Per Query:         {per_query_ns:>17.0f}ns │")
    print(f"│ Successful:        {successful:>20,} │")
    print("│ Path: ctypes → Rust Encoder → asyncio TCP        │")
    print("└" + "─" * 50 + "┘")
    
    await driver.close()

if __name__ == "__main__":
    asyncio.run(main())
