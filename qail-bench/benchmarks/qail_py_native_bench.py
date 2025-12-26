#!/usr/bin/env python3
"""
QAIL-PY NATIVE ASYNC BENCHMARK: 50 Million Query Stress Test

Uses native Python asyncio (no Tokio bridge) for maximum performance.

Usage:
    cd qail-bench/benchmarks
    python qail_py_native_bench.py
"""

import asyncio
import os
import time
from qail import QailCmd, AsyncPgDriver, encode_batch

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
    
    # Use native Python async driver
    driver = await AsyncPgDriver.connect(host, port, user, database, password if password else None)
    
    print("🚀 QAIL-PY 50M STRESS TEST (NATIVE PYTHON ASYNCIO)")
    print("=" * 55)
    print(f"Total queries:    {TOTAL_QUERIES:>15,}")
    print(f"Batch size:       {BATCH_SIZE:>15,}")
    print(f"Batches:          {BATCHES:>15,}")
    print("\n⚠️  Using native asyncio - NO Tokio bridge!\n")
    
    # Pre-build batch of commands
    print("✅ Pre-building commands...")
    batch_cmds = []
    for i in range(BATCH_SIZE):
        limit = (i % 10) + 1
        cmd = QailCmd.get("harbors").columns(["id", "name"]).limit(limit)
        batch_cmds.append(cmd)
    
    print(f"✅ {len(batch_cmds):,} commands pre-built")
    print("\n📊 Executing 50 million queries...\n")
    
    start = time.perf_counter()
    successful = 0
    last_report = time.perf_counter()
    
    for batch_num in range(BATCHES):
        # Execute batch using native Python async driver
        count = await driver.pipeline_batch(batch_cmds)
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
    print("│ 50M STRESS TEST (Native Python Asyncio)          │")
    print("├" + "─" * 50 + "┤")
    print(f"│ Total Time:        {elapsed:>20.1f}s │")
    print(f"│ Queries/Second:    {qps:>20,.0f} │")
    print(f"│ Per Query:         {per_query_ns:>17.0f}ns │")
    print(f"│ Successful:        {successful:>20,} │")
    print("│ Path: Python AST → Rust Encoder → asyncio TCP    │")
    print("└" + "─" * 50 + "┘")
    
    await driver.close()

if __name__ == "__main__":
    asyncio.run(main())
