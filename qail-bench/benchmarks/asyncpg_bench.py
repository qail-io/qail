#!/usr/bin/env python3
"""
ASYNCPG 50 Million Query Benchmark (Prepared + Concurrent)

Uses prepared statements + concurrent execution for max throughput.
This is the fairest comparison with qail-py batched benchmark.

Usage:
    source .venv/bin/activate
    python asyncpg_prepared_bench.py
"""

import asyncio
import os
import time
import asyncpg

# Configuration
TOTAL_QUERIES = 50_000_000
BATCH_SIZE = 10_000
BATCHES = TOTAL_QUERIES // BATCH_SIZE
CONCURRENCY = 100  # Number of concurrent queries


async def main():
    host = os.environ.get("PG_HOST", "127.0.0.1")
    port = int(os.environ.get("PG_PORT", "5432"))
    user = os.environ.get("PG_USER", "postgres")
    database = os.environ.get("PG_DATABASE", "postgres")
    
    print(f"🔌 Connecting to {host}:{port} as {user}")
    
    conn = await asyncpg.connect(
        host=host, port=port, user=user, database=database
    )
    
    print("✅ Connected")
    
    # Prepare the statement once
    stmt = await conn.prepare("SELECT id, name FROM harbors LIMIT 10")
    
    print("🚀 ASYNCPG 50 MILLION QUERY BENCHMARK")
    print("=" * 55)
    print(f"Total queries:    {TOTAL_QUERIES:>15,}")
    print(f"Batch size:       {BATCH_SIZE:>15,}")
    print(f"Concurrency:      {CONCURRENCY:>15}")
    print("\n⚠️  Using prepared statements + concurrent execution\n")
    
    print("📊 Executing 50 million queries...\n")
    
    async def run_query():
        return await stmt.fetch()
    
    start = time.perf_counter()
    successful = 0
    last_report = time.perf_counter()
    
    for batch_num in range(BATCHES):
        # Run batch of queries concurrently
        tasks = [run_query() for _ in range(BATCH_SIZE)]
        
        # Execute in chunks to avoid too many concurrent tasks
        chunk_size = CONCURRENCY
        for i in range(0, len(tasks), chunk_size):
            chunk = tasks[i:i+chunk_size]
            await asyncio.gather(*chunk)
        
        successful += BATCH_SIZE
        
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
    print("│ 50M STRESS TEST (asyncpg prepared+concurrent)    │")
    print("├" + "─" * 50 + "┤")
    print(f"│ Total Time:        {elapsed:>20.1f}s │")
    print(f"│ Queries/Second:    {qps:>20,.0f} │")
    print(f"│ Per Query:         {per_query_ns:>17.0f}ns │")
    print(f"│ Successful:        {successful:>20,} │")
    print("│ Path: asyncpg prepared + gather                  │")
    print("└" + "─" * 50 + "┘")
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
