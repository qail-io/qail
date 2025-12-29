#!/usr/bin/env python3
"""
Python asyncpg Pipelining Benchmark
Compare with QAIL-PG query_pipeline()

Run: STAGING_DB_PASSWORD="xxx" python3 asyncpg_benchmark.py
"""

import asyncio
import os
import time
from typing import List

QUERIES_PER_BATCH = 1000
BATCHES = 1000

async def main():
    import asyncpg
    
    password = os.environ.get("STAGING_DB_PASSWORD")
    if not password:
        print("Set STAGING_DB_PASSWORD")
        return
    
    conn = await asyncpg.connect(
        host="127.0.0.1",
        port=5444,
        user="sailtix",
        password=password,
        database="swb-staging"
    )
    
    total_queries = BATCHES * QUERIES_PER_BATCH
    
    print("🚀 PYTHON ASYNCPG MILLION QUERY BENCHMARK")
    print("==========================================")
    print(f"Total queries: {total_queries:>12,}")
    print(f"Batch size:    {QUERIES_PER_BATCH:>12,}")
    print(f"Batches:       {BATCHES:>12,}\n")
    
    # Warmup
    await conn.execute("SELECT 1")
    
    # ===== PIPELINED QUERIES =====
    print("📊 Running pipeline benchmark...")
    
    start = time.perf_counter()
    successful_queries = 0
    
    for batch in range(BATCHES):
        if batch % 100 == 0:
            print(f"   Batch {batch}/{BATCHES}")
        
        # asyncpg doesn't have explicit pipeline(), but we can batch via prepared statements
        stmt = await conn.prepare("SELECT id, name FROM harbors LIMIT $1")
        
        # Execute batch of queries
        for i in range(1, QUERIES_PER_BATCH + 1):
            limit = (i % 10) + 1
            await stmt.fetch(limit)
            successful_queries += 1
    
    elapsed = time.perf_counter() - start
    
    # Results
    qps = total_queries / elapsed
    per_query_ns = int(elapsed * 1_000_000_000 / total_queries)
    
    print("\n📈 Results:")
    print("┌──────────────────────────────────────────┐")
    print("│ PYTHON ASYNCPG - ONE MILLION QUERIES     │")
    print("├──────────────────────────────────────────┤")
    print(f"│ Total Time:     {elapsed:>23.2f}s │")
    print(f"│ Queries/Second: {qps:>23,.0f} │")
    print(f"│ Per Query:      {per_query_ns:>20,}ns │")
    print(f"│ Successful:     {successful_queries:>23,} │")
    print("└──────────────────────────────────────────┘")
    
    # Compare to theoretical serial
    theoretical_serial_secs = total_queries * 0.037
    speedup = theoretical_serial_secs / elapsed
    
    print(f"\n🏆 vs Serial (37ms/query):")
    print(f"   Serial estimate:  {theoretical_serial_secs:.0f} seconds ({theoretical_serial_secs/3600:.1f} hours)")
    print(f"   Pipeline actual:  {elapsed:.1f} seconds")
    print(f"   Speedup:          {speedup:.0f}x faster!")
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
