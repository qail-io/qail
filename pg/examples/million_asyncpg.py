#!/usr/bin/env python3
"""
1 MILLION QUERY BENCHMARK - Python asyncpg

asyncpg is one of the fastest Python PostgreSQL drivers.
Uses native pipelining via async + prepared statements.

Run: python3 million_asyncpg.py
Requirements: pip install asyncpg
"""

import asyncio
import time

TOTAL_QUERIES = 1_000_000
QUERIES_PER_BATCH = 1_000
BATCHES = TOTAL_QUERIES // QUERIES_PER_BATCH

async def main():
    import asyncpg
    
    # Connect
    conn = await asyncpg.connect(
        host='127.0.0.1',
        port=5432,
        user='orion',
        database='swb_staging_local'
    )
    
    print("🐍 1 MILLION QUERY BENCHMARK - Python asyncpg")
    print("=" * 50)
    print(f"Total queries:    {TOTAL_QUERIES:>15,}")
    print(f"Batch size:       {QUERIES_PER_BATCH:>15,}")
    print(f"Batches:          {BATCHES:>15,}")
    print()
    
    # Prepare statement ONCE
    stmt = await conn.prepare("SELECT id, name FROM harbors LIMIT $1")
    print("✅ Statement prepared")
    
    # Build params
    params = [(i % 10) + 1 for i in range(QUERIES_PER_BATCH)]
    
    print(f"\n📊 Executing {TOTAL_QUERIES:,} queries...\n")
    
    start = time.perf_counter()
    successful = 0
    
    for batch in range(BATCHES):
        # asyncpg prepared statement - execute one at a time
        # (asyncpg doesn't support true pipelining on single connection)
        for p in params:
            await stmt.fetch(p)
            successful += 1
        
        # Progress
        if (batch + 1) % 100 == 0:
            elapsed = time.perf_counter() - start
            qps = successful / elapsed
            remaining = TOTAL_QUERIES - successful
            eta = remaining / qps if qps > 0 else 0
            print(f"   Batch {batch + 1}/{BATCHES}: {qps:,.0f} q/s | ETA: {eta:.0f}s")
    
    elapsed = time.perf_counter() - start
    qps = TOTAL_QUERIES / elapsed
    per_query_ns = (elapsed * 1_000_000_000) / TOTAL_QUERIES
    
    print(f"\n📈 FINAL RESULTS:")
    print("┌──────────────────────────────────────────┐")
    print("│ 1 MILLION QUERIES - Python asyncpg       │")
    print("├──────────────────────────────────────────┤")
    print(f"│ Total Time:        {elapsed:>20.1f}s │")
    print(f"│ Queries/Second:    {qps:>20,.0f} │")
    print(f"│ Per Query:         {per_query_ns:>17,.0f}ns │")
    print(f"│ Successful:        {successful:>20,} │")
    print("└──────────────────────────────────────────┘")
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
