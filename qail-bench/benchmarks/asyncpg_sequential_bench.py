#!/usr/bin/env python3
"""
ASYNCPG SEQUENTIAL QUERY BENCHMARK

Fair comparison: sequential queries, no pipelining.
Uses prepared statement for fair Rust comparison.

Usage:
    source .venv-313/bin/activate
    python asyncpg_sequential_bench.py
"""

import asyncio
import time

# Configuration - smaller sample for sequential (50M would take hours)
TOTAL_QUERIES = 1_000_000
REPORT_INTERVAL = 100_000


async def main():
    import asyncpg
    
    print("🔌 Connecting...")
    conn = await asyncpg.connect(
        host="127.0.0.1", port=5432, user="postgres", database="postgres"
    )
    print("✅ Connected")
    
    # Prepare statement
    stmt = await conn.prepare("SELECT id, name FROM harbors LIMIT 10")
    print("✅ Statement prepared")
    
    print("\n🚀 ASYNCPG SEQUENTIAL QUERY BENCHMARK")
    print("=" * 55)
    print(f"Total queries:    {TOTAL_QUERIES:>15,}")
    print("\n⚠️  Sequential execution (no pipelining)\n")
    
    print("📊 Executing queries...\n")
    
    start = time.perf_counter()
    successful = 0
    
    for i in range(TOTAL_QUERIES):
        # Execute one query at a time
        await stmt.fetch()
        successful += 1
        
        if successful % REPORT_INTERVAL == 0:
            elapsed = time.perf_counter() - start
            qps = successful / elapsed
            remaining = TOTAL_QUERIES - successful
            eta = remaining / qps if qps > 0 else 0
            pct = successful * 100 / TOTAL_QUERIES
            
            print(f"   {successful:>8,} queries | {qps:>8,.0f} q/s | ETA: {eta:.0f}s | {pct:.0f}%")
    
    elapsed = time.perf_counter() - start
    qps = TOTAL_QUERIES / elapsed
    per_query_ns = (elapsed / TOTAL_QUERIES) * 1_000_000_000
    
    print("\n📈 FINAL RESULTS:")
    print("┌" + "─" * 50 + "┐")
    print("│ SEQUENTIAL QUERIES (asyncpg)                     │")
    print("├" + "─" * 50 + "┤")
    print(f"│ Total Time:        {elapsed:>20.1f}s │")
    print(f"│ Queries/Second:    {qps:>20,.0f} │")
    print(f"│ Per Query:         {per_query_ns:>17.0f}ns │")
    print(f"│ Successful:        {successful:>20,} │")
    print("│ Mode: Sequential (no pipelining)                 │")
    print("└" + "─" * 50 + "┘")
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
