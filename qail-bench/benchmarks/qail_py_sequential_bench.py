#!/usr/bin/env python3
"""
QAIL-PY SEQUENTIAL QUERY BENCHMARK

Fair comparison: sequential queries, no pipelining.
Uses fetch_all() one query at a time.

Usage:
    source .venv-313/bin/activate
    python qail_py_sequential_bench.py
"""

import sys
import time

sys.path.insert(0, "../qail-py/python")

from qail import QailCmd, PgDriver

# Configuration - same as asyncpg sequential
TOTAL_QUERIES = 1_000_000
REPORT_INTERVAL = 100_000


def main():
    print("🔌 Connecting...")
    driver = PgDriver.connect_trust("127.0.0.1", 5432, "postgres", "postgres")
    print("✅ Connected")
    
    # Pre-build the command once
    cmd = QailCmd.get("harbors").columns(["id", "name"]).limit(10)
    print("✅ Command pre-built")
    
    print("\n🚀 QAIL-PY SEQUENTIAL QUERY BENCHMARK")
    print("=" * 55)
    print(f"Total queries:    {TOTAL_QUERIES:>15,}")
    print("\n⚠️  Sequential execution (no pipelining)\n")
    
    print("📊 Executing queries...\n")
    
    start = time.perf_counter()
    successful = 0
    
    for i in range(TOTAL_QUERIES):
        # Execute one query at a time - GIL released during call
        _rows = driver.fetch_all(cmd)
        successful += 1
        
        if successful % REPORT_INTERVAL == 0:
            elapsed = time.perf_counter() - start
            qps = successful / elapsed
            remaining = TOTAL_QUERIES - successful
            eta = remaining / qps if qps > 0 else 0
            pct = successful * 100 // TOTAL_QUERIES
            
            print(f"   {successful:>8,} queries | {qps:>8,.0f} q/s | ETA: {eta:.0f}s | {pct}%")
    
    elapsed = time.perf_counter() - start
    qps = TOTAL_QUERIES / elapsed
    per_query_ns = (elapsed / TOTAL_QUERIES) * 1_000_000_000
    
    print("\n📈 FINAL RESULTS:")
    print("┌" + "─" * 50 + "┐")
    print("│ SEQUENTIAL QUERIES (qail-py)                     │")
    print("├" + "─" * 50 + "┤")
    print(f"│ Total Time:        {elapsed:>20.1f}s │")
    print(f"│ Queries/Second:    {qps:>20,.0f} │")
    print(f"│ Per Query:         {per_query_ns:>17.0f}ns │")
    print(f"│ Successful:        {successful:>20,} │")
    print("│ Mode: Sequential (no pipelining)                 │")
    print("└" + "─" * 50 + "┘")
    
    # Compare with asyncpg
    asyncpg_qps = 17_403
    ratio = qps / asyncpg_qps
    print(f"\n📊 vs asyncpg: {ratio:.2f}x ({qps:,.0f} vs {asyncpg_qps:,} q/s)")
    
    driver.close()

if __name__ == "__main__":
    main()
