#!/usr/bin/env python3
"""
QAIL-PY BLOCKING API + GIL RELEASE BENCHMARK

Uses PyO3's blocking API with GIL release.
All I/O done in Rust Tokio, GIL released during operations.

Target: ~150-200k q/s

Usage:
    python qail_pyo3_bench.py
"""

import os
import sys
import time

# Add qail-py to path
sys.path.insert(0, "../qail-py/python")

from qail import QailCmd, PgDriver

# Configuration
TOTAL_QUERIES = 50_000_000
BATCH_SIZE = 10_000
BATCHES = TOTAL_QUERIES // BATCH_SIZE


def main():
    host = os.environ.get("PG_HOST", "127.0.0.1")
    port = int(os.environ.get("PG_PORT", "5432"))
    user = os.environ.get("PG_USER", "postgres")
    database = os.environ.get("PG_DATABASE", "postgres")
    
    print(f"🔌 Connecting to {host}:{port} as {user}")
    
    # Connect using new blocking API
    driver = PgDriver.connect_trust(host, port, user, database)
    
    print("✅ Connected")
    
    print("🚀 QAIL-PY BLOCKING API + GIL RELEASE BENCHMARK")
    print("=" * 55)
    print(f"Total queries:    {TOTAL_QUERIES:>15,}")
    print(f"Batch size:       {BATCH_SIZE:>15,}")
    print(f"Batches:          {BATCHES:>15,}")
    print("\n⚠️  Rust owns TCP, GIL released during I/O\n")
    
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
        # Execute batch - GIL released during this call
        count = driver.pipeline_batch(batch_cmds)
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
    print("│ 50M STRESS TEST (PyO3 + GIL Release)             │")
    print("├" + "─" * 50 + "┤")
    print(f"│ Total Time:        {elapsed:>20.1f}s │")
    print(f"│ Queries/Second:    {qps:>20,.0f} │")
    print(f"│ Per Query:         {per_query_ns:>17.0f}ns │")
    print(f"│ Successful:        {successful:>20,} │")
    print("│ Path: Python → Rust (Tokio TCP) → Postgres       │")
    print("└" + "─" * 50 + "┘")
    
    driver.close()

if __name__ == "__main__":
    main()
