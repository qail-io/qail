#!/usr/bin/env python3
"""
QAIL-PY COPY BULK INSERT BENCHMARK

Compares with asyncpg's COPY benchmark:
- asyncpg: 2.6M rows/s, 266 queries/s (10,000 rows per COPY)

Test Setup:
CREATE TABLE _test(a int, b int, c int, d text, e float, f int, g text);

Usage:
    # First, create table:
    psql -c "DROP TABLE IF EXISTS _test; CREATE TABLE _test(a int, b int, c int, d text, e float, f int, g text);"
    
    # Then run benchmark:
    source .venv-313/bin/activate
    python qail_copy_bench.py
"""

import sys
import time

sys.path.insert(0, "../qail-py/python")

from qail import PgDriver, QailCmd

# Configuration
TOTAL_ROWS = 2_600_000  # Match asyncpg's 2.6M rows
ROWS_PER_COPY = 10_000  # Match asyncpg's batch size
COPIES = TOTAL_ROWS // ROWS_PER_COPY  # = 260 COPY operations


def main():
    print("🔌 Connecting...")
    driver = PgDriver.connect_trust("127.0.0.1", 5432, "postgres", "postgres")
    print("✅ Connected")
    
    print("\n🚀 QAIL-PY COPY BULK INSERT BENCHMARK")
    print("=" * 55)
    print(f"Total rows:       {TOTAL_ROWS:>15,}")
    print(f"Rows per COPY:    {ROWS_PER_COPY:>15,}")
    print(f"COPY operations:  {COPIES:>15,}")
    print("\n⚠️  Using PostgreSQL COPY FROM STDIN protocol\n")
    
    # Pre-build rows
    print("✅ Pre-building rows...")
    test_row = [10, 11, 10, 'TESTTESTTEST', 10.333, 12341234, '123412341234']
    batch_rows = [test_row for _ in range(ROWS_PER_COPY)]
    print(f"✅ {ROWS_PER_COPY:,} rows pre-built")
    
    # Create QailCmd for COPY
    cmd = QailCmd.add("_test").columns(["a", "b", "c", "d", "e", "f", "g"])
    
    print("\n📊 Executing COPY operations...\n")
    
    start = time.perf_counter()
    total_inserted = 0
    
    for copy_num in range(COPIES):
        # Execute COPY - GIL released during operation
        count = driver.copy_bulk(cmd, batch_rows)
        total_inserted += count
        
        # Progress report
        if (copy_num + 1) % 10 == 0 or copy_num == 0:
            elapsed = time.perf_counter() - start
            rows_per_sec = total_inserted / elapsed if elapsed > 0 else 0
            copies_per_sec = (copy_num + 1) / elapsed if elapsed > 0 else 0
            remaining_copies = COPIES - (copy_num + 1)
            eta = remaining_copies / copies_per_sec if copies_per_sec > 0 else 0
            
            print(f"   {total_inserted:>8,} rows | {rows_per_sec:>10,.0f} rows/s | "
                  f"{copies_per_sec:>6,.1f} copies/s | ETA: {eta:.0f}s | "
                  f"COPY {copy_num+1}/{COPIES}")
    
    elapsed = time.perf_counter() - start
    rows_per_sec = TOTAL_ROWS / elapsed
    copies_per_sec = COPIES / elapsed
    ns_per_row = (elapsed / TOTAL_ROWS) * 1_000_000_000
    
    print("\n📈 FINAL RESULTS:")
    print("┌" + "─" * 50 + "┐")
    print("│ COPY BULK INSERT (qail-py)                       │")
    print("├" + "─" * 50 + "┤")
    print(f"│ Total Time:        {elapsed:>20.1f}s │")
    print(f"│ Rows/Second:       {rows_per_sec:>20,.0f} │")
    print(f"│ Copies/Second:     {copies_per_sec:>20,.1f} │")
    print(f"│ Per Row:           {ns_per_row:>17.0f}ns │")
    print(f"│ Total Inserted:    {total_inserted:>20,} │")
    print("│ Path: Python → Rust (COPY protocol) → Postgres   │")
    print("└" + "─" * 50 + "┘")
    
    # Compare with asyncpg
    asyncpg_rows_per_sec = 2_600_000
    ratio = rows_per_sec / asyncpg_rows_per_sec
    print(f"\n📊 vs asyncpg: {ratio:.2f}x ({rows_per_sec:,.0f} vs {asyncpg_rows_per_sec:,.0f} rows/s)")
    
    driver.close()

if __name__ == "__main__":
    main()
