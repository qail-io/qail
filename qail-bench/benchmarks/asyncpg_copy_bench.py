#!/usr/bin/env python3
"""
ASYNCPG COPY BULK INSERT BENCHMARK

Exact same test as qail_copy_bench.py for fair comparison.

Test Setup:
CREATE TABLE _test(a int, b int, c int, d text, e float, f int, g text);

Usage:
    source .venv-313/bin/activate
    python asyncpg_copy_bench.py
"""

import asyncio
import time

# Configuration
TOTAL_ROWS = 100_000_000  # 100 million rows
ROWS_PER_COPY = 10_000
COPIES = TOTAL_ROWS // ROWS_PER_COPY


async def main():
    import asyncpg
    
    print("🔌 Connecting...")
    conn = await asyncpg.connect(
        host="127.0.0.1", port=5432, user="postgres", database="postgres"
    )
    print("✅ Connected")
    
    # Truncate table
    await conn.execute("TRUNCATE TABLE _test")
    print("✅ Table truncated")
    
    print("\n🚀 ASYNCPG COPY BULK INSERT BENCHMARK")
    print("=" * 55)
    print(f"Total rows:       {TOTAL_ROWS:>15,}")
    print(f"Rows per COPY:    {ROWS_PER_COPY:>15,}")
    print(f"COPY operations:  {COPIES:>15,}")
    print("\n⚠️  Using PostgreSQL COPY FROM STDIN protocol\n")
    
    # Pre-build rows
    print("✅ Pre-building rows...")
    test_row = (10, 11, 10, 'TESTTESTTEST', 10.333, 12341234, '123412341234')
    batch_rows = [test_row for _ in range(ROWS_PER_COPY)]
    print(f"✅ {ROWS_PER_COPY:,} rows pre-built")
    
    print("\n📊 Executing COPY operations...\n")
    
    start = time.perf_counter()
    total_inserted = 0
    
    for copy_num in range(COPIES):
        # Execute COPY using asyncpg's copy_records_to_table
        count = await conn.copy_records_to_table(
            '_test',
            records=batch_rows,
            columns=['a', 'b', 'c', 'd', 'e', 'f', 'g']
        )
        total_inserted += ROWS_PER_COPY  # count returns number string, just use batch size
        
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
    print("│ COPY BULK INSERT (asyncpg)                       │")
    print("├" + "─" * 50 + "┤")
    print(f"│ Total Time:        {elapsed:>20.1f}s │")
    print(f"│ Rows/Second:       {rows_per_sec:>20,.0f} │")
    print(f"│ Copies/Second:     {copies_per_sec:>20,.1f} │")
    print(f"│ Per Row:           {ns_per_row:>17.0f}ns │")
    print(f"│ Total Inserted:    {total_inserted:>20,} │")
    print("│ Path: Python → asyncpg (COPY) → Postgres         │")
    print("└" + "─" * 50 + "┘")
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
