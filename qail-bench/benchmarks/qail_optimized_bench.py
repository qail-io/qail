#!/usr/bin/env python3
"""
QAIL-PY OPTIMIZED BENCHMARK: Encode Once, Execute Many

Uses the HIGH-PERFORMANCE path:
1. Encode batch ONCE at startup (single FFI call)
2. Reuse same bytes for every iteration (ZERO FFI in hot loop)

This matches Rust's pipeline_prepared_fast pattern.

Usage:
    QAIL_LIB_PATH=/path/to/target/release python qail_optimized_bench.py
"""

import asyncio
import os
import sys
import time
import struct

# Add qail-py to path
sys.path.insert(0, "../qail-py/python")

from qail.ffi import encode_uniform_batch

# Configuration
TOTAL_QUERIES = 50_000_000
BATCH_SIZE = 10_000
BATCHES = TOTAL_QUERIES // BATCH_SIZE


def _encode_startup(user: str, database: str) -> bytes:
    """Encode PostgreSQL startup message."""
    params = f"user\x00{user}\x00database\x00{database}\x00\x00"
    params_bytes = params.encode('utf-8')
    length = 4 + 4 + len(params_bytes)
    return struct.pack('>I', length) + struct.pack('>I', 196608) + params_bytes


async def _recv_msg(reader):
    """Receive one PostgreSQL message."""
    header = await reader.readexactly(5)
    msg_type = header[0:1]
    length = struct.unpack('>I', header[1:5])[0] - 4
    data = await reader.readexactly(length) if length > 0 else b''
    return msg_type, data


async def main():
    host = os.environ.get("PG_HOST", "127.0.0.1")
    port = int(os.environ.get("PG_PORT", "5432"))
    user = os.environ.get("PG_USER", "postgres")
    database = os.environ.get("PG_DATABASE", "postgres")
    
    print(f"🔌 Connecting to {host}:{port} as {user}")
    
    # Connect
    reader, writer = await asyncio.open_connection(host, port)
    
    # Handshake
    writer.write(_encode_startup(user, database))
    await writer.drain()
    
    while True:
        msg_type, data = await _recv_msg(reader)
        if msg_type == b'Z':  # ReadyForQuery
            break
        elif msg_type == b'E':
            raise RuntimeError(f"Auth error: {data}")
    
    print("✅ Connected")
    
    print("🚀 QAIL-PY OPTIMIZED BENCHMARK")
    print("=" * 55)
    print(f"Total queries:    {TOTAL_QUERIES:>15,}")
    print(f"Batch size:       {BATCH_SIZE:>15,}")
    print(f"Batches:          {BATCHES:>15,}")
    print("\n⚠️  Encode ONCE, execute MANY (zero FFI in hot loop)\n")
    
    # PRE-ENCODE BATCH ONCE (single FFI call)
    print("✅ Pre-encoding batch (single FFI call)...")
    encode_start = time.perf_counter()
    batch_bytes = encode_uniform_batch("harbors", ["id", "name"], 10, BATCH_SIZE)
    encode_time = time.perf_counter() - encode_start
    print(f"✅ Encoded {BATCH_SIZE:,} queries in {encode_time*1000:.1f}ms ({len(batch_bytes):,} bytes)")
    
    print("\n📊 Executing 50 million queries...\n")
    
    start = time.perf_counter()
    successful = 0
    last_report = time.perf_counter()
    
    for batch_num in range(BATCHES):
        # NO FFI CALL HERE - just write pre-encoded bytes!
        writer.write(batch_bytes)
        await writer.drain()
        
        # Read responses - wait for ReadyForQuery at the end
        # Each query in pipeline produces: Parse -> Bind -> RowDesc -> DataRow* -> CommandComplete
        # But we wait for the final ReadyForQuery ('Z') which signals batch complete
        query_count = 0
        while True:
            msg_type, data = await _recv_msg(reader)
            if msg_type == b'C':  # CommandComplete - one per query
                query_count += 1
            elif msg_type == b'Z':  # ReadyForQuery - batch complete
                break
            elif msg_type == b'E':
                raise RuntimeError(f"Batch error: {data}")
            # Ignore all other message types (Parse, Bind, RowDesc, DataRow)
        
        successful += query_count
        
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
    print("│ 50M STRESS TEST (Encode Once, Execute Many)      │")
    print("├" + "─" * 50 + "┤")
    print(f"│ Total Time:        {elapsed:>20.1f}s │")
    print(f"│ Queries/Second:    {qps:>20,.0f} │")
    print(f"│ Per Query:         {per_query_ns:>17.0f}ns │")
    print(f"│ Successful:        {successful:>20,} │")
    print("│ Path: Pre-encoded bytes → asyncio TCP            │")
    print("└" + "─" * 50 + "┘")
    
    # Close
    writer.write(b'X\x00\x00\x00\x04')
    await writer.drain()
    writer.close()

if __name__ == "__main__":
    asyncio.run(main())
