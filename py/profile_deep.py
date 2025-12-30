"""
Deep profiling to find exact bottleneck in AsyncPgDriver.

The AsyncPgDriver is 15.6µs slower than asyncpg per query.
Build+Encode is only 0.89µs, so where is the other 14.7µs?

Possible culprits:
1. Python row parsing (_parse_data_row, _parse_row_description)
2. Row object creation
3. encode_cmd() being called per query instead of reusing
"""

import asyncio
import time
import struct

DB_HOST = 'localhost'
DB_PORT = 5432
DB_USER = 'orion'
DB_NAME = 'swb_staging_local'

NUM_ITERATIONS = 10000

async def profile_raw_socket_query():
    """Measure raw socket I/O without AsyncPgDriver overhead"""
    from qail import Qail, encode_cmd
    
    # Connect directly (same as AsyncPgDriver)
    reader, writer = await asyncio.open_connection(DB_HOST, DB_PORT)
    
    # Startup
    params = f"user\x00{DB_USER}\x00database\x00{DB_NAME}\x00\x00".encode('utf-8')
    length = 4 + 4 + len(params)
    startup = struct.pack('>I', length) + struct.pack('>I', 196608) + params
    writer.write(startup)
    await writer.drain()
    
    # Read until ReadyForQuery
    while True:
        header = await reader.readexactly(5)
        msg_type = header[0:1]
        length = struct.unpack('>I', header[1:5])[0] - 4
        data = await reader.readexactly(length) if length > 0 else b''
        if msg_type == b'Z':
            break
    
    # Pre-build and pre-encode cmd
    cmd = (Qail.get("destinations")
           .columns(["id", "name", "slug", "is_active"])
           .order_by("name")
           .limit(10))
    wire_bytes = encode_cmd(cmd)  # Encode ONCE
    
    # Warmup
    for _ in range(100):
        writer.write(wire_bytes)
        await writer.drain()
        # Read response
        while True:
            header = await reader.readexactly(5)
            msg_type = header[0:1]
            length = struct.unpack('>I', header[1:5])[0] - 4
            data = await reader.readexactly(length) if length > 0 else b''
            if msg_type == b'Z':
                break
    
    # Benchmark - raw socket with NO parsing
    start = time.perf_counter()
    for _ in range(NUM_ITERATIONS):
        writer.write(wire_bytes)
        await writer.drain()
        # Read response (no parsing)
        while True:
            header = await reader.readexactly(5)
            msg_type = header[0:1]
            length = struct.unpack('>I', header[1:5])[0] - 4
            data = await reader.readexactly(length) if length > 0 else b''
            if msg_type == b'Z':
                break
    elapsed = time.perf_counter() - start
    
    writer.close()
    await writer.wait_closed()
    
    ops_per_sec = NUM_ITERATIONS / elapsed
    us_per_op = (elapsed / NUM_ITERATIONS) * 1_000_000
    print(f"Raw socket (no parse): {ops_per_sec:>10,.0f} ops/s  ({us_per_op:.2f} µs/op)")
    return elapsed

async def profile_raw_socket_encode_each():
    """Same as above but encode each time"""
    from qail import Qail, encode_cmd
    
    reader, writer = await asyncio.open_connection(DB_HOST, DB_PORT)
    
    # Startup  
    params = f"user\x00{DB_USER}\x00database\x00{DB_NAME}\x00\x00".encode('utf-8')
    length = 4 + 4 + len(params)
    startup = struct.pack('>I', length) + struct.pack('>I', 196608) + params
    writer.write(startup)
    await writer.drain()
    
    while True:
        header = await reader.readexactly(5)
        msg_type = header[0:1]
        length = struct.unpack('>I', header[1:5])[0] - 4
        data = await reader.readexactly(length) if length > 0 else b''
        if msg_type == b'Z':
            break
    
    # Pre-build cmd (but encode each time)
    cmd = (Qail.get("destinations")
           .columns(["id", "name", "slug", "is_active"])
           .order_by("name")
           .limit(10))
    
    # Warmup
    for _ in range(100):
        wire_bytes = encode_cmd(cmd)  # Encode each time
        writer.write(wire_bytes)
        await writer.drain()
        while True:
            header = await reader.readexactly(5)
            msg_type = header[0:1]
            length = struct.unpack('>I', header[1:5])[0] - 4
            data = await reader.readexactly(length) if length > 0 else b''
            if msg_type == b'Z':
                break
    
    # Benchmark - encode every time
    start = time.perf_counter()
    for _ in range(NUM_ITERATIONS):
        wire_bytes = encode_cmd(cmd)
        writer.write(wire_bytes)
        await writer.drain()
        while True:
            header = await reader.readexactly(5)
            msg_type = header[0:1]
            length = struct.unpack('>I', header[1:5])[0] - 4
            data = await reader.readexactly(length) if length > 0 else b''
            if msg_type == b'Z':
                break
    elapsed = time.perf_counter() - start
    
    writer.close()
    await writer.wait_closed()
    
    ops_per_sec = NUM_ITERATIONS / elapsed
    us_per_op = (elapsed / NUM_ITERATIONS) * 1_000_000
    print(f"Raw + encode each:    {ops_per_sec:>10,.0f} ops/s  ({us_per_op:.2f} µs/op)")
    return elapsed

async def main():
    print("=" * 60)
    print("Deep Profiling: Where is the 15µs overhead?")
    print(f"Iterations: {NUM_ITERATIONS:,}")
    print("=" * 60)
    print()
    
    import asyncpg
    from qail import AsyncPgDriver, Qail
    
    # Baseline asyncpg
    conn = await asyncpg.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, database=DB_NAME)
    for _ in range(100):  # warmup
        await conn.fetch("SELECT id, name, slug, is_active FROM destinations ORDER BY name LIMIT 10")
    start = time.perf_counter()
    for _ in range(NUM_ITERATIONS):
        await conn.fetch("SELECT id, name, slug, is_active FROM destinations ORDER BY name LIMIT 10")
    t_asyncpg = time.perf_counter() - start
    await conn.close()
    print(f"asyncpg:              {NUM_ITERATIONS/t_asyncpg:>10,.0f} ops/s  ({t_asyncpg/NUM_ITERATIONS*1_000_000:.2f} µs/op)")
    
    # Raw socket - no parsing (pure network)
    t_raw = await profile_raw_socket_query()
    
    # Raw socket - encode each time
    t_raw_enc = await profile_raw_socket_encode_each()
    
    # AsyncPgDriver (includes parsing)
    driver = await AsyncPgDriver.connect(DB_HOST, DB_PORT, DB_USER, DB_NAME, None)
    cmd = (Qail.get("destinations")
           .columns(["id", "name", "slug", "is_active"])
           .order_by("name")
           .limit(10))
    for _ in range(100):
        await driver.fetch_all(cmd)
    start = time.perf_counter()
    for _ in range(NUM_ITERATIONS):
        await driver.fetch_all(cmd)
    t_async = time.perf_counter() - start
    await driver.close()
    print(f"AsyncPgDriver:        {NUM_ITERATIONS/t_async:>10,.0f} ops/s  ({t_async/NUM_ITERATIONS*1_000_000:.2f} µs/op)")
    
    print()
    print("=" * 60)
    print("BREAKDOWN")
    print("=" * 60)
    
    raw_us = t_raw / NUM_ITERATIONS * 1_000_000
    raw_enc_us = t_raw_enc / NUM_ITERATIONS * 1_000_000
    async_us = t_async / NUM_ITERATIONS * 1_000_000
    asyncpg_us = t_asyncpg / NUM_ITERATIONS * 1_000_000
    
    print(f"Pure network I/O:     {raw_us:.2f} µs")
    print(f"+ encode_cmd each:    {raw_enc_us - raw_us:+.2f} µs (total: {raw_enc_us:.2f} µs)")
    print(f"+ AsyncPgDriver:      {async_us - raw_enc_us:+.2f} µs (total: {async_us:.2f} µs) ← ROW PARSING OVERHEAD")
    print()
    print(f"asyncpg total:        {asyncpg_us:.2f} µs")
    print(f"Gap vs asyncpg:       {async_us - asyncpg_us:+.2f} µs")

if __name__ == "__main__":
    asyncio.run(main())
