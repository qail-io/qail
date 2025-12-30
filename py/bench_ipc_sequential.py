"""
Benchmark IPC daemon mode for sequential queries.

Tests the qail-daemon Unix socket approach for sequential queries.
Requires qail-daemon to be running: cargo run -p qail-daemon
"""

import asyncio
import socket
import struct
import json
import time

SOCKET_PATH = "/tmp/qail.sock"
DB_HOST = 'localhost'
DB_PORT = 5432
DB_USER = 'orion'
DB_NAME = 'swb_staging_local'

NUM_QUERIES = 5000

class IpcClient:
    """Simple IPC client for qail-daemon"""
    
    def __init__(self, sock):
        self.sock = sock
    
    @classmethod
    def connect(cls):
        """Connect to qail-daemon via Unix socket"""
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(SOCKET_PATH)
        return cls(sock)
    
    def send_request(self, request: dict) -> dict:
        """Send request and receive response"""
        data = json.dumps(request).encode('utf-8')
        length = struct.pack('>I', len(data))
        self.sock.sendall(length + data)
        
        # Read response
        len_bytes = self.sock.recv(4)
        resp_len = struct.unpack('>I', len_bytes)[0]
        resp_data = b''
        while len(resp_data) < resp_len:
            resp_data += self.sock.recv(resp_len - len(resp_data))
        
        return json.loads(resp_data)
    
    def close(self):
        self.sock.close()

def bench_ipc_sequential():
    """Benchmark IPC daemon with sequential queries"""
    try:
        client = IpcClient.connect()
    except Exception as e:
        print(f"  IPC daemon not running: {e}")
        print(f"  Start daemon with: cargo run -p qail-daemon")
        return None
    
    # Connect to database
    resp = client.send_request({
        "type": "Connect",
        "host": DB_HOST,
        "port": DB_PORT,
        "user": DB_USER,
        "database": DB_NAME,
        "password": None
    })
    
    if resp.get("type") == "Error":
        print(f"  Connection error: {resp.get('message')}")
        return None
    
    # Build query request
    query = {
        "type": "Get",
        "table": "destinations",
        "columns": ["id", "name", "slug", "is_active"],
        "filter": None,
        "limit": 10
    }
    
    # Warmup
    for _ in range(100):
        resp = client.send_request(query)
    
    # Benchmark
    start = time.perf_counter()
    for _ in range(NUM_QUERIES):
        resp = client.send_request(query)
    elapsed = time.perf_counter() - start
    
    client.send_request({"type": "Close"})
    client.close()
    
    qps = NUM_QUERIES / elapsed
    us_per_op = (elapsed / NUM_QUERIES) * 1_000_000
    return ("IPC daemon", qps, elapsed, us_per_op)

async def bench_asyncpg():
    """Baseline asyncpg"""
    import asyncpg
    
    conn = await asyncpg.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, database=DB_NAME
    )
    
    sql = "SELECT id, name, slug, is_active FROM destinations ORDER BY name LIMIT 10"
    
    for _ in range(100):
        await conn.fetch(sql)
    
    start = time.perf_counter()
    for _ in range(NUM_QUERIES):
        await conn.fetch(sql)
    elapsed = time.perf_counter() - start
    
    await conn.close()
    
    qps = NUM_QUERIES / elapsed
    us_per_op = (elapsed / NUM_QUERIES) * 1_000_000
    return ("asyncpg", qps, elapsed, us_per_op)

async def bench_qail_pyo3():
    """PyO3 driver"""
    from qail import PgDriver, Qail
    
    driver = await asyncio.to_thread(
        PgDriver.connect, DB_HOST, DB_PORT, DB_USER, DB_NAME, ""
    )
    
    cmd = (Qail.get("destinations")
           .columns(["id", "name", "slug", "is_active"])
           .order_by("name")
           .limit(10))
    
    for _ in range(100):
        await asyncio.to_thread(driver.fetch_all, cmd)
    
    start = time.perf_counter()
    for _ in range(NUM_QUERIES):
        await asyncio.to_thread(driver.fetch_all, cmd)
    elapsed = time.perf_counter() - start
    
    qps = NUM_QUERIES / elapsed
    us_per_op = (elapsed / NUM_QUERIES) * 1_000_000
    return ("PyO3 driver", qps, elapsed, us_per_op)

async def main():
    print("=" * 60)
    print("Sequential Query Benchmark - Including IPC Daemon")
    print(f"Queries: {NUM_QUERIES} sequential")
    print("=" * 60)
    print()
    
    results = []
    
    print("Testing asyncpg...")
    r = await bench_asyncpg()
    results.append(r)
    print(f"  {r[0]}: {r[1]:,.0f} q/s ({r[3]:.2f} µs/query)")
    
    print("Testing PyO3 driver...")
    r = await bench_qail_pyo3()
    results.append(r)
    print(f"  {r[0]}: {r[1]:,.0f} q/s ({r[3]:.2f} µs/query)")
    
    print("Testing IPC daemon...")
    r = bench_ipc_sequential()
    if r:
        results.append(r)
        print(f"  {r[0]}: {r[1]:,.0f} q/s ({r[3]:.2f} µs/query)")
    
    print()
    print("=" * 60)
    print("RESULTS (Sequential Mode)")
    print("=" * 60)
    
    if results:
        baseline = results[0][1]  # asyncpg
        for name, qps, elapsed, us in sorted(results, key=lambda x: -x[1]):
            ratio = qps / baseline
            print(f"{name:20s} {qps:>10,.0f} q/s  {us:>7.2f} µs/query  ({ratio:.2f}x vs asyncpg)")

if __name__ == "__main__":
    asyncio.run(main())
