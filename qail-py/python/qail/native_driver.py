"""
Pure Python async PostgreSQL driver using ctypes FFI.

Uses native asyncio for I/O. Rust encoder accessed via ctypes (no PyO3).
This is the high-performance path for Python.
"""

import asyncio
import struct
from typing import Optional
from .ffi import encode_get, encode_batch_get


def _encode_startup(user: str, database: str) -> bytes:
    """Encode PostgreSQL startup message."""
    params = f"user\x00{user}\x00database\x00{database}\x00\x00"
    params_bytes = params.encode('utf-8')
    length = 4 + 4 + len(params_bytes)
    return struct.pack('>I', length) + struct.pack('>I', 196608) + params_bytes


class Row:
    """Row from query result."""
    
    def __init__(self, columns: list[Optional[bytes]], names: list[str]):
        self._columns = columns
        self._names = names
        self._name_to_idx = {n: i for i, n in enumerate(names)}
    
    def get(self, index: int) -> Optional[bytes]:
        if 0 <= index < len(self._columns):
            return self._columns[index]
        return None
    
    def get_by_name(self, name: str) -> Optional[bytes]:
        idx = self._name_to_idx.get(name)
        if idx is not None:
            return self._columns[idx]
        return None
    
    def to_dict(self) -> dict:
        result = {}
        for name, idx in self._name_to_idx.items():
            val = self._columns[idx]
            if val is None:
                result[name] = None
            else:
                s = val.decode('utf-8')
                if s == 't':
                    result[name] = True
                elif s == 'f':
                    result[name] = False
                else:
                    try:
                        result[name] = int(s)
                    except ValueError:
                        try:
                            result[name] = float(s)
                        except ValueError:
                            result[name] = s
        return result
    
    def __getitem__(self, key):
        if isinstance(key, int):
            return self.get(key)
        return self.get_by_name(key)
    
    def __len__(self):
        return len(self._columns)


class NativePgDriver:
    """
    Pure Python async PostgreSQL driver using ctypes FFI.
    
    Uses Rust qail-core via ctypes (no PyO3).
    Python asyncio handles all TCP I/O.
    """
    
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self._reader = reader
        self._writer = writer
    
    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        user: str,
        database: str,
        password: Optional[str] = None,
    ) -> "NativePgDriver":
        """Connect to PostgreSQL."""
        reader, writer = await asyncio.open_connection(host, port)
        driver = cls(reader, writer)
        await driver._handshake(user, database, password)
        return driver
    
    async def _handshake(self, user: str, database: str, password: Optional[str]):
        """Perform PostgreSQL startup handshake."""
        startup = _encode_startup(user, database)
        self._writer.write(startup)
        await self._writer.drain()
        
        while True:
            msg_type, data = await self._recv_msg()
            
            if msg_type == b'R':  # AuthenticationXXX
                auth_type = struct.unpack('>I', data[:4])[0]
                if auth_type == 0:  # AuthenticationOk
                    pass
                elif auth_type == 3:  # CleartextPassword
                    if password:
                        pwd_msg = self._encode_password_msg(password)
                        self._writer.write(pwd_msg)
                        await self._writer.drain()
                elif auth_type == 10:  # SASL
                    raise RuntimeError("SCRAM-SHA-256 not implemented. Use trust mode.")
            elif msg_type == b'K':  # BackendKeyData
                pass
            elif msg_type == b'Z':  # ReadyForQuery
                break
            elif msg_type == b'E':  # ErrorResponse
                raise RuntimeError(f"Auth error: {data}")
    
    def _encode_password_msg(self, password: str) -> bytes:
        pwd_bytes = password.encode('utf-8') + b'\x00'
        length = 4 + len(pwd_bytes)
        return b'p' + struct.pack('>I', length) + pwd_bytes
    
    async def _recv_msg(self) -> tuple[bytes, bytes]:
        header = await self._reader.readexactly(5)
        msg_type = header[0:1]
        length = struct.unpack('>I', header[1:5])[0] - 4
        data = await self._reader.readexactly(length) if length > 0 else b''
        return msg_type, data
    
    async def fetch_all(
        self, 
        table: str, 
        columns: list[str] | None = None, 
        limit: int = -1
    ) -> list[Row]:
        """Execute GET query and fetch all rows."""
        wire_bytes = encode_get(table, columns, limit)
        self._writer.write(wire_bytes)
        await self._writer.drain()
        return await self._read_rows()
    
    async def _read_rows(self) -> list[Row]:
        rows = []
        col_names = []
        
        while True:
            msg_type, data = await self._recv_msg()
            
            if msg_type == b'1':  # ParseComplete
                pass
            elif msg_type == b'2':  # BindComplete
                pass
            elif msg_type == b'T':  # RowDescription
                col_names = self._parse_row_description(data)
            elif msg_type == b'D':  # DataRow
                columns = self._parse_data_row(data)
                rows.append(Row(columns, col_names))
            elif msg_type == b'C':  # CommandComplete
                pass
            elif msg_type == b'Z':  # ReadyForQuery
                break
            elif msg_type == b'E':  # ErrorResponse
                raise RuntimeError(f"Query error: {data}")
        
        return rows
    
    def _parse_row_description(self, data: bytes) -> list[str]:
        col_count = struct.unpack('>H', data[:2])[0]
        names = []
        offset = 2
        for _ in range(col_count):
            end = data.index(b'\x00', offset)
            name = data[offset:end].decode('utf-8')
            names.append(name)
            offset = end + 1 + 18
        return names
    
    def _parse_data_row(self, data: bytes) -> list[Optional[bytes]]:
        col_count = struct.unpack('>H', data[:2])[0]
        columns = []
        offset = 2
        for _ in range(col_count):
            length = struct.unpack('>i', data[offset:offset+4])[0]
            offset += 4
            if length == -1:
                columns.append(None)
            else:
                columns.append(data[offset:offset+length])
                offset += length
        return columns
    
    async def pipeline_batch(
        self, 
        queries: list[tuple[str, list[str] | None, int]]
    ) -> int:
        """Execute batch of GET queries in single round-trip."""
        wire_bytes = encode_batch_get(queries)
        self._writer.write(wire_bytes)
        await self._writer.drain()
        return await self._read_batch_count(len(queries))
    
    async def _read_batch_count(self, expected: int) -> int:
        completed = 0
        while True:
            msg_type, data = await self._recv_msg()
            if msg_type in (b'C', b'n'):  # CommandComplete or NoData
                completed += 1
            elif msg_type == b'Z':  # ReadyForQuery
                if completed >= expected:
                    break
            elif msg_type == b'E':
                raise RuntimeError(f"Batch error: {data}")
        return completed
    
    async def close(self):
        """Close connection."""
        self._writer.write(b'X\x00\x00\x00\x04')
        await self._writer.drain()
        self._writer.close()
        await self._writer.wait_closed()
