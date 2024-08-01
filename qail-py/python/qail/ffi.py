"""
QAIL FFI wrapper using ctypes.

Provides direct access to Rust qail-core encoder without PyO3 overhead.
"""

import ctypes
import os
from pathlib import Path


def _find_library() -> str:
    """Find the qail-ffi shared library."""
    # Check common locations
    candidates = [
        # Development build
        Path(__file__).parent.parent.parent.parent.parent / "target" / "release" / "libqail_ffi.dylib",
        Path(__file__).parent.parent.parent.parent.parent / "target" / "release" / "libqail_ffi.so",
        Path(__file__).parent.parent.parent.parent.parent / "target" / "debug" / "libqail_ffi.dylib",
        Path(__file__).parent.parent.parent.parent.parent / "target" / "debug" / "libqail_ffi.so",
        # Installed
        Path("/usr/local/lib/libqail_ffi.dylib"),
        Path("/usr/local/lib/libqail_ffi.so"),
        # Environment variable
        Path(os.environ.get("QAIL_LIB_PATH", "")) / "libqail_ffi.dylib",
        Path(os.environ.get("QAIL_LIB_PATH", "")) / "libqail_ffi.so",
    ]
    
    for path in candidates:
        if path.exists():
            return str(path)
    
    raise RuntimeError(
        "Could not find libqail_ffi. Set QAIL_LIB_PATH or build with: "
        "cargo build --release -p qail-ffi"
    )


# Load library
_lib = ctypes.CDLL(_find_library())

# Define function signatures

# qail_encode_get
_lib.qail_encode_get.argtypes = [
    ctypes.c_char_p,                     # table
    ctypes.c_char_p,                     # columns (comma-sep or "*")
    ctypes.c_int64,                      # limit (-1 for none)
    ctypes.POINTER(ctypes.c_void_p),     # out_ptr
    ctypes.POINTER(ctypes.c_size_t),     # out_len
]
_lib.qail_encode_get.restype = ctypes.c_int32

# qail_encode_batch_get
_lib.qail_encode_batch_get.argtypes = [
    ctypes.POINTER(ctypes.c_char_p),     # tables[]
    ctypes.POINTER(ctypes.c_char_p),     # columns[]
    ctypes.POINTER(ctypes.c_int64),      # limits[]
    ctypes.c_size_t,                     # count
    ctypes.POINTER(ctypes.c_void_p),     # out_ptr
    ctypes.POINTER(ctypes.c_size_t),     # out_len
]
_lib.qail_encode_batch_get.restype = ctypes.c_int32

# qail_free_bytes
_lib.qail_free_bytes.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
_lib.qail_free_bytes.restype = None

# qail_last_error
_lib.qail_last_error.argtypes = []
_lib.qail_last_error.restype = ctypes.c_char_p


def _check_error(rc: int, func_name: str):
    """Check return code and raise exception on error."""
    if rc != 0:
        err = _lib.qail_last_error()
        msg = err.decode('utf-8') if err else f"Unknown error (code {rc})"
        raise RuntimeError(f"{func_name} failed: {msg}")


def encode_get(table: str, columns: list[str] | None = None, limit: int = -1) -> bytes:
    """
    Encode a GET query to PostgreSQL wire protocol bytes.
    
    Args:
        table: Table name
        columns: List of columns (None or ["*"] for all)
        limit: Row limit (-1 for no limit)
    
    Returns:
        Wire protocol bytes ready to send to PostgreSQL
    """
    ptr = ctypes.c_void_p()
    length = ctypes.c_size_t()
    
    cols_str = "*" if not columns or columns == ["*"] else ",".join(columns)
    
    rc = _lib.qail_encode_get(
        table.encode('utf-8'),
        cols_str.encode('utf-8'),
        limit,
        ctypes.byref(ptr),
        ctypes.byref(length),
    )
    _check_error(rc, "qail_encode_get")
    
    # Copy bytes and free Rust memory
    result = ctypes.string_at(ptr.value, length.value)
    _lib.qail_free_bytes(ptr, length)
    
    return result


# qail_encode_uniform_batch (HIGH PERFORMANCE)
_lib.qail_encode_uniform_batch.argtypes = [
    ctypes.c_char_p,                     # table
    ctypes.c_char_p,                     # columns
    ctypes.c_int64,                      # limit
    ctypes.c_size_t,                     # count
    ctypes.POINTER(ctypes.c_void_p),     # out_ptr
    ctypes.POINTER(ctypes.c_size_t),     # out_len
]
_lib.qail_encode_uniform_batch.restype = ctypes.c_int32


def encode_uniform_batch(
    table: str, 
    columns: list[str] | None = None, 
    limit: int = -1,
    count: int = 10000
) -> bytes:
    """
    Encode a UNIFORM batch of identical GET queries.
    
    HIGH PERFORMANCE: Call ONCE, reuse bytes for every iteration.
    No FFI overhead in hot loop.
    
    Args:
        table: Table name
        columns: Columns (None for all)
        limit: Row limit (-1 for no limit)
        count: Number of queries in batch
    
    Returns:
        Pre-encoded wire bytes for the entire batch
    """
    ptr = ctypes.c_void_p()
    length = ctypes.c_size_t()
    
    cols_str = "*" if not columns or columns == ["*"] else ",".join(columns)
    
    rc = _lib.qail_encode_uniform_batch(
        table.encode('utf-8'),
        cols_str.encode('utf-8'),
        limit,
        count,
        ctypes.byref(ptr),
        ctypes.byref(length),
    )
    _check_error(rc, "qail_encode_uniform_batch")
    
    # Copy bytes and free Rust memory
    result = ctypes.string_at(ptr.value, length.value)
    _lib.qail_free_bytes(ptr, length)
    
    return result


def encode_batch_get(
    queries: list[tuple[str, list[str] | None, int]]
) -> bytes:
    """
    Encode multiple GET queries for pipeline execution.
    
    Args:
        queries: List of (table, columns, limit) tuples
    
    Returns:
        Wire protocol bytes for all queries
    """
    count = len(queries)
    if count == 0:
        return b""
    
    # Build arrays
    tables_arr = (ctypes.c_char_p * count)()
    cols_arr = (ctypes.c_char_p * count)()
    limits_arr = (ctypes.c_int64 * count)()
    
    for i, (table, columns, limit) in enumerate(queries):
        tables_arr[i] = table.encode('utf-8')
        cols_str = "*" if not columns or columns == ["*"] else ",".join(columns)
        cols_arr[i] = cols_str.encode('utf-8')
        limits_arr[i] = limit
    
    ptr = ctypes.c_void_p()
    length = ctypes.c_size_t()
    
    rc = _lib.qail_encode_batch_get(
        tables_arr,
        cols_arr,
        limits_arr,
        count,
        ctypes.byref(ptr),
        ctypes.byref(length),
    )
    _check_error(rc, "qail_encode_batch_get")
    
    # Copy bytes and free Rust memory
    result = ctypes.string_at(ptr.value, length.value)
    _lib.qail_free_bytes(ptr, length)
    
    return result
