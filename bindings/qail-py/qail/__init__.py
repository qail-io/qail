"""
QAIL Python Bindings

Provides a Pythonic interface to QAIL (Query Abstraction Interface Language).

Example:
    >>> from qail import transpile
    >>> sql = transpile("get::users:'_")
    >>> print(sql)  # SELECT * FROM "users"
"""

import os
import sys
from ctypes import cdll, c_char_p, c_int, POINTER, c_void_p
from typing import Optional

# Find the library
def _find_library():
    """Locate libqail_ffi based on platform."""
    if sys.platform == "darwin":
        lib_name = "libqail_ffi.dylib"
    elif sys.platform == "win32":
        lib_name = "qail_ffi.dll"
    else:
        lib_name = "libqail_ffi.so"
    
    # Search paths
    search_paths = [
        os.path.dirname(__file__),  # Same directory as this module
        os.path.join(os.path.dirname(__file__), ".."),
        "/usr/local/lib",
        "/usr/lib",
    ]
    
    for path in search_paths:
        lib_path = os.path.join(path, lib_name)
        if os.path.exists(lib_path):
            return lib_path
    
    # Try system path
    return lib_name

_lib = cdll.LoadLibrary(_find_library())

# Function signatures
_lib.qail_transpile.argtypes = [c_char_p]
_lib.qail_transpile.restype = c_void_p

_lib.qail_transpile_with_dialect.argtypes = [c_char_p, c_char_p]
_lib.qail_transpile_with_dialect.restype = c_void_p

_lib.qail_parse_json.argtypes = [c_char_p]
_lib.qail_parse_json.restype = c_void_p

_lib.qail_validate.argtypes = [c_char_p]
_lib.qail_validate.restype = c_int

_lib.qail_last_error.argtypes = []
_lib.qail_last_error.restype = c_char_p

_lib.qail_free.argtypes = [c_void_p]
_lib.qail_free.restype = None

_lib.qail_version.argtypes = []
_lib.qail_version.restype = c_void_p


class QailError(Exception):
    """QAIL parsing or transpilation error."""
    pass


def transpile(qail: str, dialect: str = "postgres") -> str:
    """
    Transpile QAIL query to SQL.
    
    Args:
        qail: QAIL query string (e.g., "get::users:'_")
        dialect: SQL dialect ("postgres", "mysql", "sqlite", "sqlserver")
    
    Returns:
        SQL query string
    
    Raises:
        QailError: If parsing fails
    """
    result = _lib.qail_transpile_with_dialect(
        qail.encode("utf-8"),
        dialect.encode("utf-8")
    )
    
    if not result:
        error = _lib.qail_last_error()
        raise QailError(error.decode("utf-8") if error else "Unknown error")
    
    sql = c_char_p(result).value.decode("utf-8")
    _lib.qail_free(result)
    return sql


def parse_json(qail: str) -> str:
    """
    Parse QAIL and return AST as JSON.
    
    Args:
        qail: QAIL query string
    
    Returns:
        JSON string representing the AST
    """
    result = _lib.qail_parse_json(qail.encode("utf-8"))
    
    if not result:
        error = _lib.qail_last_error()
        raise QailError(error.decode("utf-8") if error else "Unknown error")
    
    json_str = c_char_p(result).value.decode("utf-8")
    _lib.qail_free(result)
    return json_str


def validate(qail: str) -> bool:
    """
    Validate QAIL syntax.
    
    Args:
        qail: QAIL query string
    
    Returns:
        True if valid, False otherwise
    """
    return _lib.qail_validate(qail.encode("utf-8")) == 1


def version() -> str:
    """Get QAIL version."""
    result = _lib.qail_version()
    if result:
        v = c_char_p(result).value.decode("utf-8")
        _lib.qail_free(result)
        return v
    return "unknown"


# Convenience aliases
to_sql = transpile
to_postgres = lambda q: transpile(q, "postgres")
to_mysql = lambda q: transpile(q, "mysql")
to_sqlite = lambda q: transpile(q, "sqlite")


__all__ = [
    "transpile",
    "parse_json",
    "validate",
    "version",
    "to_sql",
    "to_postgres",
    "to_mysql",
    "to_sqlite",
    "QailError",
]
