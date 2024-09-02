"""QAIL - AST-Native PostgreSQL Driver for Python.

No SQL strings anywhere. Python builds the AST → Rust encodes AST
directly to PostgreSQL wire protocol bytes.

Two driver implementations:
- PgDriver: Tokio-based (from Rust) - backward compat
- AsyncPgDriver: Native Python asyncio (faster!) - from driver.py

Usage:
    import asyncio
    from qail import QailCmd, AsyncPgDriver, Operator

    async def main():
        driver = await AsyncPgDriver.connect("localhost", 5432, "user", "db", "pass")
        
        cmd = (QailCmd.get("users")
               .columns(["id", "name"])
               .filter("active", Operator.eq(), True)
               .limit(10))
        
        rows = await driver.fetch_all(cmd)
        for row in rows:
            print(row.to_dict())

    asyncio.run(main())
"""

# Native Rust classes
from .qail import QailCmd, PgDriver, Operator, Row

# Sync encoder functions (for native Python async driver)
from .qail import encode_cmd, encode_batch

# Pure Python async driver (faster - no Tokio bridge)
from .driver import PgDriver as AsyncPgDriver

__all__ = [
    "QailCmd", "Operator", "Row",
    "PgDriver",        # Tokio-based (backward compat)
    "AsyncPgDriver",   # Native asyncio (faster)
    "encode_cmd", "encode_batch",
]
__version__ = "0.9.6"
