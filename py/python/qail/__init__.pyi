"""Type stubs for QAIL - AST-Native PostgreSQL Driver."""

from typing import Any, Dict, List, Self

class Operator:
    """Comparison operators for filter conditions."""
    
    @staticmethod
    def eq() -> Operator:
        """Equal (=)"""
        ...
    
    @staticmethod
    def ne() -> Operator:
        """Not equal (!=)"""
        ...
    
    @staticmethod
    def gt() -> Operator:
        """Greater than (>)"""
        ...
    
    @staticmethod
    def gte() -> Operator:
        """Greater than or equal (>=)"""
        ...
    
    @staticmethod
    def lt() -> Operator:
        """Less than (<)"""
        ...
    
    @staticmethod
    def lte() -> Operator:
        """Less than or equal (<=)"""
        ...
    
    @staticmethod
    def like() -> Operator:
        """LIKE pattern match"""
        ...
    
    @staticmethod
    def ilike() -> Operator:
        """Case-insensitive LIKE (ILIKE)"""
        ...
    
    @staticmethod
    def fuzzy() -> Operator:
        """Fuzzy match (~)"""
        ...
    
    @staticmethod
    def is_null() -> Operator:
        """IS NULL"""
        ...
    
    @staticmethod
    def is_not_null() -> Operator:
        """IS NOT NULL"""
        ...
    
    @staticmethod
    def in_() -> Operator:
        """IN array"""
        ...
    
    @staticmethod
    def not_in() -> Operator:
        """NOT IN array"""
        ...
    
    @staticmethod
    def between() -> Operator:
        """BETWEEN x AND y"""
        ...
    
    @staticmethod
    def not_between() -> Operator:
        """NOT BETWEEN x AND y"""
        ...
    
    @staticmethod
    def contains() -> Operator:
        """JSON/Array contains (@>)"""
        ...
    
    @staticmethod
    def key_exists() -> Operator:
        """JSON key exists (?)"""
        ...

class Qail:
    """AST builder for database queries.
    
    All methods return `Self` for fluent chaining.
    """
    
    # Static constructors
    @staticmethod
    def get(table: str) -> Qail:
        """Create a GET (SELECT) command."""
        ...
    
    @staticmethod
    def set(table: str) -> Qail:
        """Create a SET (UPDATE) command."""
        ...
    
    @staticmethod
    def del_(table: str) -> Qail:
        """Create a DEL (DELETE) command."""
        ...
    
    @staticmethod
    def add(table: str) -> Qail:
        """Create an ADD (INSERT) command."""
        ...
    
    @staticmethod
    def put(table: str) -> Qail:
        """Create a PUT (UPSERT) command."""
        ...
    
    # Column selection
    def select_all(self) -> Self:
        """Select all columns (*)."""
        ...
    
    def columns(self, cols: List[str]) -> Self:
        """Select specific columns."""
        ...
    
    def column(self, col: str) -> Self:
        """Add a single column."""
        ...
    
    # Filtering
    def filter(self, column: str, op: Operator, value: Any) -> Self:
        """Add a filter condition."""
        ...
    
    def eq(self, column: str, value: Any) -> Self:
        """Add an equality filter (shorthand)."""
        ...
    
    def or_filter(self, column: str, op: Operator, value: Any) -> Self:
        """Add an OR condition."""
        ...
    
    # Pagination & Ordering
    def limit(self, n: int) -> Self:
        """Set result limit."""
        ...
    
    def offset(self, n: int) -> Self:
        """Set result offset."""
        ...
    
    def order_by(self, column: str) -> Self:
        """Order by column ascending."""
        ...
    
    def order_by_desc(self, column: str) -> Self:
        """Order by column descending."""
        ...
    
    # Joins
    def join(self, table: str, left_col: str, right_col: str) -> Self:
        """Add an INNER JOIN."""
        ...
    
    def left_join(self, table: str, left_col: str, right_col: str) -> Self:
        """Add a LEFT JOIN."""
        ...
    
    # Grouping
    def group_by(self, cols: List[str]) -> Self:
        """Group by columns."""
        ...
    
    def having(self, column: str, op: Operator, value: Any) -> Self:
        """Add HAVING clause."""
        ...
    
    # Mutations
    def insert_columns(self, cols: List[str]) -> Self:
        """Set columns for INSERT."""
        ...
    
    def values(self, vals: List[Any]) -> Self:
        """Set values for INSERT."""
        ...
    
    def assign(self, column: str, value: Any) -> Self:
        """Set assignment for UPDATE."""
        ...
    
    # Returning
    def returning(self, cols: List[str]) -> Self:
        """Add RETURNING clause."""
        ...
    
    def returning_all(self) -> Self:
        """Return all columns."""
        ...
    
    def __repr__(self) -> str:
        ...

class Row:
    """A row from query results."""
    
    def get(self, index: int) -> Any:
        """Get column value by index."""
        ...
    
    def get_by_name(self, name: str) -> Any:
        """Get column value by name."""
        ...
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert row to Python dict."""
        ...
    
    def __getitem__(self, key: int | str) -> Any:
        """Support row[index] and row["column_name"] syntax."""
        ...
    
    def __len__(self) -> int:
        """Number of columns."""
        ...

class PgDriver:
    """AST-native PostgreSQL driver.
    
    Encodes Qail AST directly to PostgreSQL wire protocol bytes.
    No SQL strings are generated.
    """
    
    @staticmethod
    async def connect(
        host: str,
        port: int,
        user: str,
        database: str,
        password: str,
    ) -> PgDriver:
        """Connect with password authentication."""
        ...
    
    @staticmethod
    async def connect_trust(
        host: str,
        port: int,
        user: str,
        database: str,
    ) -> PgDriver:
        """Connect without password (trust mode)."""
        ...
    
    async def fetch_all(self, cmd: Qail) -> List[Row]:
        """Fetch all rows from a query."""
        ...
    
    async def fetch_one(self, cmd: Qail) -> Row:
        """Fetch one row from a query."""
        ...
    
    async def execute(self, cmd: Qail) -> int:
        """Execute a command (for mutations). Returns affected rows."""
        ...
    
    async def begin(self) -> PgDriver:
        """Begin a transaction."""
        ...
    
    async def commit(self) -> PgDriver:
        """Commit the current transaction."""
        ...
    
    async def rollback(self) -> PgDriver:
        """Rollback the current transaction."""
        ...
