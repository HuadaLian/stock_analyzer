"""
Shared pytest fixtures.

in_memory_db  — blank in-memory DuckDB with full schema applied.
                Used by all tests that need table structure but no real data.
"""

import pytest
import duckdb
from db.schema import _DDL, _MIGRATIONS


@pytest.fixture
def in_memory_db():
    """Fresh in-memory DuckDB with schema + migrations applied. Isolated per test."""
    conn = duckdb.connect(":memory:")
    conn.execute(_DDL)
    for stmt in _MIGRATIONS:
        try:
            conn.execute(stmt)
        except duckdb.Error:
            pass
    yield conn
    conn.close()
