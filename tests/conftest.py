"""
Shared pytest fixtures.

in_memory_db  — blank in-memory DuckDB with full schema applied.
                Used by all tests that need table structure but no real data.
"""

import pytest
import duckdb
from db.schema import _DDL


@pytest.fixture
def in_memory_db():
    """Fresh in-memory DuckDB with schema applied. Isolated per test."""
    conn = duckdb.connect(":memory:")
    conn.execute(_DDL)
    yield conn
    conn.close()
