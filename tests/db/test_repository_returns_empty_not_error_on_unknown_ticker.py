"""
测试目标：repository 查询一个不存在的 ticker 时，返回空结果而不是抛出异常。

意义：UI 层直接调用 repository 函数，如果函数在无数据时崩溃，页面会白屏。
     空结果由 UI 优雅处理（显示"暂无数据"），异常则不可接受。
覆盖 Phase 1 开放的三个查询函数：get_ohlcv、get_fundamentals、get_dcf_metrics。
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import duckdb
from unittest.mock import patch
from db.schema import _DDL
import db.repository as repo


@pytest.fixture
def patched_repo(in_memory_db):
    """Patch repository's _conn() to use in-memory DB instead of stock.db."""
    with patch("db.repository._conn") as mock:
        mock.return_value.__enter__ = lambda s: in_memory_db
        mock.return_value.__exit__ = lambda s, *a: None
        yield


def test_get_ohlcv_unknown_ticker_returns_empty_dataframe(in_memory_db, patched_repo):
    df = repo.get_ohlcv("XXXX")
    assert df is not None
    assert len(df) == 0


def test_get_fundamentals_unknown_ticker_returns_empty_dataframe(in_memory_db, patched_repo):
    df = repo.get_fundamentals("XXXX")
    assert df is not None
    assert len(df) == 0


def test_get_dcf_metrics_unknown_ticker_returns_none(in_memory_db, patched_repo):
    result = repo.get_dcf_metrics("XXXX")
    assert result is None
