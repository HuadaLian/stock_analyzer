"""
测试目标：repository 的 7 个 D1 用到的查询函数支持 `*, conn=` 关键字参数，
传入时直接复用调用者的连接，不再走默认的 `_conn()`。

意义：D1 单次渲染会调 ~6 次 repository.get_*；如果每次都 `_conn()`，库变大后
DuckDB 的开连接元数据 I/O 累计明显。打开一次 readonly conn 串到底是核心优化。
本组测试确保：
- 传入 conn 后，`_conn()` 完全不被调用（重要：才能真正实现"单次渲染单连接"）。
- 传入 conn 与不传 conn 返回结果一致（行为等价）。
- get_company 在 conn 模式下也只用 1 个连接（之前内部要开 2 次）。
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

import db.repository as repo


@pytest.fixture
def db_with_one_ticker(in_memory_db):
    """Seed minimal data for NVDA across the tables D1 reads."""
    in_memory_db.execute(
        "INSERT INTO companies (ticker, market, name, sector, shares_out, currency) "
        "VALUES ('NVDA', 'US', 'NVIDIA', 'Technology', 24500.0, 'USD')"
    )
    in_memory_db.execute(
        "INSERT INTO ohlcv_daily (ticker, date, open, high, low, close, volume, adj_close, market_cap) "
        "VALUES ('NVDA', DATE '2024-01-02', 100, 110, 95, 105, 1000000, 105, 2_500_000)"
    )
    in_memory_db.execute(
        "INSERT INTO ohlcv_daily (ticker, date, open, high, low, close, volume, adj_close, market_cap) "
        "VALUES ('NVDA', DATE '2024-01-03', 105, 115, 100, 110, 1100000, 110, 2_700_000)"
    )
    in_memory_db.execute(
        "INSERT INTO fundamentals_annual (ticker, fiscal_year, fiscal_end_date, currency, fcf, fcf_per_share, shares_out) "
        "VALUES ('NVDA', 2023, DATE '2023-12-31', 'USD', 25000, 1.02, 24500)"
    )
    in_memory_db.execute(
        "INSERT INTO dcf_history (ticker, fiscal_year, anchor_date, fcf_ps_avg3yr, dcf_14x, dcf_24x, dcf_34x) "
        "VALUES ('NVDA', 2023, DATE '2024-02-15', 1.0, 14, 24, 34)"
    )
    in_memory_db.execute(
        "INSERT INTO fmp_dcf_history (ticker, date, dcf_value, stock_price) "
        "VALUES ('NVDA', DATE '2024-01-03', 120.5, 110.0)"
    )
    in_memory_db.execute(
        "INSERT INTO dcf_metrics (ticker, dcf_14x, dcf_24x, dcf_34x) VALUES ('NVDA', 14, 24, 34)"
    )
    return in_memory_db


@pytest.fixture
def no_default_conn():
    """Patch _conn so any accidental fallthrough loudly fails — proves shared
    conn is actually being used end-to-end."""
    with patch("db.repository._conn") as m:
        m.side_effect = AssertionError("default _conn should not be called when conn=... is provided")
        yield m


def test_get_ohlcv_uses_supplied_conn(db_with_one_ticker, no_default_conn):
    df = repo.get_ohlcv("NVDA", conn=db_with_one_ticker)
    assert len(df) == 2
    assert no_default_conn.call_count == 0


def test_get_fundamentals_uses_supplied_conn(db_with_one_ticker, no_default_conn):
    df = repo.get_fundamentals("NVDA", conn=db_with_one_ticker)
    assert len(df) == 1
    assert df.iloc[0]["fiscal_year"] == 2023


def test_get_dcf_history_uses_supplied_conn(db_with_one_ticker, no_default_conn):
    df = repo.get_dcf_history("NVDA", conn=db_with_one_ticker)
    assert len(df) == 1


def test_get_fmp_dcf_history_uses_supplied_conn(db_with_one_ticker, no_default_conn):
    df = repo.get_fmp_dcf_history("NVDA", conn=db_with_one_ticker)
    assert len(df) == 1
    assert float(df.iloc[0]["dcf_value"]) == pytest.approx(120.5)


def test_get_dcf_metrics_uses_supplied_conn(db_with_one_ticker, no_default_conn):
    result = repo.get_dcf_metrics("NVDA", conn=db_with_one_ticker)
    assert result is not None
    assert result["ticker"] == "NVDA"


def test_get_all_tickers_uses_supplied_conn(db_with_one_ticker, no_default_conn):
    df = repo.get_all_tickers(market="US", conn=db_with_one_ticker)
    assert "NVDA" in df["ticker"].astype(str).tolist()


def test_get_company_uses_supplied_conn_only_once(db_with_one_ticker, no_default_conn):
    """以前内部开 2 个连接（一次查 companies，一次查 ohlcv_daily），现在必须共用同一个 conn。"""
    company = repo.get_company("NVDA", conn=db_with_one_ticker)
    assert company is not None
    assert company["name"] == "NVIDIA"
    # latest market_cap 来自 2024-01-03 那行
    assert company["market_cap"] == pytest.approx(2_700_000.0)
    # 关键：不能触发 _conn() 兜底
    assert no_default_conn.call_count == 0


def test_full_d1_render_path_uses_one_conn(db_with_one_ticker, no_default_conn):
    """模拟 render_d1_us 的整条数据获取路径：所有 7 个查询共享同一个 conn，
    `_conn()` 兜底永不触发。"""
    conn = db_with_one_ticker

    df_ohlcv = repo.get_ohlcv("NVDA", conn=conn)
    df_fund = repo.get_fundamentals("NVDA", conn=conn)
    df_dcf_hist = repo.get_dcf_history("NVDA", conn=conn)
    df_fmp_dcf = repo.get_fmp_dcf_history("NVDA", conn=conn)
    company = repo.get_company("NVDA", conn=conn)
    dcf_metrics = repo.get_dcf_metrics("NVDA", conn=conn)
    tickers = repo.get_all_tickers(market="US", conn=conn)

    assert len(df_ohlcv) == 2
    assert len(df_fund) == 1
    assert len(df_dcf_hist) == 1
    assert len(df_fmp_dcf) == 1
    assert company is not None
    assert dcf_metrics is not None
    assert "NVDA" in tickers["ticker"].tolist()
    # 全程没碰默认连接路径 → 验证"单次渲染单连接"成立
    assert no_default_conn.call_count == 0


def test_get_ohlcv_without_conn_still_works_via_default(in_memory_db):
    """未传 conn 时，_conn() 兜底必须仍然工作（保持向后兼容）。"""
    in_memory_db.execute(
        "INSERT INTO ohlcv_daily (ticker, date, open, high, low, close, volume, adj_close) "
        "VALUES ('AAA', DATE '2024-06-01', 1, 2, 0.5, 1.5, 100, 1.5)"
    )
    with patch("db.repository._conn") as mock:
        mock.return_value.__enter__ = lambda s: in_memory_db
        mock.return_value.__exit__ = lambda s, *a: None
        df = repo.get_ohlcv("AAA")
    assert len(df) == 1
