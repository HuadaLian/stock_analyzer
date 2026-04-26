"""
测试目标：确认 `get_all_tickers` 在 companies 为空时返回空 DataFrame，
*绝不* 退化为对 ohlcv_daily 做全表 DISTINCT 扫描。

意义：库随 bulk 变大后，老的 fallback 会让前端每次 rerun 都付一次「ohlcv 全表」
的代价（百万行级）。审计（db.us_data_audit.audit_orphan_ohlcv_tickers）确认
当前生产库孤儿数 = 0 → fallback 是死代码 → 删除并加这条测试钉住。

如果未来某次 bulk 把 companies 写空但 ohlcv_daily 还在，前端就该显示空列表
（让问题立刻可见），而不是悄悄退化到慢路径。
"""

from __future__ import annotations

from unittest.mock import patch

import db.repository as repo


def test_get_all_tickers_returns_empty_when_companies_empty_even_if_ohlcv_has_rows(in_memory_db):
    """关键不变量：companies 空时不再扫 ohlcv_daily。"""
    # 故意只塞 OHLCV，不塞 companies —— 这正是老 fallback 会触发的场景
    in_memory_db.execute(
        "INSERT INTO ohlcv_daily (ticker, date, open, high, low, close, volume, adj_close) "
        "VALUES ('ORPHAN', DATE '2024-06-01', 1, 2, 0.5, 1.5, 100, 1.5)"
    )
    df = repo.get_all_tickers(market="US", conn=in_memory_db)
    assert df.empty, "fallback 已删除：companies 空就该返回空，不能从 ohlcv 兜底"


def test_get_all_tickers_returns_empty_when_companies_empty_no_market_filter(in_memory_db):
    """无 market 过滤时也一样不能 fallback。"""
    in_memory_db.execute(
        "INSERT INTO ohlcv_daily (ticker, date, open, high, low, close, volume, adj_close) "
        "VALUES ('ORPHAN', DATE '2024-06-01', 1, 2, 0.5, 1.5, 100, 1.5)"
    )
    df = repo.get_all_tickers(conn=in_memory_db)
    assert df.empty


def test_get_all_tickers_does_not_query_ohlcv_when_companies_returns_rows(in_memory_db):
    """companies 有结果时直接返回，不应再触碰 ohlcv_daily —— 用 monkeypatch
    注入会爆炸的 ohlcv 视图来证明（如果代码里偷偷查 ohlcv 就会立刻报错）。"""
    in_memory_db.execute(
        "INSERT INTO companies (ticker, market, name, currency) "
        "VALUES ('NVDA', 'US', 'NVIDIA', 'USD')"
    )
    # 故意把 ohlcv_daily 查询替换为爆炸视图：任何 SELECT FROM ohlcv_daily 都会报错。
    in_memory_db.execute("DROP TABLE ohlcv_daily")
    in_memory_db.execute(
        "CREATE VIEW ohlcv_daily AS SELECT 1/0 AS boom"  # 触发 division by zero
    )
    df = repo.get_all_tickers(market="US", conn=in_memory_db)
    assert "NVDA" in df["ticker"].tolist()
