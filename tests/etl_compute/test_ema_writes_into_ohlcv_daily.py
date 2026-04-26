"""
测试目标：compute_ema(ticker, conn) 把 ema10/ema250 写回 ohlcv_daily 同一行，
依据该 ticker 的全部 adj_close 序列计算。

意义：图线启动时一次 SELECT 拿到 ohlcv 与 EMA，避免前端再算一次。
"""

import pandas as pd
import pytest

from etl.compute import compute_ema, compute_ema_series


def _seed_ohlcv(conn, ticker: str, prices: list[float]) -> None:
    base = pd.Timestamp("2024-01-02")
    for i, price in enumerate(prices):
        date = (base + pd.tseries.offsets.BDay(i)).date()
        conn.execute("""
            INSERT INTO ohlcv_daily
                (ticker, date, open, high, low, close, volume, adj_close, market_cap)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [ticker, date, price, price, price, price, 1_000_000, price, price * 1000])


def test_compute_ema_writes_one_row_per_date(in_memory_db):
    prices = [100.0, 102.0, 105.0, 103.0, 108.0, 110.0, 112.0]
    _seed_ohlcv(in_memory_db, "ABC", prices)

    n = compute_ema("ABC", in_memory_db)
    assert n == len(prices)

    rows = in_memory_db.execute("""
        SELECT ema10, ema250 FROM ohlcv_daily
        WHERE ticker='ABC' ORDER BY date ASC
    """).fetchall()
    expected_ema10 = compute_ema_series(pd.Series(prices), span=10).tolist()
    expected_ema250 = compute_ema_series(pd.Series(prices), span=250).tolist()

    for i, (ema10, ema250) in enumerate(rows):
        assert ema10 == pytest.approx(expected_ema10[i])
        assert ema250 == pytest.approx(expected_ema250[i])


def test_compute_ema_uses_adj_close_not_close(in_memory_db):
    """复权后图线必须用 adj_close —— 拆股日 close ≠ adj_close 时验证选对了列。"""
    in_memory_db.execute("""
        INSERT INTO ohlcv_daily (ticker, date, open, high, low, close,
                                 volume, adj_close, market_cap)
        VALUES
            ('SPLIT', '2024-01-02', 200, 200, 200, 200, 1000, 100, 100000),
            ('SPLIT', '2024-01-03', 200, 200, 200, 200, 1000, 102, 102000),
            ('SPLIT', '2024-01-04', 200, 200, 200, 200, 1000, 105, 105000)
    """)

    compute_ema("SPLIT", in_memory_db)
    row = in_memory_db.execute("""
        SELECT ema10 FROM ohlcv_daily
        WHERE ticker='SPLIT' ORDER BY date ASC LIMIT 1
    """).fetchone()
    # 第一行 EMA = 第一行 adj_close (=100)，不是 close (=200)
    assert row[0] == pytest.approx(100.0)
