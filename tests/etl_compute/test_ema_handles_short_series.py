"""
测试目标：ohlcv_daily 行数极少 (新股, 一两行) 时 compute_ema 不能崩。

意义：新上市股票头几个交易日就要能画 EMA 线 —— 不能因序列短就拒绝写入。
"""

import pytest
from etl.compute import compute_ema


def _insert(conn, ticker, date, price):
    conn.execute("""
        INSERT INTO ohlcv_daily (ticker, date, open, high, low, close,
                                 volume, adj_close, market_cap)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [ticker, date, price, price, price, price, 1000, price, price * 1000])


def test_compute_ema_no_data_returns_zero(in_memory_db):
    """完全没有 OHLCV 行时返回 0，不抛错。"""
    n = compute_ema("EMPTY", in_memory_db)
    assert n == 0


def test_compute_ema_single_row_writes_ema_equal_to_price(in_memory_db):
    """只有 1 行时 ema10 = ema250 = adj_close。"""
    _insert(in_memory_db, "NEW", "2024-01-02", 50.0)
    compute_ema("NEW", in_memory_db)
    row = in_memory_db.execute("""
        SELECT ema10, ema250 FROM ohlcv_daily WHERE ticker='NEW'
    """).fetchone()
    assert row[0] == pytest.approx(50.0)
    assert row[1] == pytest.approx(50.0)


def test_compute_ema_skips_rows_without_adj_close(in_memory_db):
    """adj_close 是 NULL 的行 (例如停牌补 0) 不参与 EMA 计算，也不会被写。"""
    in_memory_db.execute("""
        INSERT INTO ohlcv_daily (ticker, date, open, high, low, close, volume, adj_close, market_cap)
        VALUES ('GAPPY', '2024-01-02', 1, 1, 1, 1, 1, NULL, NULL)
    """)
    _insert(in_memory_db, "GAPPY", "2024-01-03", 100.0)
    _insert(in_memory_db, "GAPPY", "2024-01-04", 102.0)

    compute_ema("GAPPY", in_memory_db)
    rows = in_memory_db.execute("""
        SELECT date, ema10 FROM ohlcv_daily
        WHERE ticker='GAPPY' ORDER BY date ASC
    """).fetchall()
    # NULL adj_close 那行的 ema10 仍是 NULL
    assert rows[0][1] is None
    # 后面两行有 EMA
    assert rows[1][1] == pytest.approx(100.0)
    assert rows[2][1] is not None
