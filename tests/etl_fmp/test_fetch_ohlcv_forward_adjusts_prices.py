"""
测试目标：fetch_ohlcv 把 FMP 的原始 OHL 价格按 (adj_close / close) 比例缩放，
使 open/high/low 与 adj_close 处于同一前复权基准。

关键不变量：
- DCF 叠加线用前复权 adj_close 画在 K 线图上；如果 OHL 不缩放，分红/拆股发生时
  K 线柱与 adj_close 会错位，导致 DCF 价位线的对照失真。
- market_cap = adj_close × shares_out_raw / 1_000_000 (百万)，市值始终基于前复权价。
- 空数据应明确抛 ValueError，避免 ETL 静默写入 0 行。
"""

import pytest
from etl.sources.fmp import fetch_ohlcv


def test_fetch_ohlcv_scales_open_high_low_by_adjustment_ratio(mock_fmp):
    # close=200, adjClose=100 → 历史价相对今日缩半（一次 2:1 拆股的典型样子）
    mock_fmp.set("historical-price-eod/full", [{
        "date":     "2020-01-02",
        "open":     210.0,
        "high":     220.0,
        "low":      205.0,
        "close":    200.0,
        "adjClose": 100.0,
        "volume":   1_000_000,
    }])

    rows = fetch_ohlcv("NVDA", shares_out_raw=24_500_000_000)
    row = rows[0]

    # ratio = 100/200 = 0.5
    assert row["open"]  == pytest.approx(105.0)
    assert row["high"]  == pytest.approx(110.0)
    assert row["low"]   == pytest.approx(102.5)
    # close 字段保留原始未复权值 (供需要查看“当时实际成交价”的场景)
    assert row["close"] == pytest.approx(200.0)
    # adj_close 直接来自 FMP
    assert row["adj_close"] == pytest.approx(100.0)


def test_fetch_ohlcv_computes_market_cap_in_millions(mock_fmp):
    mock_fmp.set("historical-price-eod/full", [{
        "date":     "2024-01-02",
        "open":     500.0, "high": 502.0, "low": 498.0,
        "close":    500.0, "adjClose": 500.0,
        "volume":   1_000_000,
    }])

    rows = fetch_ohlcv("NVDA", shares_out_raw=24_500_000_000)

    # market_cap = 500 × 24_500_000_000 / 1_000_000 = 12_250_000 百万 = 12.25 万亿
    assert rows[0]["market_cap"] == pytest.approx(12_250_000.0)


def test_fetch_ohlcv_market_cap_is_none_when_shares_unknown(mock_fmp):
    """profile 拿不到股数时，市值列写 NULL 而不是 0，避免下游误用。"""
    mock_fmp.set("historical-price-eod/full", [{
        "date": "2024-01-02",
        "open": 500.0, "high": 502.0, "low": 498.0,
        "close": 500.0, "adjClose": 500.0, "volume": 1_000_000,
    }])

    rows = fetch_ohlcv("NVDA", shares_out_raw=None)
    assert rows[0]["market_cap"] is None


def test_fetch_ohlcv_raises_when_no_history_returned(mock_fmp):
    """FMP 返回空数组时必须报错，否则会写 0 行还显示成功。"""
    mock_fmp.set("historical-price-eod/full", [])
    with pytest.raises(ValueError, match="no price history"):
        fetch_ohlcv("FAKE", shares_out_raw=1_000_000)


def test_fetch_ohlcv_accepts_date_window_and_passes_to_source(mock_fmp):
    mock_fmp.set("historical-price-eod/full", [{
        "date": "2024-01-02",
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.0,
        "adjClose": 100.0,
        "volume": 1_000_000,
    }])

    rows = fetch_ohlcv(
        "NVDA",
        shares_out_raw=24_500_000_000,
        date_from="2010-01-01",
        date_to="2026-01-01",
    )

    assert len(rows) == 1
    endpoint, params = mock_fmp.calls[-1]
    assert endpoint == "historical-price-eod/full"
    assert params["from"] == "2010-01-01"
    assert params["to"] == "2026-01-01"
