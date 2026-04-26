"""
测试目标：span=250 的 EMA 同样严格等同 pandas `.ewm`。

span=250 对应 ~1 年交易日，是图上长期趋势线。短序列 (< 250 行) 也应能产出
逐行 EMA，不能因为窗口未填满就返回 NaN（pandas 默认行为正是如此，但仍
显式锁定，防回归）。
"""

import pandas as pd
import numpy as np

from etl.compute import compute_ema_series


def test_ema250_matches_pandas_ewm_span250():
    rng = np.random.default_rng(seed=42)
    prices = pd.Series(100.0 + rng.standard_normal(800).cumsum())
    expected = prices.ewm(span=250, adjust=False).mean()
    actual = compute_ema_series(prices, span=250)
    pd.testing.assert_series_equal(actual, expected)


def test_ema250_returns_value_for_every_row_even_if_short():
    """20 行 < span — 仍每行有 EMA 值（adjust=False 不依赖窗口填满）。"""
    prices = pd.Series([100.0 + i for i in range(20)])
    ema = compute_ema_series(prices, span=250)
    assert ema.notna().all()
    assert len(ema) == 20
