"""
测试目标：compute_ema_series(values, span=10) 必须严格等同于 pandas
`.ewm(span=10, adjust=False).mean()`。

意义：这是预计算 EMA 与图线对账的唯一基准。如果未来有人为了"性能"换成
其它平滑方式（SMA、Wilder's smoothing 等），必须先在这里失败提醒。
"""

import pandas as pd
import pytest

from etl.compute import compute_ema_series


def test_ema10_matches_pandas_ewm_span10():
    prices = pd.Series([100.0, 101.0, 102.5, 99.0, 105.0, 110.0,
                        108.0, 107.5, 112.0, 115.0, 113.0, 116.0])
    expected = prices.ewm(span=10, adjust=False).mean()
    actual = compute_ema_series(prices, span=10)
    pd.testing.assert_series_equal(actual, expected)


def test_ema_first_value_equals_first_price():
    """adjust=False 的 EMA 第一个值等于序列第一个观测，便于人工验证。"""
    prices = pd.Series([42.0, 50.0, 55.0])
    ema = compute_ema_series(prices, span=10)
    assert ema.iloc[0] == pytest.approx(42.0)
