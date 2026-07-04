"""D1 主图：FMP DCF 为单条横线等结构断言。"""

import pandas as pd
import pytest

from dashboards.d1_fcf_multiple import _build_chart, _fundamentals_rows_for_dcf_chart


def _minimal_ohlcv(n=5):
    dates = pd.date_range("2024-01-02", periods=n, freq="B")
    return pd.DataFrame(
        {
            "date": dates,
            "open": [100.0] * n,
            "high": [102.0] * n,
            "low": [99.0] * n,
            "close": [101.0] * n,
            "volume": [1_000_000] * n,
            "adj_close": [101.0] * n,
            "ema10": [100.5] * n,
            "ema250": [95.0] * n,
        }
    )


def test_fundamentals_for_dcf_chart_excludes_flagged_keeps_scaled():
    df = pd.DataFrame(
        {
            "fiscal_year": [2023, 2024, 2025],
            "fcf_per_share": [1.0, 2.0, 3.0],
            "fcf_quality": ["ok", "flagged_magnitude", "scaled_1000"],
        }
    )
    t = _fundamentals_rows_for_dcf_chart(df)
    assert set(t["fiscal_year"].tolist()) == {2023, 2025}


def test_single_fmp_dcf_trace_is_horizontal_latest_value():
    df_ohlcv = _minimal_ohlcv()
    df_dcf_hist = pd.DataFrame()
    df_fmp = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
            "dcf_value": [50.0, 55.5],
            "stock_price": [100.0, 101.0],
        }
    )
    fig = _build_chart(df_ohlcv, df_dcf_hist, df_fmp, "TEST")
    fmp_traces = [t for t in fig.data if getattr(t, "name", None) == "FMP DCF"]
    assert len(fmp_traces) == 1
    y = list(fmp_traces[0].y)
    assert len(y) == 2
    assert y[0] == pytest.approx(55.5) and y[1] == pytest.approx(55.5)
    # Y 仅日 K：min(low)=99，max(high)×2=204（与 FMP/EMA 无关）
    yaxis = fig.layout.yaxis
    assert float(yaxis.range[0]) == pytest.approx(99.0)
    assert float(yaxis.range[1]) == pytest.approx(204.0)
