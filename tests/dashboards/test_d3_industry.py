"""D3 行业分布：纯函数与 Plotly 结构（不启动 Streamlit）。"""

import pandas as pd
import pytest

import dashboards.d3_industry as d3


@pytest.mark.parametrize(
    "value,expected_sub",
    [
        (1e12, "$1.00T"),
        (5e9, "$5.00B"),
        (3.5e6, "$3.5M"),
        (500_000.0, "$500,000"),
        (None, "-"),
    ],
)
def test_format_revenue_usd(value, expected_sub):
    assert expected_sub in d3._format_revenue_usd(value) or d3._format_revenue_usd(value) == expected_sub


def _fx_stub():
    rates = {"USD": (1.0, True), "HKD": (0.128, True), "EUR": (1.10, True)}

    def inner(c: str):
        return rates.get(d3._normalize_currency_code(c), (1.0, False))

    return inner


def test_convert_revenue_uses_fund_currency_mcap_uses_listing(monkeypatch):
    monkeypatch.setattr(d3, "_cached_fx_to_usd", _fx_stub())
    peers = pd.DataFrame(
        {
            "ticker": ["A", "B"],
            "name": ["Co A", "Co B"],
            "revenue": [100.0, 1000.0],
            "currency": ["USD", "HKD"],
            "fund_currency": ["USD", "HKD"],
            "market_cap": [500.0, 200.0],
        }
    )
    out, warns = d3._convert_to_usd(peers)
    assert abs(float(out.loc[out["ticker"] == "A", "revenue_usd"].iloc[0]) - 100e6) < 1e-3
    assert abs(float(out.loc[out["ticker"] == "B", "revenue_usd"].iloc[0]) - 1000e6 * 0.128) < 1e-3
    assert abs(float(out.loc[out["ticker"] == "A", "market_cap_usd"].iloc[0]) - 500e6) < 1e-3
    assert abs(float(out.loc[out["ticker"] == "B", "market_cap_usd"].iloc[0]) - 200e6 * 0.128) < 1e-3
    assert warns == []


def test_convert_lists_mismatch_when_listing_differs_from_fund(monkeypatch):
    monkeypatch.setattr(d3, "_cached_fx_to_usd", _fx_stub())
    peers = pd.DataFrame(
        {
            "ticker": ["Z"],
            "name": ["Zed"],
            "revenue": [10.0],
            "currency": ["HKD"],
            "fund_currency": ["USD"],
            "market_cap": [20.0],
        }
    )
    _, warns = d3._convert_to_usd(peers)
    assert any("挂牌币 HKD" in w and "财报库币种 USD" in w for w in warns)


def test_convert_non_usd_fund_applies_fx(monkeypatch):
    monkeypatch.setattr(d3, "_cached_fx_to_usd", _fx_stub())
    peers = pd.DataFrame(
        {
            "ticker": ["C"],
            "name": ["Euro Co"],
            "revenue": [50.0],
            "currency": ["USD"],
            "fund_currency": ["EUR"],
            "market_cap": [100.0],
        }
    )
    out, _ = d3._convert_to_usd(peers)
    assert abs(float(out["revenue_usd"].iloc[0]) - 50e6 * 1.10) < 1e-3


def test_build_market_cap_revenue_scatter_log_axes_and_star(monkeypatch):
    monkeypatch.setattr(d3, "_cached_fx_to_usd", lambda c: (1.0, True))
    rows = []
    for i in range(10):
        rows.append(
            {
                "ticker": f"X{i}",
                "name": f"PeerCo{i} Long Display Name",
                "currency": "USD",
                "fund_currency": "USD",
                "fiscal_year": 2024,
                "revenue": float(120 - i * 10),
                "market_cap": float(800.0 + i * 50),
            }
        )
    rows.append(
        {
            "ticker": "X10",
            "name": "SmallCap Inc",
            "currency": "USD",
            "fund_currency": "USD",
            "fiscal_year": 2024,
            "revenue": 15.0,
            "market_cap": 50.0,
        }
    )
    rows.append(
        {
            "ticker": "NVDA",
            "name": "NVIDIA Corporation",
            "currency": "USD",
            "fund_currency": "USD",
            "fiscal_year": 2024,
            "revenue": 25.0,
            "market_cap": 200.0,
        }
    )
    peers, _ = d3._convert_to_usd(pd.DataFrame(rows))
    fig = d3._build_market_cap_revenue_scatter(peers, "NVDA")
    assert fig is not None
    assert fig.layout.xaxis.type == "log"
    assert fig.layout.yaxis.type == "log"
    star = [t for t in fig.data if getattr(t.marker, "symbol", None) == "star"]
    assert len(star) == 1
    assert list(star[0].text) == ["  NVDA"]
    labeled = [t for t in fig.data if t.mode == "markers+text" and t.text and getattr(t.marker, "symbol", None) != "star"]
    assert labeled and len(list(labeled[0].text)) == 10


def test_build_distribution_figure_ccdf_and_target_star(monkeypatch):
    monkeypatch.setattr(d3, "_cached_fx_to_usd", lambda c: (1.0, True))
    peers = pd.DataFrame(
        {
            "ticker": ["NVDA", "AMD", "INTC"],
            "name": ["NVIDIA", "AMD", "Intel"],
            "currency": ["USD", "USD", "USD"],
            "fund_currency": ["USD", "USD", "USD"],
            "fiscal_year": [2024, 2024, 2024],
            "revenue": [130.0, 25.0, 54.0],
        }
    )
    peers, _ = d3._convert_to_usd(peers)
    fig, summary = d3._build_distribution_figure(peers, "NVDA")

    assert summary["total"] == 3
    assert summary["rank"] == 1
    assert summary["target_revenue"] == pytest.approx(130e6, rel=1e-6)

    names = [t.name for t in fig.data]
    assert "同业累计分布" in names or any(
        getattr(t, "line", None) and getattr(t.line, "shape", None) == "hv" for t in fig.data
    )
    star_traces = [t for t in fig.data if t.mode and "markers" in t.mode and "text" in t.mode]
    assert star_traces, "目标 ticker 应为 markers+text（黄星+标签）"
    assert fig.layout.yaxis.type == "log"
    ticktext = list(fig.layout.yaxis.ticktext or [])
    assert "$1M" in ticktext and "$1B" in ticktext and "$1T" in ticktext


def test_build_distribution_figure_peer_hover_customdata(monkeypatch):
    monkeypatch.setattr(d3, "_cached_fx_to_usd", lambda c: (1.0, True))
    peers = pd.DataFrame(
        {
            "ticker": ["A", "B"],
            "name": ["Alpha", "Beta"],
            "currency": ["USD", "USD"],
            "fund_currency": ["USD", "USD"],
            "fiscal_year": [2023, 2023],
            "revenue": [10.0, 20.0],
        }
    )
    peers, _ = d3._convert_to_usd(peers)
    fig, _ = d3._build_distribution_figure(peers, "A")
    marker_traces = [
        t for t in fig.data
        if t.type == "scatter" and t.mode == "markers" and t.hovertemplate
    ]
    assert marker_traces, "同业散点应有 hovertemplate"
    ht = marker_traces[0].hovertemplate
    assert "customdata" in ht or "%{y" in ht


def test_peer_table_column_rename_smoke():
    peers = pd.DataFrame(
        {
            "ticker": ["NVDA"],
            "name": ["NVIDIA"],
            "currency": ["USD"],
            "fiscal_year": [2024],
            "revenue_usd": [1.3e11],
        }
    )
    df = peers.copy()
    df["revenue_fmt"] = df["revenue_usd"].apply(d3._format_revenue_usd)
    df["fiscal_year"] = df["fiscal_year"].astype("Int64")
    df = df.rename(
        columns={
            "ticker": "代码",
            "name": "公司名",
            "currency": "货币",
            "fiscal_year": "财年",
            "revenue_fmt": "最新总收入 (USD)",
        }
    )[["代码", "公司名", "货币", "财年", "最新总收入 (USD)"]]
    assert list(df.columns) == ["代码", "公司名", "货币", "财年", "最新总收入 (USD)"]
    assert df.iloc[0]["货币"] == "USD"
