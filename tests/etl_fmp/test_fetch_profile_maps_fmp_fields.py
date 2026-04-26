"""
测试目标：fetch_profile 把 FMP /profile 的响应正确映射到 companies 表所需字段。

关键不变量：
- shares_out 必须从原始股数 (单位：股) 转换为百万股 (companies.shares_out 约定单位)。
- 原始股数仍以 _shares_out_raw 透传给下游 (用于 OHLCV 市值与 FCF/股 计算)。
- 字段双名兼容：companyName / name、exchange / exchangeShortName、mktCap / marketCap、
  sharesOutstanding / outstandingShares —— FMP 不同端点字段名不一致，map 函数必须都接住。
- currency 缺省回退 'USD' 并大写。
"""

import pytest
from etl.sources.fmp import fetch_profile


def test_fetch_profile_converts_shares_to_millions(mock_fmp):
    mock_fmp.set("profile", [{
        "companyName": "NVIDIA Corporation",
        "exchange": "NASDAQ",
        "exchangeFullName": "NASDAQ Global Select",
        "country": "US",
        "sector": "Technology",
        "industry": "Semiconductors",
        "currency": "usd",
        "description": "GPU maker",
        "sharesOutstanding": 24_500_000_000,   # 245 亿股
        "mktCap": 3_000_000_000_000,
    }])

    profile = fetch_profile("nvda")

    # ticker 强制大写
    assert profile["ticker"] == "NVDA"
    # 245 亿股 → 24500 百万股
    assert profile["shares_out"] == 24_500.0
    # 原始股数透传
    assert profile["_shares_out_raw"] == 24_500_000_000.0
    # 市值透传
    assert profile["_market_cap"] == 3_000_000_000_000.0
    # currency 大写
    assert profile["currency"] == "USD"
    # market 固定为 US (Phase 2 仅美股)
    assert profile["market"] == "US"
    assert profile["country"] == "US"
    assert profile["exchange_full_name"] == "NASDAQ Global Select"
    assert profile.get("_is_etf") is False
    assert profile.get("_is_fund") is False


def test_fetch_profile_falls_back_to_alternate_field_names(mock_fmp):
    """FMP 不同端点用不同字段名；map 函数必须都识别。"""
    mock_fmp.set("profile", [{
        "name":              "Alt Corp",            # 不是 companyName
        "exchangeShortName": "NYSE",                # 不是 exchange
        "outstandingShares": 1_000_000_000,         # 不是 sharesOutstanding
        "marketCap":         500_000_000,           # 不是 mktCap
    }])

    profile = fetch_profile("ALT")

    assert profile["name"] == "Alt Corp"
    assert profile["exchange"] == "NYSE"
    assert profile["shares_out"] == 1_000.0
    assert profile["_market_cap"] == 500_000_000.0


def test_fetch_profile_defaults_currency_to_usd(mock_fmp):
    """profile 缺 currency 时默认 USD，避免 NULL 写入 companies.currency。"""
    mock_fmp.set("profile", [{"companyName": "X"}])

    profile = fetch_profile("X")
    assert profile["currency"] == "USD"


def test_fetch_profile_handles_missing_shares(mock_fmp):
    """缺股数时 shares_out 与 _shares_out_raw 应为 None，下游需能处理。"""
    mock_fmp.set("profile", [{"companyName": "Tiny Co"}])

    profile = fetch_profile("TINY")
    assert profile["shares_out"] is None
    assert profile["_shares_out_raw"] is None


def test_fetch_profile_derives_shares_from_market_cap_when_field_missing(mock_fmp):
    """
    /stable/profile 自 2025 起不再返回 sharesOutstanding；当 marketCap 与 price
    都在时必须用 marketCap/price 兜底，否则下游 fcf_per_share 全是 None。
    """
    mock_fmp.set("profile", [{
        "companyName": "NVIDIA Corporation",
        "marketCap":  5_062_002_467_464,
        "price":      207.0,
        # 注意：故意不给 sharesOutstanding / outstandingShares
    }])

    profile = fetch_profile("NVDA")
    expected_raw = 5_062_002_467_464 / 207.0
    assert profile["_shares_out_raw"] == pytest.approx(expected_raw)
    assert profile["shares_out"] == pytest.approx(expected_raw / 1_000_000)
