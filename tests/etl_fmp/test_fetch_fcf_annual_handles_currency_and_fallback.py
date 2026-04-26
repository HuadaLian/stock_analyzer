"""
测试目标：fetch_fcf_annual 正确处理 FCF 的多种边界场景，并在
reportedCurrency != USD 时自动调用汇率端点把 FCF 折算到美元。

关键不变量：
- 输出 currency 始终为 'USD'；reporting_currency 保留原始币种 (例如 'CNY')。
- fcf / fcf_per_share 都已是美元。fx_to_usd 写入“当年 1 报告币 = N USD”的实际倍率。
- FCF 优先用 freeCashFlow；缺失时回退 operatingCashFlow - |capitalExpenditure|。
  capex 在 FMP 多数为负值，用 abs() 保证语义一致。
- fcf_per_share 必须用最新 shares_out_raw 重算 (与前复权 adj_close 同基准)，
  而不是当年 weighted-average shares —— 这是 DCF 价位线对齐 K 线的前提。
- 非 FCF 字段 (revenue, roic 等) 此阶段全部为 None，留给后续阶段填充。
"""

import pytest
from etl.sources.fmp import fetch_fcf_annual


def test_fetch_fcf_normalizes_currency_to_usd_for_chinese_adr(mock_fmp):
    """
    BABA 等中概股 reportedCurrency='CNY'。fetch_fcf_annual 应自动换汇：
    输出 currency='USD'、reporting_currency='CNY'、fx_to_usd=当年汇率，
    fcf 字段以美元百万存储。
    """
    mock_fmp.set("cash-flow-statement", [{
        "date": "2024-03-31", "fillingDate": "2024-06-25",
        "reportedCurrency": "cny",
        "freeCashFlow": 100_000_000_000,           # 1000 亿 CNY
        "weightedAverageShsOut": 2_400_000_000,
    }])
    # CNYUSD 直接报价：1 CNY = 0.14 USD
    mock_fmp.set(
        "historical-price-eod/full",
        [{"date": "2024-03-31", "close": 0.14}],
        symbol="CNYUSD",
    )

    rows = fetch_fcf_annual("BABA", shares_out_raw=2_400_000_000)
    row = rows[0]

    assert row["currency"] == "USD"
    assert row["reporting_currency"] == "CNY"
    assert row["fx_to_usd"] == pytest.approx(0.14)
    # 1000 亿 CNY × 0.14 = 140 亿 USD = 14000 百万
    assert row["fcf"] == pytest.approx(14_000.0)
    # per-share = 14000 百万 USD ÷ 24 亿股 = 14_000_000_000 / 2_400_000_000 = 5.833
    assert row["fcf_per_share"] == pytest.approx(14_000_000_000 / 2_400_000_000)


def test_fetch_fcf_falls_back_to_inverted_pair_when_direct_quote_missing(mock_fmp):
    """
    某些币种 FMP 只提供 USD{CCY} 行情 (例如 USDJPY)；直接 {CCY}USD 抓不到时
    应自动切换到 USDJPY 倒算：1 JPY = 1 / USDJPY。
    """
    mock_fmp.set("cash-flow-statement", [{
        "date": "2024-03-31",
        "reportedCurrency": "JPY",
        "freeCashFlow": 1_000_000_000_000,   # 1 万亿 JPY
        "weightedAverageShsOut": 1_000_000_000,
    }])
    # JPYUSD 端点空 → 触发回退
    mock_fmp.set("historical-price-eod/full", [], symbol="JPYUSD")
    # USDJPY = 150 → 1 JPY = 1/150 ≈ 0.006667 USD
    mock_fmp.set(
        "historical-price-eod/full",
        [{"date": "2024-03-31", "close": 150.0}],
        symbol="USDJPY",
    )

    rows = fetch_fcf_annual("SONY", shares_out_raw=1_000_000_000)
    row = rows[0]

    assert row["reporting_currency"] == "JPY"
    assert row["fx_to_usd"] == pytest.approx(1 / 150.0)
    # 1e12 JPY / 150 = 6.667e9 USD = 6666.67 百万
    assert row["fcf"] == pytest.approx(1_000_000_000_000 / 150.0 / 1_000_000)


def test_fetch_fcf_skips_fx_call_when_already_usd(mock_fmp):
    """USD 报告无需调用汇率端点；fx_to_usd 直接为 1.0。"""
    mock_fmp.set("cash-flow-statement", [{
        "date": "2024-01-26",
        "reportedCurrency": "USD",
        "freeCashFlow":          60_922_000_000,
        "weightedAverageShsOut": 24_600_000_000,
    }])
    # 不设置 historical-price-eod/full —— 若意外被调用，stub 会抛 AssertionError

    rows = fetch_fcf_annual("NVDA", shares_out_raw=24_500_000_000)
    row = rows[0]

    assert row["currency"] == "USD"
    assert row["reporting_currency"] == "USD"
    assert row["fx_to_usd"] == pytest.approx(1.0)
    assert row["fcf"] == pytest.approx(60_922.0)


def test_fetch_fcf_uses_freecashflow_when_present(mock_fmp):
    mock_fmp.set("cash-flow-statement", [{
        "date": "2024-01-26",
        "reportedCurrency": "USD",
        "freeCashFlow":          60_922_000_000,   # 直接给值
        "operatingCashFlow":     64_000_000_000,   # 应被忽略
        "capitalExpenditure":  -3_500_000_000,
        "weightedAverageShsOut": 24_600_000_000,
    }])

    rows = fetch_fcf_annual("NVDA", shares_out_raw=24_500_000_000)

    # fcf 单位转百万：60922
    assert rows[0]["fcf"] == pytest.approx(60_922.0)


def test_fetch_fcf_falls_back_to_ocf_minus_capex_when_freecashflow_missing(mock_fmp):
    """老年份数据有时缺 freeCashFlow，必须走回退公式。"""
    mock_fmp.set("cash-flow-statement", [{
        "date": "2010-01-31",
        "reportedCurrency": "USD",
        # 缺 freeCashFlow
        "operatingCashFlow":   3_000_000_000,
        "capitalExpenditure":  -800_000_000,    # FMP 习惯负号
        "weightedAverageShsOut": 600_000_000,
    }])

    rows = fetch_fcf_annual("NVDA", shares_out_raw=600_000_000)

    # 3_000_000_000 - |-800_000_000| = 2_200_000_000 → 2200 百万
    assert rows[0]["fcf"] == pytest.approx(2_200.0)


def test_fetch_fcf_per_share_uses_latest_shares_not_historical(mock_fmp):
    """
    业务约束：DCF 叠加线画在前复权 adj_close 上，所以历史每年的 fcf_per_share
    都必须除以「最新」总股本 (shares_out_raw)，而不是当年 weighted-average shares。
    否则发生过拆股的公司，历史 DCF 价位线会与 K 线错位。
    """
    mock_fmp.set("cash-flow-statement", [{
        "date": "2010-01-31",
        "reportedCurrency": "USD",
        "freeCashFlow": 1_000_000_000,
        "weightedAverageShsOut": 500_000_000,    # 历史股数 5 亿
    }])

    # 最新股本 50 亿 (假设期间发生过 1:10 拆股)
    rows = fetch_fcf_annual("NVDA", shares_out_raw=5_000_000_000)

    # fcf_per_share 应该用最新股本：1_000_000_000 / 5_000_000_000 = 0.2
    assert rows[0]["fcf_per_share"] == pytest.approx(0.2)
    # weighted-average shares (历史) 仍写入 shares_out 列 (单位百万)，仅做存档用
    assert rows[0]["shares_out"] == pytest.approx(500.0)


def test_fetch_fcf_leaves_non_fcf_columns_null(mock_fmp):
    """阶段 2 只写 FCF；revenue/roic 等列须为 None，避免覆盖后续阶段的填充。"""
    mock_fmp.set("cash-flow-statement", [{
        "date": "2024-01-26",
        "reportedCurrency": "USD",
        "freeCashFlow": 60_922_000_000,
        "weightedAverageShsOut": 24_600_000_000,
    }])

    rows = fetch_fcf_annual("NVDA", shares_out_raw=24_500_000_000)
    row = rows[0]

    # source 标记为 fmp，便于后续审计
    assert row["source"] == "fmp"
    # 非 FCF 字段应全为 None
    for col in ("revenue", "gross_profit", "net_income", "eps",
                "roic", "return_on_equity", "total_equity"):
        assert row[col] is None, f"{col} 应在阶段 2 留空"


def test_fetch_fcf_raises_on_empty_response(mock_fmp):
    mock_fmp.set("cash-flow-statement", [])
    with pytest.raises(ValueError, match="no cash flow data"):
        fetch_fcf_annual("FAKE", shares_out_raw=1_000_000)
