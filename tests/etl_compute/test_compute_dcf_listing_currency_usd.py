"""非 USD 上市货币下，dcf_metrics.latest_price 应经市值比折成 USD 再算潜力。"""

from __future__ import annotations

import pytest
from etl.compute import compute_dcf_lines


def _seed_fcf(conn, ticker: str, year_to_fcf_ps: dict[int, float]) -> None:
    base = {col: None for col in [
        "revenue", "revenue_per_share", "gross_profit", "gross_margin",
        "operating_income", "operating_margin", "net_income", "profit_margin",
        "eps", "depreciation", "effective_tax_rate", "dividend_per_share",
        "total_equity", "long_term_debt", "working_capital",
        "book_value_per_share", "tangible_bv_per_share",
        "roic", "return_on_capital", "return_on_equity",
    ]}
    cols = ["ticker", "fiscal_year", "fiscal_end_date", "filing_date",
            "currency", "fcf", "fcf_per_share", "shares_out", "source"] + list(base.keys())
    placeholders = ", ".join(["?"] * len(cols))
    sql = f"INSERT INTO fundamentals_annual ({', '.join(cols)}) VALUES ({placeholders})"
    for year, fcf_ps in year_to_fcf_ps.items():
        conn.execute(sql, [ticker, year, f"{year}-12-31", f"{year+1}-03-01",
                           "USD", fcf_ps * 1000, fcf_ps, 1000.0, "fmp"]
                          + [None] * len(base))


def test_latest_price_usd_uses_market_cap_ratio_when_listing_not_usd(in_memory_db):
    """与 PEP.BA 类场景：adj_close 为上市地货币，用 market_cap_usd/market_cap 折 USD。"""
    in_memory_db.execute(
        "INSERT INTO companies (ticker, market, name, currency) VALUES ('LIS1', 'US', 'ListCo', 'ARS')"
    )
    _seed_fcf(in_memory_db, "LIS1", {2022: 1.0, 2023: 2.0, 2024: 3.0})
    in_memory_db.execute(
        """
        INSERT INTO ohlcv_daily
            (ticker, date, open, high, low, close, volume, adj_close, market_cap, market_cap_usd)
        VALUES ('LIS1', DATE '2024-12-31', 1, 1, 1, 1, 1000, 13070.0, 295182059.0, 211940.71860286748)
        """
    )
    compute_dcf_lines("LIS1", in_memory_db)
    row = in_memory_db.execute(
        "SELECT latest_price, short_potential FROM dcf_metrics WHERE ticker='LIS1'"
    ).fetchone()
    px_usd, short_p = row
    exp = 13070.0 * (211940.71860286748 / 295182059.0)
    assert px_usd == pytest.approx(exp, rel=1e-6)
    assert short_p is not None
    assert short_p == pytest.approx(max(0.0, (exp - 68.0) / 68.0))
