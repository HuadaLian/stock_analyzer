"""
测试目标：当 3-year rolling avg fcf_per_share ≤ 0 但 5-year avg > 3-year avg 时
fallback 到 5 年窗口；如果 5 年也是负的就保留 3 年值（取较大者）。

意义：亏损年覆盖 3 年滚动窗时，如果再延长 2 年能得到正值，画在图上才有意义；
但如果延长后还是负值，强行切大窗口反而失真。
"""

import pytest
from etl.compute import compute_dcf_history


def _seed_fcf(conn, ticker, year_to_fcf_ps):
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


def test_dcf_history_falls_back_to_5yr_when_3yr_is_negative(in_memory_db):
    """
    年序列: 2018=10, 2019=10, 2020=-1, 2021=-2, 2022=-3
    2022 的 3yr avg = mean(-1,-2,-3) = -2 (≤0)
    2022 的 5yr avg = mean(10,10,-1,-2,-3) = 2.8
    所以 fallback 后 fcf_ps_avg3yr = 2.8
    """
    _seed_fcf(in_memory_db, "DIP", {2018: 10.0, 2019: 10.0, 2020: -1.0,
                                    2021: -2.0, 2022: -3.0})
    compute_dcf_history("DIP", in_memory_db)
    row = in_memory_db.execute("""
        SELECT fcf_ps_avg3yr, dcf_14x FROM dcf_history
        WHERE ticker='DIP' AND fiscal_year=2022
    """).fetchone()
    assert row[0] == pytest.approx(2.8)
    assert row[1] == pytest.approx(14 * 2.8)


def test_dcf_history_keeps_negative_when_5yr_also_negative(in_memory_db):
    """
    全亏损：2020=-10..2024=-10 → 3yr avg = -10, 5yr avg = -10
    fallback 保留较大者（这里相等），不应突然返回 None 或 0。
    """
    _seed_fcf(in_memory_db, "LOSE",
              {2020: -10.0, 2021: -10.0, 2022: -10.0, 2023: -10.0, 2024: -10.0})
    compute_dcf_history("LOSE", in_memory_db)
    row = in_memory_db.execute("""
        SELECT fcf_ps_avg3yr, dcf_34x FROM dcf_history
        WHERE ticker='LOSE' AND fiscal_year=2024
    """).fetchone()
    assert row[0] == pytest.approx(-10.0)
    assert row[1] == pytest.approx(-340.0)
