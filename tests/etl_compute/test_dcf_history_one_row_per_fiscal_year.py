"""
测试目标：compute_dcf_history 把每个有 fcf_per_share 的财年都写入 dcf_history
（不只是最新年），dcf_Nx = N × fcf_ps_avg3yr。

意义：D1 图上的阶梯线需要历史每一档的位置，不能只存最新一行。
"""

import pytest
from etl.compute import compute_dcf_history


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


def test_dcf_history_writes_one_row_per_year(in_memory_db):
    _seed_fcf(in_memory_db, "MULTI", {2020: 1.0, 2021: 1.0, 2022: 2.0,
                                      2023: 3.0, 2024: 4.0})
    n = compute_dcf_history("MULTI", in_memory_db)
    assert n == 5

    rows = in_memory_db.execute("""
        SELECT fiscal_year, fcf_ps_avg3yr, dcf_14x, dcf_24x, dcf_34x
        FROM dcf_history WHERE ticker='MULTI'
        ORDER BY fiscal_year ASC
    """).fetchall()
    assert [r[0] for r in rows] == [2020, 2021, 2022, 2023, 2024]

    # 2024 年: avg = mean(2.0, 3.0, 4.0) = 3.0
    year_2024 = rows[-1]
    assert year_2024[1] == pytest.approx(3.0)
    assert year_2024[2] == pytest.approx(14 * 3.0)
    assert year_2024[3] == pytest.approx(24 * 3.0)
    assert year_2024[4] == pytest.approx(34 * 3.0)


def test_dcf_history_returns_zero_when_no_fcf(in_memory_db):
    n = compute_dcf_history("EMPTY", in_memory_db)
    assert n == 0
    rows = in_memory_db.execute("SELECT * FROM dcf_history WHERE ticker='EMPTY'").fetchall()
    assert rows == []
