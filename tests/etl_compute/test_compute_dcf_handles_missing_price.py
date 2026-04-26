"""
测试目标：FCF 数据已有、但 ohlcv_daily 还没抓的中间状态下，
compute_dcf_lines 仍能把 DCF 三档线写进 dcf_metrics，并把
latest_price / short_potential / invest_potential 留为 NULL，
不应抛错或拒绝写入。

意义：ETL 各阶段独立可重跑，预计算阶段不能因为 OHLCV 缺失就阻断。
"""

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


def test_compute_dcf_writes_metrics_with_null_potentials_when_no_ohlcv(in_memory_db):
    """无 OHLCV 数据时 DCF 三档线照常写入，潜力字段为 NULL。"""
    _seed_fcf(in_memory_db, "NOPRICE", {2022: 1.0, 2023: 2.0, 2024: 3.0})
    # 故意不写 ohlcv_daily

    result = compute_dcf_lines("NOPRICE", in_memory_db)
    assert result["fcf_per_share_avg3yr"] == pytest.approx(2.0)
    assert result["latest_price"]     is None
    assert result["short_potential"]  is None
    assert result["invest_potential"] is None

    row = in_memory_db.execute("""
        SELECT dcf_14x, dcf_24x, dcf_34x,
               latest_price, short_potential, invest_potential
        FROM dcf_metrics WHERE ticker='NOPRICE'
    """).fetchone()
    d14, d24, d34, price, short_pot, invest_pot = row
    assert d14 == pytest.approx(28.0)
    assert d24 == pytest.approx(48.0)
    assert d34 == pytest.approx(68.0)
    assert price      is None
    assert short_pot  is None
    assert invest_pot is None


def test_compute_dcf_potentials_are_null_when_dcf_lines_nonpositive(in_memory_db):
    """
    亏损股：5yr 兜底后 fcf_per_share_avg3yr 仍 ≤ 0 → dcf_14x/24x/34x 全 ≤ 0。
    估值线非正时不能作分母，潜力字段必须 NULL；不能写出虚假的 0 让 UI 误以为合理。
    """
    _seed_fcf(in_memory_db, "LOSE", {
        2020: -10.0, 2021: -10.0, 2022: -10.0, 2023: -10.0, 2024: -10.0,
    })
    in_memory_db.execute("""
        INSERT INTO ohlcv_daily
            (ticker, date, open, high, low, close, volume, adj_close, market_cap)
        VALUES ('LOSE', '2024-12-31', 5, 5, 5, 5, 1000, 5.0, 5000)
    """)

    compute_dcf_lines("LOSE", in_memory_db)
    row = in_memory_db.execute("""
        SELECT dcf_34x, latest_price, short_potential, invest_potential
        FROM dcf_metrics WHERE ticker='LOSE'
    """).fetchone()
    d34, price, short_pot, invest_pot = row
    assert d34 < 0
    assert price == pytest.approx(5.0)   # OHLCV 仍写入
    assert short_pot  is None            # dcf_34x ≤ 0 → None
    assert invest_pot is None            # dcf_24x ≤ 0 → None
