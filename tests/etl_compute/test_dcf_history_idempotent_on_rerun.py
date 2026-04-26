"""
测试目标：compute_dcf_history 可重复执行且不会写重复行；
当 fundamentals_annual 变化时，dcf_history 对应年份应更新（而非新增重复）。
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


def test_dcf_history_idempotent_and_updates_existing_rows(in_memory_db):
    _seed_fcf(in_memory_db, "IDEM", {2022: 1.0, 2023: 2.0})

    n1 = compute_dcf_history("IDEM", in_memory_db)
    n2 = compute_dcf_history("IDEM", in_memory_db)
    assert n1 == 2
    assert n2 == 2

    count_before = in_memory_db.execute(
        "SELECT COUNT(*) FROM dcf_history WHERE ticker='IDEM'"
    ).fetchone()[0]
    assert count_before == 2

    avg_before = in_memory_db.execute(
        "SELECT fcf_ps_avg3yr FROM dcf_history WHERE ticker='IDEM' AND fiscal_year=2023"
    ).fetchone()[0]
    assert avg_before == pytest.approx(1.5)

    # 修改源表后重算：2023 年 avg 应从 mean(1,2)=1.5 更新到 mean(1,5)=3.0。
    in_memory_db.execute(
        """
        UPDATE fundamentals_annual
        SET fcf_per_share = 5.0, fcf = 5000.0
        WHERE ticker = 'IDEM' AND fiscal_year = 2023
        """
    )
    n3 = compute_dcf_history("IDEM", in_memory_db)
    assert n3 == 2

    count_after = in_memory_db.execute(
        "SELECT COUNT(*) FROM dcf_history WHERE ticker='IDEM'"
    ).fetchone()[0]
    assert count_after == 2

    row_after = in_memory_db.execute(
        """
        SELECT fcf_ps_avg3yr, dcf_24x
        FROM dcf_history
        WHERE ticker='IDEM' AND fiscal_year=2023
        """
    ).fetchone()
    assert row_after[0] == pytest.approx(3.0)
    assert row_after[1] == pytest.approx(72.0)
