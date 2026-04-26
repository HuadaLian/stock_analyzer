"""
测试目标：compute_dcf_lines 正确执行 3 年滚动均值的 DCF 估值逻辑，
并在 3 年均值 ≤ 0 时回退到 5 年窗口。

业务背景：DCF 叠加线 = N × (近 3 年 fcf_per_share 均值)，N ∈ {14, 24, 34}。
亏损公司 (近 3 年累计 FCF 为负) 用 5 年窗口能稀释偶发亏损；若 5 年窗口仍更糟
则保留 3 年值（保守）。
"""

import pytest
from etl.compute import compute_dcf_lines


def _seed_fcf(conn, ticker: str, year_to_fcf: dict[int, float]) -> None:
    """往 fundamentals_annual 插入指定年份的 fcf_per_share 数据。"""
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

    for year, fcf_ps in year_to_fcf.items():
        values = [
            ticker, year, f"{year}-12-31", f"{year+1}-03-01",
            "USD", fcf_ps * 1000, fcf_ps, 1000.0, "fmp",
        ] + [None] * len(base)
        conn.execute(sql, values)


def test_compute_dcf_uses_3yr_rolling_avg_for_positive_fcf(in_memory_db):
    """3 年均值为正时，dcf_metrics = 14/24/34 × 近 3 年 fcf_ps 均值。"""
    _seed_fcf(in_memory_db, "NVDA", {2022: 1.0, 2023: 2.0, 2024: 3.0})

    result = compute_dcf_lines("NVDA", in_memory_db)

    # 近 3 年均值 = (1+2+3)/3 = 2.0
    assert result["fcf_per_share_avg3yr"] == pytest.approx(2.0)

    metrics = in_memory_db.execute("""
        SELECT fcf_per_share_avg3yr, dcf_14x, dcf_24x, dcf_34x
        FROM dcf_metrics WHERE ticker='NVDA'
    """).fetchone()
    avg3, d14, d24, d34 = metrics
    assert avg3 == pytest.approx(2.0)
    assert d14  == pytest.approx(28.0)
    assert d24  == pytest.approx(48.0)
    assert d34  == pytest.approx(68.0)


def test_compute_dcf_falls_back_to_5yr_window_when_3yr_avg_negative(in_memory_db):
    """
    构造场景：早期盈利、近期亏损。
    近 3 年 (2022~2024) 均值 = (-5 + -10 + -20)/3 = -11.67
    近 5 年 (2020~2024) 均值 = (10 + 10 - 5 - 10 - 20)/5 = -3.0
    -3.0 > -11.67，应回退到 5 年窗口。
    """
    _seed_fcf(in_memory_db, "LOSER", {
        2020: 10.0, 2021: 10.0,
        2022: -5.0, 2023: -10.0, 2024: -20.0,
    })

    result = compute_dcf_lines("LOSER", in_memory_db)
    assert result["fcf_per_share_avg3yr"] == pytest.approx(-3.0)


def test_compute_dcf_returns_empty_for_unknown_ticker(in_memory_db):
    """没有 FCF 数据时返回空 dict，不应抛错也不应写入 dcf_metrics。"""
    result = compute_dcf_lines("NOTEXIST", in_memory_db)
    assert result == {}

    count = in_memory_db.execute(
        "SELECT COUNT(*) FROM dcf_metrics WHERE ticker='NOTEXIST'"
    ).fetchone()[0]
    assert count == 0


def test_compute_dcf_appends_today_to_extend_step_line_to_chart_edge(in_memory_db):
    """
    DCF 叠加线是阶梯图，最后一段必须延伸到“今天”才能画到 K 线最右侧。
    所以 dcf_df 的行数 = 历年 FCF 行数 + 1 (额外的 today 行)。
    """
    _seed_fcf(in_memory_db, "NVDA", {2022: 1.0, 2023: 2.0, 2024: 3.0})
    result = compute_dcf_lines("NVDA", in_memory_db)

    df = result["dcf_df"]
    assert len(df) == 4   # 3 个财年 + 1 个 today

    # 最后两行的 dcf 值应相等（阶梯延伸，不向上跳）
    last = df.iloc[-1]
    prev = df.iloc[-2]
    assert last["dcf_14x"] == prev["dcf_14x"]
    assert last["dcf_24x"] == prev["dcf_24x"]
    assert last["dcf_34x"] == prev["dcf_34x"]
