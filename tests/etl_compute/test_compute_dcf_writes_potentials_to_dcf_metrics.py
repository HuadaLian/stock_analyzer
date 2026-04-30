"""
测试目标：compute_dcf_lines 端到端 — 把历年 FCF 与最新 OHLCV 联起来，
正确写入 dcf_metrics 全部字段 (含 latest_price / short_potential / invest_potential)。

意义：UI 直接 SELECT 这张表来画做空/投资潜力柱状图，所以本测试守住
预计算层与展示层之间的契约。
"""

import pytest
from etl.compute import compute_dcf_lines


def _seed_fcf(conn, ticker: str, year_to_fcf_ps: dict[int, float]) -> None:
    """往 fundamentals_annual 写入指定年份的 fcf_per_share。"""
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


def _seed_ohlcv(conn, ticker: str, date: str, adj_close: float) -> None:
    conn.execute("""
        INSERT INTO ohlcv_daily
            (ticker, date, open, high, low, close, volume, adj_close, market_cap)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [ticker, date, adj_close, adj_close, adj_close, adj_close,
          1_000_000, adj_close, adj_close * 1000.0])


def test_compute_dcf_writes_potentials_with_overpriced_stock(in_memory_db):
    """
    构造：3 年 fcf_ps 均值=2 → 14x=28 / 24x=48 / 34x=68
    最新价格=100 (高于 34x)
    short_potential = (100-68)/68 ≈ 0.4706
    invest_potential = (28-100)/28 ≈ -2.5714
    """
    _seed_fcf(in_memory_db, "RICH", {2022: 1.0, 2023: 2.0, 2024: 3.0})
    _seed_ohlcv(in_memory_db, "RICH", "2024-12-31", 100.0)

    result = compute_dcf_lines("RICH", in_memory_db)

    # 返回值携带最新价与潜力，UI 不需要再查表
    assert result["latest_price"] == pytest.approx(100.0)
    assert result["short_potential"]  == pytest.approx((100 - 68) / 68)
    assert result["invest_potential"] == pytest.approx((28 - 100) / 28)

    # 数据库一行已写入
    row = in_memory_db.execute("""
        SELECT fcf_per_share_avg3yr, dcf_14x, dcf_24x, dcf_34x,
               latest_price, latest_price_date, short_potential, invest_potential
        FROM dcf_metrics WHERE ticker='RICH'
    """).fetchone()
    avg3, d14, d24, d34, price, price_date, short_pot, invest_pot = row

    assert avg3 == pytest.approx(2.0)
    assert d14  == pytest.approx(28.0)
    assert d24  == pytest.approx(48.0)
    assert d34  == pytest.approx(68.0)
    assert price == pytest.approx(100.0)
    assert str(price_date) == "2024-12-31"
    assert short_pot  == pytest.approx((100 - 68) / 68)
    assert invest_pot == pytest.approx((28 - 100) / 28)


def test_compute_dcf_writes_potentials_with_underpriced_stock(in_memory_db):
    """
    最新价格=10 (远低于 14x=28)
    short_potential 应被 max(0,..) 夹到 0
    invest_potential = (28-10)/28 ≈ 0.6429
    """
    _seed_fcf(in_memory_db, "CHEAP", {2022: 1.0, 2023: 2.0, 2024: 3.0})
    _seed_ohlcv(in_memory_db, "CHEAP", "2024-12-31", 10.0)

    compute_dcf_lines("CHEAP", in_memory_db)
    row = in_memory_db.execute("""
        SELECT short_potential, invest_potential
        FROM dcf_metrics WHERE ticker='CHEAP'
    """).fetchone()
    short_pot, invest_pot = row
    assert short_pot == 0.0
    assert invest_pot == pytest.approx((28 - 10) / 28)


def test_compute_dcf_uses_latest_date_when_multiple_ohlcv_rows(in_memory_db):
    """OHLCV 有多行时必须取最大 date 那一行的 adj_close，而不是任意行。"""
    _seed_fcf(in_memory_db, "X", {2022: 1.0, 2023: 2.0, 2024: 3.0})
    _seed_ohlcv(in_memory_db, "X", "2024-01-02", 50.0)
    _seed_ohlcv(in_memory_db, "X", "2024-12-31", 200.0)   # 最新
    _seed_ohlcv(in_memory_db, "X", "2024-06-15", 80.0)

    compute_dcf_lines("X", in_memory_db)
    row = in_memory_db.execute("""
        SELECT latest_price, latest_price_date FROM dcf_metrics WHERE ticker='X'
    """).fetchone()
    price, price_date = row
    assert price == pytest.approx(200.0)
    assert str(price_date) == "2024-12-31"
