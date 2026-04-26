"""
测试目标：anchor_date 应对齐到 filing_date 当天或之前最近交易日；
若 filing_date 之前没有任何 ohlcv_daily，则回退为 filing_date 本身。
"""

from etl.compute import compute_dcf_history


def _seed_fcf(conn, ticker: str, fiscal_year: int, filing_date: str, fcf_ps: float) -> None:
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
    conn.execute(sql, [
        ticker,
        fiscal_year,
        f"{fiscal_year}-12-31",
        filing_date,
        "USD",
        fcf_ps * 1000,
        fcf_ps,
        1000.0,
        "fmp",
    ] + [None] * len(base))


def _seed_ohlcv(conn, ticker: str, dates: list[str]) -> None:
    for d in dates:
        conn.execute(
            """
            INSERT INTO ohlcv_daily (ticker, date, adj_close)
            VALUES (?, ?, ?)
            """,
            [ticker, d, 100.0],
        )


def test_anchor_date_snaps_to_latest_trading_day_before_filing(in_memory_db):
    # 2025-03-08 是周六，最近交易日应为 2025-03-07。
    _seed_fcf(in_memory_db, "SNAP", fiscal_year=2024, filing_date="2025-03-08", fcf_ps=2.0)
    _seed_ohlcv(in_memory_db, "SNAP", ["2025-03-06", "2025-03-07", "2025-03-10"])

    compute_dcf_history("SNAP", in_memory_db)

    row = in_memory_db.execute(
        """
        SELECT anchor_date
        FROM dcf_history
        WHERE ticker='SNAP' AND fiscal_year=2024
        """
    ).fetchone()
    assert str(row[0]) == "2025-03-07"


def test_anchor_date_falls_back_to_filing_date_when_no_prior_ohlcv(in_memory_db):
    _seed_fcf(in_memory_db, "NOHIST", fiscal_year=2024, filing_date="2025-03-08", fcf_ps=2.0)
    _seed_ohlcv(in_memory_db, "NOHIST", ["2025-03-10", "2025-03-11"])  # 全部晚于 filing_date

    compute_dcf_history("NOHIST", in_memory_db)

    row = in_memory_db.execute(
        """
        SELECT anchor_date
        FROM dcf_history
        WHERE ticker='NOHIST' AND fiscal_year=2024
        """
    ).fetchone()
    assert str(row[0]) == "2025-03-08"
