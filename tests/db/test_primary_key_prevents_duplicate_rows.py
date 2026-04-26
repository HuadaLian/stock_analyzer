"""
测试目标：主键约束生效，重复写入同一主键时数据库报错而不是静默新增一行。

意义：主键是防止 ETL 重复运行导致数据膨胀的第一道防线。
验证五张核心表：companies / fundamentals_annual / ohlcv_daily /
dcf_history / fmp_dcf_history。
"""

import pytest


COMPANY_ROW = ("NVDA", "US", "NVIDIA Corporation", "NASDAQ",
               "Technology", "Semiconductors", "USD", "GPU maker", 24500.0, None)

FUNDAMENTAL_COLS = [
    "ticker", "fiscal_year", "fiscal_end_date", "filing_date",
    "currency", "reporting_currency", "fx_to_usd",
    "fcf", "fcf_per_share", "shares_out", "source",
]
FUNDAMENTAL_ROW = ("NVDA", 2024, "2025-01-26", "2025-03-05",
                   "USD", "USD", 1.0,
                   60922.0, 2.48, 24400.0, "fmp")

OHLCV_COLS = ["ticker", "date", "open", "high", "low", "close",
              "volume", "adj_close", "market_cap"]
OHLCV_ROW = ("NVDA", "2024-01-02", 495.0, 502.0, 490.0, 500.0,
             42000000, 500.0, 1220000.0)

DCF_HISTORY_COLS = ["ticker", "fiscal_year", "anchor_date",
                    "fcf_ps_avg3yr", "dcf_14x", "dcf_24x", "dcf_34x"]
DCF_HISTORY_ROW = ("NVDA", 2024, "2025-03-05", 2.0, 28.0, 48.0, 68.0)

FMP_DCF_COLS = ["ticker", "date", "dcf_value", "stock_price"]
FMP_DCF_ROW = ("NVDA", "2024-12-31", 80.0, 100.0)


def test_companies_rejects_duplicate_ticker(in_memory_db):
    sql = """INSERT INTO companies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    in_memory_db.execute(sql, COMPANY_ROW)
    with pytest.raises(Exception):
        in_memory_db.execute(sql, COMPANY_ROW)


def test_fundamentals_annual_rejects_duplicate_ticker_and_year(in_memory_db):
    placeholders = ", ".join(["?"] * len(FUNDAMENTAL_COLS))
    sql = (f"INSERT INTO fundamentals_annual ({', '.join(FUNDAMENTAL_COLS)}) "
           f"VALUES ({placeholders})")
    in_memory_db.execute(sql, FUNDAMENTAL_ROW)
    with pytest.raises(Exception):
        in_memory_db.execute(sql, FUNDAMENTAL_ROW)


def test_ohlcv_daily_rejects_duplicate_ticker_and_date(in_memory_db):
    placeholders = ", ".join(["?"] * len(OHLCV_COLS))
    sql = (f"INSERT INTO ohlcv_daily ({', '.join(OHLCV_COLS)}) "
           f"VALUES ({placeholders})")
    in_memory_db.execute(sql, OHLCV_ROW)
    with pytest.raises(Exception):
        in_memory_db.execute(sql, OHLCV_ROW)


def test_dcf_history_rejects_duplicate_ticker_and_year(in_memory_db):
    placeholders = ", ".join(["?"] * len(DCF_HISTORY_COLS))
    sql = (f"INSERT INTO dcf_history ({', '.join(DCF_HISTORY_COLS)}) "
           f"VALUES ({placeholders})")
    in_memory_db.execute(sql, DCF_HISTORY_ROW)
    with pytest.raises(Exception):
        in_memory_db.execute(sql, DCF_HISTORY_ROW)


def test_fmp_dcf_history_rejects_duplicate_ticker_and_date(in_memory_db):
    placeholders = ", ".join(["?"] * len(FMP_DCF_COLS))
    sql = (f"INSERT INTO fmp_dcf_history ({', '.join(FMP_DCF_COLS)}) "
           f"VALUES ({placeholders})")
    in_memory_db.execute(sql, FMP_DCF_ROW)
    with pytest.raises(Exception):
        in_memory_db.execute(sql, FMP_DCF_ROW)
