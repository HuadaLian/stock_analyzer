"""
测试目标：loader 的三个 upsert 函数支持安全重跑——同一主键写入两次只产生一行，
且第二次的字段值覆盖第一次。

意义：ETL 通常会被定时调度反复执行；幂等性是保证数据库不膨胀且能顺利“补抓昨日”
      的前提。本测试把 fetch_* 的产出结构直接喂给 loader，端到端验证契约一致。
"""

from etl.loader import (
    upsert_company,
    upsert_ohlcv_daily,
    upsert_fundamentals_annual,
)


# ---------------------------------------------------------------------------
# Helper：构造与 fetch_profile / fetch_ohlcv / fetch_fcf_annual 一致结构的样本行
# ---------------------------------------------------------------------------

def _company_row(name: str = "NVIDIA Corporation") -> dict:
    return {
        "ticker": "NVDA", "market": "US", "name": name,
        "exchange": "NASDAQ", "exchange_full_name": "NASDAQ Global Select",
        "country": "US", "sector": "Technology",
        "industry": "Semiconductors", "currency": "USD",
        "description": "GPU maker", "shares_out": 24_500.0,
    }


def _ohlcv_row(date: str = "2024-01-02", close: float = 500.0) -> dict:
    return {
        "ticker": "NVDA", "date": date,
        "open": 495.0, "high": 502.0, "low": 490.0,
        "close": close, "volume": 42_000_000,
        "adj_close": close, "market_cap": close * 24_500.0,
    }


def _fundamental_row(year: int = 2024, fcf: float = 60_922.0) -> dict:
    """与 _FCF_COLS 一致的 29 列。"""
    base = {col: None for col in [
        "revenue", "revenue_per_share", "gross_profit", "gross_margin",
        "operating_income", "operating_margin", "net_income", "profit_margin",
        "eps", "depreciation", "effective_tax_rate", "dividend_per_share",
        "total_equity", "long_term_debt", "working_capital",
        "book_value_per_share", "tangible_bv_per_share",
        "roic", "return_on_capital", "return_on_equity",
    ]}
    base.update({
        "ticker": "NVDA", "fiscal_year": year,
        "fiscal_end_date": f"{year}-01-26", "filing_date": f"{year}-03-05",
        "currency": "USD",
        "reporting_currency": "USD", "fx_to_usd": 1.0,
        "fcf": fcf, "fcf_per_share": fcf / 24_500.0,
        "shares_out": 24_400.0, "source": "fmp",
    })
    return base


# ---------------------------------------------------------------------------
# 测试
# ---------------------------------------------------------------------------

def test_upsert_company_overwrites_on_repeat(in_memory_db):
    upsert_company(in_memory_db, _company_row(name="Old Name"))
    upsert_company(in_memory_db, _company_row(name="New Name"))

    rows = in_memory_db.execute(
        "SELECT name FROM companies WHERE ticker='NVDA'"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "New Name"


def test_upsert_ohlcv_daily_is_idempotent_across_overlapping_batches(in_memory_db):
    """模拟两次抓取：第二次包含与第一次重叠的日期，且新值覆盖。"""
    upsert_ohlcv_daily(in_memory_db, [
        _ohlcv_row("2024-01-02", close=500.0),
        _ohlcv_row("2024-01-03", close=501.0),
    ])
    upsert_ohlcv_daily(in_memory_db, [
        _ohlcv_row("2024-01-03", close=999.9),   # 重叠日期，新值
        _ohlcv_row("2024-01-04", close=502.0),   # 新增日期
    ])

    count = in_memory_db.execute(
        "SELECT COUNT(*) FROM ohlcv_daily WHERE ticker='NVDA'"
    ).fetchone()[0]
    assert count == 3   # 没有重复行

    new_close = in_memory_db.execute(
        "SELECT close FROM ohlcv_daily WHERE ticker='NVDA' AND date='2024-01-03'"
    ).fetchone()[0]
    assert new_close == 999.9   # 后写覆盖前写


def test_upsert_fundamentals_annual_is_idempotent(in_memory_db):
    upsert_fundamentals_annual(in_memory_db, [_fundamental_row(2024, fcf=60_000.0)])
    upsert_fundamentals_annual(in_memory_db, [_fundamental_row(2024, fcf=60_922.0)])

    rows = in_memory_db.execute(
        "SELECT fiscal_year, fcf FROM fundamentals_annual WHERE ticker='NVDA'"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][1] == 60_922.0


def test_upsert_ohlcv_empty_list_is_safe(in_memory_db):
    """fetch_ohlcv 偶尔返回空列表 (虽然 fmp.py 已抛错)；loader 自身也要 no-op。"""
    upsert_ohlcv_daily(in_memory_db, [])
    count = in_memory_db.execute("SELECT COUNT(*) FROM ohlcv_daily").fetchone()[0]
    assert count == 0
