"""db.company_portrait: SQL aggregates on minimal schema."""

from __future__ import annotations

from db.company_portrait import compute_company_portrait


def test_company_portrait_counts(in_memory_db):
    in_memory_db.execute(
        "INSERT INTO companies (ticker, market, name, country, currency) "
        "VALUES ('NVDA', 'US', 'NVIDIA', 'US', 'USD'), "
        "('AAA', 'CN', 'Alpha', 'CN', 'CNY')"
    )
    in_memory_db.execute(
        "INSERT INTO ohlcv_daily (ticker, date, open, high, low, close, volume, adj_close, market_cap) "
        "VALUES ('NVDA', DATE '2024-01-03', 1,1,1,1,1,1, 6000), ('AAA', DATE '2024-01-03', 1,1,1,1,1,1, 100)"
    )
    in_memory_db.execute(
        "INSERT INTO fundamentals_annual (ticker, fiscal_year, fiscal_end_date, currency, revenue, fcf) "
        "VALUES ('NVDA', 2023, DATE '2023-12-31', 'USD', 1000, 100)"
    )
    p = compute_company_portrait(in_memory_db, high_mcap_millions=5000.0)
    assert p["companies_total"] == 2
    assert p["fundamentals_annual"]["distinct_companies_with_revenue_and_fcf_same_year"] == 1
    hm = p["high_mcap"]
    assert hm["count_companies_at_or_above_threshold"] == 1
    assert hm["count_with_annual_revenue_and_fcf"] == 1
    assert hm["sample_missing_rev_fcf"] == []
