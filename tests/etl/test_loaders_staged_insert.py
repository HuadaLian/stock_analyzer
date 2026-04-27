"""Phase 1: confirm every loader's staged-DataFrame INSERT path is correct.

For each upsert function:
- Empty list → no-op (no exception, no row).
- First insert: row count and field values match input.
- Second insert with same PK: row count unchanged; DO UPDATE columns refreshed.
- Same connection used for multiple upserts in sequence (view name uniqueness).
"""

from __future__ import annotations

import datetime as dt

import pytest


# ---------- ohlcv_daily ----------


def test_upsert_ohlcv_daily_empty_is_noop(in_memory_db):
    from etl.loader import upsert_ohlcv_daily

    upsert_ohlcv_daily(in_memory_db, [])
    assert in_memory_db.execute("SELECT COUNT(*) FROM ohlcv_daily").fetchone()[0] == 0


def test_upsert_ohlcv_daily_inserts_then_updates(in_memory_db):
    from etl.loader import upsert_ohlcv_daily

    rows = [
        {
            "ticker": "AAA",
            "date": dt.date(2024, 1, 2),
            "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05,
            "volume": 100, "adj_close": 1.04, "market_cap": 1000.0,
        },
        {
            "ticker": "AAA",
            "date": dt.date(2024, 1, 3),
            "open": 1.05, "high": 1.2, "low": 1.0, "close": 1.15,
            "volume": 200, "adj_close": 1.14, "market_cap": 1010.0,
        },
    ]
    upsert_ohlcv_daily(in_memory_db, rows)
    assert in_memory_db.execute("SELECT COUNT(*) FROM ohlcv_daily").fetchone()[0] == 2

    rows[0]["close"] = 9.99
    rows[0]["adj_close"] = 9.98
    upsert_ohlcv_daily(in_memory_db, rows)
    assert in_memory_db.execute("SELECT COUNT(*) FROM ohlcv_daily").fetchone()[0] == 2
    close = in_memory_db.execute(
        "SELECT close FROM ohlcv_daily WHERE ticker='AAA' AND date='2024-01-02'"
    ).fetchone()[0]
    assert close == pytest.approx(9.99)


def test_upsert_ohlcv_daily_does_not_clobber_ema(in_memory_db):
    """OHLCV upsert must leave ema10/ema250 alone (separate writer fills them)."""
    from etl.loader import upsert_ohlcv_daily, upsert_ohlcv_ema

    upsert_ohlcv_daily(in_memory_db, [{
        "ticker": "AAA", "date": dt.date(2024, 1, 2),
        "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05,
        "volume": 100, "adj_close": 1.04, "market_cap": 1000.0,
    }])
    upsert_ohlcv_ema(in_memory_db, [{
        "ticker": "AAA", "date": dt.date(2024, 1, 2),
        "ema10": 1.5, "ema250": 2.5,
    }])
    upsert_ohlcv_daily(in_memory_db, [{
        "ticker": "AAA", "date": dt.date(2024, 1, 2),
        "open": 9.0, "high": 9.1, "low": 8.9, "close": 9.05,
        "volume": 999, "adj_close": 9.04, "market_cap": 9000.0,
    }])
    e10, e250 = in_memory_db.execute(
        "SELECT ema10, ema250 FROM ohlcv_daily WHERE ticker='AAA'"
    ).fetchone()
    assert e10 == pytest.approx(1.5)
    assert e250 == pytest.approx(2.5)


# ---------- ohlcv_ema ----------


def test_upsert_ohlcv_ema_empty_is_noop(in_memory_db):
    from etl.loader import upsert_ohlcv_ema

    upsert_ohlcv_ema(in_memory_db, [])
    assert in_memory_db.execute("SELECT COUNT(*) FROM ohlcv_daily").fetchone()[0] == 0


def test_upsert_ohlcv_ema_updates_existing_rows(in_memory_db):
    from etl.loader import upsert_ohlcv_daily, upsert_ohlcv_ema

    upsert_ohlcv_daily(in_memory_db, [{
        "ticker": "AAA", "date": dt.date(2024, 1, 2),
        "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05,
        "volume": 100, "adj_close": 1.04, "market_cap": 1000.0,
    }])

    upsert_ohlcv_ema(in_memory_db, [{
        "ticker": "AAA", "date": dt.date(2024, 1, 2),
        "ema10": 1.0, "ema250": 2.0,
    }])
    e10, e250, close = in_memory_db.execute(
        "SELECT ema10, ema250, close FROM ohlcv_daily WHERE ticker='AAA'"
    ).fetchone()
    assert (e10, e250) == (pytest.approx(1.0), pytest.approx(2.0))
    assert close == pytest.approx(1.05)

    upsert_ohlcv_ema(in_memory_db, [{
        "ticker": "AAA", "date": dt.date(2024, 1, 2),
        "ema10": 5.0, "ema250": 6.0,
    }])
    e10, e250 = in_memory_db.execute(
        "SELECT ema10, ema250 FROM ohlcv_daily WHERE ticker='AAA'"
    ).fetchone()
    assert (e10, e250) == (pytest.approx(5.0), pytest.approx(6.0))


# ---------- fundamentals_annual (FCF + income separately) ----------


def _fcf_row(ticker: str, year: int, **overrides) -> dict:
    base = {
        "ticker": ticker, "fiscal_year": year,
        "fiscal_end_date": dt.date(year, 12, 31),
        "filing_date": dt.date(year + 1, 3, 1),
        "currency": "USD", "reporting_currency": "USD", "fx_to_usd": 1.0,
        "fcf": 100.0, "fcf_per_share": 1.0, "shares_out": 100.0, "source": "fmp",
        "revenue": None, "revenue_per_share": None,
        "gross_profit": None, "gross_margin": None,
        "operating_income": None, "operating_margin": None,
        "net_income": None, "profit_margin": None,
        "eps": None, "depreciation": None,
        "effective_tax_rate": None, "dividend_per_share": None,
        "total_equity": None, "long_term_debt": None, "working_capital": None,
        "book_value_per_share": None, "tangible_bv_per_share": None,
        "roic": None, "return_on_capital": None, "return_on_equity": None,
    }
    base.update(overrides)
    return base


def test_upsert_fundamentals_annual_inserts_then_updates_only_listed_cols(in_memory_db):
    from etl.loader import upsert_fundamentals_annual, upsert_income_statement_annual

    upsert_fundamentals_annual(in_memory_db, [_fcf_row("AAA", 2023, fcf=100.0)])
    upsert_income_statement_annual(in_memory_db, [{
        "ticker": "AAA", "fiscal_year": 2023,
        "revenue": 500.0, "operating_income": 50.0,
        "depreciation": 10.0, "interest_expense": 5.0,
    }])

    row = in_memory_db.execute(
        "SELECT fcf, revenue, operating_income, interest_expense "
        "FROM fundamentals_annual WHERE ticker='AAA' AND fiscal_year=2023"
    ).fetchone()
    assert row == (pytest.approx(100.0), pytest.approx(500.0),
                   pytest.approx(50.0), pytest.approx(5.0))

    upsert_fundamentals_annual(in_memory_db, [_fcf_row("AAA", 2023, fcf=200.0)])
    row = in_memory_db.execute(
        "SELECT fcf, revenue FROM fundamentals_annual WHERE ticker='AAA'"
    ).fetchone()
    # fcf should be refreshed; revenue must be left alone (not in DO UPDATE list).
    assert row == (pytest.approx(200.0), pytest.approx(500.0))


def test_upsert_income_coalesces_interest_expense(in_memory_db):
    """upsert_interest_expense_annual fills interest; later income upsert with
    interest_expense=None must not blank it out (COALESCE)."""
    from etl.loader import (upsert_fundamentals_annual,
                            upsert_interest_expense_annual,
                            upsert_income_statement_annual)

    upsert_fundamentals_annual(in_memory_db, [_fcf_row("AAA", 2023)])
    upsert_interest_expense_annual(in_memory_db, [{
        "ticker": "AAA", "fiscal_year": 2023, "interest_expense": 7.5,
    }])
    upsert_income_statement_annual(in_memory_db, [{
        "ticker": "AAA", "fiscal_year": 2023,
        "revenue": 500.0, "operating_income": 50.0,
        "depreciation": 10.0, "interest_expense": None,
    }])
    interest = in_memory_db.execute(
        "SELECT interest_expense FROM fundamentals_annual WHERE ticker='AAA'"
    ).fetchone()[0]
    assert interest == pytest.approx(7.5)


# ---------- dcf_history / fmp_dcf_history ----------


def test_upsert_dcf_history_inserts_then_updates(in_memory_db):
    from etl.loader import upsert_dcf_history

    rows = [{
        "ticker": "AAA", "fiscal_year": 2023,
        "anchor_date": dt.date(2024, 3, 1),
        "fcf_ps_avg3yr": 1.0, "dcf_14x": 14.0, "dcf_24x": 24.0, "dcf_34x": 34.0,
    }]
    upsert_dcf_history(in_memory_db, rows)
    upsert_dcf_history(in_memory_db, [{**rows[0], "dcf_14x": 99.0}])
    val = in_memory_db.execute(
        "SELECT dcf_14x FROM dcf_history WHERE ticker='AAA' AND fiscal_year=2023"
    ).fetchone()[0]
    assert val == pytest.approx(99.0)


def test_upsert_fmp_dcf_history_inserts_then_updates(in_memory_db):
    from etl.loader import upsert_fmp_dcf_history

    rows = [
        {"ticker": "AAA", "date": dt.date(2024, 1, 2), "dcf_value": 50.0, "stock_price": 60.0},
        {"ticker": "AAA", "date": dt.date(2024, 1, 3), "dcf_value": 51.0, "stock_price": 61.0},
    ]
    upsert_fmp_dcf_history(in_memory_db, rows)
    assert in_memory_db.execute("SELECT COUNT(*) FROM fmp_dcf_history").fetchone()[0] == 2

    upsert_fmp_dcf_history(in_memory_db, [{**rows[0], "dcf_value": 5.5}])
    val = in_memory_db.execute(
        "SELECT dcf_value FROM fmp_dcf_history WHERE ticker='AAA' AND date='2024-01-02'"
    ).fetchone()[0]
    assert val == pytest.approx(5.5)


# ---------- management / segment / geography (DELETE + insert) ----------


def test_upsert_management_replaces_old_titles(in_memory_db):
    from etl.loader import upsert_management

    upsert_management(in_memory_db, [
        {"ticker": "AAA", "name": "Alice", "title": "CEO", "updated_at": dt.date(2024, 1, 1)},
        {"ticker": "AAA", "name": "Bob",   "title": "CFO", "updated_at": dt.date(2024, 1, 1)},
    ])
    assert in_memory_db.execute("SELECT COUNT(*) FROM management WHERE ticker='AAA'").fetchone()[0] == 2

    # Bob leaves; only CEO row should remain.
    upsert_management(in_memory_db, [
        {"ticker": "AAA", "name": "Alice", "title": "CEO", "updated_at": dt.date(2024, 6, 1)},
    ])
    rows = in_memory_db.execute(
        "SELECT title, name FROM management WHERE ticker='AAA' ORDER BY title"
    ).fetchall()
    assert rows == [("CEO", "Alice")]


def test_upsert_revenue_by_segment_replaces_old_segments(in_memory_db):
    from etl.loader import upsert_revenue_by_segment

    upsert_revenue_by_segment(in_memory_db, [
        {"ticker": "AAA", "fiscal_year": 2023, "segment": "Cloud", "revenue": 100.0, "pct": 0.5},
        {"ticker": "AAA", "fiscal_year": 2023, "segment": "Ads",   "revenue": 100.0, "pct": 0.5},
    ])
    upsert_revenue_by_segment(in_memory_db, [
        {"ticker": "AAA", "fiscal_year": 2023, "segment": "Cloud", "revenue": 200.0, "pct": 1.0},
    ])
    rows = in_memory_db.execute(
        "SELECT segment, revenue FROM revenue_by_segment WHERE ticker='AAA' ORDER BY segment"
    ).fetchall()
    assert rows == [("Cloud", pytest.approx(200.0))]


def test_upsert_revenue_by_geography_replaces_old_regions(in_memory_db):
    from etl.loader import upsert_revenue_by_geography

    upsert_revenue_by_geography(in_memory_db, [
        {"ticker": "AAA", "fiscal_year": 2023, "region": "US", "revenue": 100.0, "pct": 0.5},
        {"ticker": "AAA", "fiscal_year": 2023, "region": "EU", "revenue": 100.0, "pct": 0.5},
    ])
    upsert_revenue_by_geography(in_memory_db, [
        {"ticker": "AAA", "fiscal_year": 2023, "region": "US", "revenue": 300.0, "pct": 1.0},
    ])
    rows = in_memory_db.execute(
        "SELECT region, revenue FROM revenue_by_geography WHERE ticker='AAA' ORDER BY region"
    ).fetchall()
    assert rows == [("US", pytest.approx(300.0))]


# ---------- multi-call view-name uniqueness ----------


def test_multiple_loaders_in_sequence_do_not_collide(in_memory_db):
    """All upserts called back-to-back on one connection: no view-name collision."""
    from etl.loader import (upsert_company, upsert_fundamentals_annual,
                            upsert_income_statement_annual, upsert_ohlcv_daily,
                            upsert_ohlcv_ema, upsert_dcf_history,
                            upsert_fmp_dcf_history, upsert_management)

    upsert_company(in_memory_db, {
        "ticker": "AAA", "market": "US", "name": "A Co",
        "exchange": "NASDAQ", "exchange_full_name": None, "country": "US",
        "sector": "Tech", "industry": "Software", "currency": "USD",
        "description": "x", "shares_out": 1.0,
    })
    upsert_fundamentals_annual(in_memory_db, [_fcf_row("AAA", 2023)])
    upsert_income_statement_annual(in_memory_db, [{
        "ticker": "AAA", "fiscal_year": 2023, "revenue": 1.0,
        "operating_income": 1.0, "depreciation": 1.0, "interest_expense": 1.0,
    }])
    upsert_ohlcv_daily(in_memory_db, [{
        "ticker": "AAA", "date": dt.date(2024, 1, 2),
        "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05,
        "volume": 100, "adj_close": 1.04, "market_cap": 1000.0,
    }])
    upsert_ohlcv_ema(in_memory_db, [{
        "ticker": "AAA", "date": dt.date(2024, 1, 2),
        "ema10": 1.0, "ema250": 1.0,
    }])
    upsert_dcf_history(in_memory_db, [{
        "ticker": "AAA", "fiscal_year": 2023, "anchor_date": dt.date(2024, 3, 1),
        "fcf_ps_avg3yr": 1.0, "dcf_14x": 14.0, "dcf_24x": 24.0, "dcf_34x": 34.0,
    }])
    upsert_fmp_dcf_history(in_memory_db, [{
        "ticker": "AAA", "date": dt.date(2024, 1, 2),
        "dcf_value": 1.0, "stock_price": 1.0,
    }])
    upsert_management(in_memory_db, [{
        "ticker": "AAA", "name": "Alice", "title": "CEO", "updated_at": dt.date(2024, 1, 1),
    }])

    # No exception above already proves view-name uniqueness; sanity-check counts.
    assert in_memory_db.execute("SELECT COUNT(*) FROM ohlcv_daily").fetchone()[0] == 1
    assert in_memory_db.execute("SELECT COUNT(*) FROM fundamentals_annual").fetchone()[0] == 1
