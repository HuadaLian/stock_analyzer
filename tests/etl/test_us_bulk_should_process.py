"""Tests for ``etl.us_bulk_run._should_process`` (done / stale / retry-failed semantics)."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from etl.us_bulk_run import _last_error_looks_ohlcv_only, _should_process


def test_last_error_ohlcv_only_detects_fmp_message() -> None:
    assert _last_error_looks_ohlcv_only("FMP returned no price history for AAPL")
    assert _last_error_looks_ohlcv_only("no price history")
    assert not _last_error_looks_ohlcv_only("HTTP 500")
    assert not _last_error_looks_ohlcv_only(None)


def test_force_always_processes(in_memory_db) -> None:
    in_memory_db.execute(
        "INSERT INTO etl_us_bulk_state (ticker, status, step, last_error) VALUES ('AAPL', 'done', '', NULL)"
    )
    assert _should_process(
        in_memory_db,
        "AAPL",
        force=True,
        retry_failed=False,
        stale_days=None,
        retry_failed_include_ohlcv=False,
    )


def test_done_skipped_without_stale(in_memory_db) -> None:
    in_memory_db.execute(
        "INSERT INTO etl_us_bulk_state (ticker, status, step, last_error) VALUES ('AAPL', 'done', '', NULL)"
    )
    assert not _should_process(
        in_memory_db,
        "AAPL",
        force=False,
        retry_failed=False,
        stale_days=None,
        retry_failed_include_ohlcv=False,
    )


def test_done_reprocessed_when_any_slice_stale(in_memory_db) -> None:
    """One old MAX(date) among OHLCV / fundamentals / FMP DCF / company row triggers refresh."""
    stale_days = 7
    old = (date.today() - timedelta(days=30)).isoformat()
    recent = (date.today() - timedelta(days=1)).isoformat()
    in_memory_db.execute(
        "INSERT INTO etl_us_bulk_state (ticker, status, step, last_error) VALUES ('AAPL', 'done', '', NULL)"
    )
    in_memory_db.execute(
        """
        INSERT INTO companies (ticker, market, name, updated_at)
        VALUES ('AAPL', 'US', 'Apple Inc', ?::TIMESTAMP)
        """,
        [f"{recent} 00:00:00"],
    )
    in_memory_db.execute(
        """
        INSERT INTO ohlcv_daily (ticker, date, open, high, low, close, volume, adj_close, market_cap)
        VALUES ('AAPL', ?::DATE, 1, 1, 1, 1, 1, 1, 1)
        """,
        [old],
    )
    in_memory_db.execute(
        """
        INSERT INTO fundamentals_annual (
            ticker, fiscal_year, fiscal_end_date, filing_date, currency, reporting_currency,
            fx_to_usd, fcf, fcf_per_share, shares_out, source
        ) VALUES (
            'AAPL', 2023, ?::DATE, ?::DATE, 'USD', 'USD', 1.0, 1.0, 1.0, 1.0, 'fmp'
        )
        """,
        [recent, recent],
    )
    in_memory_db.execute(
        """
        INSERT INTO fmp_dcf_history (ticker, date, dcf_value, stock_price)
        VALUES ('AAPL', ?::DATE, 1.0, 1.0)
        """,
        [recent],
    )
    assert _should_process(
        in_memory_db,
        "AAPL",
        force=False,
        retry_failed=False,
        stale_days=stale_days,
        retry_failed_include_ohlcv=False,
    )


def test_done_not_reprocessed_when_slices_fresh(in_memory_db) -> None:
    stale_days = 30
    recent = (date.today() - timedelta(days=3)).isoformat()
    in_memory_db.execute(
        "INSERT INTO etl_us_bulk_state (ticker, status, step, last_error) VALUES ('AAPL', 'done', '', NULL)"
    )
    in_memory_db.execute(
        """
        INSERT INTO companies (ticker, market, name, updated_at)
        VALUES ('AAPL', 'US', 'Apple Inc', ?::TIMESTAMP)
        """,
        [f"{recent} 00:00:00"],
    )
    in_memory_db.execute(
        """
        INSERT INTO ohlcv_daily (ticker, date, open, high, low, close, volume, adj_close, market_cap)
        VALUES ('AAPL', ?::DATE, 1, 1, 1, 1, 1, 1, 1)
        """,
        [recent],
    )
    in_memory_db.execute(
        """
        INSERT INTO fundamentals_annual (
            ticker, fiscal_year, fiscal_end_date, filing_date, currency, reporting_currency,
            fx_to_usd, fcf, fcf_per_share, shares_out, source
        ) VALUES (
            'AAPL', 2023, ?::DATE, ?::DATE, 'USD', 'USD', 1.0, 1.0, 1.0, 1.0, 'fmp'
        )
        """,
        [recent, recent],
    )
    in_memory_db.execute(
        """
        INSERT INTO fmp_dcf_history (ticker, date, dcf_value, stock_price)
        VALUES ('AAPL', ?::DATE, 1.0, 1.0)
        """,
        [recent],
    )
    assert not _should_process(
        in_memory_db,
        "AAPL",
        force=False,
        retry_failed=False,
        stale_days=stale_days,
        retry_failed_include_ohlcv=False,
    )


def test_failed_retry_skips_ohlcv_only_by_default(in_memory_db) -> None:
    in_memory_db.execute(
        """
        INSERT INTO etl_us_bulk_state (ticker, status, step, last_error)
        VALUES ('AAPL', 'failed', '', 'FMP returned no price history for AAPL')
        """
    )
    assert not _should_process(
        in_memory_db,
        "AAPL",
        force=False,
        retry_failed=True,
        stale_days=None,
        retry_failed_include_ohlcv=False,
    )


def test_failed_retry_includes_ohlcv_when_flagged(in_memory_db) -> None:
    in_memory_db.execute(
        """
        INSERT INTO etl_us_bulk_state (ticker, status, step, last_error)
        VALUES ('AAPL', 'failed', '', 'FMP returned no price history for AAPL')
        """
    )
    assert _should_process(
        in_memory_db,
        "AAPL",
        force=False,
        retry_failed=True,
        stale_days=None,
        retry_failed_include_ohlcv=True,
    )


def test_failed_retry_non_ohlcv_error(in_memory_db) -> None:
    in_memory_db.execute(
        """
        INSERT INTO etl_us_bulk_state (ticker, status, step, last_error)
        VALUES ('AAPL', 'failed', '', 'profile API timeout')
        """
    )
    assert _should_process(
        in_memory_db,
        "AAPL",
        force=False,
        retry_failed=True,
        stale_days=None,
        retry_failed_include_ohlcv=False,
    )


@pytest.mark.parametrize("status", ["pending", "interrupted", "running"])
def test_non_terminal_states_process(in_memory_db, status: str) -> None:
    in_memory_db.execute(
        f"""
        INSERT INTO etl_us_bulk_state (ticker, status, step, last_error)
        VALUES ('AAPL', '{status}', '', NULL)
        """
    )
    assert _should_process(
        in_memory_db,
        "AAPL",
        force=False,
        retry_failed=False,
        stale_days=None,
        retry_failed_include_ohlcv=False,
    )
