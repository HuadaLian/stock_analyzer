"""Phase 2: apply_ticker_bundle skips compute_ema / compute_dcf_history when
the bundle adds no new rows that would change the inputs.

Two checks per skip:
- short-circuit fires when the gating data exists (no recompute called)
- short-circuit does NOT fire when gating data is absent or partial
"""

from __future__ import annotations

import datetime as dt

import pytest


# ---------- _ema_already_filled ----------


def test_ema_filled_returns_true_when_all_ema_set(in_memory_db):
    from etl.loader import upsert_ohlcv_daily, upsert_ohlcv_ema
    from etl.us_ticker_bundle import _ema_already_filled

    upsert_ohlcv_daily(in_memory_db, [
        {"ticker": "AAA", "date": dt.date(2024, 1, 2),
         "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05,
         "volume": 100, "adj_close": 1.04, "market_cap": 1000.0},
        {"ticker": "AAA", "date": dt.date(2024, 1, 3),
         "open": 1.05, "high": 1.2, "low": 1.0, "close": 1.15,
         "volume": 200, "adj_close": 1.14, "market_cap": 1010.0},
    ])
    upsert_ohlcv_ema(in_memory_db, [
        {"ticker": "AAA", "date": dt.date(2024, 1, 2), "ema10": 1.0, "ema250": 1.0},
        {"ticker": "AAA", "date": dt.date(2024, 1, 3), "ema10": 1.1, "ema250": 1.1},
    ])
    assert _ema_already_filled(in_memory_db, "AAA") is True


def test_ema_filled_returns_false_when_any_ema_missing(in_memory_db):
    from etl.loader import upsert_ohlcv_daily, upsert_ohlcv_ema
    from etl.us_ticker_bundle import _ema_already_filled

    upsert_ohlcv_daily(in_memory_db, [
        {"ticker": "AAA", "date": dt.date(2024, 1, 2),
         "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05,
         "volume": 100, "adj_close": 1.04, "market_cap": 1000.0},
        {"ticker": "AAA", "date": dt.date(2024, 1, 3),
         "open": 1.05, "high": 1.2, "low": 1.0, "close": 1.15,
         "volume": 200, "adj_close": 1.14, "market_cap": 1010.0},
    ])
    # Only fill ema for one of the two rows.
    upsert_ohlcv_ema(in_memory_db, [
        {"ticker": "AAA", "date": dt.date(2024, 1, 2), "ema10": 1.0, "ema250": 1.0},
    ])
    assert _ema_already_filled(in_memory_db, "AAA") is False


def test_ema_filled_returns_true_when_no_priced_rows(in_memory_db):
    """No rows for ticker → no NULL ema10 → returns True (nothing to compute)."""
    from etl.us_ticker_bundle import _ema_already_filled

    assert _ema_already_filled(in_memory_db, "AAA") is True


# ---------- _dcf_history_already_filled ----------


def test_dcf_history_filled_returns_true_when_row_exists(in_memory_db):
    from etl.loader import upsert_dcf_history
    from etl.us_ticker_bundle import _dcf_history_already_filled

    upsert_dcf_history(in_memory_db, [{
        "ticker": "AAA", "fiscal_year": 2023, "anchor_date": dt.date(2024, 3, 1),
        "fcf_ps_avg3yr": 1.0, "dcf_14x": 14.0, "dcf_24x": 24.0, "dcf_34x": 34.0,
    }])
    assert _dcf_history_already_filled(in_memory_db, "AAA") is True


def test_dcf_history_filled_returns_false_when_no_rows(in_memory_db):
    from etl.us_ticker_bundle import _dcf_history_already_filled

    assert _dcf_history_already_filled(in_memory_db, "AAA") is False


# ---------- apply_ticker_bundle wiring (mock compute_ema / compute_dcf_history) ----------


def _common_profile() -> dict:
    return {
        "ticker": "AAA", "market": "US", "name": "A Co", "exchange": "NASDAQ",
        "exchange_full_name": None, "country": "US",
        "sector": "Tech", "industry": "Software", "currency": "USD",
        "description": "x", "shares_out": 1.0, "_shares_out_raw": 1e6,
        "_market_cap": 1e9, "_is_etf": False, "_is_fund": False,
    }


def _seed_warm_state(conn) -> None:
    """Pre-fill ohlcv_daily (with EMA) + dcf_history so short-circuits would fire."""
    from etl.loader import upsert_ohlcv_daily, upsert_ohlcv_ema, upsert_dcf_history

    upsert_ohlcv_daily(conn, [{
        "ticker": "AAA", "date": dt.date(2024, 1, 2),
        "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05,
        "volume": 100, "adj_close": 1.04, "market_cap": 1000.0,
    }])
    upsert_ohlcv_ema(conn, [{
        "ticker": "AAA", "date": dt.date(2024, 1, 2), "ema10": 1.0, "ema250": 1.0,
    }])
    upsert_dcf_history(conn, [{
        "ticker": "AAA", "fiscal_year": 2023, "anchor_date": dt.date(2024, 3, 1),
        "fcf_ps_avg3yr": 1.0, "dcf_14x": 14.0, "dcf_24x": 24.0, "dcf_34x": 34.0,
    }])


def test_apply_skips_compute_ema_when_no_new_ohlcv_and_filled(in_memory_db, monkeypatch):
    from etl.us_ticker_bundle import TickerBundle, apply_ticker_bundle
    from etl.us_run_options import USRunOptions

    _seed_warm_state(in_memory_db)

    called = {"ema": 0, "dcf_hist": 0}
    monkeypatch.setattr("etl.us_ticker_bundle.compute_ema",
                        lambda t, c: called.__setitem__("ema", called["ema"] + 1) or 0)
    monkeypatch.setattr("etl.us_ticker_bundle.compute_dcf_history",
                        lambda t, c: called.__setitem__("dcf_hist", called["dcf_hist"] + 1) or 0)
    monkeypatch.setattr("etl.us_ticker_bundle.compute_dcf_lines", lambda t, c: None)

    bundle = TickerBundle(
        ticker="AAA", profile=_common_profile(),
        fcf_rows=[], income_rows=[], ohlcv_rows=[],
        ohlcv_from=None, ohlcv_to=None, fmp_dcf_rows=[],
    )
    apply_ticker_bundle(in_memory_db, bundle, USRunOptions(skip_optional=True, verbose=False))

    assert called["ema"] == 0
    assert called["dcf_hist"] == 0


def test_apply_runs_compute_ema_when_new_ohlcv_present(in_memory_db, monkeypatch):
    from etl.us_ticker_bundle import TickerBundle, apply_ticker_bundle
    from etl.us_run_options import USRunOptions

    _seed_warm_state(in_memory_db)

    called = {"ema": 0, "dcf_hist": 0}
    monkeypatch.setattr("etl.us_ticker_bundle.compute_ema",
                        lambda t, c: called.__setitem__("ema", called["ema"] + 1) or 1)
    monkeypatch.setattr("etl.us_ticker_bundle.compute_dcf_history",
                        lambda t, c: called.__setitem__("dcf_hist", called["dcf_hist"] + 1) or 1)
    monkeypatch.setattr("etl.us_ticker_bundle.compute_dcf_lines", lambda t, c: None)

    bundle = TickerBundle(
        ticker="AAA", profile=_common_profile(),
        fcf_rows=[], income_rows=[],
        ohlcv_rows=[{
            "ticker": "AAA", "date": dt.date(2024, 1, 3),
            "open": 1.05, "high": 1.2, "low": 1.0, "close": 1.15,
            "volume": 200, "adj_close": 1.14, "market_cap": 1010.0,
        }],
        ohlcv_from="2024-01-03", ohlcv_to="2024-01-03", fmp_dcf_rows=[],
    )
    apply_ticker_bundle(in_memory_db, bundle, USRunOptions(skip_optional=True, verbose=False))

    assert called["ema"] == 1
    # No new fcf_rows → dcf_history short-circuit still fires.
    assert called["dcf_hist"] == 0


def test_apply_runs_compute_ema_when_ema_partial(in_memory_db, monkeypatch):
    """Even with no new OHLCV, if EMA columns have any NULL the recompute must run
    (covers backfill / partially-migrated rows)."""
    from etl.loader import upsert_ohlcv_daily
    from etl.us_ticker_bundle import TickerBundle, apply_ticker_bundle
    from etl.us_run_options import USRunOptions

    # Insert ohlcv but leave ema NULL — short-circuit must NOT fire.
    upsert_ohlcv_daily(in_memory_db, [{
        "ticker": "AAA", "date": dt.date(2024, 1, 2),
        "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05,
        "volume": 100, "adj_close": 1.04, "market_cap": 1000.0,
    }])

    called = {"ema": 0}
    monkeypatch.setattr("etl.us_ticker_bundle.compute_ema",
                        lambda t, c: called.__setitem__("ema", called["ema"] + 1) or 1)
    monkeypatch.setattr("etl.us_ticker_bundle.compute_dcf_history", lambda t, c: 0)
    monkeypatch.setattr("etl.us_ticker_bundle.compute_dcf_lines", lambda t, c: None)

    bundle = TickerBundle(
        ticker="AAA", profile=_common_profile(),
        fcf_rows=[], income_rows=[], ohlcv_rows=[],
        ohlcv_from=None, ohlcv_to=None, fmp_dcf_rows=[],
    )
    apply_ticker_bundle(in_memory_db, bundle, USRunOptions(skip_optional=True, verbose=False))

    assert called["ema"] == 1


def test_apply_runs_compute_dcf_history_when_new_fcf_rows(in_memory_db, monkeypatch):
    from etl.us_ticker_bundle import TickerBundle, apply_ticker_bundle
    from etl.us_run_options import USRunOptions

    _seed_warm_state(in_memory_db)

    called = {"dcf_hist": 0}
    monkeypatch.setattr("etl.us_ticker_bundle.compute_ema", lambda t, c: 0)
    monkeypatch.setattr("etl.us_ticker_bundle.compute_dcf_history",
                        lambda t, c: called.__setitem__("dcf_hist", called["dcf_hist"] + 1) or 1)
    monkeypatch.setattr("etl.us_ticker_bundle.compute_dcf_lines", lambda t, c: None)

    bundle = TickerBundle(
        ticker="AAA", profile=_common_profile(),
        fcf_rows=[{
            "ticker": "AAA", "fiscal_year": 2024,
            "fiscal_end_date": dt.date(2024, 12, 31), "filing_date": dt.date(2025, 3, 1),
            "currency": "USD", "reporting_currency": "USD", "fx_to_usd": 1.0,
            "fcf": 100.0, "fcf_per_share": 1.0, "shares_out": 100.0, "source": "fmp",
            "revenue": None, "revenue_per_share": None,
            "gross_profit": None, "gross_margin": None,
            "operating_income": None, "operating_margin": None,
            "net_income": None, "profit_margin": None,
            "eps": None, "depreciation": None, "effective_tax_rate": None,
            "dividend_per_share": None, "total_equity": None,
            "long_term_debt": None, "working_capital": None,
            "book_value_per_share": None, "tangible_bv_per_share": None,
            "roic": None, "return_on_capital": None, "return_on_equity": None,
        }],
        income_rows=[], ohlcv_rows=[],
        ohlcv_from=None, ohlcv_to=None, fmp_dcf_rows=[],
    )
    apply_ticker_bundle(in_memory_db, bundle, USRunOptions(skip_optional=True, verbose=False))

    assert called["dcf_hist"] == 1
