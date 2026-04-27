"""Phase 3: incremental EMA in the fetch worker matches the full ewm pass.

The worker advances EMA from a prior tail value across new bars. If this drifts
from the full-history pandas ``ewm(adjust=False)`` numbers, the chart and the
DB will silently disagree — this test pins them together.
"""

from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd
import pytest

from etl.compute import compute_ema_series
from etl.us_ticker_bundle import _advance_ema


def _ohlcv_row(t: str, day: int, price: float) -> dict:
    return {
        "ticker": t, "date": dt.date(2024, 1, 1) + dt.timedelta(days=day - 1),
        "open": price, "high": price + 0.1, "low": price - 0.1, "close": price,
        "volume": 100, "adj_close": price, "market_cap": 1000.0,
    }


def test_advance_ema_matches_full_ewm_when_split_at_arbitrary_point():
    """Full-history ewm = (full ewm up to k) then increment from that tail.

    Pick a split index k, run ewm on rows[0:k], take the tail (last value),
    advance with _advance_ema across rows[k:], and compare the second half
    element-by-element to ewm(rows[0:N])[k:].
    """
    rng = np.random.default_rng(seed=42)
    prices = (rng.normal(loc=100, scale=5, size=400)).astype(float)
    rows = [_ohlcv_row("AAA", i + 1, float(p)) for i, p in enumerate(prices)]

    full = pd.Series(prices, dtype="float64")
    full_ema10 = compute_ema_series(full, span=10).to_numpy()
    full_ema250 = compute_ema_series(full, span=250).to_numpy()

    k = 250  # arbitrary split that's not at edge
    tail_e10 = float(full_ema10[k - 1])
    tail_e250 = float(full_ema250[k - 1])
    last_date = rows[k - 1]["date"]

    incr = _advance_ema(
        rows[k:],
        last_ema10=tail_e10,
        last_ema250=tail_e250,
        last_ema_date=last_date,
    )

    assert len(incr) == len(rows) - k
    incr_e10 = np.array([r["ema10"] for r in incr])
    incr_e250 = np.array([r["ema250"] for r in incr])
    np.testing.assert_allclose(incr_e10, full_ema10[k:], rtol=1e-12, atol=1e-12)
    np.testing.assert_allclose(incr_e250, full_ema250[k:], rtol=1e-12, atol=1e-12)


def test_advance_ema_drops_rows_at_or_before_cutoff():
    """Bars that overlap the prior tail must be ignored — the tail already
    includes their effect, so re-applying would double-count."""
    rows = [_ohlcv_row("AAA", i + 1, 100.0 + i) for i in range(5)]
    out = _advance_ema(
        rows,
        last_ema10=100.0,
        last_ema250=100.0,
        last_ema_date=rows[2]["date"],  # cutoff = day 3
    )
    # Only days 4 and 5 should advance.
    assert [r["date"] for r in out] == [rows[3]["date"], rows[4]["date"]]


def test_advance_ema_empty_input_returns_empty():
    out = _advance_ema([], last_ema10=1.0, last_ema250=1.0, last_ema_date=dt.date(2024, 1, 1))
    assert out == []


def test_advance_ema_skips_rows_with_null_adj_close():
    rows = [_ohlcv_row("AAA", 1, 100.0)]
    rows[0]["adj_close"] = None
    out = _advance_ema(rows, last_ema10=1.0, last_ema250=1.0, last_ema_date=None)
    assert out == []


# ---------- batch context prefetch ----------


def test_load_batch_fetch_context_returns_last_ema_tail(in_memory_db):
    from etl.loader import upsert_ohlcv_daily, upsert_ohlcv_ema
    from etl.us_ticker_bundle import load_batch_fetch_context

    upsert_ohlcv_daily(in_memory_db, [
        {"ticker": "AAA", "date": dt.date(2024, 1, 2),
         "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0,
         "volume": 100, "adj_close": 1.0, "market_cap": 1000.0},
        {"ticker": "AAA", "date": dt.date(2024, 1, 3),
         "open": 1.05, "high": 1.2, "low": 1.0, "close": 1.1,
         "volume": 200, "adj_close": 1.1, "market_cap": 1010.0},
        {"ticker": "BBB", "date": dt.date(2024, 1, 2),
         "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0,
         "volume": 100, "adj_close": 1.0, "market_cap": 1000.0},
    ])
    upsert_ohlcv_ema(in_memory_db, [
        {"ticker": "AAA", "date": dt.date(2024, 1, 2), "ema10": 0.9, "ema250": 0.8},
        {"ticker": "AAA", "date": dt.date(2024, 1, 3), "ema10": 0.95, "ema250": 0.85},
        # BBB intentionally has no EMA → tail should be missing for BBB.
    ])

    ctx = load_batch_fetch_context(in_memory_db, ["AAA", "BBB", "CCC"])

    assert ctx["AAA"].last_ema10 == pytest.approx(0.95)
    assert ctx["AAA"].last_ema250 == pytest.approx(0.85)
    assert str(ctx["AAA"].last_ema_date)[:10] == "2024-01-03"

    # BBB has no EMA filled → tail must be None so apply falls back to compute_ema.
    assert ctx["BBB"].last_ema10 is None
    assert ctx["BBB"].last_ema250 is None
    assert ctx["BBB"].last_ema_date is None

    # CCC was never inserted at all → all None including ohlcv_max_date.
    assert ctx["CCC"].last_ema10 is None
    assert ctx["CCC"].ohlcv_max_date is None


# ---------- apply path uses worker-computed ema_rows ----------


def _seed_ohlcv_with_ema(conn) -> None:
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


def _common_profile() -> dict:
    return {
        "ticker": "AAA", "market": "US", "name": "A Co", "exchange": "NASDAQ",
        "exchange_full_name": None, "country": "US",
        "sector": "Tech", "industry": "Software", "currency": "USD",
        "description": "x", "shares_out": 1.0, "_shares_out_raw": 1e6,
        "_market_cap": 1e9, "_is_etf": False, "_is_fund": False,
    }


def test_apply_uses_worker_ema_rows_and_skips_compute_ema(in_memory_db, monkeypatch):
    """When bundle.ema_rows is non-empty, apply just upserts them — no full read."""
    from etl.us_ticker_bundle import TickerBundle, apply_ticker_bundle
    from etl.us_run_options import USRunOptions

    _seed_ohlcv_with_ema(in_memory_db)

    called = {"compute_ema": 0}
    monkeypatch.setattr("etl.us_ticker_bundle.compute_ema",
                        lambda t, c: called.__setitem__("compute_ema", called["compute_ema"] + 1) or 0)
    monkeypatch.setattr("etl.us_ticker_bundle.compute_dcf_history", lambda t, c: 0)
    monkeypatch.setattr("etl.us_ticker_bundle.compute_dcf_lines", lambda t, c: None)

    bundle = TickerBundle(
        ticker="AAA", profile=_common_profile(),
        fcf_rows=[], income_rows=[],
        ohlcv_rows=[{
            "ticker": "AAA", "date": dt.date(2024, 1, 3),
            "open": 1.05, "high": 1.2, "low": 1.0, "close": 1.15,
            "volume": 200, "adj_close": 1.14, "market_cap": 1010.0,
        }],
        ema_rows=[{
            "ticker": "AAA", "date": dt.date(2024, 1, 3),
            "ema10": 1.0254545454545454, "ema250": 1.0011155378486055,
        }],
        ohlcv_from="2024-01-03", ohlcv_to="2024-01-03", fmp_dcf_rows=[],
    )
    apply_ticker_bundle(in_memory_db, bundle, USRunOptions(skip_optional=True, verbose=False))

    assert called["compute_ema"] == 0
    # Tail row from worker should be in DB now.
    e10, e250 = in_memory_db.execute(
        "SELECT ema10, ema250 FROM ohlcv_daily WHERE ticker='AAA' AND date='2024-01-03'"
    ).fetchone()
    assert e10 == pytest.approx(1.0254545454545454)
    assert e250 == pytest.approx(1.0011155378486055)


def test_apply_falls_back_to_compute_ema_when_worker_did_not_compute(in_memory_db, monkeypatch):
    """Cold-start ticker (no prior EMA tail) → bundle.ema_rows empty → full path runs."""
    from etl.us_ticker_bundle import TickerBundle, apply_ticker_bundle
    from etl.us_run_options import USRunOptions

    called = {"compute_ema": 0}
    monkeypatch.setattr("etl.us_ticker_bundle.compute_ema",
                        lambda t, c: called.__setitem__("compute_ema", called["compute_ema"] + 1) or 1)
    monkeypatch.setattr("etl.us_ticker_bundle.compute_dcf_history", lambda t, c: 0)
    monkeypatch.setattr("etl.us_ticker_bundle.compute_dcf_lines", lambda t, c: None)

    bundle = TickerBundle(
        ticker="AAA", profile=_common_profile(),
        fcf_rows=[], income_rows=[],
        ohlcv_rows=[{
            "ticker": "AAA", "date": dt.date(2024, 1, 2),
            "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05,
            "volume": 100, "adj_close": 1.04, "market_cap": 1000.0,
        }],
        ema_rows=[],  # worker had no tail to start from
        ohlcv_from=None, ohlcv_to=None, fmp_dcf_rows=[],
    )
    apply_ticker_bundle(in_memory_db, bundle, USRunOptions(skip_optional=True, verbose=False))

    assert called["compute_ema"] == 1


# ---------- maybe_advance_ema gating ----------


def test_maybe_advance_ema_skips_when_no_tail():
    from etl.us_ticker_bundle import (TickerBundle, TickerFetchContext,
                                       _maybe_advance_ema)

    bundle = TickerBundle(
        ticker="AAA",
        ohlcv_rows=[_ohlcv_row("AAA", 1, 100.0)],
    )
    ctx = TickerFetchContext(annual_from=None, ohlcv_max_date=None,
                             fmp_dcf_max_date=None,
                             last_ema_date=None, last_ema10=None, last_ema250=None)
    _maybe_advance_ema(bundle, ctx)
    assert bundle.ema_rows == []


def test_maybe_advance_ema_skips_when_no_new_ohlcv():
    from etl.us_ticker_bundle import (TickerBundle, TickerFetchContext,
                                       _maybe_advance_ema)

    bundle = TickerBundle(ticker="AAA", ohlcv_rows=[])
    ctx = TickerFetchContext(annual_from=None, ohlcv_max_date=None,
                             fmp_dcf_max_date=None,
                             last_ema_date=dt.date(2024, 1, 1),
                             last_ema10=1.0, last_ema250=1.0)
    _maybe_advance_ema(bundle, ctx)
    assert bundle.ema_rows == []


def test_maybe_advance_ema_populates_when_tail_and_rows_present():
    from etl.us_ticker_bundle import (TickerBundle, TickerFetchContext,
                                       _maybe_advance_ema)

    bundle = TickerBundle(
        ticker="AAA",
        ohlcv_rows=[_ohlcv_row("AAA", 2, 105.0)],
    )
    ctx = TickerFetchContext(annual_from=None, ohlcv_max_date=dt.date(2024, 1, 1),
                             fmp_dcf_max_date=None,
                             last_ema_date=dt.date(2024, 1, 1),
                             last_ema10=100.0, last_ema250=100.0)
    _maybe_advance_ema(bundle, ctx)
    assert len(bundle.ema_rows) == 1
    # Spot-check formula: e10 = 105 * 2/11 + 100 * 9/11 ≈ 100.909
    assert bundle.ema_rows[0]["ema10"] == pytest.approx(100.0 * 9 / 11 + 105.0 * 2 / 11)
