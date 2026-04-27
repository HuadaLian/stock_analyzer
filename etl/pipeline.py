"""Shared US ticker ETL pipeline (used by etl_run.py and bulk runner)."""

from __future__ import annotations

from etl.us_run_options import USRunOptions, _log
from etl.us_ticker_bundle import apply_ticker_bundle, fetch_ticker_bundle, load_batch_fetch_context


def run_us_ticker(conn, ticker: str, options: USRunOptions | None = None) -> None:
    """Run full US ETL for one ticker (same steps as legacy ``etl_run.run_ticker``)."""
    opts = options or USRunOptions()
    ticker = ticker.upper()
    ctx_map = load_batch_fetch_context(conn, [ticker])
    bundle = fetch_ticker_bundle(ticker, ctx_map[ticker], opts)
    apply_ticker_bundle(conn, bundle, opts)


__all__ = ["USRunOptions", "_log", "run_us_ticker"]
