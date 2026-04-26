"""Shared US ticker ETL pipeline (used by etl_run.py and bulk runner)."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from etl.compute import compute_dcf_history, compute_dcf_lines, compute_ema
from etl.loader import (
    upsert_company,
    upsert_fundamentals_annual,
    upsert_income_statement_annual,
    upsert_interest_expense_annual,
    upsert_management,
    upsert_ohlcv_daily,
    upsert_revenue_by_geography,
    upsert_revenue_by_segment,
)
from etl.sources.fmp import (
    fetch_fcf_annual,
    fetch_income_statement_annual,
    fetch_management,
    fetch_ohlcv,
    fetch_profile,
    fetch_revenue_by_geography,
    fetch_revenue_by_segment,
)
from etl.sources.fmp_dcf import load_fmp_dcf_history


@dataclass
class USRunOptions:
    """Options for ``run_us_ticker``."""

    skip_optional: bool = False
    """If True, skip management, segment/geo revenue, interest expense (steps 9–12)."""
    verbose: bool = True
    """Print progress lines to stdout."""


def _log(msg: str, *, verbose: bool) -> None:
    if verbose:
        print(msg)


def _ohlcv_start_from_annual_rows(
    fcf_rows: list[dict],
    income_rows: list[dict],
) -> str | None:
    """Use annual-fundamentals coverage window as OHLCV history start.

    This keeps daily price history aligned with annual statement depth
    (currently 15 years), instead of repeatedly pulling much older bars.
    """
    candidates: list[str] = []
    for row in fcf_rows:
        d = row.get("fiscal_end_date")
        if d:
            candidates.append(str(d)[:10])
    for row in income_rows:
        d = row.get("fiscal_end_date")
        if d:
            candidates.append(str(d)[:10])
    return min(candidates) if candidates else None


def _annual_incremental_from(conn, ticker: str) -> str | None:
    """Return overlap window start for annual statement refresh.

    First run: None (pulls ANNUAL_HISTORY_LIMIT rows).
    Subsequent runs: go back one fiscal year from the latest stored year so
    revised filings are still picked up while staying incremental.
    """
    row = conn.execute(
        "SELECT MAX(fiscal_end_date) FROM fundamentals_annual WHERE ticker = ?",
        [ticker],
    ).fetchone()
    if not row or row[0] is None:
        return None
    latest_year = int(str(row[0])[:4])
    return f"{latest_year - 1:04d}-01-01"


def _ohlcv_incremental_from(
    conn,
    ticker: str,
    annual_start: str | None,
) -> str | None:
    """First run uses annual_start; later runs fetch from next missing day."""
    row = conn.execute(
        "SELECT MAX(date) FROM ohlcv_daily WHERE ticker = ?",
        [ticker],
    ).fetchone()
    if not row or row[0] is None:
        return annual_start
    next_day = datetime.strptime(str(row[0])[:10], "%Y-%m-%d").date() + timedelta(days=1)
    if annual_start:
        annual_date = datetime.strptime(annual_start, "%Y-%m-%d").date()
        if next_day < annual_date:
            next_day = annual_date
    return next_day.isoformat()


def run_us_ticker(conn, ticker: str, options: USRunOptions | None = None) -> None:
    """Run full US ETL for one ticker (same steps as legacy ``etl_run.run_ticker``)."""
    opts = options or USRunOptions()
    ticker = ticker.upper()
    v = opts.verbose

    _log(f"\n{'='*50}", verbose=v)
    _log(f"  {ticker}", verbose=v)
    _log(f"{'='*50}", verbose=v)

    total_steps = 12

    # 1. Profile → companies
    _log(f"  [1/{total_steps}] Fetching profile...", verbose=v)
    profile = fetch_profile(ticker)
    if profile.get("_is_etf") or profile.get("_is_fund"):
        raise ValueError(f"{ticker}: FMP marks isEtf/isFund — skip (not operating common stock)")
    upsert_company(conn, profile)
    shares_out_raw = profile.get("_shares_out_raw")
    if shares_out_raw:
        _log(f"        {profile['name']} | {profile['sector']} | shares: {shares_out_raw:,.0f}", verbose=v)
    else:
        _log(f"        {profile['name']}", verbose=v)

    annual_from = _annual_incremental_from(conn, ticker)

    # 2. Cash flow → fundamentals_annual (FCF columns)
    _log(f"  [2/{total_steps}] Fetching annual FCF...", verbose=v)
    try:
        fcf_rows = fetch_fcf_annual(ticker, shares_out_raw, date_from=annual_from)
    except TypeError:
        fcf_rows = fetch_fcf_annual(ticker, shares_out_raw)
    upsert_fundamentals_annual(conn, fcf_rows)
    _log(f"        {len(fcf_rows)} annual FCF rows written", verbose=v)

    # 3. Income statement → fundamentals_annual
    _log(f"  [3/{total_steps}] Fetching annual income statement...", verbose=v)
    try:
        income_rows = fetch_income_statement_annual(ticker, date_from=annual_from)
    except TypeError:
        income_rows = fetch_income_statement_annual(ticker)
    upsert_income_statement_annual(conn, income_rows)
    _log(f"        {len(income_rows)} annual income rows written", verbose=v)

    # 4. OHLCV (history depth aligned to annual statements window)
    annual_window_start = _ohlcv_start_from_annual_rows(fcf_rows, income_rows)
    ohlcv_from = _ohlcv_incremental_from(conn, ticker, annual_window_start)
    ohlcv_to = str(date.today())

    _log(f"  [4/{total_steps}] Fetching daily OHLCV...", verbose=v)
    if ohlcv_from and ohlcv_from > ohlcv_to:
        ohlcv_rows = []
    else:
        ohlcv_rows = fetch_ohlcv(ticker, shares_out_raw, date_from=ohlcv_from, date_to=ohlcv_to)
    upsert_ohlcv_daily(conn, ohlcv_rows)
    _log(f"        {len(ohlcv_rows):,} daily bars written ({ohlcv_from} -> {ohlcv_to})", verbose=v)

    # 5. FMP DCF history
    _log(f"  [5/{total_steps}] Fetching FMP DCF history...", verbose=v)
    try:
        fmp_dcf_rows = load_fmp_dcf_history(ticker, conn)
        _log(f"        {fmp_dcf_rows:,} FMP DCF rows written", verbose=v)
    except Exception as e:
        _log(f"        skipped: {e}", verbose=v)

    # 6. EMA
    _log(f"  [6/{total_steps}] Computing EMA10/EMA250...", verbose=v)
    ema_rows = compute_ema(ticker, conn)
    _log(f"        {ema_rows:,} EMA rows updated", verbose=v)

    # 7. DCF history
    _log(f"  [7/{total_steps}] Computing DCF history...", verbose=v)
    dcf_hist_rows = compute_dcf_history(ticker, conn)
    _log(f"        {dcf_hist_rows:,} DCF history rows written", verbose=v)

    # 8. DCF metrics
    _log(f"  [8/{total_steps}] Computing latest DCF metrics...", verbose=v)
    result = compute_dcf_lines(ticker, conn)
    if result:
        avg = result["fcf_per_share_avg3yr"]
        _log(
            f"        3yr avg FCF/share: ${avg:.2f} | "
            f"14x=${14 * avg:.2f}  24x=${24 * avg:.2f}  34x=${34 * avg:.2f}",
            verbose=v,
        )
    else:
        _log("        No FCF data available for DCF computation", verbose=v)

    if opts.skip_optional:
        _log(f"  (steps 9–12 skipped: management, segment/geo, interest)", verbose=v)
        _log(f"  Done: {ticker}", verbose=v)
        return

    # 9. Management
    _log(f"  [9/{total_steps}] Fetching management...", verbose=v)
    try:
        mgmt_rows = fetch_management(ticker)
        upsert_management(conn, mgmt_rows)
        _log(f"        {len(mgmt_rows)} management rows written", verbose=v)
    except Exception as e:
        _log(f"        skipped: {e}", verbose=v)

    # 10. Segment
    _log(f"  [10/{total_steps}] Fetching revenue by segment...", verbose=v)
    try:
        seg_rows = fetch_revenue_by_segment(ticker)
        upsert_revenue_by_segment(conn, seg_rows)
        _log(f"        {len(seg_rows)} segment rows written", verbose=v)
    except Exception as e:
        _log(f"        skipped: {e}", verbose=v)

    # 11. Geography
    _log(f"  [11/{total_steps}] Fetching revenue by geography...", verbose=v)
    try:
        geo_rows = fetch_revenue_by_geography(ticker)
        upsert_revenue_by_geography(conn, geo_rows)
        _log(f"        {len(geo_rows)} geography rows written", verbose=v)
    except Exception as e:
        _log(f"        skipped: {e}", verbose=v)

    # 12. Interest expense (reuse already-fetched income statement rows)
    _log(f"  [12/{total_steps}] Writing annual interest expense (cached income rows)...", verbose=v)
    try:
        interest_rows = [
            {
                "ticker": row["ticker"],
                "fiscal_year": row["fiscal_year"],
                "interest_expense": row.get("interest_expense"),
            }
            for row in income_rows
            if row.get("interest_expense") is not None
        ]
        upsert_interest_expense_annual(conn, interest_rows)
        _log(f"        {len(interest_rows)} interest rows written", verbose=v)
    except Exception as e:
        _log(f"        skipped: {e}", verbose=v)

    _log(f"  Done: {ticker}", verbose=v)
