"""US ticker fetch/apply split: parallel HTTP fetch, single-thread DuckDB writer.

``fetch_ticker_bundle`` performs only FMP / in-memory work (no DB connection).
``apply_ticker_bundle`` performs upserts + compute steps (requires one write conn).
``load_batch_fetch_context`` issues a small number of SQL reads for a batch of tickers.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

import duckdb

from etl.compute import compute_dcf_history, compute_dcf_lines, compute_ema
from etl.loader import (
    upsert_company,
    upsert_fundamentals_annual,
    upsert_income_statement_annual,
    upsert_interest_expense_annual,
    upsert_management,
    upsert_ohlcv_daily,
    upsert_ohlcv_ema,
    upsert_revenue_by_geography,
    upsert_revenue_by_segment,
    upsert_fmp_dcf_history,
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
from etl.sources.fmp import load_api_key
from etl.sources.fmp_dcf import fetch_fmp_dcf_history
from etl.us_run_options import USRunOptions, _log

_log_timing = logging.getLogger(__name__)


def _etl_phase_timing_enabled() -> bool:
    """Set env ``STOCK_ANALYZER_ETL_TIMING=1`` to log per-phase seconds (fetch / apply)."""
    return os.environ.get("STOCK_ANALYZER_ETL_TIMING", "").strip().lower() in ("1", "true", "yes")


def _ohlcv_start_from_annual_rows(
    fcf_rows: list[dict],
    income_rows: list[dict],
) -> str | None:
    """Use annual-fundamentals coverage window as OHLCV history start."""
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


@dataclass
class TickerFetchContext:
    """Per-ticker DB-derived hints for FMP fetch (from a single prefetch query batch)."""

    annual_from: str | None
    ohlcv_max_date: object | None  # DATE from DuckDB
    fmp_dcf_max_date: object | None
    # Tail of the existing EMA series — when present the fetch worker can advance
    # EMA incrementally and hand the writer pre-computed rows (no full re-read).
    last_ema_date: object | None = None
    last_ema10: float | None = None
    last_ema250: float | None = None


@dataclass
class TickerBundle:
    ticker: str
    profile: dict | None = None
    fcf_rows: list = field(default_factory=list)
    income_rows: list = field(default_factory=list)
    ohlcv_rows: list = field(default_factory=list)
    ohlcv_from: str | None = None
    ohlcv_to: str | None = None
    fmp_dcf_rows: list = field(default_factory=list)
    mgmt_rows: list | None = None
    seg_rows: list | None = None
    geo_rows: list | None = None
    # Pre-computed EMA rows when the worker advanced the series. Empty list means
    # "writer should run full compute_ema" (cold start or partial-EMA tail).
    ema_rows: list = field(default_factory=list)
    error: str | None = None


def load_batch_fetch_context(conn: duckdb.DuckDBPyConnection, tickers: list[str]) -> dict[str, TickerFetchContext]:
    """Load annual/ohlcv/fmp-dcf high-water marks for many tickers in few queries."""
    tickers = [t.upper() for t in tickers if t]
    if not tickers:
        return {}
    ph = ",".join("?" * len(tickers))
    fa_rows = conn.execute(
        f"SELECT ticker, MAX(fiscal_end_date) FROM fundamentals_annual WHERE ticker IN ({ph}) GROUP BY ticker",
        tickers,
    ).fetchall()
    oh_rows = conn.execute(
        f"SELECT ticker, MAX(date) FROM ohlcv_daily WHERE ticker IN ({ph}) GROUP BY ticker",
        tickers,
    ).fetchall()
    dcf_rows = conn.execute(
        f"SELECT ticker, MAX(date) FROM fmp_dcf_history WHERE ticker IN ({ph}) GROUP BY ticker",
        tickers,
    ).fetchall()
    fa_map = {str(r[0]).upper(): r[1] for r in fa_rows}
    oh_map = {str(r[0]).upper(): r[1] for r in oh_rows}
    dcf_map = {str(r[0]).upper(): r[1] for r in dcf_rows}

    # Tail EMA per ticker: pick the latest row that already has ema10 filled, so
    # the worker can advance the series incrementally instead of re-reading the
    # full history. Tickers with no filled EMA tail fall back to compute_ema.
    ema_rows = conn.execute(
        f"""
        SELECT o.ticker, o.date, o.ema10, o.ema250
        FROM ohlcv_daily o
        JOIN (
            SELECT ticker, MAX(date) AS mx
            FROM ohlcv_daily
            WHERE ticker IN ({ph}) AND ema10 IS NOT NULL AND ema250 IS NOT NULL
            GROUP BY ticker
        ) m
        ON o.ticker = m.ticker AND o.date = m.mx
        """,
        tickers,
    ).fetchall()
    ema_map = {
        str(r[0]).upper(): (r[1], r[2], r[3])
        for r in ema_rows
    }

    out: dict[str, TickerFetchContext] = {}
    for t in tickers:
        mx = fa_map.get(t)
        annual_from = None
        if mx is not None:
            latest_year = int(str(mx)[:4])
            annual_from = f"{latest_year - 1:04d}-01-01"
        ema_tail = ema_map.get(t)
        out[t] = TickerFetchContext(
            annual_from=annual_from,
            ohlcv_max_date=oh_map.get(t),
            fmp_dcf_max_date=dcf_map.get(t),
            last_ema_date=ema_tail[0] if ema_tail else None,
            last_ema10=float(ema_tail[1]) if ema_tail and ema_tail[1] is not None else None,
            last_ema250=float(ema_tail[2]) if ema_tail and ema_tail[2] is not None else None,
        )
    return out


def _ohlcv_from_prefetch(ohlcv_max_date: object | None, annual_window_start: str | None) -> str | None:
    """Mirror ``_ohlcv_incremental_from`` using prefetched MAX(date)."""
    if ohlcv_max_date is None:
        return annual_window_start
    next_day = datetime.strptime(str(ohlcv_max_date)[:10], "%Y-%m-%d").date() + timedelta(days=1)
    if annual_window_start:
        annual_date = datetime.strptime(annual_window_start, "%Y-%m-%d").date()
        if next_day < annual_date:
            next_day = annual_date
    return next_day.isoformat()


def _format_fetch_phase_seconds(ph: dict[str, float], *, skip_optional: bool) -> str:
    parts = [
        f"profile={ph.get('profile', 0):.2f}s",
        f"fcf={ph.get('fcf', 0):.2f}s",
        f"income={ph.get('income', 0):.2f}s",
        f"ohlcv={ph.get('ohlcv', 0):.2f}s",
        f"fmp_dcf={ph.get('fmp_dcf', 0):.2f}s",
    ]
    if not skip_optional:
        parts.append(f"optional_mgmt_seg_geo={ph.get('optional', 0):.2f}s")
    return " ".join(parts)


def _normalize_refresh_mode(opts: USRunOptions) -> str:
    m = (getattr(opts, "refresh_mode", None) or "full").strip().lower()
    return m if m in ("full", "ohlcv", "fundamentals", "fmp_dcf") else "full"


def _log_fetch_timing_done(t: str, ph: dict[str, float], *, skip_optional: bool) -> None:
    if not _etl_phase_timing_enabled():
        return
    _keys = ["profile", "fcf", "income", "ohlcv", "fmp_dcf"]
    if not skip_optional:
        _keys.append("optional")
    tot = sum(ph.get(k, 0.0) for k in _keys)
    _log_timing.info(
        "ETL fetch timing %s: %s | total=%.2fs",
        t,
        _format_fetch_phase_seconds(ph, skip_optional=skip_optional),
        tot,
    )


def _log_fetch_timing_failed(t: str, ph: dict[str, float], *, skip_optional: bool, err: BaseException) -> None:
    if not _etl_phase_timing_enabled():
        return
    _log_timing.info(
        "ETL fetch timing %s (FAILED): %s | err=%s",
        t,
        _format_fetch_phase_seconds(ph, skip_optional=skip_optional),
        repr(err),
    )


# Same multipliers as pandas Series.ewm(span=N, adjust=False).mean(): α = 2 / (N + 1).
# Hard-coded so the worker doesn't import pandas just to read two constants; matches
# the formula in compute.compute_ema_series.
_ALPHA10 = 2.0 / (10 + 1)
_ALPHA250 = 2.0 / (250 + 1)


def _advance_ema(
    ohlcv_rows: list[dict],
    *,
    last_ema10: float,
    last_ema250: float,
    last_ema_date,
) -> list[dict]:
    """Advance EMA10/EMA250 across new bars from a known prior tail value.

    Caller must pass ``last_ema*`` as the EMA at ``last_ema_date`` (i.e. the most
    recent priced bar already in the DB). Bars in ``ohlcv_rows`` that fall on or
    before ``last_ema_date`` are dropped — they are by construction already
    included in the tail value, and re-applying them would double-count.

    Output rows have keys (ticker, date, ema10, ema250) — directly consumable by
    ``upsert_ohlcv_ema``.
    """
    if not ohlcv_rows:
        return []

    cutoff = str(last_ema_date)[:10] if last_ema_date is not None else ""

    bars = []
    for r in ohlcv_rows:
        if r.get("adj_close") is None:
            continue
        if cutoff and str(r["date"])[:10] <= cutoff:
            continue
        bars.append(r)
    bars.sort(key=lambda r: str(r["date"])[:10])

    out: list[dict] = []
    e10, e250 = float(last_ema10), float(last_ema250)
    for r in bars:
        p = float(r["adj_close"])
        e10 = p * _ALPHA10 + e10 * (1.0 - _ALPHA10)
        e250 = p * _ALPHA250 + e250 * (1.0 - _ALPHA250)
        out.append({
            "ticker": r["ticker"],
            "date": r["date"],
            "ema10": e10,
            "ema250": e250,
        })
    return out


def _maybe_advance_ema(bundle: "TickerBundle", ctx: TickerFetchContext) -> None:
    """Populate ``bundle.ema_rows`` from the prior tail (when known) + new bars.

    No-op when the bundle has no OHLCV rows or the ticker has no usable EMA tail
    (fresh ticker / partially backfilled column). The apply phase falls back to
    the full ``compute_ema(ticker, conn)`` path in those cases.
    """
    if not bundle.ohlcv_rows:
        return
    if ctx.last_ema10 is None or ctx.last_ema250 is None:
        return
    bundle.ema_rows = _advance_ema(
        bundle.ohlcv_rows,
        last_ema10=ctx.last_ema10,
        last_ema250=ctx.last_ema250,
        last_ema_date=ctx.last_ema_date,
    )


def _ema_already_filled(conn: duckdb.DuckDBPyConnection, ticker: str) -> bool:
    """True iff every priced ohlcv_daily row for ``ticker`` already has ema10 filled.

    Used to skip ``compute_ema`` when the bundle adds 0 new bars: the EMA series
    is a pure function of the existing adj_close column, so re-running yields the
    same numbers we already have. The query short-circuits at the first NULL row.
    """
    row = conn.execute(
        """
        SELECT 1 FROM ohlcv_daily
        WHERE ticker = ? AND adj_close IS NOT NULL AND ema10 IS NULL
        LIMIT 1
        """,
        [ticker],
    ).fetchone()
    return row is None


def _dcf_history_already_filled(conn: duckdb.DuckDBPyConnection, ticker: str) -> bool:
    """True iff dcf_history has at least one row for ``ticker``.

    Used to skip ``compute_dcf_history`` when the bundle adds 0 new fcf rows:
    the recompute reads only fundamentals_annual.fcf_per_share — unchanged inputs
    yield identical outputs.
    """
    row = conn.execute(
        "SELECT 1 FROM dcf_history WHERE ticker = ? LIMIT 1",
        [ticker],
    ).fetchone()
    return row is not None


def _apply_fmp_dcf_only(
    conn: duckdb.DuckDBPyConnection,
    bundle: TickerBundle,
    options: USRunOptions,
) -> None:
    """Writer phase for ``refresh_mode=fmp_dcf``: profile + FMP DCF history only."""
    opts = options
    t = bundle.ticker.upper()
    v = opts.verbose
    _tm = _etl_phase_timing_enabled()
    _ta0 = time.perf_counter()
    profile = bundle.profile
    assert profile is not None

    _log(f"\n{'='*50}", verbose=v)
    _log(f"  {t}  (refresh_mode=fmp_dcf)", verbose=v)
    _log(f"{'='*50}", verbose=v)

    _log("  [1/2] Applying profile...", verbose=v)
    t0 = time.perf_counter()
    upsert_company(conn, profile)
    w_company = time.perf_counter() - t0

    _log("  [2/2] Writing FMP DCF history...", verbose=v)
    t0 = time.perf_counter()
    upsert_fmp_dcf_history(conn, bundle.fmp_dcf_rows)
    w_fmp = time.perf_counter() - t0
    _log(f"        {len(bundle.fmp_dcf_rows):,} FMP DCF rows written", verbose=v)
    _log(f"  Done: {t}", verbose=v)
    if _tm:
        _log_timing.info(
            "ETL apply timing %s (fmp_dcf): company=%.2fs fmp_dcf=%.2fs apply_total=%.2fs",
            t,
            w_company,
            w_fmp,
            time.perf_counter() - _ta0,
        )


def fetch_ticker_bundle(ticker: str, ctx: TickerFetchContext, options: USRunOptions) -> TickerBundle:
    """HTTP-only phase (safe to run concurrently across tickers)."""
    t = ticker.upper()
    opts = options
    mode = _normalize_refresh_mode(opts)
    bundle = TickerBundle(ticker=t)
    ph: dict[str, float] = {}
    t0 = time.perf_counter()

    def _timing_skip_optional() -> bool:
        return bool(opts.skip_optional or mode == "fmp_dcf")

    try:
        profile = fetch_profile(t)
        ph["profile"] = time.perf_counter() - t0
        t0 = time.perf_counter()
        if profile.get("_is_etf") or profile.get("_is_fund"):
            raise ValueError(f"{t}: FMP marks isEtf/isFund — skip (not operating common stock)")
        bundle.profile = profile
        shares_out_raw = profile.get("_shares_out_raw")

        if mode == "fmp_dcf":
            bundle.fcf_rows = []
            bundle.income_rows = []
            bundle.ohlcv_rows = []
            bundle.ohlcv_from = None
            bundle.ohlcv_to = None
            ph["fcf"] = ph["income"] = ph["ohlcv"] = 0.0
            key = load_api_key()
            fmp_date_from = None
            if ctx.fmp_dcf_max_date is not None:
                fmp_date_from = str(ctx.fmp_dcf_max_date)[:10]
            try:
                rows = fetch_fmp_dcf_history(t, key, date_from=fmp_date_from)
            except TypeError:
                rows = fetch_fmp_dcf_history(t, key)
            if fmp_date_from:
                rows = [r for r in rows if str(r.get("date", "")) >= fmp_date_from]
            bundle.fmp_dcf_rows = rows
            ph["fmp_dcf"] = time.perf_counter() - t0
            t0 = time.perf_counter()
            bundle.mgmt_rows = bundle.seg_rows = bundle.geo_rows = None
            ph["optional"] = time.perf_counter() - t0
            _log_fetch_timing_done(t, ph, skip_optional=True)
            return bundle

        if mode == "ohlcv":
            bundle.fcf_rows = []
            bundle.income_rows = []
            ph["fcf"] = time.perf_counter() - t0
            t0 = time.perf_counter()
            ph["income"] = time.perf_counter() - t0
            t0 = time.perf_counter()
            ohlcv_from = _ohlcv_from_prefetch(ctx.ohlcv_max_date, None)
            ohlcv_to = str(date.today())
            try:
                if ohlcv_from and ohlcv_from > ohlcv_to:
                    bundle.ohlcv_rows = []
                else:
                    bundle.ohlcv_rows = fetch_ohlcv(t, shares_out_raw, date_from=ohlcv_from, date_to=ohlcv_to)
                bundle.ohlcv_from = ohlcv_from
                bundle.ohlcv_to = ohlcv_to
            finally:
                ph["ohlcv"] = time.perf_counter() - t0
            t0 = time.perf_counter()
            bundle.fmp_dcf_rows = []
            ph["fmp_dcf"] = time.perf_counter() - t0
            t0 = time.perf_counter()
            if not opts.skip_optional:
                try:
                    bundle.mgmt_rows = fetch_management(t)
                except Exception:
                    bundle.mgmt_rows = []
                try:
                    bundle.seg_rows = fetch_revenue_by_segment(t)
                except Exception:
                    bundle.seg_rows = []
                try:
                    bundle.geo_rows = fetch_revenue_by_geography(t)
                except Exception:
                    bundle.geo_rows = []
            ph["optional"] = time.perf_counter() - t0
            _maybe_advance_ema(bundle, ctx)
            _log_fetch_timing_done(t, ph, skip_optional=opts.skip_optional)
            return bundle

        annual_from = ctx.annual_from
        try:
            fcf_rows = fetch_fcf_annual(t, shares_out_raw, date_from=annual_from)
        except TypeError:
            fcf_rows = fetch_fcf_annual(t, shares_out_raw)
        bundle.fcf_rows = fcf_rows
        ph["fcf"] = time.perf_counter() - t0
        t0 = time.perf_counter()

        try:
            income_rows = fetch_income_statement_annual(t, date_from=annual_from)
        except TypeError:
            income_rows = fetch_income_statement_annual(t)
        bundle.income_rows = income_rows
        ph["income"] = time.perf_counter() - t0
        t0 = time.perf_counter()

        if mode == "fundamentals":
            ph["ohlcv"] = time.perf_counter() - t0
            t0 = time.perf_counter()
            bundle.ohlcv_rows = []
            bundle.ohlcv_from = None
            bundle.ohlcv_to = None
        else:
            annual_window_start = _ohlcv_start_from_annual_rows(fcf_rows, income_rows)
            ohlcv_from = _ohlcv_from_prefetch(ctx.ohlcv_max_date, annual_window_start)
            ohlcv_to = str(date.today())
            try:
                if ohlcv_from and ohlcv_from > ohlcv_to:
                    bundle.ohlcv_rows = []
                else:
                    bundle.ohlcv_rows = fetch_ohlcv(t, shares_out_raw, date_from=ohlcv_from, date_to=ohlcv_to)
                bundle.ohlcv_from = ohlcv_from
                bundle.ohlcv_to = ohlcv_to
            finally:
                ph["ohlcv"] = time.perf_counter() - t0
            t0 = time.perf_counter()

        key = load_api_key()
        fmp_date_from = None
        if ctx.fmp_dcf_max_date is not None:
            fmp_date_from = str(ctx.fmp_dcf_max_date)[:10]
        try:
            rows = fetch_fmp_dcf_history(t, key, date_from=fmp_date_from)
        except TypeError:
            rows = fetch_fmp_dcf_history(t, key)
        if fmp_date_from:
            rows = [r for r in rows if str(r.get("date", "")) >= fmp_date_from]
        bundle.fmp_dcf_rows = rows
        ph["fmp_dcf"] = time.perf_counter() - t0
        t0 = time.perf_counter()

        if not opts.skip_optional:
            try:
                bundle.mgmt_rows = fetch_management(t)
            except Exception:
                bundle.mgmt_rows = []
            try:
                bundle.seg_rows = fetch_revenue_by_segment(t)
            except Exception:
                bundle.seg_rows = []
            try:
                bundle.geo_rows = fetch_revenue_by_geography(t)
            except Exception:
                bundle.geo_rows = []
        ph["optional"] = time.perf_counter() - t0

        _maybe_advance_ema(bundle, ctx)
        _log_fetch_timing_done(t, ph, skip_optional=opts.skip_optional)
        return bundle
    except Exception as e:
        _log_fetch_timing_failed(t, ph, skip_optional=_timing_skip_optional(), err=e)
        raise


def apply_ticker_bundle(
    conn: duckdb.DuckDBPyConnection,
    bundle: TickerBundle,
    options: USRunOptions,
) -> None:
    """Single-writer DB phase (must not run concurrently on the same connection)."""
    opts = options
    t = bundle.ticker.upper()
    v = opts.verbose
    _tm = _etl_phase_timing_enabled()
    _ta0 = time.perf_counter()
    if bundle.error:
        raise RuntimeError(bundle.error)
    if not bundle.profile:
        raise RuntimeError(f"{t}: missing profile in bundle")

    if _normalize_refresh_mode(opts) == "fmp_dcf":
        _apply_fmp_dcf_only(conn, bundle, opts)
        return

    total_steps = 12
    profile = bundle.profile
    shares_out_raw = profile.get("_shares_out_raw")

    _log(f"\n{'='*50}", verbose=v)
    _log(f"  {t}", verbose=v)
    _log(f"{'='*50}", verbose=v)

    _log(f"  [1/{total_steps}] Applying profile...", verbose=v)
    t0 = time.perf_counter()
    upsert_company(conn, profile)
    w_company = time.perf_counter() - t0
    if shares_out_raw:
        _log(f"        {profile['name']} | {profile['sector']} | shares: {shares_out_raw:,.0f}", verbose=v)
    else:
        _log(f"        {profile['name']}", verbose=v)

    _log(f"  [2/{total_steps}] Writing annual FCF...", verbose=v)
    t0 = time.perf_counter()
    upsert_fundamentals_annual(conn, bundle.fcf_rows)
    w_fcf = time.perf_counter() - t0
    _log(f"        {len(bundle.fcf_rows)} annual FCF rows written", verbose=v)

    _log(f"  [3/{total_steps}] Writing annual income statement...", verbose=v)
    t0 = time.perf_counter()
    upsert_income_statement_annual(conn, bundle.income_rows)
    w_income = time.perf_counter() - t0
    _log(f"        {len(bundle.income_rows)} annual income rows written", verbose=v)

    ohlcv_from = bundle.ohlcv_from
    ohlcv_to = bundle.ohlcv_to or str(date.today())
    if ohlcv_from is None:
        ohlcv_from = _ohlcv_from_prefetch(
            None,
            _ohlcv_start_from_annual_rows(bundle.fcf_rows, bundle.income_rows),
        )
    _log(f"  [4/{total_steps}] Writing daily OHLCV...", verbose=v)
    t0 = time.perf_counter()
    upsert_ohlcv_daily(conn, bundle.ohlcv_rows)
    w_ohlcv = time.perf_counter() - t0
    _log(f"        {len(bundle.ohlcv_rows):,} daily bars written ({ohlcv_from} -> {ohlcv_to})", verbose=v)

    _log(f"  [5/{total_steps}] Writing FMP DCF history...", verbose=v)
    t0 = time.perf_counter()
    upsert_fmp_dcf_history(conn, bundle.fmp_dcf_rows)
    w_fmp_dcf = time.perf_counter() - t0
    _log(f"        {len(bundle.fmp_dcf_rows):,} FMP DCF rows written", verbose=v)

    _log(f"  [6/{total_steps}] Computing EMA10/EMA250...", verbose=v)
    t0 = time.perf_counter()
    if not bundle.ohlcv_rows and _ema_already_filled(conn, t):
        ema_rows = 0
        _log("        skipped (no new OHLCV; ema10/ema250 already filled)", verbose=v)
    elif bundle.ema_rows:
        # Worker advanced EMA from the prefetched tail — writer just upserts.
        upsert_ohlcv_ema(conn, bundle.ema_rows)
        ema_rows = len(bundle.ema_rows)
        _log(f"        {ema_rows:,} EMA rows upserted from worker", verbose=v)
    else:
        # Cold start (no prior EMA tail) or partial-EMA history — fall back to
        # the full pandas ewm pass. Cost is one-off per ticker.
        ema_rows = compute_ema(t, conn)
        _log(f"        {ema_rows:,} EMA rows updated", verbose=v)
    c_ema = time.perf_counter() - t0

    _log(f"  [7/{total_steps}] Computing DCF history...", verbose=v)
    t0 = time.perf_counter()
    if not bundle.fcf_rows and _dcf_history_already_filled(conn, t):
        dcf_hist_rows = 0
        _log("        skipped (no new FCF rows; dcf_history already populated)", verbose=v)
    else:
        dcf_hist_rows = compute_dcf_history(t, conn)
        _log(f"        {dcf_hist_rows:,} DCF history rows written", verbose=v)
    c_dcf_hist = time.perf_counter() - t0

    _log(f"  [8/{total_steps}] Computing latest DCF metrics...", verbose=v)
    t0 = time.perf_counter()
    result = compute_dcf_lines(t, conn)
    c_dcf_lines = time.perf_counter() - t0
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
        _log(f"  Done: {t}", verbose=v)
        if _tm:
            w_small = w_company + w_fcf + w_income
            w_all = w_small + w_ohlcv + w_fmp_dcf
            c_all = c_ema + c_dcf_hist + c_dcf_lines
            _log_timing.info(
                "ETL apply timing %s: writes company+fcf+income=%.2fs | write_ohlcv=%.2fs (%d rows) | "
                "write_fmp_dcf=%.2fs | compute_ema=%.2fs | compute_dcf_history=%.2fs | compute_dcf_lines=%.2fs | "
                "writes_subtotal=%.2fs compute_subtotal=%.2fs apply_total=%.2fs",
                t,
                w_small,
                w_ohlcv,
                len(bundle.ohlcv_rows),
                w_fmp_dcf,
                c_ema,
                c_dcf_hist,
                c_dcf_lines,
                w_all,
                c_all,
                time.perf_counter() - _ta0,
            )
        return

    income_rows = bundle.income_rows

    _log(f"  [9/{total_steps}] Writing management...", verbose=v)
    t0 = time.perf_counter()
    if bundle.mgmt_rows is not None:
        upsert_management(conn, bundle.mgmt_rows)
        _log(f"        {len(bundle.mgmt_rows)} management rows written", verbose=v)
    w_mgmt = time.perf_counter() - t0

    _log(f"  [10/{total_steps}] Writing revenue by segment...", verbose=v)
    t0 = time.perf_counter()
    if bundle.seg_rows is not None:
        upsert_revenue_by_segment(conn, bundle.seg_rows)
        _log(f"        {len(bundle.seg_rows)} segment rows written", verbose=v)
    w_seg = time.perf_counter() - t0

    _log(f"  [11/{total_steps}] Writing revenue by geography...", verbose=v)
    t0 = time.perf_counter()
    if bundle.geo_rows is not None:
        upsert_revenue_by_geography(conn, bundle.geo_rows)
        _log(f"        {len(bundle.geo_rows)} geography rows written", verbose=v)
    w_geo = time.perf_counter() - t0

    _log(f"  [12/{total_steps}] Writing annual interest expense...", verbose=v)
    t0 = time.perf_counter()
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
    w_interest = time.perf_counter() - t0

    _log(f"  Done: {t}", verbose=v)
    if _tm:
        w_small = w_company + w_fcf + w_income
        w_all = w_small + w_ohlcv + w_fmp_dcf + w_mgmt + w_seg + w_geo + w_interest
        c_all = c_ema + c_dcf_hist + c_dcf_lines
        _log_timing.info(
            "ETL apply timing %s: writes company+fcf+income=%.2fs | write_ohlcv=%.2fs (%d rows) | write_fmp_dcf=%.2fs | "
            "compute_ema=%.2fs | compute_dcf_history=%.2fs | compute_dcf_lines=%.2fs | optional_writes=%.2fs | "
            "writes_subtotal=%.2fs compute_subtotal=%.2fs apply_total=%.2fs",
            t,
            w_small,
            w_ohlcv,
            len(bundle.ohlcv_rows),
            w_fmp_dcf,
            c_ema,
            c_dcf_hist,
            c_dcf_lines,
            w_mgmt + w_seg + w_geo + w_interest,
            w_all,
            c_all,
            time.perf_counter() - _ta0,
        )
