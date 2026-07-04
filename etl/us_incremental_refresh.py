"""Incremental US refresh for existing DB tickers (no universe crawl).

Why this exists:
- `etl.us_bulk_run` refreshes tickers by crawling an FMP active-universe list.
  When the universe cache is missing/expired, that crawl can take a long time.
- For day-to-day maintenance ("update daily bars, latest fundamentals/annual
  statements, latest price, and required precomputes"), it is usually enough to
  refresh tickers that already exist in our `companies` table.

This script:
- Selects tickers from `companies` (market='US') and refreshes those that are
  stale by N days (default: 3) based on DB high-water marks.
- Uses the same core pipeline as the app single-ticker fill:
  `etl.pipeline.run_us_ticker(..., USRunOptions(...))`
- Optionally snapshots `stock.db` → `stock_read.db` periodically so Streamlit
  can keep reading a consistent replica on Windows.

Usage:
  conda activate stock_analyzer
  cd <repo>
  python -m etl.us_incremental_refresh --stale-days 3 --refresh-mode full --skip-optional
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from db.schema import DB_PATH, get_conn, init_db
from etl.pipeline import USRunOptions, run_us_ticker
from etl.snapshot import snapshot_db


def _setup_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("us_incremental_refresh")


def _date_to_str(d) -> str | None:
    if d is None:
        return None
    return str(d)[:10]


def _stale_by(max_date: object | None, *, cutoff: date) -> bool:
    if max_date is None:
        return True
    try:
        d = date.fromisoformat(str(max_date)[:10])
    except Exception:
        return True
    return d < cutoff


def _pick_actionable_tickers(conn, *, stale_days: int) -> list[str]:
    """Tickers already in DB that look stale in any key table."""
    cutoff = date.today() - timedelta(days=max(0, int(stale_days)))
    rows = conn.execute(
        """
        WITH base AS (
          SELECT upper(trim(cast(ticker AS VARCHAR))) AS ticker
          FROM companies
          WHERE ticker IS NOT NULL
            AND trim(cast(ticker AS VARCHAR)) != ''
            AND upper(coalesce(market, '')) = 'US'
            -- Avoid non-US listings stored in companies with mixed ticker suffixes.
            AND NOT (upper(cast(ticker AS VARCHAR)) LIKE '%.HK')
            AND NOT (upper(cast(ticker AS VARCHAR)) LIKE '%.SS')
            AND NOT (upper(cast(ticker AS VARCHAR)) LIKE '%.SZ')
            AND NOT (upper(cast(ticker AS VARCHAR)) LIKE '%.BJ')
        ),
        o AS (
          SELECT upper(trim(cast(ticker AS VARCHAR))) AS ticker, MAX(date) AS mx
          FROM ohlcv_daily
          GROUP BY 1
        ),
        f AS (
          SELECT upper(trim(cast(ticker AS VARCHAR))) AS ticker, MAX(fiscal_end_date) AS mx
          FROM fundamentals_annual
          GROUP BY 1
        ),
        d AS (
          SELECT upper(trim(cast(ticker AS VARCHAR))) AS ticker, MAX(date) AS mx
          FROM fmp_dcf_history
          GROUP BY 1
        )
        SELECT
          b.ticker,
          o.mx AS ohlcv_max,
          f.mx AS fund_max,
          d.mx AS dcf_max
        FROM base b
        LEFT JOIN o o ON o.ticker = b.ticker
        LEFT JOIN f f ON f.ticker = b.ticker
        LEFT JOIN d d ON d.ticker = b.ticker
        ORDER BY 1
        """
    ).fetchall()
    out: list[str] = []
    for t, o_mx, f_mx, d_mx in rows:
        if (
            _stale_by(o_mx, cutoff=cutoff)
            or _stale_by(f_mx, cutoff=cutoff)
            or _stale_by(d_mx, cutoff=cutoff)
        ):
            out.append(str(t))
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Incremental refresh for existing US tickers in DB")
    p.add_argument("--init-db", action="store_true", help="Run init_db() first")
    p.add_argument("--stale-days", type=int, default=3, help="Refresh tickers stale by >= N days (default: 3)")
    p.add_argument(
        "--refresh-mode",
        choices=("full", "ohlcv", "fundamentals", "fmp_dcf"),
        default="full",
        help="Subset of FMP fetch+apply (same as us_bulk_run)",
    )
    p.add_argument("--skip-optional", action="store_true", help="Skip management / segment / geo / interest")
    p.add_argument("--limit", type=int, default=0, help="Max tickers to process (0 = no limit)")
    p.add_argument("--sleep-ms", type=int, default=120, help="Sleep between tickers (ms)")
    p.add_argument(
        "--only-analyzed",
        action="store_true",
        help="Only refresh tickers present in saved_tables/us_tracker.json (much faster for daily use).",
    )
    p.add_argument("--snapshot-every", type=int, default=25, help="Snapshot every N tickers (0 = disable)")
    p.add_argument(
        "--snapshot-path",
        type=str,
        default=str(DB_PATH.parent / "stock_read.db"),
        help="Snapshot destination (default: stock_read.db next to stock.db)",
    )
    args = p.parse_args()

    log = _setup_logging()

    if args.init_db:
        init_db()

    stale_days = max(0, int(args.stale_days))
    limit = max(0, int(args.limit))
    sleep_s = max(0, int(args.sleep_ms)) / 1000.0
    snap_every = max(0, int(args.snapshot_every))
    snap_path = Path(args.snapshot_path).expanduser()

    with get_conn() as conn:
        tickers = _pick_actionable_tickers(conn, stale_days=stale_days)
    if args.only_analyzed:
        try:
            from analysis_tracker import get_analyzed_tickers

            analyzed = get_analyzed_tickers()
            keep = {str(t).strip().upper() for t in (analyzed or {}).keys()}
            tickers = [t for t in tickers if t in keep]
        except Exception:
            # If tracker missing/corrupt, fall back to DB selection.
            pass
    if limit:
        tickers = tickers[:limit]

    log.info("Actionable tickers: %s (stale_days=%s)", f"{len(tickers):,}", stale_days)
    if not tickers:
        log.info("Nothing to do.")
        return

    opts = USRunOptions(
        skip_optional=bool(args.skip_optional),
        verbose=False,
        refresh_mode=str(args.refresh_mode),
    )

    ok = 0
    failed = 0
    t0 = time.perf_counter()
    # Windows / DuckDB: cannot copy stock.db while a writer connection is open.
    # Process in batches: open writer, apply N tickers, close, snapshot, repeat.
    batch_size = snap_every if snap_every > 0 else len(tickers)
    n = len(tickers)
    i = 0
    while i < n:
        batch = tickers[i : i + batch_size]
        with get_conn() as conn:
            for t in batch:
                i += 1
                try:
                    run_us_ticker(conn, t, opts)
                    ok += 1
                except Exception as exc:
                    failed += 1
                    log.warning("FAILED %s: %s", t, str(exc)[:160])
                if sleep_s:
                    time.sleep(sleep_s)

        if snap_every:
            snap_ok, _bytes, msg = snapshot_db(DB_PATH, snap_path)
            if snap_ok:
                log.info("Snapshot refreshed: %s", msg)
            else:
                log.warning("Snapshot failed: %s", msg)

    # Final snapshot
    if snap_every:
        snap_ok, _bytes, msg = snapshot_db(DB_PATH, snap_path)
        if snap_ok:
            log.info("Final snapshot refreshed: %s", msg)
        else:
            log.warning("Final snapshot failed: %s", msg)

    log.info(
        "DONE ok=%s failed=%s elapsed=%.1fs",
        ok,
        failed,
        time.perf_counter() - t0,
    )


if __name__ == "__main__":
    main()

