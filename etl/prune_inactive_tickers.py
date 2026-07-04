"""US universe hygiene: FMP profile backfill, delisted prune, CIK dedupe.

1. ``--backfill-profiles`` — fetch FMP ``/profile`` per ticker, fill ``cik`` and
   ``is_actively_trading`` (required before inactive prune is meaningful).
2. ``--dry-run`` / ``--execute`` — delete rows for ``is_actively_trading = false``.
3. ``--dedupe-cik`` — per SEC CIK keep one primary listing (NYSE/NASDAQ, active,
   latest OHLCV), delete other tickers from all satellite tables.

Does not touch ``backtest_runs`` or notes with ``ticker IS NULL``.

Usage::

    python -m etl.prune_inactive_tickers --backfill-profiles --execute --progress-every 200
    python -m etl.prune_inactive_tickers --dry-run
    python -m etl.prune_inactive_tickers --execute
    python -m etl.prune_inactive_tickers --dedupe-cik --dry-run
    python -m etl.prune_inactive_tickers --dedupe-cik --execute
    python -m etl.prune_inactive_tickers --report-cik-duplicates
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from db.schema import get_conn, init_db
from etl.loader import upsert_company
from etl.sources.fmp import fetch_profile

_log = logging.getLogger(__name__)

# Child tables referencing ``ticker`` (companies deleted last).
_TICKER_TABLES: list[tuple[str, str]] = [
    ("ohlcv_daily", "ticker"),
    ("ohlcv_minute", "ticker"),
    ("fundamentals_annual", "ticker"),
    ("fundamentals_quarterly", "ticker"),
    ("estimates", "ticker"),
    ("revenue_by_segment", "ticker"),
    ("revenue_by_geography", "ticker"),
    ("management", "ticker"),
    ("dcf_metrics", "ticker"),
    ("dcf_history", "ticker"),
    ("fmp_dcf_history", "ticker"),
    ("factor_scores", "ticker"),
    ("price_alerts", "ticker"),
    ("notes", "ticker"),
    ("etl_us_bulk_state", "ticker"),
]


def _inactive_tickers_subquery(market: str | None) -> str:
    m = (market or "US").strip().upper()
    return f"""
        SELECT ticker FROM companies
        WHERE is_actively_trading = false
          AND market = '{m.replace("'", "''")}'
    """


def report_cik_duplicates(conn) -> list[tuple[str, int, str]]:
    rows = conn.execute(
        """
        SELECT cik, count(*) AS n,
               string_agg(ticker, ', ' ORDER BY ticker) AS tickers
        FROM companies
        WHERE cik IS NOT NULL AND trim(cast(cik AS VARCHAR)) != ''
        GROUP BY cik
        HAVING count(*) > 1
        ORDER BY n DESC, cik
        """
    ).fetchall()
    return [(str(r[0]), int(r[1]), str(r[2] or "")) for r in rows]


def tickers_needing_profile(conn, *, market: str) -> list[str]:
    m = market.strip().upper().replace("'", "''")
    rows = conn.execute(
        f"""
        SELECT ticker FROM companies
        WHERE market = '{m}'
          AND (
                cik IS NULL OR trim(cast(cik AS VARCHAR)) = ''
             OR is_actively_trading IS NULL
          )
        ORDER BY ticker
        """
    ).fetchall()
    return [str(r[0]) for r in rows if r and r[0]]


def backfill_profiles(
    *,
    market: str,
    limit: int | None,
    progress_every: int,
    dry_run: bool,
    sleep_ms: int = 0,
) -> tuple[int, int, int]:
    """Return (processed_count, updated_ok, errors)."""
    with get_conn() as conn:
        tickers_all = tickers_needing_profile(conn, market=market)
    tickers = tickers_all[: int(limit)] if limit is not None else tickers_all
    if dry_run:
        print(f"Backfill profile: would process {len(tickers)} tickers (market={market}).")
        return len(tickers), 0, 0
    ok = err = 0
    t0 = time.perf_counter()
    with get_conn() as conn:
        for i, t in enumerate(tickers, start=1):
            try:
                prof = fetch_profile(t)
                upsert_company(conn, prof)
            except Exception as e:
                err += 1
                _log.warning("profile %s: %s", t, e)
            else:
                ok += 1
            if i == 1 or i % max(1, progress_every) == 0 or i == len(tickers):
                dt = time.perf_counter() - t0
                rate = i / dt if dt > 0 else 0
                print(f"  backfill i={i}/{len(tickers)} ok={ok} err={err} rate={rate:.2f}/s")
            if sleep_ms > 0:
                time.sleep(float(sleep_ms) / 1000.0)
    print(f"SUMMARY backfill processed={len(tickers)} ok={ok} err={err}")
    return len(tickers), ok, err


def _dedupe_cik_secondary_subquery(market: str) -> str:
    """Tickers to DROP: same CIK as another row, not the chosen primary."""
    m = market.strip().upper().replace("'", "''")
    return f"""
        WITH mx AS (
            SELECT ticker, max(date) AS d FROM ohlcv_daily GROUP BY 1
        ),
        ranked AS (
            SELECT
                c.ticker,
                c.cik,
                ROW_NUMBER() OVER (
                    PARTITION BY c.cik
                    ORDER BY
                        CASE WHEN c.is_actively_trading = true THEN 1 ELSE 0 END DESC,
                        CASE WHEN upper(trim(coalesce(c.exchange, ''))) IN (
                            'NYSE', 'NASDAQ', 'AMEX', 'NYSEARCA', 'ARCA', 'BATS'
                        ) THEN 1 ELSE 0 END DESC,
                        coalesce(mx.d, DATE '1970-01-01') DESC,
                        c.ticker ASC
                ) AS rn
            FROM companies c
            LEFT JOIN mx ON mx.ticker = c.ticker
            WHERE c.market = '{m}'
              AND c.cik IS NOT NULL
              AND trim(cast(c.cik AS VARCHAR)) != ''
        )
        SELECT ticker FROM ranked WHERE rn > 1
    """


def delete_tickers_everywhere(conn, tickers_sql_subquery: str) -> None:
    for table, col in _TICKER_TABLES:
        try:
            conn.execute(f"DELETE FROM {table} WHERE {col} IN ({tickers_sql_subquery})")
        except Exception as e:
            print(f"WARN: {table}: {e}")
    conn.execute(f"DELETE FROM companies WHERE ticker IN ({tickers_sql_subquery})")


def dedupe_cik(*, market: str, dry_run: bool) -> int:
    sub = _dedupe_cik_secondary_subquery(market)
    with get_conn() as conn:
        n = int(conn.execute(f"SELECT count(*) FROM ({sub}) t").fetchone()[0])
        if dry_run:
            print(f"CIK dedupe: would delete {n} secondary tickers (market={market}).")
            return n
        if n == 0:
            print("CIK dedupe: nothing to delete.")
            return 0
        delete_tickers_everywhere(conn, sub)
        print(f"CIK dedupe: deleted {n} secondary tickers (market={market}).")
        return n


def prune_inactive(*, market: str, dry_run: bool) -> int:
    sub = _inactive_tickers_subquery(market)
    with get_conn() as conn:
        n = int(conn.execute(f"SELECT count(*) FROM ({sub}) t").fetchone()[0])
        if dry_run:
            print(f"Would delete {n} tickers (market={market}, is_actively_trading=false).")
            return n
        if n == 0:
            print("No inactive tickers to delete.")
            return 0
        delete_tickers_everywhere(conn, sub)
        print(f"Deleted {n} inactive tickers (market={market}).")
        return n


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    p = argparse.ArgumentParser(
        description="US profile backfill, prune inactive tickers, CIK dedupe, duplicate report"
    )
    p.add_argument("--market", type=str, default="US", help="Market filter (default US)")
    p.add_argument("--dry-run", action="store_true", help="Count only (no writes)")
    p.add_argument("--execute", action="store_true", help="Perform writes / deletes")
    p.add_argument(
        "--backfill-profiles",
        action="store_true",
        help="Fetch FMP profile for rows missing cik or is_actively_trading",
    )
    p.add_argument(
        "--dedupe-cik",
        action="store_true",
        help="Delete secondary US listings sharing the same CIK (keep one primary)",
    )
    p.add_argument("--progress-every", type=int, default=500, help="Progress log interval (backfill)")
    p.add_argument("--limit", type=int, default=None, help="Max tickers for backfill (debug)")
    p.add_argument(
        "--sleep-ms",
        type=int,
        default=50,
        help="Pause between profile API calls during backfill (default 50ms, 0 to disable)",
    )
    p.add_argument(
        "--report-cik-duplicates",
        action="store_true",
        help="Print CIKs with more than one ticker (same issuer, multiple listings)",
    )
    args = p.parse_args()

    init_db()

    if args.report_cik_duplicates:
        with get_conn(readonly=True) as conn:
            rows = report_cik_duplicates(conn)
        print(f"CIK groups with >1 ticker: {len(rows)}")
        for cik, n, tickers in rows[:200]:
            print(f"  {cik}  n={n}  {tickers[:200]}{'...' if len(tickers) > 200 else ''}")
        if len(rows) > 200:
            print(f"  ... and {len(rows) - 200} more groups")
        return

    if args.backfill_profiles:
        if not args.execute and not args.dry_run:
            p.error("--backfill-profiles requires --execute or --dry-run")
        backfill_profiles(
            market=args.market,
            limit=args.limit,
            progress_every=max(1, int(args.progress_every)),
            dry_run=bool(args.dry_run and not args.execute),
            sleep_ms=max(0, int(args.sleep_ms)),
        )
        return

    if args.dedupe_cik:
        if not args.execute and not args.dry_run:
            p.error("--dedupe-cik requires --execute or --dry-run")
        dedupe_cik(market=args.market, dry_run=bool(args.dry_run and not args.execute))
        return

    if not args.dry_run and not args.execute:
        p.error("Specify --dry-run or --execute (inactive prune), or other modes above")

    prune_inactive(market=args.market, dry_run=not args.execute)


if __name__ == "__main__":
    main()
