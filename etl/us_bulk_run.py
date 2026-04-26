"""
US full-market ETL with checkpointing (SEC universe via us_universe).

Usage:
    python -m etl.us_bulk_run --init-db
    python -m etl.us_bulk_run --rate-limit-ms 400 --skip-optional
    python -m etl.us_bulk_run --limit 50 --retry-failed
    python -m etl.us_bulk_run --force

Requires FMP_API_KEY. Logs to logs/us_bulk_<timestamp>.log
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

from db.schema import get_conn, init_db, DB_PATH
from etl.pipeline import USRunOptions, run_us_ticker
from etl.snapshot import snapshot_db
from us_universe import fetch_us_universe

_REPO_ROOT = Path(__file__).resolve().parents[1]
_LOG_DIR = _REPO_ROOT / "logs"
_REPORTS_DIR = _REPO_ROOT / "reports"

_SKIP_MSG = "skip (not operating common stock)"


def _setup_logging(log_path: Path) -> None:
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def _mark_state(conn, ticker: str, status: str, step: str = "", last_error: str | None = None) -> None:
    err = (last_error or "")[:2000]
    conn.execute(
        """
        INSERT INTO etl_us_bulk_state (ticker, status, step, last_error, updated_at)
        VALUES (?, ?, ?, ?, now())
        ON CONFLICT (ticker) DO UPDATE SET
            status = excluded.status,
            step = excluded.step,
            last_error = excluded.last_error,
            updated_at = now()
        """,
        [ticker, status, step, err or None],
    )


def _iter_batches(items, batch_size: int):
    """Yield successive batches; batch_size <= 0 yields the whole list once.

    Used to slice the ticker stream so bulk can periodically close the DuckDB
    write connection and let readers (Streamlit / audit / snapshot) grab the
    file. batch_size=0 preserves the legacy "one long-lived connection" behavior.
    """
    if batch_size <= 0:
        yield list(items)
        return
    buf: list = []
    for it in items:
        buf.append(it)
        if len(buf) >= batch_size:
            yield buf
            buf = []
    if buf:
        yield buf


def _open_conn_with_retry(log, max_attempts: int = 6, base_delay_s: float = 1.0):
    """Open a write connection, retrying with exponential backoff if Windows
    reports the file is locked (another process grabbed the snapshot window).

    DuckDB on Windows enforces single-writer file locking; in the gap between
    `conn.close()` and the next `get_conn()` a reader (or another bulk) may
    briefly hold the file. Backoff: 1s, 2s, 4s, 8s, 16s, 32s — total ~63s.
    """
    last_err: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return get_conn()
        except Exception as e:
            msg = str(e)
            looks_like_lock = (
                "Cannot open file" in msg
                or "另一个程序正在使用" in msg
                or "being used by another process" in msg
            )
            if not looks_like_lock or attempt == max_attempts:
                raise
            wait = base_delay_s * (2 ** (attempt - 1))
            log.warning(
                "DB locked on reopen (attempt %d/%d): %s — sleeping %.1fs",
                attempt, max_attempts, msg[:120], wait,
            )
            last_err = e
            time.sleep(wait)
    # Defensive: should never reach here (loop either returns or raises)
    raise RuntimeError(f"_open_conn_with_retry exhausted: {last_err}")


def _cleanup_stale_running(conn) -> int:
    """Mark rows stuck in `running` as `interrupted` so a previous crash
    doesn't masquerade as an active run. `interrupted` falls through
    `_should_process`'s default branch and gets re-attempted next run, while
    remaining distinguishable from `failed` (which only retries with the flag).
    """
    n = conn.execute(
        "SELECT COUNT(*) FROM etl_us_bulk_state WHERE status = 'running'"
    ).fetchone()[0]
    if n:
        conn.execute(
            """
            UPDATE etl_us_bulk_state
            SET status     = 'interrupted',
                last_error = COALESCE(last_error, '') || ' [cleared stale running on startup]',
                updated_at = now()
            WHERE status = 'running'
            """
        )
    return int(n)


def _should_process(conn, ticker: str, *, force: bool, retry_failed: bool) -> bool:
    if force:
        return True
    row = conn.execute(
        "SELECT status FROM etl_us_bulk_state WHERE ticker = ?",
        [ticker],
    ).fetchone()
    if not row:
        return True
    st = row[0]
    if st == "done":
        return False
    if st == "skipped":
        return False
    if st == "failed":
        return retry_failed
    if st == "running":
        return True
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="US bulk ETL (checkpointed)")
    parser.add_argument("--init-db", action="store_true", help="Run init_db() before starting")
    parser.add_argument("--force", action="store_true", help="Re-run all tickers including done/skipped")
    parser.add_argument("--retry-failed", action="store_true", help="Re-run previously failed tickers")
    parser.add_argument("--skip-optional", action="store_true", help="Skip management / segment / geo / interest")
    parser.add_argument("--limit", type=int, default=0, help="Max tickers to process (0 = no limit)")
    parser.add_argument("--rate-limit-ms", type=int, default=350, help="Sleep between tickers (ms)")
    parser.add_argument("--universe-refresh", action="store_true", help="Force refresh SEC universe cache")
    parser.add_argument(
        "--reconnect-every", type=int, default=0,
        help="Close+reopen the DuckDB write connection every N tickers (0 = never). "
             "Opens a periodic window for readers/snapshot to grab stock.db. "
             "Recommend 50-100; smaller values increase per-batch reconnect overhead.",
    )
    parser.add_argument(
        "--reconnect-pause-ms", type=int, default=1000,
        help="Sleep between batches after closing the write conn (only with --reconnect-every>0). "
             "Default 1000ms gives readers enough time to open before bulk reopens.",
    )
    parser.add_argument(
        "--snapshot-every", type=int, default=0,
        help="Copy stock.db → --snapshot-path every K batches (requires --reconnect-every>0). "
             "0 = disabled. Snapshot runs in the reconnect window so readers see a consistent file.",
    )
    parser.add_argument(
        "--snapshot-path", type=str, default="",
        help=f"Destination for --snapshot-every (default: {DB_PATH.parent / 'stock_read.db'}). "
             "Streamlit reads this file when STOCK_ANALYZER_READ_DB points at it.",
    )
    args = parser.parse_args()

    if args.init_db:
        init_db()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = _LOG_DIR / f"us_bulk_{ts}.log"
    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    _setup_logging(log_path)
    log = logging.getLogger("us_bulk")

    with get_conn() as _cleanup_conn:
        n_stale = _cleanup_stale_running(_cleanup_conn)
    if n_stale:
        log.warning(
            "Cleared %d stale 'running' rows from etl_us_bulk_state → 'interrupted'",
            n_stale,
        )

    log.info("Loading US universe (SEC + filters)...")
    universe = fetch_us_universe(force_refresh=args.universe_refresh)
    tickers = list(universe.keys())
    log.info("Universe size: %s tickers (ordered by market cap)", f"{len(tickers):,}")

    opts = USRunOptions(skip_optional=args.skip_optional, verbose=False)
    processed = 0
    ok = 0
    failed = 0
    skipped_state = 0
    skipped_etf = 0

    delay_s = max(0, args.rate_limit_ms) / 1000.0
    pause_s = max(0, args.reconnect_pause_ms) / 1000.0
    reconnect_every = max(0, args.reconnect_every)
    snapshot_every = max(0, args.snapshot_every)
    snapshot_path = (
        Path(args.snapshot_path).expanduser()
        if args.snapshot_path
        else (DB_PATH.parent / "stock_read.db")
    )

    if snapshot_every and not reconnect_every:
        log.warning(
            "--snapshot-every=%d ignored: requires --reconnect-every > 0 (need a write-lock release window)",
            snapshot_every,
        )
        snapshot_every = 0

    if reconnect_every:
        log.info(
            "Short-connection mode: reopen every %d tickers, pause %.1fs between batches",
            reconnect_every, pause_s,
        )
    if snapshot_every:
        log.info(
            "Snapshot mode: copy stock.db → %s every %d batch(es)",
            snapshot_path, snapshot_every,
        )

    stop_loop = False
    for batch_idx, batch in enumerate(_iter_batches(tickers, reconnect_every)):
        if stop_loop:
            break

        with _open_conn_with_retry(log) as conn:
            for ticker in batch:
                if args.limit and processed >= args.limit:
                    stop_loop = True
                    break
                if not _should_process(conn, ticker, force=args.force, retry_failed=args.retry_failed):
                    skipped_state += 1
                    continue

                processed += 1
                _mark_state(conn, ticker, "running", "start")

                try:
                    run_us_ticker(conn, ticker, opts)
                    _mark_state(conn, ticker, "done", "complete", None)
                    ok += 1
                    log.info("OK %s", ticker)
                except ValueError as e:
                    if _SKIP_MSG in str(e):
                        _mark_state(conn, ticker, "skipped", "etf_or_fund", str(e))
                        skipped_etf += 1
                        log.info("%s skipped (ETF/fund per FMP)", ticker)
                    else:
                        _mark_state(conn, ticker, "failed", "error", str(e))
                        failed += 1
                        log.error("FAIL %s: %s", ticker, e)
                except Exception as e:
                    _mark_state(conn, ticker, "failed", "error", str(e))
                    failed += 1
                    log.exception("FAIL %s: %s", ticker, e)

                time.sleep(delay_s)

        # Snapshot first (writer conn is closed; file is quiescent), then
        # the window pause runs while readers / OS finish whatever they need.
        if snapshot_every and (batch_idx + 1) % snapshot_every == 0:
            ok_snap, bytes_done, msg = snapshot_db(DB_PATH, snapshot_path)
            if ok_snap:
                log.info("Snapshot batch %d: %s", batch_idx, msg)
            else:
                log.warning("Snapshot batch %d failed: %s", batch_idx, msg)

        # Window pause: only when caller asked for short-connection mode AND
        # there's another batch coming. Releases the file lock so a snapshot
        # or Streamlit read-only conn can grab it.
        if reconnect_every and not stop_loop and pause_s > 0:
            log.info("Batch %d done — pausing %.1fs (window for readers)", batch_idx, pause_s)
            time.sleep(pause_s)

    log.info(
        "Finished: attempted=%s ok=%s failed=%s skipped_done_or_skipped=%s skipped_etf_fund=%s log=%s",
        processed,
        ok,
        failed,
        skipped_state,
        skipped_etf,
        log_path,
    )
    print(f"\nLog file: {log_path}")


if __name__ == "__main__":
    main()
